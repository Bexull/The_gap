import asyncio
from datetime import datetime, timedelta
from ...database.sql_client import SQL
from ...config.settings import active_timers, MERCHANT_ID, task_time_tracker, frozen_tasks_info
from ...utils.time_utils import seconds_to_hms, align_seconds, TIMER_TICK_SECONDS
from ...utils.freeze_time_utils import accumulate_freeze_time, read_freeze_time


_TRACKER_INTERVAL_SECONDS = 15
_tracker_loop_task = None


def _ensure_tracker_loop():
    """Гарантирует запуск фонового обновления накопленного времени"""

    global _tracker_loop_task

    if _tracker_loop_task is None or _tracker_loop_task.done():
        _tracker_loop_task = asyncio.create_task(_time_tracker_loop())


def _cleanup_task_tracking(task_id: int, keep_frozen: bool = False):
    """Удаляет данные трекера для задания"""

    tracker_entry = task_time_tracker.pop(task_id, None)

    if not keep_frozen:
        frozen_tasks_info.pop(task_id, None)

    # Если больше нет активных заданий, останавливаем фоновой луп
    global _tracker_loop_task
    if not task_time_tracker and _tracker_loop_task is not None and not _tracker_loop_task.done():
        _tracker_loop_task.cancel()

    return tracker_entry


async def update_timer(context, chat_id, message_id, task, total_seconds, reply_markup=None, comment=None):
    """Обновление таймера задания, основанное на централизованном трекере времени"""

    task_id = task['task_id']

    # Проверяем, есть ли уже активный таймер для этого задания
    if task_id in active_timers:
        print(f"⚠️ [WARNING] Таймер для задания {task_id} уже существует, не создаем новый")
        return

    _ensure_tracker_loop()

    now = datetime.now()
    allocated_seconds = int(total_seconds)

    # 1. Читаем накопленное время из БД (единый источник правды!)
    previous_elapsed = read_freeze_time(task_id)
    
    # 2. Создаем tracker для текущей сессии (начинается с 0)
    task_time_tracker[task_id] = {
        'elapsed_seconds': 0.0,  # Текущая сессия начинается с 0!
        'allocated_seconds': allocated_seconds,
        'previous_elapsed': previous_elapsed,  # Храним для расчетов
        'last_tick': now,
        'original_start_time': now,
        'remaining_seconds': allocated_seconds - previous_elapsed
    }

    # Сохраняем сведения для UI
    active_timers[task_id] = {
        'chat_id': chat_id,
        'message_id': message_id,
        'task': task,
        'allocated_seconds': allocated_seconds,
        'reply_markup': reply_markup,
        'comment': comment,
        'last_rendered_remaining': None
    }

    # 3. Запускаем render loop
    asyncio.create_task(_render_timer_loop(context, task_id))


async def _render_timer_loop(context, task_id):
    """Обновляет сообщение с таймером, используя данные из трекера"""

    keep_frozen = False

    try:
        while task_id in active_timers:
            timer_info = active_timers.get(task_id)
            if not timer_info:
                break

            chat_id = timer_info['chat_id']
            message_id = timer_info['message_id']
            task = timer_info['task']
            reply_markup = timer_info['reply_markup']
            comment = timer_info.get('comment')

            # Получаем данные из трекера
            tracker_entry = task_time_tracker.get(task_id)
            if not tracker_entry:
                print(f"⚠️ [WARNING] Данные трекера для задания {task_id} отсутствуют, прекращаем обновление")
                break

            # Общее затраченное = предыдущее (из БД) + текущая сессия
            current_session_elapsed = tracker_entry.get('elapsed_seconds', 0)
            previous_elapsed = tracker_entry.get('previous_elapsed', 0)
            total_elapsed = previous_elapsed + current_session_elapsed
            
            # Выравниваем для отображения
            total_elapsed = align_seconds(total_elapsed, mode='round')
            
            # Вычисляем remaining
            allocated_seconds = tracker_entry.get('allocated_seconds', 0)
            remaining_seconds = max(0, int(allocated_seconds - total_elapsed))
            
            # Обновляем tracker
            tracker_entry['remaining_seconds'] = remaining_seconds

            # Проверяем статус задания
            task_status_df = SQL.sql_select('wms', f"""
                SELECT status FROM wms_bot.shift_tasks
                WHERE id = {task_id} AND merchant_code = '{MERCHANT_ID}'
            """)

            if task_status_df.empty:
                print(f"⚠️ [WARNING] Задание {task_id} не найдено в БД, останавливаем таймер")
                break

            current_status = task_status_df.iloc[0]['status']

            if current_status == 'Заморожено':
                await _handle_freeze_state(context, task_id, tracker_entry, timer_info)
                keep_frozen = True
                break

            if current_status not in ['Выполняется', 'На доработке']:
                print(f"ℹ️ Таймер задания {task_id} остановлен, статус: {current_status}")
                break

            minutes = remaining_seconds // 60
            seconds = remaining_seconds % 60
            remaining_str = f"{minutes:02d}:{seconds:02d}"

            if timer_info.get('last_rendered_remaining') == remaining_str:
                await asyncio.sleep(_TRACKER_INTERVAL_SECONDS)
                continue

            # Логируем изменение таймера (пропускаем первое обновление с None)
            if timer_info.get('last_rendered_remaining') is not None:
                print(
                    f"🕒 [TIMER] task_id={task_id} remaining_before={timer_info.get('last_rendered_remaining')} -> {remaining_str}"
                )

            message = (
                f"📄 *Номер задания:* {task_id}\n"
                f"✅ *Задание выполняется*\n\n"
                f"📝 *Наименование:* {task['task_name']}\n"
                f"📦 *Группа товаров:* {task.get('product_group', '—')}\n"
                f"📍 *Слот:* {task.get('slot', '—')}\n"
                f"🏢 *Поставщик:* {task.get('provider', 'Не указан')}\n"
                f"⏱ *Выделенное время:* {task['duration']}\n"
                f"⏳ *Оставшееся время:* {remaining_str}"
            )

            if comment and str(comment).strip():
                message += f"\n💬 *Комментарий:* {comment}"

            try:
                await context.bot.edit_message_text(
                    chat_id=chat_id,
                    message_id=message_id,
                    text=message,
                    parse_mode='Markdown',
                    reply_markup=reply_markup
                )
                timer_info['last_rendered_remaining'] = remaining_str
            except Exception as render_error:
                print(f"❌ [ERROR] Ошибка обновления таймера {task_id}: {render_error}")

            await asyncio.sleep(_TRACKER_INTERVAL_SECONDS)

    finally:
        active_timers.pop(task_id, None)
        _cleanup_task_tracking(task_id, keep_frozen=keep_frozen)


async def _time_tracker_loop():
    """Фоново увеличивает отработанное время для всех отслеживаемых заданий"""

    global _tracker_loop_task

    try:
        while True:
            if not task_time_tracker:
                break

            now = datetime.now()

            for task_id, entry in list(task_time_tracker.items()):
                last_tick = entry.get('last_tick', now)

                try:
                    delta = (now - last_tick).total_seconds()
                except Exception:
                    delta = 0

                if delta < 0:
                    delta = 0

                entry['elapsed_seconds'] = float(entry.get('elapsed_seconds', 0)) + delta
                entry['last_tick'] = now

                allocated = float(entry.get('allocated_seconds', 0))
                elapsed_aligned = align_seconds(entry['elapsed_seconds'], mode='round')
                entry['elapsed_seconds'] = elapsed_aligned
                entry['remaining_seconds'] = max(0, int(allocated - elapsed_aligned))

            await asyncio.sleep(_TRACKER_INTERVAL_SECONDS)

    except Exception as tracker_error:
        print(f"❌ [ERROR] Ошибка в трекере времени: {tracker_error}")
    finally:
        _tracker_loop_task = None


async def _handle_freeze_state(context, task_id, tracker_entry, timer_info):
    """Формирует UI для замороженного задания"""

    # 1. Накапливаем время в БД
    current_session_seconds = tracker_entry.get('elapsed_seconds', 0)
    total_elapsed = accumulate_freeze_time(task_id, current_session_seconds)
    
    # 2. Вычисляем remaining на основе накопленного
    allocated = tracker_entry.get('allocated_seconds', 0)
    remaining_seconds = max(0, int(allocated - total_elapsed))
    remaining_time_str = str(timedelta(seconds=remaining_seconds)).split('.')[0]
    
    # 3. Minimal info для UI
    frozen_tasks_info[task_id] = {
        'freeze_time': datetime.now(),
        'original_start_time': tracker_entry.get('original_start_time')
    }

    # 4. Отображаем
    task = timer_info['task']
    message = (
        f"📄 *Номер задания:* {task_id}\n"
        f"❄️ *Задание заморожено*\n\n"
        f"📝 *Наименование:* {task['task_name']}\n"
        f"📦 *Группа товаров:* {task.get('product_group', '—')}\n"
        f"📍 *Слот:* {task.get('slot', '—')}\n"
        f"🏢 *Поставщик:* {task.get('provider', 'Не указан')}\n"
        f"⏱ *Выделенное время:* {task['duration']}\n"
        f"⏳ *Оставшееся время:* {remaining_time_str}\n"
        f"⏸️ *Таймер остановлен*\n\n"
        f"*ℹ️ Завершите спец-задание, чтобы продолжить*"
    )

    try:
        await context.bot.edit_message_text(
            chat_id=timer_info['chat_id'],
            message_id=timer_info['message_id'],
            text=message,
            parse_mode='Markdown'
        )
    except Exception as freeze_render_error:
        print(f"❌ [ERROR] Ошибка отображения заморозки таймера {task_id}: {freeze_render_error}")


async def stop_timer_for_task(task_id: int, context, reason: str = "задание завершено"):
    """Останавливает таймер для конкретного задания"""

    try:
        timer_info = active_timers.get(task_id)
        tracker_entry = task_time_tracker.get(task_id)

        if not timer_info:
            return

        remaining_seconds = 0
        keep_frozen = False

        if tracker_entry:
            allocated = tracker_entry.get('allocated_seconds', 0)
            current_session_elapsed = tracker_entry.get('elapsed_seconds', 0)
            previous_elapsed = tracker_entry.get('previous_elapsed', 0)
            total_elapsed = previous_elapsed + current_session_elapsed
            remaining_seconds = max(0, int(allocated - total_elapsed))

        # Если заморозка - накапливаем время в БД
        if "заморозка" in reason.lower() or "заморожено" in reason.lower():
            if tracker_entry:
                # 1. Берем время текущей сессии из tracker
                current_session_seconds = tracker_entry.get('elapsed_seconds', 0)
                
                # 2. Накапливаем в БД
                total_elapsed = accumulate_freeze_time(task_id, current_session_seconds)
                
                # 3. Пересчитываем remaining на основе накопленного времени
                allocated = tracker_entry.get('allocated_seconds', 0)
                remaining_seconds = max(0, int(allocated - total_elapsed))
            
            # 4. Сохраняем minimal info для UI
            frozen_tasks_info[task_id] = {
                'freeze_time': datetime.now(),
                'original_start_time': tracker_entry.get('original_start_time') if tracker_entry else None
            }
            keep_frozen = True

        remaining_time_str = str(timedelta(seconds=int(remaining_seconds))).split('.')[0]

        try:
            freeze_info = ""
            if "заморозка" in reason.lower() or "заморожено" in reason.lower():
                freeze_info = f"\n⏳ *Оставшееся время:* {remaining_time_str}"

            await context.bot.edit_message_text(
                chat_id=timer_info['chat_id'],
                message_id=timer_info['message_id'],
                text=(
                    f"📄 *Номер задания:* {task_id}\n"
                    f"⏹️ *Таймер остановлен*\n\n"
                    f"📝 *Наименование:* {timer_info['task']['task_name']}\n"
                    f"📦 *Группа товаров:* {timer_info['task'].get('product_group', '—')}\n"
                    f"📍 *Слот:* {timer_info['task'].get('slot', '—')}\n"
                    f"🏢 *Поставщик:* {timer_info['task'].get('provider', 'Не указан')}\n"
                    f"⏱ *Выделенное время:* {timer_info['task']['duration']}{freeze_info}\n"
                    f"⏸️ *Причина остановки:* {reason}"
                ),
                parse_mode='Markdown'
            )
        except Exception as render_error:
            print(f"❌ [ERROR] Ошибка при остановке таймера для задания {task_id}: {render_error}")

        active_timers.pop(task_id, None)
        _cleanup_task_tracking(task_id, keep_frozen=keep_frozen)

    except Exception as e:
        print(f"❌ [ERROR] Критическая ошибка при остановке таймера для задания {task_id}: {e}")