import asyncio
from datetime import datetime, timedelta
from ...database.sql_client import SQL
from ...config.settings import active_timers, MERCHANT_ID, task_time_tracker, frozen_tasks_info
from ...utils.time_utils import seconds_to_hms, align_seconds, TIMER_TICK_SECONDS


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

    # Получаем сохраненные данные о замороженном задании, если они есть
    frozen_info = frozen_tasks_info.get(task_id, {})

    elapsed_seconds = float(frozen_info.get('elapsed_seconds', 0))
    remaining_hint = frozen_info.get('remaining_seconds')
    allocated_hint = frozen_info.get('allocated_seconds')

    total_seconds = int(total_seconds)

    if allocated_hint is not None:
        allocated_seconds = int(allocated_hint)
    else:
        allocated_seconds = total_seconds

    if remaining_hint is not None:
        remaining_seconds = max(0, int(remaining_hint))
        allocated_seconds = max(allocated_seconds, int(elapsed_seconds + remaining_seconds))
    else:
        remaining_seconds = max(0, allocated_seconds - int(elapsed_seconds))

    original_start_time = frozen_info.get('original_start_time')
    if original_start_time is None:
        original_start_time = now - timedelta(seconds=elapsed_seconds)

    # Регистрируем задание в централизованном трекере
    task_time_tracker[task_id] = {
        'elapsed_seconds': elapsed_seconds,
        'allocated_seconds': allocated_seconds,
        'last_tick': now,
        'original_start_time': original_start_time,
        'remaining_seconds': remaining_seconds
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

    # Запускаем обновление отображения таймера
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

            elapsed_seconds = tracker_entry.get('elapsed_seconds', 0)
            allocated_seconds = tracker_entry.get('allocated_seconds', 0)
            elapsed_seconds = align_seconds(elapsed_seconds, mode='round')
            tracker_entry['elapsed_seconds'] = elapsed_seconds

            remaining_seconds = max(0, int(allocated_seconds - elapsed_seconds))
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
        tracker_entry = _cleanup_task_tracking(task_id, keep_frozen=keep_frozen)

        if tracker_entry:
            current_elapsed = int(max(0, tracker_entry.get('elapsed_seconds', 0)))
            
            # ИСПРАВЛЕНИЕ: При заморозке НАКАПЛИВАЕМ время
            if keep_frozen:
                # Читаем старое freeze_time из БД и прибавляем к нему
                freeze_df = SQL.sql_select('wms', f"""
                    SELECT freeze_time FROM wms_bot.shift_tasks
                    WHERE id = {task_id} AND merchant_code = '{MERCHANT_ID}'
                """)
                
                previous_elapsed = 0
                if not freeze_df.empty and freeze_df.iloc[0]['freeze_time']:
                    freeze_time_raw = freeze_df.iloc[0]['freeze_time']
                    # Парсим предыдущее freeze_time
                    if isinstance(freeze_time_raw, str):
                        time_parts = freeze_time_raw.split(':')
                        if len(time_parts) >= 2:
                            hours = int(time_parts[0])
                            minutes = int(time_parts[1])
                            seconds = int(time_parts[2]) if len(time_parts) > 2 else 0
                            previous_elapsed = hours * 3600 + minutes * 60 + seconds
                    elif hasattr(freeze_time_raw, 'total_seconds'):
                        previous_elapsed = int(freeze_time_raw.total_seconds())
                    elif hasattr(freeze_time_raw, 'hour'):
                        previous_elapsed = freeze_time_raw.hour * 3600 + freeze_time_raw.minute * 60 + freeze_time_raw.second
                
                total_elapsed = previous_elapsed + current_elapsed
                formatted = seconds_to_hms(total_elapsed)
                print(f"💾 freeze_time НАКОПЛЕН для задачи {task_id}: {seconds_to_hms(previous_elapsed)} + {seconds_to_hms(current_elapsed)} = {formatted}")
            else:
                formatted = seconds_to_hms(current_elapsed)
                print(f"💾 freeze_time обновлён для задачи {task_id}: {formatted}")

            SQL.sql_delete('wms', f"""
                UPDATE wms_bot.shift_tasks
                SET freeze_time = '{formatted}'
                WHERE id = {task_id}
            """)


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

    remaining_seconds = max(0, int(tracker_entry.get('remaining_seconds', tracker_entry.get('allocated_seconds', 0) - tracker_entry.get('elapsed_seconds', 0))))
    remaining_time_str = str(timedelta(seconds=remaining_seconds)).split('.')[0]

    from ...config.settings import frozen_tasks_info
    
    # ИСПРАВЛЕНИЕ: Читаем актуальное freeze_time из БД и накапливаем
    freeze_df = SQL.sql_select('wms', f"""
        SELECT freeze_time FROM wms_bot.shift_tasks
        WHERE id = {task_id} AND merchant_code = '{MERCHANT_ID}'
    """)
    
    previous_elapsed = 0
    if not freeze_df.empty and freeze_df.iloc[0]['freeze_time']:
        freeze_time_raw = freeze_df.iloc[0]['freeze_time']
        # Парсим предыдущее freeze_time
        if isinstance(freeze_time_raw, str):
            time_parts = freeze_time_raw.split(':')
            if len(time_parts) >= 2:
                hours = int(time_parts[0])
                minutes = int(time_parts[1])
                seconds = int(time_parts[2]) if len(time_parts) > 2 else 0
                previous_elapsed = hours * 3600 + minutes * 60 + seconds
        elif hasattr(freeze_time_raw, 'total_seconds'):
            previous_elapsed = int(freeze_time_raw.total_seconds())
        elif hasattr(freeze_time_raw, 'hour'):
            previous_elapsed = freeze_time_raw.hour * 3600 + freeze_time_raw.minute * 60 + freeze_time_raw.second
    
    # Накапливаем elapsed
    current_elapsed = int(tracker_entry.get('elapsed_seconds', 0))
    total_accumulated_elapsed = previous_elapsed + current_elapsed
    
    frozen_tasks_info[task_id] = {
        'freeze_time': datetime.now(),
        'elapsed_seconds': total_accumulated_elapsed,  # Накопленное!
        'remaining_seconds': remaining_seconds,
        'original_start_time': tracker_entry.get('original_start_time'),
        'allocated_seconds': tracker_entry.get('allocated_seconds')
    }
    
    print(f"🔄 [FREEZE_STATE] Обновлен frozen_tasks_info для задания {task_id}: previous={previous_elapsed}s + current={current_elapsed}s = total_elapsed={total_accumulated_elapsed}s")

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
        elapsed_seconds = 0
        keep_frozen = False

        if tracker_entry:
            elapsed_seconds = tracker_entry.get('elapsed_seconds', 0)
            allocated = tracker_entry.get('allocated_seconds', 0)
            remaining_seconds = max(0, int(allocated - elapsed_seconds))

        if "заморозка" in reason.lower() or "заморожено" in reason.lower():
            # ИСПРАВЛЕНИЕ: Читаем актуальное freeze_time из БД перед обновлением frozen_tasks_info
            freeze_df = SQL.sql_select('wms', f"""
                SELECT freeze_time FROM wms_bot.shift_tasks
                WHERE id = {task_id} AND merchant_code = '{MERCHANT_ID}'
            """)
            
            previous_elapsed = 0
            if not freeze_df.empty and freeze_df.iloc[0]['freeze_time']:
                freeze_time_raw = freeze_df.iloc[0]['freeze_time']
                # Парсим предыдущее freeze_time
                if isinstance(freeze_time_raw, str):
                    time_parts = freeze_time_raw.split(':')
                    if len(time_parts) >= 2:
                        hours = int(time_parts[0])
                        minutes = int(time_parts[1])
                        seconds = int(time_parts[2]) if len(time_parts) > 2 else 0
                        previous_elapsed = hours * 3600 + minutes * 60 + seconds
                elif hasattr(freeze_time_raw, 'total_seconds'):
                    previous_elapsed = int(freeze_time_raw.total_seconds())
                elif hasattr(freeze_time_raw, 'hour'):
                    previous_elapsed = freeze_time_raw.hour * 3600 + freeze_time_raw.minute * 60 + freeze_time_raw.second
            
            # Накапливаем актуальное elapsed
            total_accumulated_elapsed = previous_elapsed + int(elapsed_seconds)
            
            # ВСЕГДА обновляем frozen_tasks_info актуальным накопленным значением
            frozen_tasks_info[task_id] = {
                'freeze_time': datetime.now(),
                'elapsed_seconds': total_accumulated_elapsed,  # Актуальное накопленное!
                'remaining_seconds': int(remaining_seconds),
                'original_start_time': tracker_entry.get('original_start_time') if tracker_entry else None,
                'allocated_seconds': tracker_entry.get('allocated_seconds') if tracker_entry else int(total_accumulated_elapsed + remaining_seconds)
            }
            
            print(f"🔄 [FREEZE] Обновлен frozen_tasks_info для задания {task_id}: previous={previous_elapsed}s + current={int(elapsed_seconds)}s = total_elapsed={total_accumulated_elapsed}s")
            
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

        # ИСПРАВЛЕНИЕ: Используем уже вычисленное накопленное значение из frozen_tasks_info
        if "заморозка" in reason.lower() or "заморожено" in reason.lower():
            # Берем накопленное значение из frozen_tasks_info (уже вычислено выше)
            total_elapsed = frozen_tasks_info[task_id]['elapsed_seconds']
            formatted_total = seconds_to_hms(total_elapsed)
            print(f"💾 freeze_time НАКОПЛЕН при stop_timer для задачи {task_id}: {formatted_total}")
        else:
            current_elapsed = int(max(0, tracker_entry.get('elapsed_seconds', 0)) if tracker_entry else 0)
            formatted_total = seconds_to_hms(current_elapsed)
            print(f"💾 freeze_time обновлён при stop_timer для задачи {task_id}: {formatted_total}")
        
        SQL.sql_delete('wms', f"""
            UPDATE wms_bot.shift_tasks
            SET freeze_time = '{formatted_total}'
            WHERE id = {task_id}
        """)

        active_timers.pop(task_id, None)
        _cleanup_task_tracking(task_id, keep_frozen=keep_frozen)

    except Exception as e:
        print(f"❌ [ERROR] Критическая ошибка при остановке таймера для задания {task_id}: {e}")