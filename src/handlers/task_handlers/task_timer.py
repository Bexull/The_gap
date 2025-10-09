import asyncio
from datetime import datetime
from ...database.sql_client import SQL
from ...config.settings import active_timers, MERCHANT_ID
from ...utils.time_utils import seconds_to_hms, TIMER_TICK_SECONDS
from ...utils.freeze_time_utils import calculate_remaining_time, get_task_timing_info


async def update_timer(context, chat_id, message_id, task, total_seconds, reply_markup=None, comment=None):
    """
    Обновление таймера задания
    
    НОВАЯ ЛОГИКА: Таймер только ПОКАЗЫВАЕТ оставшееся время, читая из БД
    НЕ накапливает время - это делается через update_freeze_time_on_pause()
    """
    task_id = task['task_id']

    # Проверяем, есть ли уже активный таймер для этого задания
    if task_id in active_timers:
        print(f"⚠️ [WARNING] Таймер для задания {task_id} уже существует, не создаем новый")
        return

    # Сохраняем информацию для UI (НЕ для логики времени!)
    active_timers[task_id] = {
        'chat_id': chat_id,
        'message_id': message_id,
        'task': task,
        'allocated_seconds': int(total_seconds),
        'reply_markup': reply_markup,
        'comment': comment,
        'last_rendered_remaining': None
    }

    # Запускаем render loop
    asyncio.create_task(_render_timer_loop(context, task_id))


async def _render_timer_loop(context, task_id):
    """
    Обновляет сообщение с таймером каждые 15 секунд
    
    НОВАЯ ЛОГИКА:
    - Читает time_begin, freeze_time, task_duration из БД
    - Вычисляет: remaining = task_duration - freeze_time - (now - time_begin)
    - НЕ накапливает время в памяти!
    """
    try:
        while task_id in active_timers:
            timer_info = active_timers.get(task_id)
            if not timer_info:
                break

            chat_id = timer_info['chat_id']
            message_id = timer_info['message_id']
            task = timer_info['task']
            reply_markup = timer_info.get('reply_markup')
            comment = timer_info.get('comment', '')

            # Читаем текущий статус из БД
            try:
                status_df = SQL.sql_select('wms', f"""
                    SELECT status
                    FROM wms_bot.shift_tasks
                    WHERE id = {task_id}
                    AND merchant_code = '{MERCHANT_ID}'
                    LIMIT 1
                """)

                if status_df.empty:
                    print(f"⚠️ [WARNING] Задание {task_id} не найдено, останавливаем таймер")
                    del active_timers[task_id]
                    break

                current_status = status_df.iloc[0]['status']

                # Останавливаем таймер если статус изменился
                if current_status not in ['Выполняется', 'На доработке']:
                    print(f"ℹ️ Таймер задания {task_id} остановлен, статус: {current_status}")
                    del active_timers[task_id]
                    break

            except Exception as e:
                print(f"❌ [ERROR] Ошибка проверки статуса задания {task_id}: {e}")
                await asyncio.sleep(TIMER_TICK_SECONDS)
                continue

            # Вычисляем оставшееся время из БД
            try:
                remaining_seconds = calculate_remaining_time(task_id)
                
                # Округляем до 15 секунд вниз для плавности
                remaining_seconds = (remaining_seconds // 15) * 15
                
                # Проверяем изменилось ли время для обновления
                last_rendered = timer_info.get('last_rendered_remaining')
                if last_rendered == remaining_seconds:
                    # Время не изменилось, не обновляем сообщение
                    await asyncio.sleep(TIMER_TICK_SECONDS)
                    continue
                
                # Обновляем last_rendered
                active_timers[task_id]['last_rendered_remaining'] = remaining_seconds

                # Формируем текст сообщения используя централизованную функцию
                from ...utils.message_formatter import format_task_message
                
                # Добавляем task_duration для вычисления времени
                task['task_duration'] = seconds_to_hms(timer_info['allocated_seconds'])
                task['id'] = task_id
                if comment:
                    task['comment'] = comment
                
                text = format_task_message(task, status="Выполняется")

                # Обновляем сообщение
                try:
                    await context.bot.edit_message_text(
                        chat_id=chat_id,
                        message_id=message_id,
                        text=text,
                        reply_markup=reply_markup
                    )
                except Exception as e:
                    # Сообщение могло быть удалено или изменено - останавливаем таймер
                    print(f"⚠️ [WARNING] Не удалось обновить сообщение для задания {task_id}: {e}")
                    del active_timers[task_id]
                    break

            except Exception as e:
                print(f"❌ [ERROR] Ошибка обновления таймера для задания {task_id}: {e}")

            # Ждем до следующего тика
            await asyncio.sleep(TIMER_TICK_SECONDS)

    except Exception as e:
        print(f"❌ [ERROR] Критическая ошибка в таймере задания {task_id}: {e}")
        if task_id in active_timers:
            del active_timers[task_id]


async def stop_timer(task_id: int):
    """
    Остановка таймера для задания
    
    Args:
        task_id: ID задания
    """
    if task_id in active_timers:
        del active_timers[task_id]
        print(f"⏹️ [STOP] Таймер задания {task_id} остановлен вручную")


async def restart_timer(context, task_id: int):
    """
    Перезапуск таймера для задания (например, после возврата на доработку)
    
    Args:
        context: контекст бота
        task_id: ID задания
    """
    # Останавливаем старый таймер если есть
    await stop_timer(task_id)
    
    # Получаем информацию о задании из БД
    try:
        task_df = SQL.sql_select('wms', f"""
            SELECT id, task_name, product_group, slot, task_duration, comment, provider
            FROM wms_bot.shift_tasks
            WHERE id = {task_id}
            AND merchant_code = '{MERCHANT_ID}'
            LIMIT 1
        """)
        
        if task_df.empty:
            print(f"⚠️ [WARNING] Задание {task_id} не найдено для перезапуска таймера")
            return
        
        row = task_df.iloc[0]
        
        # Парсим task_duration
        task_duration_raw = row['task_duration']
        if hasattr(task_duration_raw, 'hour'):  # time object
            total_seconds = task_duration_raw.hour * 3600 + task_duration_raw.minute * 60 + task_duration_raw.second
        elif hasattr(task_duration_raw, 'total_seconds'):  # timedelta
            total_seconds = int(task_duration_raw.total_seconds())
        else:
            total_seconds = 900  # 15 минут по умолчанию
        
        # Создаем task dict
        task = {
            'task_id': int(row['id']),
            'task_name': row['task_name'],
            'product_group': row.get('product_group', 'Не указана'),
            'slot': row.get('slot', 'Не указан'),
            'provider': row.get('provider', 'Не указан')
        }
        
        # TODO: Получить chat_id, message_id, reply_markup из контекста
        # Это нужно будет реализовать если используется restart_timer
        
        print(f"🔄 [RESTART] Перезапуск таймера для задания {task_id}")
        
    except Exception as e:
        print(f"❌ [ERROR] Ошибка перезапуска таймера для задания {task_id}: {e}")
