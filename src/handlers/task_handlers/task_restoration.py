import asyncio
import pandas as pd
from datetime import datetime, timedelta
import datetime as dt
from telegram import Update
from telegram.ext import CallbackContext
from ...database.sql_client import SQL
from ...config.settings import MERCHANT_ID
from ...utils.task_utils import parse_task_duration
from ...keyboards.opv_keyboards import get_task_keyboard
from .task_timer import update_timer


async def restore_frozen_task_if_needed(staff_id: str, context: CallbackContext = None, send_message: bool = False):
    """
    Проверяет и восстанавливает замороженное задание, если нет активных спец-заданий
    
    Args:
        staff_id: ID сотрудника
        context: Контекст для отправки уведомлений (опционально)
    
    Returns:
        bool: True если задание было восстановлено, False если нет
    """
    try:
        # Проверяем есть ли активные спец-задания у пользователя
        special_task_df = SQL.sql_select('wms', f"""
            SELECT id FROM wms_bot.shift_tasks
            WHERE user_id = '{staff_id}'
              AND status = 'Выполняется'
              AND priority = '111'
              AND merchant_code = '{MERCHANT_ID}'
        """)
        
        # Если есть активные спец-задания - не восстанавливаем
        if not special_task_df.empty:
            return False
        
        # Ищем замороженные задания для конкретного пользователя
        # Пробуем разные варианты поиска
        frozen_task_df = SQL.sql_select('wms', f"""
            SELECT id, task_name, product_group, slot, task_duration, comment, user_id
            FROM wms_bot.shift_tasks
            WHERE user_id = '{staff_id}'
              AND status = 'Заморожено'
              AND merchant_code = '{MERCHANT_ID}'
            ORDER BY time_begin DESC LIMIT 1
        """)
        
        # Если не найдено, попробуем поиск без кавычек (для числовых ID)
        if frozen_task_df.empty:
            try:
                staff_id_int = int(staff_id)
                frozen_task_df = SQL.sql_select('wms', f"""
                    SELECT id, task_name, product_group, slot, task_duration, comment, user_id
                    FROM wms_bot.shift_tasks
                    WHERE user_id = {staff_id_int}
                      AND status = 'Заморожено'
                      AND merchant_code = '{MERCHANT_ID}'
                    ORDER BY time_begin DESC LIMIT 1
                """)
            except ValueError:
                pass
        
        if frozen_task_df.empty:
            return False
        
        # Восстанавливаем замороженное задание
        frozen_task = frozen_task_df.iloc[0]
        now = datetime.now()
        now_str = now.strftime('%Y-%m-%d %H:%M:%S')
        
        # Импортируем глобальное хранилище
        from ...config.settings import frozen_tasks_info
        
        # Получаем task_id для проверки frozen_tasks_info
        task_id = int(frozen_task['id'])
        
        # Определяем время начала - используем original_start_time если доступен
        time_begin_to_use = now_str  # По умолчанию текущее время
        if task_id in frozen_tasks_info and 'original_start_time' in frozen_tasks_info[task_id]:
            original_start_time = frozen_tasks_info[task_id]['original_start_time']
            if isinstance(original_start_time, datetime):
                time_begin_to_use = original_start_time.strftime('%Y-%m-%d %H:%M:%S')
                print(f"🔧 [FIX] Используем оригинальное время начала для задания {task_id}: {time_begin_to_use}")
            else:
                print(f"⚠️ [WARNING] original_start_time для задания {task_id} не является datetime объектом: {type(original_start_time)}")
        else:
            print(f"⚠️ [WARNING] Нет информации о original_start_time для задания {task_id}, используем текущее время")
        
        # Обновляем статус задания на "Выполняется" с правильным временем начала
        SQL.sql_delete('wms', f"""
            UPDATE wms_bot.shift_tasks
            SET status = 'Выполняется',
                time_begin = '{time_begin_to_use}'
            WHERE id = {frozen_task['id']}
        """)
        
        # Отправляем сообщение с деталями восстановленного задания только если запрошено
        if send_message and context:
            try:
                # Получаем chat_id пользователя
                opv_userid_df = SQL.sql_select('wms', f"""
                    SELECT userid FROM wms_bot.bot_auth WHERE employee_id = '{staff_id}'
                """)
                
                if not opv_userid_df.empty:
                    opv_user_id = int(opv_userid_df.iloc[0]['userid'])
                    
                    # Получаем task_id
                    task_id = int(frozen_task['id'])
                    
                    # Используем общую функцию для получения оставшегося времени
                    from ...utils.task_utils import get_task_remaining_time
                    
                    # Упрощенный лог восстановления
                    
                    # Получаем оставшееся и прошедшее время
                    total_seconds, elapsed_seconds = get_task_remaining_time(task_id, frozen_task['task_duration'])
                    from ...utils.time_utils import align_seconds, seconds_to_hms
                    total_seconds = align_seconds(total_seconds, mode='ceil')
                    elapsed_seconds = align_seconds(elapsed_seconds, mode='round')

                    print(
                        f"🕒 [RESTORE] task_id={task_id} elapsed={seconds_to_hms(elapsed_seconds)} remaining={seconds_to_hms(total_seconds)}"
                    )
                    
                    # Отправляем сообщение с деталями задания
                    reply_markup = get_task_keyboard()
                    
                    # Используем общую функцию для форматирования информации о времени
                    from ...utils.task_utils import format_task_time_info
                    remaining_time, elapsed_info = format_task_time_info(total_seconds, elapsed_seconds)
                    
                    message = (
                        f"📋 *Текущее задание*\n\n"
                        f"🆔 ID: `{frozen_task['id']}`\n"
                        f"📌 Название: *{frozen_task['task_name']}*\n"
                        f"📦 Группа: {frozen_task['product_group']}\n"
                        f"📍 Слот: {frozen_task['slot']}\n"
                        f"⏰ Время начала: {now.strftime('%H:%M:%S')}\n"
                        f"⏳ Плановая длительность: {frozen_task['task_duration']} мин{elapsed_info}\n"
                        f"⌛ Оставшееся время: {remaining_time}\n"
                        f"▶️ Статус: Выполняется"
                    )
                    
                    if frozen_task['comment']:
                        message += f"\n💬 Комментарий: {frozen_task['comment']}"
                    
                    sent_msg = await context.bot.send_message(
                        chat_id=opv_user_id,
                        text=message,
                        parse_mode='Markdown',
                        reply_markup=reply_markup
                    )
                    
                    # Запускаем таймер
                    task_data = {
                        'task_id': frozen_task['id'],
                        'task_name': frozen_task['task_name'],
                        'product_group': frozen_task['product_group'],
                        'slot': frozen_task['slot'],
                        'duration': frozen_task['task_duration']
                    }
                    
                    # Краткий лог запуска таймера
                    
                    # Проверяем, есть ли уже активный таймер для этого задания
                    from ...config.settings import active_timers, frozen_tasks_info
                    if task_id in active_timers:
                        print(f"⚠️ [WARNING] Таймер для задания {task_id} уже запущен, пропускаем повторный запуск")
                    else:
                        # ОТЛАДКА: Логируем значения при восстановлении
                        frozen_info = frozen_tasks_info.get(task_id, {})
                        print(f"🔍 [RESTORE DEBUG] task_id={task_id}")
                        print(f"   frozen_info: {frozen_info}")
                        print(f"   total_seconds: {total_seconds}")
                        
                        # Запускаем таймер только если его еще нет
                        allocated_seconds = frozen_tasks_info.get(task_id, {}).get('allocated_seconds', total_seconds)
                        print(f"   allocated_seconds: {allocated_seconds}")
                        
                        asyncio.create_task(
                            update_timer(context, sent_msg.chat_id, sent_msg.message_id, task_data, allocated_seconds, reply_markup)
                        )
                        print(
                            f"🕒 [RESTORE] timer restarted for task_id={task_id} allocated={seconds_to_hms(allocated_seconds)}"
                        )
                    
            except Exception as e:
                pass
                    
        return True
        
    except Exception as e:
        return False
