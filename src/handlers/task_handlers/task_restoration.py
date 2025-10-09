import asyncio
import pandas as pd
from datetime import datetime, timedelta
import datetime as dt
from telegram import Update
from telegram.ext import CallbackContext
from ...database.sql_client import SQL
from ...config.settings import MERCHANT_ID
from ...utils.task_utils import parse_task_duration
from ...utils.time_utils import seconds_to_hms
from ...utils.freeze_time_utils import read_freeze_time
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
              AND status IN ('Заморожено', 'На доработке')
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
                      AND status IN ('Заморожено', 'На доработке')
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
        task_id = int(frozen_task['id'])
        
        # Обновляем статус задания на "Выполняется" и устанавливаем time_begin
        SQL.sql_delete('wms', f"""
            UPDATE wms_bot.shift_tasks
            SET status = 'Выполняется',
                time_begin = '{now_str}'
            WHERE id = {task_id}
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
                    
                    # 1. Читаем elapsed из БД
                    elapsed_seconds = read_freeze_time(task_id)
                    
                    # 2. Вычисляем remaining
                    allocated_seconds = parse_task_duration(frozen_task['task_duration'])
                    remaining_seconds = max(0, allocated_seconds - elapsed_seconds)
                    
                    # Отправляем сообщение с деталями задания
                    reply_markup = get_task_keyboard()
                    
                    # 3. Формируем сообщение с информацией о времени
                    message = (
                        f"📋 *Текущее задание*\n\n"
                        f"🆔 ID: `{frozen_task['id']}`\n"
                        f"📌 Название: *{frozen_task['task_name']}*\n"
                        f"📦 Группа: {frozen_task['product_group']}\n"
                        f"📍 Слот: {frozen_task['slot']}\n"
                        f"⏰ Время начала: {now.strftime('%H:%M:%S')}\n"
                        f"⏱ Плановая длительность: {frozen_task['task_duration']} мин\n"
                        f"⏳ Оставшееся время: {seconds_to_hms(remaining_seconds)}"
                    )
                    
                    if elapsed_seconds > 0:
                        message += f"\n⏱ Уже затрачено: {seconds_to_hms(elapsed_seconds)}"
                    
                    message += f"\n▶️ Статус: Выполняется"
                    
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
                    
                    # Проверяем, есть ли уже активный таймер для этого задания
                    from ...config.settings import active_timers
                    if task_id in active_timers:
                        print(f"⚠️ [WARNING] Таймер для задания {task_id} уже запущен, пропускаем повторный запуск")
                    else:
                        # Запускаем таймер
                        asyncio.create_task(
                            update_timer(context, sent_msg.chat_id, sent_msg.message_id, task_data, allocated_seconds, reply_markup)
                        )
                    
            except Exception as e:
                pass
                    
        return True
        
    except Exception as e:
        return False
