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
from .task_restoration import restore_frozen_task_if_needed


async def complete_special_task_directly(update: Update, context: CallbackContext, task: dict):
    """
    Завершает спец-задание (приоритет 111) без требования фото
    """
    query = update.callback_query
    staff_id = context.user_data.get('staff_id')
    
    try:
        now = datetime.now()
        now_str = now.strftime('%Y-%m-%d %H:%M:%S')
        
        # Завершаем спец-задание
        SQL.sql_delete('wms', f"""
            UPDATE wms_bot.shift_tasks
            SET status = 'Проверено',
                time_end = '{now_str}'
            WHERE id = {task['task_id']}
        """)
        
        # Получаем время задания для учета в отработанном времени
        task_seconds = parse_task_duration(task['duration'])
        
        # Добавляем время к отработанному
        current_worked = context.user_data.get('worked_seconds', 0)
        new_worked = current_worked + task_seconds
        context.user_data['worked_seconds'] = new_worked
        
        print(f"⏰ Обновлено время работы (спец-задание): {current_worked}s + {task_seconds}s = {new_worked}s")
        
        # Проверяем замороженные задания и восстанавливаем их
        # Не проверяем time_end, так как задания на доработке могут иметь time_end
        frozen_task_df = SQL.sql_select('wms', f"""
            SELECT id, task_name, product_group, slot, task_duration, comment
            FROM wms_bot.shift_tasks
            WHERE user_id = '{staff_id}'
              AND status IN ('Заморожено', 'На доработке')
              AND merchant_code = '{MERCHANT_ID}'
            ORDER BY time_begin DESC LIMIT 1
        """)
        
        success_message = f"✅ Спец-задание №{task['task_id']} ({task['task_name']}) завершено!"
        
        if not frozen_task_df.empty:
            # Восстанавливаем замороженное задание
            frozen_task = frozen_task_df.iloc[0]
            now_str = now.strftime('%Y-%m-%d %H:%M:%S')
            frozen_task_id = int(frozen_task['id'])
            
            # Устанавливаем время начала для разморозки
            SQL.sql_delete('wms', f"""
                UPDATE wms_bot.shift_tasks
                SET status = 'Выполняется',
                    time_begin = '{now_str}'
                WHERE id = {frozen_task_id}
            """)
            
            # Перезапускаем таймер для восстановленного задания
            try:
                # Импортируем утилиты для работы со временем
                from ...utils.time_utils import align_seconds, seconds_to_hms
                
                # Получаем данные задания для таймера
                task_data = {
                    'task_id': frozen_task['id'],
                    'task_name': frozen_task['task_name'],
                    'product_group': frozen_task['product_group'],
                    'slot': frozen_task['slot'],
                    'duration': frozen_task['task_duration']
                }
                
                # Получаем task_id
                task_id = int(frozen_task['id'])
                
                # 1. Читаем elapsed из БД
                elapsed_seconds = read_freeze_time(task_id)
                
                # 2. Вычисляем remaining
                allocated_seconds = parse_task_duration(frozen_task['task_duration'])
                remaining_seconds = max(0, allocated_seconds - elapsed_seconds)
                
                # Получаем chat_id пользователя
                opv_userid_df = SQL.sql_select('wms', f"""
                    SELECT userid FROM wms_bot.bot_auth WHERE employee_id = '{staff_id}'
                """)
                
                if not opv_userid_df.empty:
                    opv_user_id = int(opv_userid_df.iloc[0]['userid'])
                    
                    # Отправляем сообщение о восстановлении задания
                    reply_markup = get_task_keyboard()
                    
                    # 3. Формируем сообщение с информацией о времени
                    message = (
                        f"📄 *Номер задания:* {frozen_task['id']}\n"
                        f"🔄 *Задание восстановлено*\n\n"
                        f"📝 *Наименование:* {frozen_task['task_name']}\n"
                        f"📦 *Группа товаров:* {frozen_task['product_group']}\n"
                        f"📍 *Слот:* {frozen_task['slot']}\n"
                        f"⏱ *Выделенное время:* {frozen_task['task_duration']}\n"
                        f"⏳ *Оставшееся время:* {seconds_to_hms(remaining_seconds)}"
                    )
                    
                    if elapsed_seconds > 0:
                        message += f"\n⏱ *Уже затрачено:* {seconds_to_hms(elapsed_seconds)}"
                    
                    if frozen_task['comment']:
                        message += f"\n💬 *Комментарий:* {frozen_task['comment']}"
                    
                    sent_msg = await context.bot.send_message(
                        chat_id=opv_user_id,
                        text=message,
                        parse_mode='Markdown',
                        reply_markup=reply_markup
                    )
                    
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
                print(f"⚠️ Ошибка при восстановлении замороженного задания: {e}")
            
            # Показываем сообщение о завершении спец-задания
            await query.edit_message_text(
                f"{success_message}\n\n"
                f"🔄 Восстановлено текущее задание №{frozen_task['id']}\n"
                f"📌 {frozen_task['task_name']}\n\n"
                f"Таймер перезапущен, можете продолжить работу"
            )
        else:
            # Просто показываем сообщение об успешном завершении
            await query.edit_message_text(success_message)
        
        # Очищаем контекст
        context.user_data.pop('task', None)
        context.user_data.pop('photos', None)
        context.user_data.pop('photo_request_time', None)
        context.user_data['late_warning_sent'] = False
        
        # Дополнительно проверяем и восстанавливаем замороженные задания
        await restore_frozen_task_if_needed(staff_id, context)
        
    except Exception as e:
        print(f"❌ Ошибка при завершении спец-задания: {e}")
        await query.edit_message_text("⚠️ Ошибка при завершении спец-задания. Попробуйте позже.")
