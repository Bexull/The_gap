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
from .task_restoration import restore_frozen_task_if_needed


async def complete_special_task_directly(update: Update, context: CallbackContext, task: dict):
    """
    Завершает спец-задание (приоритет 111) без требования фото
    """
    query = update.callback_query
    staff_id = context.user_data.get('staff_id')
    
    # Импортируем глобальное хранилище в начале функции
    from ...config.settings import frozen_tasks_info
    
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
        frozen_task_df = SQL.sql_select('wms', f"""
            SELECT id, task_name, product_group, slot, task_duration, comment
            FROM wms_bot.shift_tasks
            WHERE user_id = '{staff_id}'
              AND status = 'Заморожено'
              AND time_end IS NULL
              AND merchant_code = '{MERCHANT_ID}'
            ORDER BY time_begin DESC LIMIT 1
        """)
        
        success_message = f"✅ Спец-задание №{task['task_id']} ({task['task_name']}) завершено!"
        
        if not frozen_task_df.empty:
            # Восстанавливаем замороженное задание
            frozen_task = frozen_task_df.iloc[0]
            now_str = now.strftime('%Y-%m-%d %H:%M:%S')
            # Получаем task_id для проверки frozen_tasks_info
            frozen_task_id = int(frozen_task['id'])
            
            # Определяем время начала - используем original_start_time если доступен
            time_begin_to_use = now_str  # По умолчанию текущее время
            if frozen_task_id in frozen_tasks_info and 'original_start_time' in frozen_tasks_info[frozen_task_id]:
                original_start_time = frozen_tasks_info[frozen_task_id]['original_start_time']
                if isinstance(original_start_time, datetime):
                    time_begin_to_use = original_start_time.strftime('%Y-%m-%d %H:%M:%S')
                    print(f"🔧 [FIX] Используем оригинальное время начала для восстановленного задания {frozen_task_id}: {time_begin_to_use}")
                else:
                    print(f"⚠️ [WARNING] original_start_time для восстановленного задания {frozen_task_id} не является datetime объектом: {type(original_start_time)}")
            else:
                print(f"⚠️ [WARNING] Нет информации о original_start_time для восстановленного задания {frozen_task_id}, используем текущее время")
            
            SQL.sql_delete('wms', f"""
                UPDATE wms_bot.shift_tasks
                SET status = 'Выполняется',
                    time_begin = '{time_begin_to_use}'
                WHERE id = {frozen_task['id']}
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
                
                # Если есть информация о замороженном задании в глобальном хранилище,
                # используем ее для расчета оставшегося времени
                total_seconds = 0
                elapsed_seconds = 0
                
                if task_id in frozen_tasks_info:
                    # Используем сохраненное оставшееся время
                    total_seconds = frozen_tasks_info[task_id].get('remaining_seconds', 0)
                    elapsed_seconds = frozen_tasks_info[task_id].get('elapsed_seconds', 0)
                    total_seconds = align_seconds(total_seconds, mode='ceil')
                    elapsed_seconds = align_seconds(elapsed_seconds, mode='round')

                    print(
                        f"🕒 [RESTORE] task_id={task_id} after special elapsed={seconds_to_hms(elapsed_seconds)} remaining={seconds_to_hms(total_seconds)}"
                    )
                else:
                    # Если нет данных в хранилище, рассчитываем по старой схеме
                    full_duration = parse_task_duration(frozen_task['task_duration'])
                    total_seconds = full_duration
                
                # Получаем chat_id пользователя
                opv_userid_df = SQL.sql_select('wms', f"""
                    SELECT userid FROM wms_bot.bot_auth WHERE employee_id = '{staff_id}'
                """)
                
                if not opv_userid_df.empty:
                    opv_user_id = int(opv_userid_df.iloc[0]['userid'])
                    
                    # Отправляем сообщение о восстановлении задания
                    reply_markup = get_task_keyboard()
                    
                    # Форматируем оставшееся время
                    remaining_time = str(timedelta(seconds=total_seconds)).split('.')[0]
                    
                    # Если прошедшее время больше 0, показываем его
                    elapsed_info = ""
                    if elapsed_seconds > 0:
                        elapsed_time = str(timedelta(seconds=elapsed_seconds)).split('.')[0]
                        elapsed_info = f"\n⏱ *Уже затрачено:* {elapsed_time}"
                    
                    message = (
                        f"📄 *Номер задания:* {frozen_task['id']}\n"
                        f"🔄 *Задание восстановлено*\n\n"
                        f"📝 *Наименование:* {frozen_task['task_name']}\n"
                        f"📦 *Группа товаров:* {frozen_task['product_group']}\n"
                        f"📍 *Слот:* {frozen_task['slot']}\n"
                        f"⏱ *Выделенное время:* {frozen_task['task_duration']}{elapsed_info}\n"
                        f"⏳ *Оставшееся время:* {remaining_time}"
                    )
                    
                    if frozen_task['comment']:
                        message += f"\n💬 *Комментарий:* {frozen_task['comment']}"
                    
                    sent_msg = await context.bot.send_message(
                        chat_id=opv_user_id,
                        text=message,
                        parse_mode='Markdown',
                        reply_markup=reply_markup
                    )
                    
                    # Добавляем логи перед запуском таймера
                    
                    # Проверяем, есть ли уже активный таймер для этого задания
                    from ...config.settings import active_timers
                    if task_id in active_timers:
                        print(f"⚠️ [WARNING] Таймер для задания {task_id} уже запущен, пропускаем повторный запуск")
                    else:
                        # Запускаем таймер только если его еще нет
                        # ИСПРАВЛЕНИЕ: используем total_seconds (remaining), а не allocated_seconds
                        # total_seconds уже содержит remaining_seconds из frozen_tasks_info
                        allocated_seconds = frozen_tasks_info.get(task_id, {}).get('allocated_seconds', total_seconds)
                        asyncio.create_task(
                            update_timer(context, sent_msg.chat_id, sent_msg.message_id, task_data, total_seconds, reply_markup)
                        )
                        print(
                            f"🕒 [RESTORE] timer restarted after special for task_id={task_id} remaining={seconds_to_hms(total_seconds)} allocated={seconds_to_hms(allocated_seconds)}"
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
        
        # Очищаем данные о задании из глобального хранилища
        task_id = task['task_id']
        if task_id in frozen_tasks_info:
            del frozen_tasks_info[task_id]
            print(f"🧹 Удалены данные о замороженном задании {task_id} из глобального хранилища")
        
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
