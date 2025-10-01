import asyncio
import pandas as pd
from datetime import datetime, timedelta
import datetime as dt
from telegram import Update
from telegram.ext import CallbackContext, ContextTypes
from ...database.sql_client import SQL
from ...config.settings import MERCHANT_ID
from ...utils.task_utils import parse_task_duration
from ...keyboards.opv_keyboards import get_special_task_keyboard, get_task_keyboard
from .task_timer import update_timer


async def complete_special_task_inline(update: Update, context: CallbackContext):
    """Завершает спец-задание через инлайн кнопку"""
    query = update.callback_query
    await query.answer()
    
    staff_id = context.user_data.get('staff_id')
    if not staff_id:
        await query.edit_message_text("⚠️ Ваш ID не найден в системе.")
        return

    # Проверяем активное спец-задание
    extra_task_df = SQL.sql_select('wms', f"""
        SELECT id, task_name, time_begin
        FROM wms_bot.shift_tasks
        WHERE user_id = '{staff_id}'
          AND status = 'Выполняется'
          AND time_end IS NULL
          AND priority = '111'
          AND merchant_code = '{MERCHANT_ID}'
        ORDER BY time_begin DESC LIMIT 1
    """)

    if extra_task_df.empty:
        await query.edit_message_text("У вас нет активных спец-заданий.")
        return

    task = extra_task_df.iloc[0]
    now = datetime.now()

    # Получаем время задания для учета в отработанном времени
    task_duration_df = SQL.sql_select('wms', f"""
        SELECT task_duration FROM wms_bot.shift_tasks WHERE id = {task['id']} AND merchant_code = '{MERCHANT_ID}'
    """)
    
    if not task_duration_df.empty:
        duration_raw = task_duration_df.iloc[0]['task_duration']
        task_seconds = parse_task_duration(duration_raw)
        
        # Добавляем время к отработанному
        current_worked = context.user_data.get('worked_seconds', 0)
        new_worked = current_worked + task_seconds
        context.user_data['worked_seconds'] = new_worked
        
        print(f"⏰ Обновлено время работы (спец-задание): {current_worked}s + {task_seconds}s = {new_worked}s")

    # Завершаем спец-задание
    now_str = now.strftime('%Y-%m-%d %H:%M:%S')
    SQL.sql_delete('wms', f"""
        UPDATE wms_bot.shift_tasks
        SET status = 'Проверено',
            time_end = '{now_str}'
        WHERE id = {task['id']}
    """)

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

    success_message = f"✅ Спец-задание №{task['id']} ({task['task_name']}) завершено!"

    if not frozen_task_df.empty:
        # Восстанавливаем замороженное задание
        frozen_task = frozen_task_df.iloc[0]
        now_str = now.strftime('%Y-%m-%d %H:%M:%S')
        SQL.sql_delete('wms', f"""
            UPDATE wms_bot.shift_tasks
            SET status = 'Выполняется',
                time_begin = '{now_str}'
            WHERE id = {frozen_task['id']}
        """)
        
        # Перезапускаем таймер для восстановленного задания
        try:
            # Получаем данные задания для таймера
            task_data = {
                'task_id': frozen_task['id'],
                'task_name': frozen_task['task_name'],
                'product_group': frozen_task['product_group'],
                'slot': frozen_task['slot'],
                'duration': frozen_task['task_duration']
            }
            
            # Получаем task_id
            task_id = frozen_task['id']
            
            # Используем общую функцию для получения оставшегося времени
            from ...utils.task_utils import get_task_remaining_time
            total_seconds, elapsed_seconds = get_task_remaining_time(task_id, frozen_task['task_duration'])
            
            # Получаем chat_id пользователя
            opv_userid_df = SQL.sql_select('wms', f"""
                SELECT userid FROM wms_bot.bot_auth WHERE employee_id = '{staff_id}'
            """)
            
            if not opv_userid_df.empty:
                opv_user_id = int(opv_userid_df.iloc[0]['userid'])
                
                # Отправляем сообщение о восстановлении задания
                reply_markup = get_task_keyboard()
                
                # Используем общую функцию для форматирования информации о времени
                from ...utils.task_utils import format_task_time_info
                remaining_time, elapsed_info = format_task_time_info(total_seconds, elapsed_seconds)
                
                # Добавляем форматирование для Markdown
                if elapsed_info:
                    elapsed_info = elapsed_info.replace("Уже затрачено:", "*Уже затрачено:*")
                
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
                
                # Проверяем, есть ли уже активный таймер для этого задания
                from ...config.settings import active_timers
                
                task_id = frozen_task['id']
                if task_id in active_timers:
                    print(f"⚠️ [WARNING] Таймер для задания {task_id} уже существует в special_tasks, не создаем новый")
                else:
                    # Запускаем новый таймер
                    asyncio.create_task(
                        update_timer(context, sent_msg.chat_id, sent_msg.message_id, task_data, total_seconds, reply_markup)
                    )
                
        except Exception as e:
            pass

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
    
    # Дополнительно проверяем и восстанавливаем замороженные задания с отправкой сообщения
    from .task_restoration import restore_frozen_task_if_needed
    await restore_frozen_task_if_needed(staff_id, context, send_message=True)


async def complete_the_extra_task(update: Update, context: CallbackContext):
    """Завершает активное дополнительное задание из расписания (priority = 111) - команда"""
    staff_id = context.user_data.get('staff_id')

    # Проверяем активное доп. задание
    extra_task_df = SQL.sql_select('wms', f"""
        SELECT id, task_name, time_begin
        FROM wms_bot.shift_tasks
        WHERE user_id = '{staff_id}'
          AND status = 'Выполняется'
          AND time_end IS NULL
          AND priority = '111'
          AND merchant_code = '{MERCHANT_ID}'
        ORDER BY time_begin DESC LIMIT 1
    """)

    if extra_task_df.empty:
        await update.message.reply_text("У вас нет активных дополнительных заданий.")
        return

    task = extra_task_df.iloc[0]
    now = datetime.now()

    # Завершаем задание
    now_str = now.strftime('%Y-%m-%d %H:%M:%S')
    SQL.sql_delete('wms', f"""
        UPDATE wms_bot.shift_tasks
        SET status = 'Проверено',
            time_end = '{now_str}'
        WHERE id = {task['id']}
    """)

    # Восстанавливаем замороженные задания после завершения спец-задания
    from .task_restoration import restore_frozen_task_if_needed
    await restore_frozen_task_if_needed(staff_id, context)
    
    await update.message.reply_text(
        f"✅ Дополнительное задание №{task['id']} ({task['task_name']}) завершено!"
    )


async def set_special_task(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Команда /set для принудительного назначения спец-задания (ТЕСТОВАЯ)"""
    
    staff_id = context.user_data.get('staff_id')
    
    if not staff_id:
        await update.message.reply_text("❌ Сначала авторизуйтесь командой /start")
        return
    
    try:
        # Используем универсальную функцию автоматического назначения
        from .auto_special_task_handler import auto_assign_special_task
        
        result = await auto_assign_special_task(staff_id, context)
        
        if not result['success']:
            await update.message.reply_text(f"❌ {result['error']}")
            return
        
        # Если все прошло успешно, уведомления уже отправлены автоматически
        await update.message.reply_text("✅ Спец-задание успешно назначено!")
        
    except Exception as e:
        print(f"❌ Ошибка в set_special_task: {e}")
        await update.message.reply_text("❌ Произошла ошибка при назначении спец-задания")
