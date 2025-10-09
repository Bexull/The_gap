import pandas as pd
from datetime import datetime, timedelta
import datetime as dt
from telegram import Update
from telegram.ext import CallbackContext
from ...database.sql_client import SQL
from ...utils.time_utils import get_task_date, get_current_slot
from ...config.settings import MERCHANT_ID
from ...utils.task_utils import check_user_task_status
from ...keyboards.opv_keyboards import get_sector_keyboard, get_task_in_progress_keyboard
from .task_timer import update_timer
from .task_execution import restore_frozen_task_if_needed


async def get_task(update: Update, context: CallbackContext):
    """Получение нового задания"""
    query = update.callback_query
    await query.answer()
    
    employee_id = context.user_data.get('staff_id')
    if not employee_id:
        await query.edit_message_text("⚠️ Ваш ID не найден в системе.")
        return

    # Проверяем и восстанавливаем замороженные задания если нужно
    await restore_frozen_task_if_needed(employee_id, context)

    # Проверяем статус заданий пользователя
    status_check = check_user_task_status(employee_id)
    if status_check['blocked']:
        reply_markup = status_check.get('reply_markup')
        await query.edit_message_text(status_check['message'], reply_markup=reply_markup)
        return

    # Проверяем выбор сектора
    if not context.user_data.get('sector_selected'):
        sectors_df = SQL.sql_select('stock', """SELECT DISTINCT "Сектор" FROM public.task_schedule WHERE "Дата" = current_date""")
        sectors = sectors_df['Сектор'].dropna().tolist()
        
        reply_markup = get_sector_keyboard(sectors)
        await query.edit_message_text("Выберите сектор, с которым будете работать:", reply_markup=reply_markup)
        return

    # Если сектор выбран - назначаем задание
    await assign_task_from_sector(update, context)


async def assign_task_from_sector(update: Update, context: CallbackContext):
    """Назначение задания из выбранного сектора"""
    query = update.callback_query
    await query.answer()

    employee_id = context.user_data.get('staff_id')
    sector = context.user_data.get('sector')
    shift = context.user_data.get('shift')

    if not all([employee_id, sector, shift]):
        await query.edit_message_text("⚠️ Недостаточно данных для назначения.")
        return

    try:
        # Назначение задания из сектора
        # Проверяем статус заданий пользователя
        status_check = check_user_task_status(employee_id)
        if status_check['blocked']:
            reply_markup = status_check.get('reply_markup')
            await query.edit_message_text(status_check['message'], reply_markup=reply_markup)
            return

        # Получаем пол
        gender_df = SQL.sql_select('wms', f"SELECT gender FROM wms_bot.t_staff WHERE id = '{employee_id}'")
        opv_gender = gender_df.iloc[0]['gender'].strip().upper() if not gender_df.empty else 'U'
        
        # Получаем ФИО из контекста
        operator_full_name = context.user_data.get('staff_name', 'ОПВ')

        # Переводим shift в нужный вид
        shift_ru = 'День' if shift == 'day' else 'Ночь'

        task_date = get_task_date(shift)
        
        # Получаем текущий слот
        current_slot = get_current_slot(shift)
        
        # Если слот не определен (вне рабочего времени), не выдаем задания
        if current_slot is None:
            await query.edit_message_text("❌ Получение заданий доступно только в рабочее время.")
            return

        # Выбираем задание из shift_tasks с учетом слота
        # Задания предыдущих слотов "перетекают" на текущий слот
        sql_query = f"""
            SELECT * FROM wms_bot.shift_tasks
            WHERE task_date = '{task_date}'
              AND shift = '{shift_ru}'
              AND sector = '{sector}'
              AND slot <= {current_slot}
              AND is_constant_task = true
              AND merchant_code ='{MERCHANT_ID}'
              AND (status IS NULL OR status = 'В ожидании')
            ORDER BY slot ASC, priority ASC
        """
        task_df = SQL.sql_select('wms', sql_query)

        if task_df.empty:
            await query.edit_message_text("❌ Нет доступных заданий.")
            return

        # Фильтруем по полу
        task_df = task_df[
            (task_df['gender'].isnull()) |
            (task_df['gender'].str.upper() == 'U') |
            (task_df['gender'].str.upper() == opv_gender)
        ]

        if task_df.empty:
            await query.edit_message_text("❌ Нет подходящих по полу заданий.")
            return

        # Берём первое задание (уже отсортировано по slot ASC, priority ASC)
        task_row = task_df.iloc[0]
        now = datetime.now()

        # Обновляем статус задания + записываем ФИО оператора
        employment_type = context.user_data.get('employment_type', 'main')
        now_str = now.strftime('%Y-%m-%d %H:%M:%S')
        SQL.sql_delete('wms', f"""
            UPDATE wms_bot.shift_tasks
            SET status = 'Выполняется',
                user_id = '{employee_id}',
                time_begin = '{now_str}',
                freeze_time = '00:00:00',
                part_time = '{employment_type}',
                operator_name = '{operator_full_name}'
            WHERE id = {task_row['id']}
        """)

        # Парсим время выполнения
        try:
            duration_raw = task_row['task_duration']

            if isinstance(duration_raw, dt.time):
                total_seconds = duration_raw.hour * 3600 + duration_raw.minute * 60 + duration_raw.second
            else:
                time_str = str(duration_raw).strip()
                t = datetime.strptime(time_str, '%H:%M:%S')
                total_seconds = t.hour * 3600 + t.minute * 60 + t.second

        except Exception as e:
            # Ошибка парсинга, используем значение по умолчанию
            total_seconds = 900  # дефолт 15 мин если что-то не так

        # Сохраняем задание в контекст
        task = {
            'task_id': task_row['id'],
            'task_name': task_row['task_name'],
            'product_group': task_row['product_group'],
            'slot': task_row['slot'],
            'provider': task_row.get('provider', 'Не указан'),
            'duration': task_row['task_duration'],
            'assigned_time': now
        }
        context.user_data['task'] = task
        context.user_data.pop('photos', None)
        context.user_data.pop('photo_request_time', None)
        context.user_data['late_warning_sent'] = False

        # Формируем сообщение с таймером используя централизованную функцию
        from ...utils.message_formatter import format_task_message
        message = format_task_message(task_row, status="Получено")

        # Добавляем клавиатуру с кнопкой "Завершить задачу"
        reply_markup = get_task_in_progress_keyboard()
        sent_msg = await query.edit_message_text(message, reply_markup=reply_markup)

        # Запускаем таймер!
        import asyncio
        from ...config.settings import active_timers
        
        # Проверяем, есть ли уже активный таймер для этого задания
        if task['task_id'] in active_timers:
            print(f"⚠️ [WARNING] Таймер для задания {task['task_id']} уже существует в task_assignment, не создаем новый")
        else:
            asyncio.create_task(
                update_timer(context, sent_msg.chat_id, sent_msg.message_id, task, total_seconds, reply_markup)
            )

    except Exception as e:
        await query.edit_message_text("⚠️ Произошла ошибка при назначении задания.")
