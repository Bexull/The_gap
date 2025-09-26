import pandas as pd
from datetime import datetime, timedelta
from telegram import Update
from telegram.ext import CallbackContext
from ..database.sql_client import SQL
from ..keyboards.auth_keyboards import get_shift_keyboard, get_role_keyboard, get_employment_keyboard
from ..keyboards.opv_keyboards import get_sector_keyboard
from ..keyboards.zs_keyboards import get_zs_main_menu_keyboard
from ..utils.time_utils import get_task_date
from ..utils.task_utils import check_user_task_status
from ..utils.navigation import navigation_history
from ..config.settings import zav_on_shift, MERCHANT_ID

async def shift_start(update: Update, context: CallbackContext):
    context.user_data['worked_seconds'] = 0
    """Начало смены - выбор типа смены"""
    reply_markup = get_shift_keyboard()
    
    if update.message:
        await update.message.reply_text("Выберите смену:", reply_markup=reply_markup)
    else:
        await update.callback_query.edit_message_text("Выберите смену:", reply_markup=reply_markup)
    
    # Добавляем меню выбора смены в историю
    user_id = update.effective_user.id
    navigation_history.add_menu(user_id, 'shift_choice')

async def shift_choice(update: Update, context: CallbackContext):
    """Обработчик выбора смены"""
    query = update.callback_query
    await query.answer()
    shift = query.data.lower()
    
    reply_markup = get_role_keyboard(shift)
    await query.edit_message_text(f"Вы выбрали смену: {shift}. Теперь выберите роль:", reply_markup=reply_markup)

async def role_choice(update: Update, context: CallbackContext):
    """Обработчик выбора роли"""
    query = update.callback_query
    await query.answer()
    role, shift = query.data.split('_')
    
    context.user_data.update({
        'role': role,
        'shift': shift
    })

    try:
        # Сохраняем старт смены в базу
        session_row = pd.DataFrame([{
            'employee_id': context.user_data['staff_id'],
            'role': role,
            'shift_type': shift,
            'start_time': datetime.now(),
            'end_time': None,
            'load_date': pd.to_datetime('today').date(),
            'merchantid': MERCHANT_ID 
        }])
        SQL.sql_execute_df('wms', session_row, 'wms_bot.shift_sessions1')
    except Exception as e:
        print(f"❌ Ошибка записи начала смены: {e}")

    if role == 'opv':
        reply_markup = get_employment_keyboard()
        await query.edit_message_text("Выберите тип занятости:", reply_markup=reply_markup)
        # Добавляем меню выбора типа занятости в историю
        navigation_history.add_menu(update.effective_user.id, 'employment_choice')
    else:
        # Для ЗС сразу показываем меню
        if update.effective_user.id not in zav_on_shift:
            zav_on_shift.append(update.effective_user.id)

        reply_markup = get_zs_main_menu_keyboard()
        await query.edit_message_text(
            f"Вы выбрали роль: {role.upper()} на смене: {shift}.",
            reply_markup=reply_markup
        )
        # Добавляем главное меню ЗС в историю
        navigation_history.add_menu(update.effective_user.id, 'zs_main_menu')

async def employment_type_choice(update: Update, context: CallbackContext):
    """Обработчик выбора типа занятости"""
    query = update.callback_query
    await query.answer()

    employment_type = query.data.replace('employment_', '')
    context.user_data['employment_type'] = employment_type

    # Логируем выбор типа занятости
    try:
        from datetime import datetime
        log_entry = f"{datetime.now().strftime('%Y-%m-%d %H:%M:%S')} - Пользователь {context.user_data['staff_id']} ({context.user_data.get('staff_name', 'Неизвестно')}) выбрал тип занятости: '{employment_type}'\n"
        
        with open('employment_choice_log.txt', 'a', encoding='utf-8') as f:
            f.write(log_entry)
    except Exception as e:
        print(f"⚠️ Ошибка записи в лог: {e}")

    # Тип занятости сохраняется в context.user_data и будет использован при назначении заданий
    print(f"✅ Тип занятости '{employment_type}' сохранен в контекст для пользователя {context.user_data['staff_id']}")

    # Переход к выбору сектора
    shift = context.user_data.get('shift')
    shift_ru = 'День' if shift == 'day' else 'Ночь'
    task_date = get_task_date(shift)

    sectors_df = SQL.sql_select('wms', f"""SELECT DISTINCT sector FROM wms_bot.shift_tasks WHERE task_date =  '{task_date}' AND shift = '{shift_ru}' and merchant_code='{MERCHANT_ID}'""")
    sectors = sectors_df['sector'].dropna().tolist()

    if not sectors:
        await query.edit_message_text("❌ Нет доступных секторов для этой смены.")
        return

    reply_markup = get_sector_keyboard(sectors)
    await query.edit_message_text("Выберите сектор, с которым будете работать в эту смену:", reply_markup=reply_markup)
    # Добавляем меню выбора сектора в историю
    navigation_history.add_menu(update.effective_user.id, 'sector_choice')

async def sector_select_and_confirm(update: Update, context: CallbackContext):
    """Обработчик выбора сектора"""
    query = update.callback_query
    await query.answer()
    
    sector = query.data.replace('sectorchoice_', '')
    context.user_data.update({
        'sector': sector,
        'sector_selected': True
    })

    from ..keyboards.opv_keyboards import get_task_confirmation_keyboard
    reply_markup = get_task_confirmation_keyboard()

    await query.edit_message_text(
        f"✅ Вы выбрали сектор: *{sector}*\nТеперь можно брать задания.",
        parse_mode='Markdown',
        reply_markup=reply_markup
    )
    # Добавляем меню подтверждения задания в историю
    navigation_history.add_menu(update.effective_user.id, 'task_confirmation')

async def shift_end(update: Update, context: CallbackContext):
    """Завершение смены"""
    staff_id = context.user_data.get('staff_id')
    role = context.user_data.get('role')

    if not staff_id or not role:
        await update.message.reply_text("Сначала начните смену командой /start.")
        return

    # Проверяем активные задания перед завершением смены
    active_tasks_df = SQL.sql_select('wms', f"""
        SELECT id FROM wms_bot.shift_tasks
        WHERE user_id = '{staff_id}'
          AND status IN ('Выполняется', 'На доработке', 'Заморожено')
          AND merchant_code = '{MERCHANT_ID}'
    """)

    if not active_tasks_df.empty:
        await update.message.reply_text("⚠️ У вас есть незавершённые задания. Завершите их перед завершением смены.")
        return

    now = datetime.now()

    try:
        # Обновляем конец смены
        SQL.sql_delete('wms', f"""
            UPDATE wms_bot.shift_sessions1
            SET end_time = '{now}' 
            WHERE employee_id = '{staff_id}' AND end_time IS NULL
        """)

        # Получаем данные смены
        session_df = SQL.sql_select('wms', f"""
            SELECT start_time
            FROM wms_bot.shift_sessions1
            WHERE employee_id = '{staff_id}'
              AND end_time = '{now}'
            ORDER BY start_time DESC LIMIT 1
        """)

        if session_df.empty:
            await update.message.reply_text("Не удалось найти активную смену.")
            return

        # Рассчитываем продолжительность смены
        start_time = pd.to_datetime(session_df.iloc[0]['start_time'])
        shift_duration = now - start_time
        shift_hours = round(shift_duration.total_seconds() / 3600, 2)

        # Получаем общее время работы из БД (сумма всех подтвержденных заданий)
        from ..utils.task_utils import get_total_worked_time_from_db
        shift = context.user_data.get('shift')  # Получаем смену
        total_worked_seconds = get_total_worked_time_from_db(staff_id, shift)  # Передаем смену
        
        # Также учитываем время из контекста (на случай если есть незаписанные данные)
        context_worked_seconds = context.user_data.get('worked_seconds', 0)
        
        # Берем максимальное значение для надежности
        final_worked_seconds = max(total_worked_seconds, context_worked_seconds)
        
        # Форматируем время работы
        worked_time = str(timedelta(seconds=final_worked_seconds))
        worked_hours = round(final_worked_seconds / 3600, 2)
        
        # Формируем сообщение о завершении смены
        completion_message = (
            f"✅ *Смена завершена!*\n\n"
            f"⏱ *Продолжительность смены:* {shift_hours} ч\n"
            f"⏰ *Отработано заданий:* {worked_time} ({worked_hours} ч)\n\n"
            f"📊 *Статистика:*\n"
            f"• Время в смене: {shift_hours} ч\n"
            f"• Время на заданиях: {worked_hours} ч"
        )
        
        await update.message.reply_text(completion_message, parse_mode='Markdown')


    except Exception as e:
        print(f"❌ Ошибка завершения смены: {e}")
        await update.message.reply_text("Ошибка при завершении смены. Обратитесь к администратору.")

async def exit_session(update: Update, context: CallbackContext):
    """Выход из текущей сессии"""
    role = context.user_data.get('role')

    if role == 'opv':
        staff_id = context.user_data.get('staff_id')
        
        # Проверяем статус заданий пользователя
        status_check = check_user_task_status(staff_id)
        if status_check['blocked']:
            reply_markup = status_check.get('reply_markup')
            await update.message.reply_text(status_check['message'], reply_markup=reply_markup)
            return

        # Если нет блокировок, показываем кнопку получения задания
        from ..keyboards.opv_keyboards import get_next_task_keyboard
        reply_markup = get_next_task_keyboard()
        await update.message.reply_text("Вы завершили текущую сессию. Готовы взять новое задание? 👇", reply_markup=reply_markup)
    elif role == 'zs':
        reply_markup = get_zs_main_menu_keyboard()
        await update.message.reply_text("Вы завершили текущую сессию. Что хотите посмотреть?", reply_markup=reply_markup)
    else:
        await update.message.reply_text("Вы ещё не выбрали роль. Введите /start для начала работы.")
