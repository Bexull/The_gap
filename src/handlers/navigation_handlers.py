"""
Обработчики навигации для возврата к предыдущим меню
"""

from telegram import Update
from telegram.ext import CallbackContext
from ..utils.navigation import navigation_history, MENU_NAMES
from ..keyboards.auth_keyboards import get_shift_keyboard, get_role_keyboard, get_employment_keyboard
from ..keyboards.opv_keyboards import get_sector_keyboard, get_task_confirmation_keyboard, get_next_task_keyboard
from ..keyboards.zs_keyboards import get_zs_main_menu_keyboard, get_opv_list_keyboard
from ..database.sql_client import SQL
from ..utils.time_utils import get_task_date
from ..config.settings import MERCHANT_ID

async def back_to_previous_menu(update: Update, context: CallbackContext):
    """Обработчик кнопки 'Назад' - возврат к предыдущему меню"""
    query = update.callback_query
    await query.answer()
    
    user_id = update.effective_user.id
    previous_menu = navigation_history.get_previous_menu(user_id)
    
    if not previous_menu:
        # Если нет предыдущего меню, возвращаемся к началу
        await back_to_start(update, context)
        return
    
    menu_name = previous_menu['name']
    menu_data = previous_menu.get('data', {})
    
    # Обрабатываем разные типы меню
    if menu_name == 'start':
        await handle_start_menu(update, context)
    elif menu_name == 'shift_choice':
        await handle_shift_choice_menu(update, context)
    elif menu_name == 'role_choice':
        await handle_role_choice_menu(update, context)
    elif menu_name == 'employment_choice':
        await handle_employment_choice_menu(update, context)
    elif menu_name == 'sector_choice':
        await handle_sector_choice_menu(update, context)
    elif menu_name == 'task_confirmation':
        await handle_task_confirmation_menu(update, context)
    elif menu_name == 'zs_main_menu':
        await handle_zs_main_menu(update, context)
    elif menu_name == 'opv_list':
        await handle_opv_list_menu(update, context)
    else:
        # Если неизвестное меню, возвращаемся к началу
        await back_to_start(update, context)

async def back_to_start(update: Update, context: CallbackContext):
    """Возврат к началу - очищает историю и запускает /start"""
    query = update.callback_query
    await query.answer()
    
    user_id = update.effective_user.id
    navigation_history.clear_history(user_id)
    context.user_data.clear()
    
    # Запускаем процесс авторизации заново
    try:
        from ..database.sql_client import SQL
        from ..keyboards.auth_keyboards import get_contact_keyboard
        
        auth_check = SQL.sql_select('wms', f"""
            SELECT phone, fio, employee_id
            FROM wms_bot.bot_auth
            WHERE userid = {user_id}
        """)
        
        if not auth_check.empty:
            record = auth_check.iloc[0]
            context.user_data.update({
                'phone': record['phone'],
                'staff_name': record['fio'],
                'staff_id': record['employee_id']
            })
            await query.edit_message_text(f"✅ Добро пожаловать, {record['fio']}!")
            from .shift_handlers import shift_start
            await shift_start(update, context)
        else:
            reply_markup = get_contact_keyboard()
            await query.edit_message_text("Пожалуйста, авторизуйтесь, отправив свой номер телефона:", reply_markup=reply_markup)
            navigation_history.add_menu(user_id, 'auth')

    except Exception as e:
        print(f"❌ Ошибка возврата к началу: {e}")
        await query.edit_message_text("Произошла ошибка. Попробуйте команду /start")

# Обработчики для разных типов меню
async def handle_start_menu(update: Update, context: CallbackContext):
    """Обработка возврата к меню начала"""
    query = update.callback_query
    reply_markup = get_shift_keyboard()
    await query.edit_message_text("Выберите смену:", reply_markup=reply_markup)

async def handle_shift_choice_menu(update: Update, context: CallbackContext):
    """Обработка возврата к выбору смены"""
    query = update.callback_query
    shift = context.user_data.get('shift', 'day')
    reply_markup = get_role_keyboard(shift)
    await query.edit_message_text(f"Вы выбрали смену: {shift}. Теперь выберите роль:", reply_markup=reply_markup)

async def handle_role_choice_menu(update: Update, context: CallbackContext):
    """Обработка возврата к выбору роли"""
    query = update.callback_query
    role = context.user_data.get('role')
    shift = context.user_data.get('shift')
    
    if role == 'opv':
        reply_markup = get_employment_keyboard()
        await query.edit_message_text("Выберите тип занятости:", reply_markup=reply_markup)
    else:
        from ..config.settings import zav_on_shift
        if update.effective_user.id not in zav_on_shift:
            zav_on_shift.append(update.effective_user.id)
        
        reply_markup = get_zs_main_menu_keyboard()
        await query.edit_message_text(
            f"Вы выбрали роль: {role.upper()} на смене: {shift}.",
            reply_markup=reply_markup
        )

async def handle_employment_choice_menu(update: Update, context: CallbackContext):
    """Обработка возврата к выбору типа занятости"""
    query = update.callback_query
    reply_markup = get_employment_keyboard()
    await query.edit_message_text("Выберите тип занятости:", reply_markup=reply_markup)

async def handle_sector_choice_menu(update: Update, context: CallbackContext):
    """Обработка возврата к выбору сектора"""
    query = update.callback_query
    shift = context.user_data.get('shift')
    shift_ru = 'День' if shift == 'day' else 'Ночь'
    task_date = get_task_date(shift)

    print("shift:", shift)
    print("shift_ru:", shift_ru)
    print("task_date:", task_date)

    print(f"""
    SELECT DISTINCT sector FROM wms_bot.shift_tasks 
    WHERE task_date = '{task_date}' AND shift = '{shift_ru}' AND merchant_code='{MERCHANT_ID}'
    """)
    
    sql_sectors = f"""
        SELECT DISTINCT sector FROM wms_bot.shift_tasks 
        WHERE task_date = '{task_date}' AND shift = '{shift_ru}' and merchant_code='{MERCHANT_ID}'
    """
    print("🔎 SQL (sectors):", sql_sectors)
    sectors_df = SQL.sql_select('wms', sql_sectors)
    sectors = sectors_df['sector'].dropna().tolist()
    
    if not sectors:
        await query.edit_message_text("❌ Нет доступных секторов для этой смены.")
        return
    
    reply_markup = get_sector_keyboard(sectors)
    await query.edit_message_text("Выберите сектор, с которым будете работать в эту смену:", reply_markup=reply_markup)

async def handle_task_confirmation_menu(update: Update, context: CallbackContext):
    """Обработка возврата к подтверждению задания"""
    query = update.callback_query
    sector = context.user_data.get('sector', '')
    reply_markup = get_task_confirmation_keyboard()
    await query.edit_message_text(
        f"✅ Вы выбрали сектор: *{sector}*\nТеперь можно брать задания.",
        parse_mode='Markdown',
        reply_markup=reply_markup
    )

async def handle_zs_main_menu(update: Update, context: CallbackContext):
    """Обработка возврата к главному меню ЗС"""
    query = update.callback_query
    reply_markup = get_zs_main_menu_keyboard()
    await query.edit_message_text("Главное меню ЗС:", reply_markup=reply_markup)

async def handle_opv_list_menu(update: Update, context: CallbackContext):
    """Обработка возврата к списку ОПВ"""
    query = update.callback_query
    reply_markup = get_opv_list_keyboard()
    await query.edit_message_text("Список ОПВ:", reply_markup=reply_markup)
    
