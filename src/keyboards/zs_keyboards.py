from telegram import InlineKeyboardButton, InlineKeyboardMarkup

def get_zs_main_menu_keyboard():
    """Главное меню ЗС"""
    keyboard = [
        [InlineKeyboardButton("Список ОПВ на смене 📋", callback_data='opv_list_on_shift')],
        [InlineKeyboardButton("Список ОПВ завершивших смену ✅", callback_data='opv_list_completed')],
        [InlineKeyboardButton("🔙 Назад", callback_data='back_to_previous')]
    ]
    return InlineKeyboardMarkup(keyboard)

def get_opv_list_keyboard():
    """Клавиатура списка ОПВ"""
    keyboard = [
        [InlineKeyboardButton("✅ Список ОПВ - свободные", callback_data='opv_free')],
        [InlineKeyboardButton("⏳ Список ОПВ - занятые", callback_data='opv_busy')],
        [InlineKeyboardButton("🔙 Назад", callback_data='back_to_previous')]
    ]
    return InlineKeyboardMarkup(keyboard)

def get_opv_names_keyboard(opv_list, callback_prefix):
    """Клавиатура с именами ОПВ"""
    keyboard = [
        [InlineKeyboardButton(row['user_name'], callback_data=f"{callback_prefix}_{row['employee_id']}")]
        for _, row in opv_list.iterrows()
    ]
    keyboard.append([InlineKeyboardButton("🔙 Назад", callback_data='back_to_previous')])
    return InlineKeyboardMarkup(keyboard)

def get_zs_review_keyboard(task_id, opv_employee_id):
    """Клавиатура для ЗС под сообщением проверки задания"""
    keyboard = [
        [
            InlineKeyboardButton("✅ Подтвердить", callback_data=f"approve_{task_id}|{opv_employee_id}"),
            InlineKeyboardButton("🔁 Вернуть", callback_data=f"start_reject_{task_id}|{opv_employee_id}")
        ]
    ]
    return InlineKeyboardMarkup(keyboard)