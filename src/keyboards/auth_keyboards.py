from telegram import InlineKeyboardButton, InlineKeyboardMarkup, KeyboardButton, ReplyKeyboardMarkup

def get_shift_keyboard():
    """Клавиатура выбора смены"""
    keyboard = [
        [InlineKeyboardButton("День 🌇", callback_data='day')],
        [InlineKeyboardButton("Ночь 🌃", callback_data='night')],
        [InlineKeyboardButton("🔙 Назад", callback_data='back_to_previous')]
    ]
    return InlineKeyboardMarkup(keyboard)

def get_role_keyboard(shift):
    """Клавиатура выбора роли"""
    keyboard = [
        [InlineKeyboardButton("ОПВ", callback_data=f'opv_{shift}')],
        [InlineKeyboardButton("ЗС", callback_data=f'zs_{shift}')],
        [InlineKeyboardButton("🔙 Назад", callback_data='back_to_previous')]
    ]
    return InlineKeyboardMarkup(keyboard)

def get_employment_keyboard():
    """Клавиатура выбора типа занятости"""
    keyboard = [
        [InlineKeyboardButton("Основная смена", callback_data='employment_main')],
        [InlineKeyboardButton("Парттайм", callback_data='employment_part_time')],
        [InlineKeyboardButton("🔙 Назад", callback_data='back_to_previous')]
    ]
    return InlineKeyboardMarkup(keyboard)

def get_contact_keyboard():
    """Клавиатура для отправки контакта"""
    button = KeyboardButton("Отправить номер телефона 📱", request_contact=True)
    return ReplyKeyboardMarkup([[button]], resize_keyboard=True, one_time_keyboard=True)
