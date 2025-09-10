from telegram import InlineKeyboardButton, InlineKeyboardMarkup

def get_sector_keyboard(sectors):
    """Клавиатура выбора сектора"""
    keyboard = [[InlineKeyboardButton(sector, callback_data=f'sectorchoice_{sector}')] for sector in sectors]
    keyboard.append([InlineKeyboardButton("🔙 Назад", callback_data='back_to_previous')])
    return InlineKeyboardMarkup(keyboard)

def get_task_keyboard():
    keyboard = [
        [InlineKeyboardButton("📋 Посмотреть задачу", callback_data="show_task")],
        [InlineKeyboardButton("✅ Завершить задачу", callback_data="complete_task_inline")]
    ]
    return InlineKeyboardMarkup(keyboard)

def get_task_confirmation_keyboard():
    """Клавиатура подтверждения получения задания"""
    keyboard = [
        [InlineKeyboardButton("Получить задание", callback_data='get_task')],
        [InlineKeyboardButton("🔙 Назад", callback_data='back_to_previous')]
    ]
    return InlineKeyboardMarkup(keyboard)

def get_next_task_keyboard():
    """Клавиатура для получения следующего задания"""
    keyboard = [
        [InlineKeyboardButton("Получить задание", callback_data='get_task')],
        [InlineKeyboardButton("🔙 Назад", callback_data='back_to_previous')]
    ]
    return InlineKeyboardMarkup(keyboard)

def get_task_in_progress_keyboard():
    """Клавиатура для задания в процессе выполнения"""
    keyboard = [
        [InlineKeyboardButton("✅ Завершить задачу", callback_data='complete_task_inline')],
        [InlineKeyboardButton("🔙 Назад", callback_data='back_to_previous')]
    ]
    return InlineKeyboardMarkup(keyboard)

def get_photo_upload_keyboard():
    """Клавиатура для загрузки фото"""
    keyboard = [
        [InlineKeyboardButton("✅ Завершить задачу", callback_data='complete_task_inline')],
        [InlineKeyboardButton("🔙 Назад", callback_data='back_to_previous')]
    ]
    return InlineKeyboardMarkup(keyboard)

def get_special_task_keyboard():
    """Клавиатура для спец-задания (приоритет 111)"""
    keyboard = [
        [InlineKeyboardButton("✅ Завершить спец-задание", callback_data='complete_special_task')]
    ]
    return InlineKeyboardMarkup(keyboard)