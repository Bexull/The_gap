import pandas as pd
from telegram import Update
from telegram.ext import CallbackContext
from ..database.sql_client import SQL
from ..keyboards.auth_keyboards import get_contact_keyboard, get_shift_keyboard
from .shift_handlers import shift_start
from ..utils.navigation import navigation_history

async def start(update: Update, context: CallbackContext):
    """Обработчик команды /start - авторизация пользователя"""
    user_id = update.effective_user.id
    
    try:
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
            await update.message.reply_text(f"✅ Добро пожаловать, {record['fio']}!")
            await shift_start(update, context)
        else:
            reply_markup = get_contact_keyboard()
            await update.message.reply_text("Пожалуйста, авторизуйтесь, отправив свой номер телефона:", reply_markup=reply_markup)
            # Добавляем меню авторизации в историю
            navigation_history.add_menu(update.effective_user.id, 'auth')

    except Exception as e:
        print(f"❌ Ошибка авторизации: {e}")
        await update.message.reply_text("Произошла ошибка при проверке авторизации. Обратитесь к администратору.")

async def handle_contact(update: Update, context: CallbackContext):
    """Обработчик отправки контакта для авторизации"""
    contact = update.message.contact
    user_phone = contact.phone_number[-7:]  # последние 7 цифр
    
    try:
        staff_df = SQL.sql_select('wms', "select id,cell_phone, concat(bs.name, ' ', bs.surname) fio, gender from wms_bot.t_staff bs")
        match = staff_df[staff_df['cell_phone'].astype(str).str.endswith(user_phone)]

        if not match.empty:
            context.user_data.update({
                'phone': contact.phone_number,
                'staff_id': match.iloc[0]['id'],
                'staff_name': match.iloc[0]['fio']
            })

            # Сохраняем в базу
            auth_data = pd.DataFrame([{
                'userid': update.effective_user.id,
                'phone': contact.phone_number,
                'fio': match.iloc[0]['fio'],
                'employee_id': match.iloc[0]['id']
            }])
            
            SQL.sql_execute_df('wms', auth_data, 'wms_bot.bot_auth(userid, phone, fio, employee_id)')
            await update.message.reply_text(f"✅ Вы авторизованы как: {match.iloc[0]['fio']}")
            await shift_start(update, context)
        else:
            await update.message.reply_text("❌ Вы не авторизованы. Обратитесь к администратору.")
            
    except Exception as e:
        print(f"❌ Ошибка обработки контакта: {e}")
        await update.message.reply_text("Произошла ошибка при обработке вашего номера.")


