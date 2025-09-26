from datetime import datetime
import pandas as pd
from telegram import Update
from telegram.ext import CallbackContext
from ..utils.opv_utils import get_free_opv_for_special_tasks, get_busy_opv_for_special_tasks, force_assign_tasks_by_time
from ..config.settings import ADMIN_ID

async def see_free_opv(update: Update, context: CallbackContext):
    """Команда /see для просмотра свободных ОПВ для спец-заданий"""
    
    # Проверяем права администратора
    if update.effective_user.id != ADMIN_ID:
        await update.message.reply_text("❌ У вас нет прав для выполнения этой команды.")
        return
    
    try:
        # Получаем свободных ОПВ
        free_opv_df, shift_ru, shift_en = get_free_opv_for_special_tasks()
        
        # Получаем занятых ОПВ
        busy_opv_df, _, _ = get_busy_opv_for_special_tasks()
        
        # Формируем сообщение
        message = f"👥 *Статус ОПВ для спец-заданий*\n\n"
        message += f"🕐 *Смена:* {shift_ru} ({shift_en})\n"
        message += f"📅 *Дата:* {datetime.now().strftime('%d.%m.%Y %H:%M')}\n\n"
        
        # Свободные ОПВ
        if not free_opv_df.empty:
            message += f"✅ *Свободные ОПВ ({len(free_opv_df)}):*\n"
            for _, opv in free_opv_df.iterrows():
                gender_emoji = "👨" if opv['gender'] == 'M' else "👩" if opv['gender'] == 'F' else "👤"
                message += f"{gender_emoji} {opv['fio']} ({opv['gender']})\n"
        else:
            message += "❌ *Свободных ОПВ нет*\n"
        
        message += "\n"
        
        # Занятые ОПВ
        if not busy_opv_df.empty:
            message += f"🔒 *Занятые спец-заданиями ({len(busy_opv_df)}):*\n"
            for _, opv in busy_opv_df.iterrows():
                gender_emoji = "👨" if opv['gender'] == 'M' else "👩" if opv['gender'] == 'F' else "👤"
                time_begin = opv['time_begin'].strftime('%H:%M') if pd.notnull(opv['time_begin']) else 'неизвестно'
                message += f"{gender_emoji} {opv['fio']} - {opv['task_name']} (с {time_begin})\n"
        else:
            message += "🆓 *Занятых спец-заданиями нет*\n"
        
        await update.message.reply_text(message, parse_mode='Markdown')
        
    except Exception as e:
        await update.message.reply_text(f"❌ Ошибка при получении данных: {e}")

async def set_push_opv(update: Update, context: CallbackContext):
    """Команда /set_push_opv для принудительной раздачи заданий по времени"""
    
    # Проверяем права администратора
    if update.effective_user.id != ADMIN_ID:
        await update.message.reply_text("❌ У вас нет прав для выполнения этой команды.")
        return
    
    # Проверяем аргументы команды
    if not context.args:
        await update.message.reply_text(
            "❌ Укажите время в формате HH:MM\n"
            "Пример: /set_push_opv 12:30"
        )
        return
    
    time_str = context.args[0]
    
    # Проверяем формат времени
    try:
        from datetime import datetime
        datetime.strptime(time_str, '%H:%M')
    except ValueError:
        await update.message.reply_text(
            "❌ Неверный формат времени. Используйте HH:MM\n"
            "Пример: /set_push_opv 12:30"
        )
        return
    
    try:
        # Принудительно назначаем задания
        result = await force_assign_tasks_by_time(context, time_str)
        
        # Отправляем результат
        if len(result) > 4000:  # Telegram лимит на сообщение
            # Разбиваем на части
            parts = result.split('\n')
            current_message = ""
            
            for part in parts:
                if len(current_message + part + '\n') > 4000:
                    await update.message.reply_text(current_message)
                    current_message = part + '\n'
                else:
                    current_message += part + '\n'
            
            if current_message:
                await update.message.reply_text(current_message)
        else:
            await update.message.reply_text(result)
            
    except Exception as e:
        await update.message.reply_text(f"❌ Ошибка при выполнении команды: {e}")
