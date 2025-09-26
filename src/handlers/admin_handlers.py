import asyncio
from telegram import Update
from telegram.ext import ContextTypes
from ..database.sql_client import SQL
from ..config.settings import ADMIN_ID


async def send_notification(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Обработчик команды /send_notification для отправки уведомлений всем пользователям"""
    
    # Проверяем, что команду использует администратор
    if update.effective_user.id != ADMIN_ID:
        await update.message.reply_text("❌ У вас нет прав для использования этой команды.")
        return
    
    # Запрашиваем текст уведомления
    await update.message.reply_text(
        "📝 Введите текст уведомления, которое будет отправлено всем пользователям:"
    )
    
    # Сохраняем состояние для ожидания текста
    context.user_data['waiting_for_notification_text'] = True


async def handle_notification_text(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Обработчик текста уведомления"""
    
    # Проверяем, что пользователь - администратор и ожидается текст
    if (update.effective_user.id != ADMIN_ID or 
        not context.user_data.get('waiting_for_notification_text', False)):
        return
    
    notification_text = update.message.text
    
    # Убираем флаг ожидания
    context.user_data['waiting_for_notification_text'] = False
    
    # Отправляем подтверждение
    await update.message.reply_text(
        f"📤 Отправляю уведомление всем пользователям...\n\n"
        f"Текст: {notification_text}"
    )
    
    # Получаем список активных пользователей и отправляем уведомления
    success_count, error_count = await send_notification_to_all_users(context, notification_text)
    
    # Отправляем отчет
    await update.message.reply_text(
        f"✅ Уведомление отправлено!\n\n"
        f"📊 Статистика:\n"
        f"• Успешно отправлено: {success_count}\n"
        f"• Ошибок: {error_count}"
    )


async def send_notification_to_all_users(context: ContextTypes.DEFAULT_TYPE, message_text: str):
    """Отправляет уведомление всем активным пользователям"""
    
    try:
        # Получаем список всех пользователей из базы данных
        users_query = """
        SELECT DISTINCT userid 
        FROM wms_bot.bot_auth 
        WHERE userid IS NOT NULL
        """
        
        users_df = SQL.sql_select('wms', users_query)
        
        if users_df.empty:
            return 0, 0
        
        success_count = 0
        error_count = 0
        
        # Отправляем сообщение каждому пользователю
        for _, row in users_df.iterrows():
            user_id = int(row['userid'])  # Преобразуем int64 в обычный int
            
            try:
                await context.bot.send_message(
                    chat_id=user_id,
                    text=f"📢 <b>Уведомление от администратора</b>\n\n{message_text}",
                    parse_mode='HTML'
                )
                success_count += 1
                
                # Небольшая задержка между отправками, чтобы не превысить лимиты API
                await asyncio.sleep(0.1)
                
            except Exception as e:
                print(f"Ошибка отправки сообщения пользователю {user_id}: {e}")
                error_count += 1
        
        return success_count, error_count
        
    except Exception as e:
        print(f"Ошибка при получении списка пользователей: {e}")
        return 0, 1
