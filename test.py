from telegram import Update
from telegram.ext import Application, CommandHandler, MessageHandler, filters, ContextTypes
import asyncio
import logging

# Включаем логирование
logging.basicConfig(format='%(asctime)s - %(name)s - %(levelname)s - %(message)s', level=logging.INFO)
logger = logging.getLogger(__name__)

TOKEN = "8390538420:AAG2I_hUmJq_s5jovhrucOYuQDO2RVYIcyk" #ТОКЕН bekzhan_ds

# Сохраняем топики по чатам {chat_id: {topic_id: topic_name}}
chat_topics = {}


async def start_command(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Команда /start - показывает инструкции"""
    if update.message.chat.type in ["group", "supergroup"]:
        await update.message.reply_text(
            "✅ Привет! Я бот для работы с топиками.\n\n"
            "📋 Доступные команды:\n"
            "/topics - показать все найденные топики\n"
            "/scan - попробовать найти топики автоматически\n"
            "/clear - очистить список топиков\n\n"
            "ℹ️ Также я автоматически сохраняю ID топиков из сообщений."
        )
    else:
        await update.message.reply_text("Этот бот работает только в группах с топиками!")


async def my_chat_member_handler(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Обработчик события добавления бота в группу"""
    if update.my_chat_member:
        new_status = update.my_chat_member.new_chat_member.status
        chat = update.my_chat_member.chat
        
        if new_status in ["member", "administrator"]:
            # Бот был добавлен в группу
            logger.info(f"Бот добавлен в группу: {chat.title} (ID: {chat.id})")
            
            # Инициализируем список топиков для этого чата
            if chat.id not in chat_topics:
                chat_topics[chat.id] = {}
            
            # Отправляем приветственное сообщение и запускаем поиск топиков
            try:
                await context.bot.send_message(
                    chat_id=chat.id,
                    text="✅ Бот успешно добавлен в группу!\n"
                         "🔍 Начинаю поиск топиков...\n"
                         "Используйте /topics чтобы увидеть найденные топики."
                )
                
                # Запускаем сканирование топиков
                await scan_topics(chat.id, context)
                
            except Exception as e:
                logger.error(f"Ошибка при отправке приветственного сообщения: {e}")


async def scan_topics(chat_id: int, context: ContextTypes.DEFAULT_TYPE):
    """Попытка автоматического сканирования топиков"""
    try:
        # Попробуем отправить сообщения в разные возможные топики
        found_topics = {}
        
        # Проверяем топики с ID от 1 до 50 (обычно топики имеют небольшие ID)
        for topic_id in range(1, 51):
            try:
                # Пытаемся отправить тестовое сообщение в топик
                message = await context.bot.send_message(
                    chat_id=chat_id,
                    text="🔍 Тестовое сообщение для поиска топиков (будет удалено)",
                    message_thread_id=topic_id
                )
                
                # Если сообщение отправилось, значит топик существует
                # Пробуем получить название топика
                try:
                    # Получаем информацию о форуме
                    chat_info = await context.bot.get_chat(chat_id)
                    topic_name = f"Топик {topic_id}"  # Базовое название
                    
                    # К сожалению, Bot API не предоставляет прямого способа получить название топика
                    # Но мы можем попробовать получить его из сообщения
                    if message.reply_to_message and hasattr(message.reply_to_message, 'forum_topic_created'):
                        topic_name = message.reply_to_message.forum_topic_created.name
                    
                except:
                    topic_name = f"Топик {topic_id}"
                
                found_topics[topic_id] = topic_name
                
                # Добавляем в наш список
                if chat_id not in chat_topics:
                    chat_topics[chat_id] = {}
                chat_topics[chat_id][topic_id] = topic_name
                
                # Удаляем тестовое сообщение
                try:
                    await context.bot.delete_message(chat_id=chat_id, message_id=message.message_id)
                except:
                    pass  # Если не можем удалить - не страшно
                
                # Небольшая задержка чтобы не спамить
                await asyncio.sleep(0.1)
                
            except Exception:
                # Топик не существует или нет прав - пропускаем
                continue
        
        # Отправляем результат
        if found_topics:
            topics_text = "\n".join([f"📌 {name} (ID: {topic_id})" for topic_id, name in found_topics.items()])
            await context.bot.send_message(
                chat_id=chat_id,
                text=f"✅ Найдено топиков: {len(found_topics)}\n\n{topics_text}\n\n"
                     f"Используйте /topics для просмотра списка."
            )
        else:
            await context.bot.send_message(
                chat_id=chat_id,
                text="❌ Не удалось найти топики автоматически.\n"
                     "💡 Попробуйте написать по одному сообщению в каждый топик."
            )
            
    except Exception as e:
        logger.error(f"Ошибка при сканировании топиков: {e}")


async def scan_topics_command(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Команда для ручного запуска сканирования топиков"""
    if update.message.chat.type not in ["group", "supergroup"]:
        await update.message.reply_text("Эта команда работает только в группах!")
        return
    
    chat_id = update.message.chat.id
    await update.message.reply_text("🔍 Начинаю сканирование топиков...")
    await scan_topics(chat_id, context)


async def show_topics_command(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Показать все найденные топики"""
    if update.message.chat.type not in ["group", "supergroup"]:
        await update.message.reply_text("Эта команда работает только в группах!")
        return
    
    chat_id = update.message.chat.id
    
    if chat_id not in chat_topics or not chat_topics[chat_id]:
        await update.message.reply_text(
            "📭 Топики пока не найдены.\n"
            "Попробуйте:\n"
            "• /scan - автоматический поиск\n"
            "• Написать сообщение в каждый топик"
        )
        return
    
    topics_dict = chat_topics[chat_id]
    topics_text = "\n".join([f"📌 {name} (ID: {topic_id})" for topic_id, name in topics_dict.items()])
    
    await update.message.reply_text(
        f"📋 Найденные топики ({len(topics_dict)}):\n\n{topics_text}"
    )


async def clear_topics_command(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Очистить список топиков"""
    if update.message.chat.type not in ["group", "supergroup"]:
        await update.message.reply_text("Эта команда работает только в группах!")
        return
    
    chat_id = update.message.chat.id
    
    if chat_id in chat_topics:
        chat_topics[chat_id].clear()
    
    await update.message.reply_text("🗑️ Список топиков очищен.")


async def catch_topics(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Автоматическое сохранение ID топиков из сообщений"""
    if not update.message:
        return
    
    chat_id = update.message.chat.id
    thread_id = update.message.message_thread_id
    
    # Если есть thread_id, сохраняем его
    if thread_id:
        if chat_id not in chat_topics:
            chat_topics[chat_id] = {}
        
        # Если это новый топик - уведомляем и пытаемся получить название
        if thread_id not in chat_topics[chat_id]:
            # Пытаемся получить название топика
            topic_name = f"Топик {thread_id}"  # Базовое название
            
            # Пробуем получить название из различных источников
            try:
                # Если это сообщение о создании топика
                if update.message.forum_topic_created:
                    topic_name = update.message.forum_topic_created.name
                # Если это ответ на сообщение о создании топика
                elif (update.message.reply_to_message and 
                      update.message.reply_to_message.forum_topic_created):
                    topic_name = update.message.reply_to_message.forum_topic_created.name
                # Попробуем получить из пользовательских данных или других способов
                else:
                    # Можно добавить дополнительные способы получения названий топиков
                    pass
                    
            except Exception as e:
                logger.debug(f"Не удалось получить название топика: {e}")
            
            chat_topics[chat_id][thread_id] = topic_name
            
            # Отправляем уведомление в General (без thread_id)
            try:
                await context.bot.send_message(
                    chat_id=chat_id,
                    text=f"📌 Обнаружен новый топик!\n"
                         f"Название: {topic_name}\nID: {thread_id}\n"
                         f"Всего топиков: {len(chat_topics[chat_id])}",
                    message_thread_id=None
                )
            except Exception as e:
                logger.error(f"Ошибка при отправке уведомления о новом топике: {e}")


def main():
    """Основная функция запуска бота"""
    app = Application.builder().token(TOKEN).build()

    # Обработчик добавления бота в группу
    app.add_handler(MessageHandler(filters.StatusUpdate.NEW_CHAT_MEMBERS, my_chat_member_handler))
    
    # Команды
    app.add_handler(CommandHandler("start", start_command))
    app.add_handler(CommandHandler("topics", show_topics_command))
    app.add_handler(CommandHandler("scan", scan_topics_command))
    app.add_handler(CommandHandler("clear", clear_topics_command))

    # Ловим все сообщения для автоматического поиска топиков
    app.add_handler(MessageHandler(filters.ALL, catch_topics))

    print("🤖 Бот запущен и готов к работе!")
    print("📋 Доступные команды:")
    print("   /topics - показать найденные топики")
    print("   /scan - сканировать топики автоматически") 
    print("   /clear - очистить список топиков")
    
    app.run_polling(allowed_updates=["message", "my_chat_member"])


if __name__ == "__main__":
    main()