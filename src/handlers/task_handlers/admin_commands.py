import asyncio
import logging
from telegram import Update, InlineKeyboardButton, InlineKeyboardMarkup
from telegram.ext import ContextTypes

logger = logging.getLogger(__name__)


async def clear_topic_handler(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """
    Обработчик команды /clear для очистки топика от сообщений
    Удаляет все сообщения в топике, включая команды, но сохраняет закрепленные сообщения
    """
    
    # Проверяем что команда вызвана в группе/супергруппе
    if update.message.chat.type not in ["group", "supergroup"]:
        await update.message.reply_text("❌ Эта команда работает только в группах!")
        return
    
    # Проверяем что команда вызвана в топике
    if not update.message.message_thread_id:
        await update.message.reply_text("❌ Эта команда должна использоваться внутри топика!")
        return
    
    chat_id = update.message.chat.id
    thread_id = update.message.message_thread_id
    user = update.message.from_user
    
    try:
        # Получаем информацию о правах пользователя
        chat_member = await context.bot.get_chat_member(chat_id, user.id)
        
        # Проверяем права пользователя (админ или создатель)
        if chat_member.status not in ["administrator", "creator"]:
            await update.message.reply_text("❌ У вас нет прав для очистки топика!")
            return
        
        # Отправляем подтверждающее сообщение
        confirm_msg = await update.message.reply_text(
            "🧹 Начинаю очистку топика...\n"
            "⏳ Это может занять некоторое время."
        )
        
        deleted_count = 0
        errors_count = 0
        skipped_count = 0
        
        # Получаем последние сообщения и удаляем их пачками
        current_message_id = update.message.message_id
        batch_size = 100
        
        for offset in range(0, 1000, batch_size):
            batch_deleted = 0
            
            for msg_id in range(current_message_id - offset, 
                              max(1, current_message_id - offset - batch_size), -1):
                
                # Сначала проверяем содержимое сообщения на наличие "@" в начале
                should_skip = False
                try:
                    # Получаем информацию о сообщении
                    message = await context.bot.forward_message(
                        chat_id=chat_id,
                        from_chat_id=chat_id,
                        message_id=msg_id,
                        message_thread_id=thread_id
                    )
                    
                    # Проверяем текст сообщения
                    message_text = ""
                    if message.text:
                        message_text = message.text.strip()
                    elif message.caption:
                        message_text = message.caption.strip()
                    
                    # Если сообщение начинается с "@", считаем его защищенным
                    if message_text.startswith("@"):
                        should_skip = True
                        skipped_count += 1
                    
                    # Удаляем пересланное сообщение
                    await context.bot.delete_message(chat_id, message.message_id)
                    
                except Exception:
                    # Если не удалось получить сообщение, продолжаем попытку удаления
                    pass
                
                # Если сообщение помечено для пропуска, переходим к следующему
                if should_skip:
                    continue
                
                try:
                    # Пытаемся удалить сообщение
                    await context.bot.delete_message(
                        chat_id=chat_id, 
                        message_id=msg_id
                    )
                    deleted_count += 1
                    batch_deleted += 1
                    
                    # Небольшая задержка чтобы не превысить лимиты API
                    await asyncio.sleep(0.1)
                    
                except Exception as e:
                    error_msg = str(e).lower()
                    if "message to delete not found" not in error_msg:
                        if any(keyword in error_msg for keyword in [
                            "message can't be deleted", 
                            "pinned", 
                            "can't delete",
                            "bad request"
                        ]):
                            # Это может быть закрепленное или служебное сообщение
                            skipped_count += 1
                        else:
                            errors_count += 1
            
            # Если в пачке ничего не удалили, прекращаем
            if batch_deleted == 0:
                break
            
            # Обновляем статус каждые 50 удаленных сообщений
            if deleted_count % 50 == 0 and deleted_count > 0:
                try:
                    await context.bot.edit_message_text(
                        chat_id=chat_id,
                        message_id=confirm_msg.message_id,
                        text=f"🧹 Очистка в процессе...\n"
                             f"✅ Удалено: {deleted_count} сообщений\n"
                             f"⚠️ Пропущено: {skipped_count}"
                    )
                except:
                    pass
        
        # Удаляем саму команду /clear
        try:
            await context.bot.delete_message(chat_id, update.message.message_id)
            deleted_count += 1
        except Exception as e:
            pass
        
        # Финальное сообщение
        final_text = f"✅ Очистка топика завершена!\n\n" \
                    f"📊 Статистика:\n" \
                    f"• Удалено сообщений: {deleted_count}\n"
        
        if skipped_count > 0:
            final_text += f"• Пропущено (@ или закрепленные): {skipped_count}\n"
        
        if deleted_count == 0:
            final_text = "ℹ️ В топике не найдено сообщений для удаления."
        
        try:
            await context.bot.edit_message_text(
                chat_id=chat_id,
                message_id=confirm_msg.message_id,
                text=final_text
            )
            
            # Удаляем финальное сообщение через 10 секунд
            await asyncio.sleep(10)
            try:
                await context.bot.delete_message(chat_id, confirm_msg.message_id)
            except:
                pass
                
        except:
            # Если не можем отредактировать, отправляем новое сообщение
            temp_msg = await context.bot.send_message(
                chat_id=chat_id,
                text=final_text,
                message_thread_id=thread_id
            )
            
            # Удаляем временное сообщение через 10 секунд
            await asyncio.sleep(10)
            try:
                await context.bot.delete_message(chat_id, temp_msg.message_id)
            except:
                pass
    
    except Exception as e:
        logger.error(f"Ошибка при очистке топика: {e}")
        try:
            error_msg = await update.message.reply_text(
                f"❌ Произошла ошибка при очистке топика:\n{str(e)}"
            )
            
            # Удаляем сообщение об ошибке и команду через 10 секунд
            await asyncio.sleep(10)
            try:
                await context.bot.delete_message(chat_id, error_msg.message_id)
                await context.bot.delete_message(chat_id, update.message.message_id)
            except:
                pass
        except:
            pass


async def clear_topic_confirm_handler(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """
    Альтернативная версия с подтверждением перед очисткой
    Использует inline-кнопки для подтверждения действия
    """
    
    # Проверяем что команда вызвана в топике
    if not update.message.message_thread_id:
        await update.message.reply_text("❌ Эта команда должна использоваться внутри топика!")
        return
    
    # Проверяем права
    chat_id = update.message.chat.id
    user = update.message.from_user
    
    try:
        chat_member = await context.bot.get_chat_member(chat_id, user.id)
        if chat_member.status not in ["administrator", "creator"]:
            await update.message.reply_text("❌ У вас нет прав для очистки топика!")
            return
    except:
        await update.message.reply_text("❌ Не удалось проверить права доступа!")
        return
    
    # Создаем кнопки подтверждения
    keyboard = [
        [
            InlineKeyboardButton("✅ Да, очистить", callback_data=f"clear_confirm_{update.message.message_thread_id}"),
            InlineKeyboardButton("❌ Отмена", callback_data="clear_cancel")
        ]
    ]
    reply_markup = InlineKeyboardMarkup(keyboard)
    
    await update.message.reply_text(
        "⚠️ Вы уверены что хотите очистить этот топик?\n\n"
        "🗑️ Будут удалены ВСЕ сообщения в топике!\n"
        "❗ Это действие нельзя отменить!",
        reply_markup=reply_markup
    )


async def clear_topic_callback_handler(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Обработчик callback'ов для подтверждения очистки топика"""
    
    query = update.callback_query
    await query.answer()
    
    if query.data == "clear_cancel":
        await query.edit_message_text("❌ Очистка топика отменена.")
        return
    
    if query.data.startswith("clear_confirm_"):
        thread_id = int(query.data.split("_")[-1])
        
        # Проверяем что пользователь имеет права
        chat_id = query.message.chat.id
        user = query.from_user
        
        try:
            chat_member = await context.bot.get_chat_member(chat_id, user.id)
            if chat_member.status not in ["administrator", "creator"]:
                await query.edit_message_text("❌ У вас нет прав для очистки топика!")
                return
        except:
            await query.edit_message_text("❌ Не удалось проверить права доступа!")
            return
        
        # Запускаем очистку
        await query.edit_message_text("🧹 Начинаю очистку топика...")
        
        # Здесь вызываем основную логику очистки (можно выделить в отдельную функцию)
        # Аналогично коду из clear_topic_handler, но адаптированному для callback
