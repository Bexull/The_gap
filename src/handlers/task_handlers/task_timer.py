import asyncio
from datetime import datetime, timedelta
from telegram.ext import CallbackContext
from ...database.sql_client import SQL
from ...config.settings import active_timers, MERCHANT_ID


async def update_timer(context, chat_id, message_id, task, total_seconds, reply_markup=None, comment=None):
    """Обновление таймера задания"""
    
    # Сохраняем информацию о таймере в глобальном хранилище
    active_timers[task['task_id']] = {
        'chat_id': chat_id,
        'message_id': message_id,
        'task': task,
        'total_seconds': total_seconds,
        'reply_markup': reply_markup
    }
    
    for remaining in range(total_seconds, -1, -15):
        try:
            # Проверяем статус задания в БД
            task_status_df = SQL.sql_select('wms', f"""
                SELECT status FROM wms_bot.shift_tasks 
                WHERE id = {task['task_id']} AND merchant_code = '{MERCHANT_ID}'
            """)
            
            if task_status_df.empty:
                break
                
            current_status = task_status_df.iloc[0]['status']
            
            # Если задание заморожено - останавливаем таймер и показываем сообщение
            if current_status == 'Заморожено':
                message = (
                    f"📄 *Номер задания:* {task['task_id']}\n"
                    f"❄️ *Задание заморожено*\n\n"
                    f"📝 *Наименование:* {task['task_name']}\n"
                    f"📦 *Группа товаров:* {task.get('product_group', '—')}\n"
                    f"📍 *Слот:* {task.get('slot', '—')}\n"
                    f"🏢 *Поставщик:* {task.get('provider', 'Не указан')}\n"
                    f"⏱ *Выделенное время:* {task['duration']}\n"
                    f"⏸️ *Таймер остановлен*\n\n"
                    f"*ℹ️ Завершите спец-задание, чтобы продолжить*"
                )
                
                await context.bot.edit_message_text(
                    chat_id=chat_id,
                    message_id=message_id,
                    text=message,
                    parse_mode='Markdown'
                )
                
                # Удаляем таймер из активных
                active_timers.pop(task['task_id'], None)
                break
            
            # Если задание завершено или имеет другой статус - останавливаем таймер
            if current_status not in ['Выполняется', 'На доработке']:
                active_timers.pop(task['task_id'], None)
                break

            minutes = remaining // 60
            seconds = remaining % 60
            remaining_str = f"{minutes:02d}:{seconds:02d}"

            message = (
                f"📄 *Номер задания:* {task['task_id']}\n"
                f"✅ *Задание выполняется*\n\n"
                f"📝 *Наименование:* {task['task_name']}\n"
                f"📦 *Группа товаров:* {task.get('product_group', '—')}\n"
                f"📍 *Слот:* {task.get('slot', '—')}\n"
                f"🏢 *Поставщик:* {task.get('provider', 'Не указан')}\n"
                f"⏱ *Выделенное время:* {task['duration']}\n"
                f"⏳ *Оставшееся время:* {remaining_str}"
            )

            if comment and str(comment).strip():
                message += f"\n💬 *Комментарий:* {comment}"

            await context.bot.edit_message_text(
                chat_id=chat_id,
                message_id=message_id,
                text=message,
                parse_mode='Markdown',
                reply_markup=reply_markup
            )

            await asyncio.sleep(15)
        except Exception as e:
            break
    
    # Удаляем таймер из активных при завершении
    active_timers.pop(task['task_id'], None)


async def stop_timer_for_task(task_id: int, context, reason: str = "задание завершено"):
    """Останавливает таймер для конкретного задания"""
    
    if task_id in active_timers:
        timer_info = active_timers[task_id]
        try:
            # Отправляем сообщение об остановке таймера
            await context.bot.edit_message_text(
                chat_id=timer_info['chat_id'],
                message_id=timer_info['message_id'],
                text=(
                    f"📄 *Номер задания:* {task_id}\n"
                    f"⏹️ *Таймер остановлен*\n\n"
                    f"📝 *Наименование:* {timer_info['task']['task_name']}\n"
                    f"📦 *Группа товаров:* {timer_info['task'].get('product_group', '—')}\n"
                    f"📍 *Слот:* {timer_info['task'].get('slot', '—')}\n"
                    f"🏢 *Поставщик:* {timer_info['task'].get('provider', 'Не указан')}\n"
                    f"⏱ *Выделенное время:* {timer_info['task']['duration']}\n"
                    f"⏸️ *Причина остановки:* {reason}"
                ),
                parse_mode='Markdown'
            )
        except Exception as e:
            pass
        
        # Удаляем таймер из активных
        active_timers.pop(task_id, None)
