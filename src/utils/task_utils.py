from datetime import datetime, timedelta
import datetime as dt
from telegram import InputMediaPhoto
from ..config.settings import ZS_GROUP_CHAT_ID, TOPIC_IDS, frozen_tasks_info, task_time_tracker
from ..keyboards.opv_keyboards import get_task_keyboard
from ..keyboards.zs_keyboards import get_zs_review_keyboard
from ..utils.time_utils import align_seconds, seconds_to_hms

def get_topic_id(sector: str) -> int:
    """Получает ID топика для сектора с обработкой различных вариантов названий"""
    if not sector:
        return None
    
    # Нормализуем название сектора
    sector_normalized = sector.strip().capitalize()
    
    # Прямое соответствие
    if sector_normalized in TOPIC_IDS:
        return TOPIC_IDS[sector_normalized]
    
    # Попробуем найти частичное соответствие
    for topic_sector, topic_id in TOPIC_IDS.items():
        if sector_normalized in topic_sector or topic_sector in sector_normalized:
            return topic_id
    
    print(f"⚠️ Топик для сектора '{sector_normalized}' не найден в TOPIC_IDS: {list(TOPIC_IDS.keys())}")
    return None

def check_user_task_status(staff_id: str):
    """Проверяет статус заданий пользователя и возвращает информацию о блокировках"""
    from ..database.sql_client import SQL
    from ..config.settings import MERCHANT_ID
    
    # Проверяем активные задания
    active_df = SQL.sql_select('wms', f"""
        SELECT id, status
        FROM wms_bot.shift_tasks
        WHERE user_id = '{staff_id}'
          AND status IN ('Выполняется', 'На доработке','Заморожено')
          AND merchant_code = '{MERCHANT_ID}'
    """)
    
    if not active_df.empty:
        return {
            'blocked': True,
            'reason': 'active_task',
            'message': (
                "⚠️ У вас уже есть активное задание.\nПожалуйста, завершите его перед тем как брать новое."
                ),
            'reply_markup': get_task_keyboard()
        }
    
    
    # Проверяем неподтвержденные задания
    pending_df = SQL.sql_select('wms', f"""
        SELECT id, task_name, status
        FROM wms_bot.shift_tasks
        WHERE user_id = '{staff_id}'
          AND status = 'Ожидает проверки'
          AND merchant_code = '{MERCHANT_ID}'
        ORDER BY time_end DESC
        LIMIT 1
    """)
    
    if not pending_df.empty:
        task_info = pending_df.iloc[0]
        return {
            'blocked': True,
            'reason': 'pending_task',
            'message': f"⏳ У вас есть задание №{task_info['id']} ({task_info['task_name']}), которое ожидает подтверждения заведующего.\n\nПожалуйста, дождитесь подтверждения перед тем как брать новое задание.",
            'task_id': task_info['id'],
            'task_name': task_info['task_name']
        }

    return {
        'blocked': False,
        'reason': None,
        'message': None
    }

async def send_task_to_zs(context, task: dict, photos: list):
    """Отправляет задание на проверку заведующему"""
    try:
        keyboard = get_zs_review_keyboard(task['task_id'], context.user_data.get('staff_id'))
        sector = context.user_data.get('sector', '').strip().capitalize()
        thread_id = get_topic_id(sector)

        # Получаем правильное время начала для вычисления времени выполнения
        task_id = task.get('task_id')
        start_time_for_calculation = task['assigned_time']
        
        # Проверяем, есть ли информация о замороженном задании
        from ..config.settings import frozen_tasks_info
        if task_id and task_id in frozen_tasks_info and 'original_start_time' in frozen_tasks_info[task_id]:
            original_start_time = frozen_tasks_info[task_id]['original_start_time']
            if isinstance(original_start_time, datetime):
                start_time_for_calculation = original_start_time
                print(f"🔧 [FIX] Используем original_start_time для вычисления времени выполнения задания {task_id}: {start_time_for_calculation}")
            else:
                print(f"⚠️ [WARNING] original_start_time для задания {task_id} не является datetime объектом: {type(original_start_time)}")
        else:
            print(f"ℹ️ [INFO] Используем assigned_time для вычисления времени выполнения задания {task_id}: {start_time_for_calculation}")
        
        tracker_entry = task_time_tracker.get(task_id)
        if tracker_entry:
            elapsed_seconds = tracker_entry.get('elapsed_seconds', 0)
        elif task_id and task_id in frozen_tasks_info:
            freeze_meta = frozen_tasks_info[task_id]
            elapsed_seconds = freeze_meta.get('elapsed_seconds', 0)
        else:
            elapsed_seconds = (datetime.now() - start_time_for_calculation).total_seconds()

        elapsed_seconds = align_seconds(elapsed_seconds, mode='round')
        time_spent = timedelta(seconds=elapsed_seconds)
        
        # Добавляем логирование для отладки
        print(f"⏰ [DEBUG] Время выполнения для задания {task_id}: {time_spent} (начало: {start_time_for_calculation}, текущее: {datetime.now()})")

        message = (
            f"📬 Задание от *{context.user_data.get('staff_name', 'ОПВ')}* завершено\n"
            f"📝 *Наименование:* {task.get('task_name', '—')}\n"
            f"📦 *Группа товаров:* {task.get('product_group', '—')}\n"
            f"📍 *Слот:* {task.get('slot', '—')}\n"
            f"🏢 *Поставщик:* {task.get('provider', 'Не указан')}\n"
            f"🏷️ *Сектор:* {sector}\n"
            f"⏱️ Время выполнения: {str(time_spent).split('.')[0]}\n"
            f"⏳ Выделенное время: {task['duration']} мин"
        )

        # Функция для отправки сообщения с обработкой ошибок топика
        async def send_with_topic_fallback(media_group=None, text=None, reply_markup=None):
            try:
                if media_group:
                    messages = await context.bot.send_media_group(
                        chat_id=ZS_GROUP_CHAT_ID,
                        media=media_group,
                        message_thread_id=thread_id
                    )
                    return messages[0].message_id
                else:
                    sent_msg = await context.bot.send_message(
                        chat_id=ZS_GROUP_CHAT_ID,
                        text=text,
                        parse_mode='Markdown',
                        reply_markup=reply_markup,
                        message_thread_id=thread_id
                    )
                    return sent_msg.message_id
            except Exception as topic_error:
                if "Message thread not found" in str(topic_error):
                    print(f"⚠️ Топик для сектора '{sector}' не найден, отправляем в основной чат")
                    # Отправляем без указания топика
                    if media_group:
                        messages = await context.bot.send_media_group(
                            chat_id=ZS_GROUP_CHAT_ID,
                            media=media_group
                        )
                        return messages[0].message_id
                    else:
                        sent_msg = await context.bot.send_message(
                            chat_id=ZS_GROUP_CHAT_ID,
                            text=text,
                            parse_mode='Markdown',
                            reply_markup=reply_markup
                        )
                        return sent_msg.message_id
                else:
                    raise topic_error

        if photos:
            media_group = []

            for i, photo in enumerate(photos):
                if i == 0:
                    media_group.append(InputMediaPhoto(media=photo, caption=message, parse_mode='Markdown'))
                else:
                    media_group.append(InputMediaPhoto(media=photo))

            message_id = await send_with_topic_fallback(media_group=media_group)
            context.user_data['last_task_message_id'] = message_id

            await send_with_topic_fallback(
                text="🔽 Выберите действие:",
                reply_markup=keyboard
            )
        else:
            message_id = await send_with_topic_fallback(
                text=message,
                reply_markup=keyboard
            )
            context.user_data['last_task_message_id'] = message_id

    except Exception as e:
        print(f"❌ Ошибка отправки в ЗС группу: {e}")
        raise

def add_worked_time(context, user_id: int, task_duration_seconds: int):
    """Добавляет время работы к общему времени пользователя"""
    try:
        # Получаем текущее время работы
        current_worked_seconds = context.user_data.get('worked_seconds', 0)
        # Добавляем время выполненного задания
        new_worked_seconds = current_worked_seconds + task_duration_seconds
        # Обновляем в контексте
        context.user_data['worked_seconds'] = new_worked_seconds
        
        print(f"⏰ Обновлено время работы для user_id={user_id}: {current_worked_seconds}s + {task_duration_seconds}s = {new_worked_seconds}s")
        return new_worked_seconds
    except Exception as e:
        print(f"❌ Ошибка обновления времени работы: {e}")
        return 0

def get_total_worked_time_from_db(staff_id: str, shift: str = None) -> int:
    """Получает общее время работы из БД по завершенным заданиям"""
    from ..database.sql_client import SQL
    from ..config.settings import MERCHANT_ID
    from .time_utils import get_task_date
    import pandas as pd
    
    try:
        # Определяем дату для поиска заданий
        if shift == 'night':
            task_date = get_task_date(shift)  # Для ночной смены берем следующую дату
        else:
            task_date = pd.to_datetime('today').date()  # Для дневной смены - сегодня
        
        # Получаем все завершенные задания за нужную дату
        completed_tasks_df = SQL.sql_select('wms', f"""
            SELECT task_duration 
            FROM wms_bot.shift_tasks
            WHERE user_id = '{staff_id}'
              AND status = 'Проверено'
              AND task_date = '{task_date}'
              AND time_end IS NOT NULL
              AND merchant_code = '{MERCHANT_ID}'
        """)
        
        total_seconds = 0
        for _, row in completed_tasks_df.iterrows():
            duration_raw = row['task_duration']
            
            if isinstance(duration_raw, str):
                # Парсим строку времени
                time_parts = duration_raw.split(':')
                if len(time_parts) >= 2:
                    hours = int(time_parts[0])
                    minutes = int(time_parts[1])
                    seconds = int(time_parts[2]) if len(time_parts) > 2 else 0
                    total_seconds += hours * 3600 + minutes * 60 + seconds
            elif hasattr(duration_raw, 'hour'):
                # Это объект time
                total_seconds += duration_raw.hour * 3600 + duration_raw.minute * 60 + duration_raw.second
            else:
                # Пробуем парсить как время
                try:
                    from datetime import datetime
                    t = datetime.strptime(str(duration_raw), '%H:%M:%S')
                    total_seconds += t.hour * 3600 + t.minute * 60 + t.second
                except:
                    # Если не получается, используем дефолт 15 минут
                    total_seconds += 900
        
        print(f"⏰ Общее время из БД для staff_id={staff_id} (shift={shift}, date={task_date}): {total_seconds} секунд")
        return total_seconds
        
    except Exception as e:
        print(f"❌ Ошибка получения времени из БД: {e}")
        return 0

def parse_task_duration(duration_raw) -> int:
    """Парсит время задания в секунды"""
    try:
        if isinstance(duration_raw, str):
            # Парсим строку времени
            time_parts = duration_raw.split(':')
            if len(time_parts) >= 2:
                hours = int(time_parts[0])
                minutes = int(time_parts[1])
                seconds = int(time_parts[2]) if len(time_parts) > 2 else 0
                return hours * 3600 + minutes * 60 + seconds
        elif hasattr(duration_raw, 'hour'):
            # Это объект time
            return duration_raw.hour * 3600 + duration_raw.minute * 60 + duration_raw.second
        else:
            # Пробуем парсить как время
            try:
                from datetime import datetime
                t = datetime.strptime(str(duration_raw), '%H:%M:%S')
                return t.hour * 3600 + t.minute * 60 + t.second
            except:
                # Если не получается, используем дефолт 15 минут
                return 900
    except Exception as e:
        print(f"❌ Ошибка парсинга времени: {e}")
        return 900  # дефолт 15 минут

def get_task_remaining_time(task_id, task_duration):
    """
    Получает выделенное время для задания с учетом возможной заморозки.
    
    Args:
        task_id: ID задания
        task_duration: Строка с длительностью задания (например, "15 мин")
        
    Returns:
        tuple: (allocated_seconds, elapsed_seconds) - выделенное время в секундах и прошедшее время
    """
    # Получаем полную длительность задания
    full_duration = parse_task_duration(task_duration)
    
    # По умолчанию используем полную длительность
    allocated_seconds = full_duration
    elapsed_seconds = 0
    
    # Если есть информация о замороженном задании, используем ее
    if task_id in frozen_tasks_info:
        # Используем сохраненное выделенное время (allocated_seconds), а не remaining_seconds
        allocated_seconds = frozen_tasks_info[task_id].get('allocated_seconds', full_duration)
        elapsed_seconds = frozen_tasks_info[task_id].get('elapsed_seconds', 0)
        print(f"🔧 [DEBUG] Восстановление из frozen_tasks_info для задания {task_id}: allocated={allocated_seconds}s, elapsed={elapsed_seconds}s")
    
    return allocated_seconds, elapsed_seconds

def format_task_time_info(total_seconds, elapsed_seconds):
    """
    Форматирует информацию о времени задания для отображения.
    
    Args:
        total_seconds: Оставшееся время в секундах
        elapsed_seconds: Прошедшее время в секундах
        
    Returns:
        tuple: (remaining_time_str, elapsed_info) - строка с оставшимся временем и информация о прошедшем времени
    """
    # Форматируем оставшееся время
    remaining_time = str(timedelta(seconds=total_seconds)).split('.')[0]
    
    # Если прошедшее время больше 0, готовим информацию о нем
    elapsed_info = ""
    if elapsed_seconds > 0:
        elapsed_time = str(timedelta(seconds=elapsed_seconds)).split('.')[0]
        elapsed_info = f"\n⏱ Уже затрачено: {elapsed_time}"
    
    return remaining_time, elapsed_info