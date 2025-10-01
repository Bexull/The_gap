import pandas as pd
from datetime import datetime, timedelta
from telegram import Update
from telegram.ext import CallbackContext
from ...database.sql_client import SQL
from ...config.settings import MERCHANT_ID
from ...keyboards.opv_keyboards import get_special_task_keyboard
from .task_timer import stop_timer_for_task


async def handle_special_task_assignment(staff_id: str, special_task_id: int, context: CallbackContext = None):
    """
    Универсальная функция для автоматической обработки назначения спец-задания
    
    Args:
        staff_id: ID сотрудника
        special_task_id: ID спец-задания
        context: Контекст для отправки уведомлений (опционально)
    
    Returns:
        dict: Результат обработки с информацией о замороженных заданиях
    """
    try:
        # 1. Проверяем активные задания пользователя
        active_tasks_query = f"""
            SELECT id, task_name, status
            FROM wms_bot.shift_tasks
            WHERE user_id = '{staff_id}'
              AND status IN ('Выполняется', 'На доработке')
              AND merchant_code = '{MERCHANT_ID}'
        """
        
        active_tasks_df = SQL.sql_select('wms', active_tasks_query)
        
        frozen_tasks_list = []
        
        if not active_tasks_df.empty:
            # 2. Замораживаем активные задания
            from ...config.settings import frozen_tasks_info
            now = datetime.now()
            now_str = now.strftime('%Y-%m-%d %H:%M:%S')
            
            # Для каждого активного задания сохраняем время заморозки и вычисляем прошедшее время
            for _, task in active_tasks_df.iterrows():
                task_id = int(task['id'])
                
                # Получаем время начала задания
                task_time_info = SQL.sql_select('wms', f"""
                    SELECT time_begin, task_duration
                    FROM wms_bot.shift_tasks
                    WHERE id = {task_id}
                """)
                
                if not task_time_info.empty:
                    time_begin = task_time_info.iloc[0]['time_begin']
                    
                    # Вычисляем прошедшее время в секундах
                    elapsed_seconds = 0
                    original_start_time = now  # По умолчанию текущее время
                    
                    if time_begin:
                        try:
                            # Преобразуем строку в datetime
                            if isinstance(time_begin, str):
                                time_begin = datetime.strptime(time_begin, '%Y-%m-%d %H:%M:%S')
                            
                            # Если это time объект, преобразуем его в datetime
                            if hasattr(time_begin, 'hour') and not hasattr(time_begin, 'year'):
                                # Это объект time, преобразуем его в datetime
                                today = datetime.today().date()
                                time_begin = datetime.combine(today, time_begin)
                            
                            # Сохраняем оригинальное время начала
                            original_start_time = time_begin
                            
                            # Теперь можем безопасно вычислить разницу
                            elapsed_seconds = int((now - time_begin).total_seconds())
                        except Exception as e:
                            print(f"Ошибка при вычислении прошедшего времени: {e}")
                            # Используем безопасный метод вычисления
                            if hasattr(time_begin, 'hour'):
                                # Если это time объект
                                current_time = now.time()
                                # Вычисляем разницу в секундах
                                elapsed_seconds = (current_time.hour - time_begin.hour) * 3600 + \
                                                 (current_time.minute - time_begin.minute) * 60 + \
                                                 (current_time.second - time_begin.second)
                                # Если получилось отрицательное значение, значит прошли сутки
                                if elapsed_seconds < 0:
                                    elapsed_seconds += 24 * 3600
                    
                    # Получаем информацию о задании для расчета оставшегося времени
                    task_duration = task_time_info.iloc[0]['task_duration']
                    full_duration = 0
                    
                    # Парсим время выполнения
                    try:
                        from ...utils.task_utils import parse_task_duration
                        full_duration = parse_task_duration(task_duration)
                    except Exception as e:
                        full_duration = 900  # По умолчанию 15 минут
                    
                    # Вычисляем оставшееся время
                    remaining_seconds = max(0, full_duration - elapsed_seconds)
                    
                    # Сохраняем информацию о замороженном задании в глобальное хранилище
                    frozen_tasks_info[task_id] = {
                        'freeze_time': now,
                        'elapsed_seconds': elapsed_seconds,
                        'remaining_seconds': remaining_seconds,
                        'allocated_seconds': int(elapsed_seconds + remaining_seconds),
                        'original_start_time': original_start_time  # ИСПРАВЛЕНИЕ: сохраняем оригинальное время начала
                    }
                    
                    # Обновляем только статус в БД
                    freeze_query = f"""
                        UPDATE wms_bot.shift_tasks
                        SET status = 'Заморожено'
                        WHERE id = {task_id}
                    """
                    
                    SQL.sql_delete('wms', freeze_query)
                
            # Если по какой-то причине не удалось обработать задания по одному, делаем общее обновление
            check_query = f"""
                SELECT COUNT(*) as count
                FROM wms_bot.shift_tasks
                WHERE user_id = '{staff_id}'
                  AND status IN ('Выполняется', 'На доработке')
                  AND merchant_code = '{MERCHANT_ID}'
            """
            
            remaining_tasks = SQL.sql_select('wms', check_query)
            if not remaining_tasks.empty and remaining_tasks.iloc[0]['count'] > 0:
                # Получаем оставшиеся задания
                remaining_tasks_df = SQL.sql_select('wms', f"""
                    SELECT id, time_begin
                    FROM wms_bot.shift_tasks
                    WHERE user_id = '{staff_id}'
                      AND status IN ('Выполняется', 'На доработке')
                      AND merchant_code = '{MERCHANT_ID}'
                """)
                
                # Обрабатываем каждое задание
                for _, task in remaining_tasks_df.iterrows():
                    task_id = int(task['id'])
                    time_begin = task['time_begin']
                    
                    # Вычисляем прошедшее время
                    elapsed_seconds = 0
                    if time_begin:
                        try:
                            # Преобразуем строку в datetime
                            if isinstance(time_begin, str):
                                time_begin = datetime.strptime(time_begin, '%Y-%m-%d %H:%M:%S')
                            
                            # Если это time объект, преобразуем его в datetime
                            if hasattr(time_begin, 'hour') and not hasattr(time_begin, 'year'):
                                # Это объект time, преобразуем его в datetime
                                today = datetime.today().date()
                                time_begin = datetime.combine(today, time_begin)
                            
                            # Теперь можем безопасно вычислить разницу
                            elapsed_seconds = int((now - time_begin).total_seconds())
                        except Exception as e:
                            print(f"Ошибка при вычислении прошедшего времени в remaining_tasks: {e}")
                            # Используем безопасный метод вычисления
                            if hasattr(time_begin, 'hour'):
                                # Если это time объект
                                current_time = now.time()
                                # Вычисляем разницу в секундах
                                elapsed_seconds = (current_time.hour - time_begin.hour) * 3600 + \
                                                 (current_time.minute - time_begin.minute) * 60 + \
                                                 (current_time.second - time_begin.second)
                                # Если получилось отрицательное значение, значит прошли сутки
                                if elapsed_seconds < 0:
                                    elapsed_seconds += 24 * 3600
                    
                    # Получаем информацию о задании для расчета оставшегося времени
                    task_info = SQL.sql_select('wms', f"""
                        SELECT task_duration FROM wms_bot.shift_tasks WHERE id = {task_id}
                    """)
                    
                    full_duration = 0
                    if not task_info.empty:
                        # Парсим время выполнения
                        try:
                            from ...utils.task_utils import parse_task_duration
                            full_duration = parse_task_duration(task_info.iloc[0]['task_duration'])
                        except Exception as e:
                            full_duration = 900  # По умолчанию 15 минут
                    
                    # Вычисляем оставшееся время
                    remaining_seconds = max(0, full_duration - elapsed_seconds)
                    
                    # Сохраняем информацию в глобальное хранилище
                    frozen_tasks_info[task_id] = {
                        'freeze_time': now,
                        'elapsed_seconds': elapsed_seconds,
                        'original_start_time': time_begin if isinstance(time_begin, datetime) else now - timedelta(seconds=elapsed_seconds),
                        'remaining_seconds': remaining_seconds,
                        'allocated_seconds': int(elapsed_seconds + remaining_seconds)
                    }
                
                # Обновляем статус в БД
                freeze_query = f"""
                    UPDATE wms_bot.shift_tasks
                    SET status = 'Заморожено'
                    WHERE user_id = '{staff_id}'
                      AND status IN ('Выполняется', 'На доработке')
                      AND merchant_code = '{MERCHANT_ID}'
                """
                
                SQL.sql_delete('wms', freeze_query)
            
            # 3. Останавливаем таймеры для замороженных заданий
            for _, task in active_tasks_df.iterrows():
                task_id = int(task['id'])
                try:
                    await stop_timer_for_task(task_id, context, "задание заморожено из-за спец-задания")
                except Exception as e:
                    pass
                
                frozen_tasks_list.append({
                    'id': task['id'],
                    'name': task['task_name'],
                    'status': task['status']
                })
            
            # 4. Уведомление о заморозке убрано - показываем только назначение спец-задания
        
        # 5. Назначаем спец-задание
        now = datetime.now()
        now_str = now.strftime('%Y-%m-%d %H:%M:%S')
        
        assign_query = f"""
            UPDATE wms_bot.shift_tasks
            SET status = 'Выполняется',
                user_id = '{staff_id}',
                time_begin = '{now_str}'
            WHERE id = {special_task_id}
        """
        
        SQL.sql_delete('wms', assign_query)
        
        # 6. Получаем информацию о спец-задании для уведомления
        special_task_info = get_special_task_info(special_task_id)
        
        # 7. Отправляем уведомление о спец-задании (если передан контекст)
        if context and special_task_info:
            await send_special_task_notification(context, special_task_info)
        
        return {
            'success': True,
            'frozen_tasks': frozen_tasks_list,
            'special_task': special_task_info
        }
        
    except Exception as e:
        return {
            'success': False,
            'error': str(e),
            'frozen_tasks': [],
            'special_task': None
        }


async def send_freeze_notification(context: CallbackContext, frozen_tasks_info: list):
    """Отправляет уведомление о заморозке заданий"""
    try:
        if not frozen_tasks_info:
            return
        
        # Получаем chat_id пользователя
        staff_id = context.user_data.get('staff_id')
        if not staff_id:
            return
        
        opv_userid_df = SQL.sql_select('wms', f"""
            SELECT userid FROM wms_bot.bot_auth WHERE employee_id = '{staff_id}'
        """)
        
        if opv_userid_df.empty:
            return
        
        opv_user_id = int(opv_userid_df.iloc[0]['userid'])
        
        # Формируем список замороженных заданий
        tasks_list = []
        for task in frozen_tasks_info:
            tasks_list.append(f"• Задание №{task['id']}: {task['name']}")
        
        freeze_message = (
            f"❄️ *Ваши текущие задания заморожены*\n\n"
            f"{chr(10).join(tasks_list)}\n\n"
            f"⏸️ Завершите спец-задание, чтобы продолжить работу.\n"
            f"⏰ Время начнется с момента завершения спец-задания."
        )
        
        await context.bot.send_message(
            chat_id=opv_user_id,
            text=freeze_message,
            parse_mode='Markdown'
        )
        
    except Exception as e:
        pass


async def send_special_task_notification(context: CallbackContext, special_task_info: dict):
    """Отправляет уведомление о назначении спец-задания"""
    try:
        # Получаем chat_id пользователя
        staff_id = context.user_data.get('staff_id')
        if not staff_id:
            return
        
        opv_userid_df = SQL.sql_select('wms', f"""
            SELECT userid FROM wms_bot.bot_auth WHERE employee_id = '{staff_id}'
        """)
        
        if opv_userid_df.empty:
            return
        
        opv_user_id = int(opv_userid_df.iloc[0]['userid'])
        
        # Формируем сообщение о спец-задании
        duration = special_task_info.get('task_duration', 'не указано')
        if duration and hasattr(duration, 'strftime'):
            duration = duration.strftime('%H:%M')
        
        message_text = (
            f"📌 *На Вас назначено спец-задание!*\n\n"
            f"📝 *Наименование:* {special_task_info.get('task_name', 'Не указано')}\n"
            f"📦 *Группа:* {special_task_info.get('product_group', 'Не указано')}\n"
            f"📍 *Слот:* {special_task_info.get('slot', 'Не указано')}\n"
            f"⏰ *Время:* {duration} мин\n\n"
            f"*Приоритет 111* - задание не требует проверки ЗС"
        )
        
        await context.bot.send_message(
            chat_id=opv_user_id,
            text=message_text,
            parse_mode='Markdown',
            reply_markup=get_special_task_keyboard()
        )
        
    except Exception as e:
        pass


def get_special_task_info(special_task_id: int) -> dict:
    """Получает информацию о спец-задании"""
    try:
        task_df = SQL.sql_select('wms', f"""
            SELECT id, task_name, product_group, slot, task_duration
            FROM wms_bot.shift_tasks
            WHERE id = {special_task_id}
        """)
        
        if not task_df.empty:
            return task_df.iloc[0].to_dict()
        return {}
        
    except Exception as e:
        return {}


async def auto_assign_special_task(staff_id: str, context: CallbackContext = None):
    """
    Автоматически находит и назначает спец-задание пользователю
    
    Args:
        staff_id: ID сотрудника
        context: Контекст для отправки уведомлений (опционально)
    
    Returns:
        dict: Результат назначения
    """
    try:
        # Ищем доступное спец-задание
        special_task_query = f"""
            SELECT id, task_name, product_group, slot, task_duration, gender
            FROM wms_bot.shift_tasks
            WHERE priority = '111'
              AND status = 'В ожидании'
              AND merchant_code = '{MERCHANT_ID}'
            LIMIT 1
        """
        
        special_task_df = SQL.sql_select('wms', special_task_query)
        
        if special_task_df.empty:
            return {
                'success': False,
                'error': 'Нет доступных спец-заданий с приоритетом 111',
                'frozen_tasks': [],
                'special_task': None
            }
        
        special_task = special_task_df.iloc[0]
        special_task_id = special_task['id']
        
        # Используем универсальную функцию обработки
        return await handle_special_task_assignment(staff_id, special_task_id, context)
        
    except Exception as e:
        return {
            'success': False,
            'error': str(e),
            'frozen_tasks': [],
            'special_task': None
        }
