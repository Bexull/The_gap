import pandas as pd
from datetime import datetime, timedelta
from telegram import Update
from telegram.ext import CallbackContext
from ...database.sql_client import SQL
from ...config.settings import MERCHANT_ID
from ...keyboards.opv_keyboards import get_special_task_keyboard
from .task_timer import stop_timer


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
            from ...utils.freeze_time_utils import update_freeze_time_on_pause
            
            # Для каждого активного задания обновляем freeze_time и ставим статус "Заморожено"
            for _, task in active_tasks_df.iterrows():
                task_id = int(task['id'])
                
                # Обновляем freeze_time (накапливаем время текущей сессии и сбрасываем time_begin)
                update_freeze_time_on_pause(task_id)
                
                # Обновляем статус
                SQL.sql_delete('wms', f"""
                    UPDATE wms_bot.shift_tasks
                    SET status = 'Заморожено'
                    WHERE id = {task_id}
                """)
                
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
                from ...utils.freeze_time_utils import update_freeze_time_on_pause
                
                # Получаем оставшиеся задания
                remaining_tasks_df = SQL.sql_select('wms', f"""
                    SELECT id
                    FROM wms_bot.shift_tasks
                    WHERE user_id = '{staff_id}'
                      AND status IN ('Выполняется', 'На доработке')
                      AND merchant_code = '{MERCHANT_ID}'
                """)
                
                # Обрабатываем каждое задание
                for _, task in remaining_tasks_df.iterrows():
                    task_id = int(task['id'])
                    
                    # Обновляем freeze_time (накапливаем время и сбрасываем time_begin)
                    update_freeze_time_on_pause(task_id)
                
                # Обновляем статус в БД
                freeze_query = f"""
                    UPDATE wms_bot.shift_tasks
                    SET status = 'Заморожено'
                    WHERE user_id = '{staff_id}'
                      AND status IN ('Выполняется', 'На доработке')
                      AND merchant_code = '{MERCHANT_ID}'
                """
                
                SQL.sql_delete('wms', freeze_query)
            
            # 3. Останавливаем таймеры и отправляем уведомления о заморозке
            for _, task in active_tasks_df.iterrows():
                task_id = int(task['id'])
                task_name = task.get('task_name', 'Неизвестное задание')
                
                # Останавливаем таймер если он есть
                try:
                    from ...config.settings import active_timers
                    if task_id in active_timers:
                        await stop_timer(task_id)
                except Exception as e:
                    pass
                
                # Отправляем уведомление о заморозке ВСЕГДА
                try:
                    opv_userid_df = SQL.sql_select('wms', f"""
                        SELECT userid FROM wms_bot.bot_auth WHERE employee_id = '{staff_id}'
                    """)
                    
                    if not opv_userid_df.empty:
                        opv_user_id = int(opv_userid_df.iloc[0]['userid'])
                        
                        await context.bot.send_message(
                            chat_id=opv_user_id,
                            text=(
                                f"❄️ *Задание заморожено!*\n\n"
                                f"📝 *Наименование:* {task_name}\n"
                                f"📋 *ID задания:* {task_id}\n\n"
                                f"Ваше задание было заморожено из-за назначения спец-задания.\n"
                                f"Вы сможете продолжить его после завершения спец-задания."
                            ),
                            parse_mode='Markdown'
                        )
                except Exception as notify_error:
                    pass
                
                frozen_tasks_list.append({
                    'id': task['id'],
                    'name': task['task_name'],
                    'status': task['status']
                })
        
        # 5. Назначаем спец-задание
        now = datetime.now()
        now_str = now.strftime('%Y-%m-%d %H:%M:%S')
        
        # Получаем ФИО оператора из БД
        operator_name_df = SQL.sql_select('wms', f"SELECT concat(name, ' ', surname) AS fio FROM wms_bot.t_staff WHERE id = '{staff_id}'")
        operator_full_name = operator_name_df.iloc[0]['fio'] if not operator_name_df.empty else 'ОПВ'
        
        # Получаем тип занятости от замороженного задания (наследуем)
        employment_type = 'main'  # По умолчанию основная смена
        try:
            # Ищем замороженное задание этого пользователя и берем его part_time
            frozen_task_df = SQL.sql_select('wms', f"""
                SELECT part_time 
                FROM wms_bot.shift_tasks 
                WHERE user_id = '{staff_id}' 
                  AND status IN ('Заморожено', 'На доработке')
                  AND merchant_code = '{MERCHANT_ID}'
                  AND part_time IS NOT NULL
                ORDER BY time_begin DESC LIMIT 1
            """)
            if hasattr(frozen_task_df, 'empty') and not frozen_task_df.empty and len(frozen_task_df) > 0:
                employment_type = frozen_task_df.iloc[0]['part_time'] or 'main'
            else:
                pass
        except Exception as e:
            pass
        
        assign_query = f"""
            UPDATE wms_bot.shift_tasks
            SET status = 'Выполняется',
                user_id = '{staff_id}',
                time_begin = '{now_str}',
                freeze_time = '00:00:00',
                part_time = '{employment_type}',
                operator_name = '{operator_full_name}'
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
