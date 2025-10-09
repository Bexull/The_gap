import pandas as pd
from datetime import datetime
from ..database.sql_client import SQL
from ..config.settings import MERCHANT_ID

def get_free_opv_for_special_tasks():
    """Получает список свободных ОПВ для спец-заданий"""
    try:
        now = datetime.now()
        
        # Определяем смену
        shift_ru = 'День' if 8 <= now.hour < 20 else 'Ночь'
        shift_en = 'day' if 8 <= now.hour < 20 else 'night'
        
        # Получаем свободных ОПВ
        print(f"🔍 Поиск свободных ОПВ для смены: {shift_ru} ({shift_en})")
        
        # Сначала проверим, есть ли вообще активные сессии
        all_sessions_df = SQL.sql_select('wms', f"""
            SELECT ss.employee_id, ss.role, ss.shift_type, ss.end_time, bs.gender, concat(bs."name", ' ', bs.surname) AS fio
            FROM wms_bot.shift_sessions1 ss
            LEFT JOIN wms_bot.t_staff bs ON bs.id = ss.employee_id::int
            WHERE ss.start_time::date = current_date
        """)
        print(f"📊 Всего сессий за сегодня: {len(all_sessions_df)}")
        if not all_sessions_df.empty:
            print(f"👥 Все сессии: {all_sessions_df[['employee_id', 'role', 'shift_type', 'end_time']].to_string()}")
        
        # Логируем запрос для отладки
        query_sql = f"""
            SELECT DISTINCT ss.employee_id, bs.gender, concat(bs."name", ' ', bs.surname) AS fio, ba.userid
            FROM wms_bot.shift_sessions1 ss
            JOIN wms_bot.t_staff bs ON bs.id = ss.employee_id::int
            JOIN wms_bot.bot_auth ba ON ba.employee_id = ss.employee_id
            WHERE ss.end_time IS NULL
              AND ss.start_time::date = current_date
              AND ss.role = 'opv'
              AND ss.shift_type = '{shift_en}'
              AND ss.merchantid = {MERCHANT_ID}
              AND NOT EXISTS (
                  SELECT 1
                  FROM wms_bot.shift_tasks st
                  WHERE st.user_id = ss.employee_id::int
                    AND st.status = 'Выполняется'
                    AND st.is_constant_task = false
                    AND st.priority = 111
                    AND st.time_end IS null
                    AND st.merchant_code = '{MERCHANT_ID}')
        """
        print(f"🔍 SQL запрос для поиска ОПВ:")
        print(f"📝 {query_sql}")
        
        opv_df = SQL.sql_select('wms', query_sql)
        
        print(f"📊 Найдено свободных ОПВ: {len(opv_df)}")
        if not opv_df.empty:
            print(f"👥 Свободные ОПВ: {', '.join(opv_df['fio'].tolist())}")
        
        return opv_df, shift_ru, shift_en
        
    except Exception as e:
        print(f"❌ Ошибка при получении свободных ОПВ: {e}")
        return pd.DataFrame(), None, None

def get_busy_opv_for_special_tasks():
    """Получает список занятых ОПВ (выполняющих спец-задания)"""
    try:
        now = datetime.now()
        
        # Определяем смену
        shift_ru = 'День' if 8 <= now.hour < 20 else 'Ночь'
        shift_en = 'day' if 8 <= now.hour < 20 else 'night'
        
        # Получаем занятых ОПВ
        busy_df = SQL.sql_select('wms', f"""
            SELECT DISTINCT ss.employee_id, bs.gender, concat(bs."name", ' ', bs.surname) AS fio, 
                   st.task_name, st.time_begin, st.id as task_id
            FROM wms_bot.shift_sessions1 ss
            JOIN wms_bot.t_staff bs ON bs.id = ss.employee_id::int
            JOIN wms_bot.shift_tasks st ON st.user_id = ss.employee_id::int
            WHERE ss.end_time IS NULL
              AND ss.start_time::date = current_date
              AND ss.role = 'opv'
              AND ss.shift_type = '{shift_en}'
              AND ss.merchantid = {MERCHANT_ID}
              AND st.status = 'Выполняется'
              AND st.is_constant_task = false
              AND st.priority = 111
              AND st.time_end IS null
              AND st.merchant_code = '{MERCHANT_ID}'
        """)
        
        return busy_df, shift_ru, shift_en
        
    except Exception as e:
        print(f"❌ Ошибка при получении занятых ОПВ: {e}")
        return pd.DataFrame(), None, None

async def force_assign_tasks_by_time(context, start_time_str):
    """Принудительно назначает задания по указанному времени"""
    try:
        from datetime import datetime
        now = datetime.now()
        today = pd.to_datetime('today').date()
        
        # Определяем смену
        shift_ru = 'День' if 8 <= now.hour < 20 else 'Ночь'
        shift_en = 'day' if 8 <= now.hour < 20 else 'night'
        
        # Получаем задания на указанное время
        schedule_df = SQL.sql_select('wms', f"""
            SELECT * FROM wms_bot.shift_tasks
            WHERE task_date = '{today}'
              AND shift = '{shift_ru}'
              AND is_constant_task = false
              AND status = 'В ожидании'
              AND merchant_code = '{MERCHANT_ID}'
              AND start_time::time = '{start_time_str}'
        """)
        
        if schedule_df.empty:
            return f"❌ Нет заданий на время {start_time_str}"
        
        
        results = []
        
        # Обрабатываем все задания на указанное время
        for _, task_row in schedule_df.iterrows():
            # Подсчитываем количество дублей
            start_time_full = task_row['start_time'].strftime('%Y-%m-%d %H:%M:%S') if pd.notnull(task_row['start_time']) else None
            task_name_escaped = str(task_row['task_name']).replace("'", "''")
            
            duplicates_df = SQL.sql_select('wms', f"""
                SELECT COUNT(*) as task_count
                FROM wms_bot.shift_tasks
                WHERE task_date = '{today}'
                AND shift = '{shift_ru}'
                AND is_constant_task = false
                AND status = 'В ожидании'
                AND start_time = '{start_time_full}'
                AND task_name = '{task_name_escaped}'
                AND merchant_code = '{MERCHANT_ID}'
            """)
            
            task_count = int(duplicates_df.iloc[0]['task_count'])
            
            # Получаем свободных ОПВ
            opv_df = SQL.sql_select('wms', f"""
                SELECT DISTINCT ss.employee_id, bs.gender, concat(bs."name", ' ', bs.surname) AS fio, ba.userid
                FROM wms_bot.shift_sessions1 ss
                JOIN wms_bot.t_staff bs ON bs.id = ss.employee_id::int
                JOIN wms_bot.bot_auth ba ON ba.employee_id = ss.employee_id
                WHERE ss.end_time IS NULL
                  AND ss.start_time::date = current_date
                  AND ss.role = 'opv'
                  AND ss.shift_type = '{shift_en}'
                  AND ss.merchantid = {MERCHANT_ID}
                  AND NOT EXISTS (
                      SELECT 1
                      FROM wms_bot.shift_tasks st
                      WHERE st.user_id = ss.employee_id::int
                        AND st.status = 'Выполняется'
                        AND st.is_constant_task = false
                        AND st.priority = 111
                        AND st.time_end IS null
                        AND st.merchant_code = '{MERCHANT_ID}')
            """)
            
            # Фильтрация по полу
            if task_row['gender'] in ['M', 'F']:
                opv_df = opv_df[opv_df['gender'].fillna('U').str.upper() == task_row['gender']]
            
            if opv_df.empty:
                results.append(f"❌ Нет подходящих ОПВ для задания {task_row['task_name']}")
                continue
            
            # Берем нужное количество ОПВ
            selected_opv = opv_df.head(task_count)
            assigned_count = 0
            
            for _, opv in selected_opv.iterrows():
                # Замораживаем активные задания ОПВ
                SQL.sql_delete('wms', f"""
                    UPDATE wms_bot.shift_tasks
                    SET status = 'Заморожено'
                    WHERE user_id = '{opv['employee_id']}'
                    AND status IN ('Выполняется')
                    AND time_end IS NULL
                """)
                
                # Назначаем задание
                task_to_assign_df = SQL.sql_select('wms', f"""
                    SELECT id FROM wms_bot.shift_tasks
                    WHERE task_date = '{today}'
                    AND shift = '{shift_ru}'
                    AND is_constant_task = false
                    AND status = 'В ожидании'
                    AND start_time = '{start_time_full}'
                    AND task_name = '{task_name_escaped}'
                    AND merchant_code = '{MERCHANT_ID}'
                    ORDER BY id LIMIT 1
                """)
                
                if task_to_assign_df.empty:
                    break
                
                task_id = int(task_to_assign_df.iloc[0]['id'])
                now_str = now.strftime('%Y-%m-%d %H:%M:%S')
                
                SQL.sql_delete('wms', f"""
                    UPDATE wms_bot.shift_tasks
                    SET status = 'Выполняется',
                        user_id = '{opv['employee_id']}',
                        time_begin = '{now_str}',
                        operator_name = '{opv['fio']}'
                    WHERE id = {task_id}
                """)
                
                # Отправляем уведомление
                from ..keyboards.opv_keyboards import get_special_task_keyboard
                
                try:
                    if task_row['task_duration'] is not None and pd.notnull(task_row['task_duration']):
                        if hasattr(task_row['task_duration'], 'strftime'):
                            duration = task_row['task_duration'].strftime('%H:%M')
                        else:
                            duration = str(task_row['task_duration'])
                    else:
                        duration = "не указано"
                except Exception as e:
                    print(f"⚠️ Ошибка форматирования task_duration: {e}")
                    duration = "не указано"
                
                chat_id = opv['userid']
                if isinstance(chat_id, pd.Series):
                    chat_id = chat_id.values[0]
                
                await context.bot.send_message(
                    chat_id=int(chat_id),
                    text=(
                        f"📌 *На Вас назначено спец-задание!*\n\n"
                        f"📝 *Наименование:* {task_row['task_name']}\n"
                        f"📦 *Группа:* {task_row['product_group']}\n"
                        f"📍 *Слот:* {task_row['slot']}\n"
                        f"⏰ *Время:* {duration} мин\n\n"
                        f"*Приоритет 111* - задание не требует проверки ЗС"
                    ),
                    parse_mode='Markdown',
                    reply_markup=get_special_task_keyboard()
                )
                
                assigned_count += 1
                results.append(f"✅ Назначено задание {task_row['task_name']} ({task_id}) для {opv['fio']}")
            
            if assigned_count == 0:
                results.append(f"❌ Не удалось назначить задание {task_row['task_name']}")
        
        # Показываем итоговую статистику
        results.append(f"📊 Всего найдено заданий на {start_time_str}: {len(schedule_df)}")
        results.append(f"✅ Успешно назначено: {assigned_count}")
        
        return "\n".join(results)
        
    except Exception as e:
        return f"❌ Ошибка при принудительном назначении: {e}"
