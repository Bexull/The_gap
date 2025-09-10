import pandas as pd
from datetime import datetime, timedelta, time
from ..database.sql_client import SQL
from ..config.settings import ZS_GROUP_CHAT_ID, MERCHANT_ID

async def schedule_tasks_from_rules(context):
    """Проверяет расписание и назначает задания из shift_tasks с is_constant_task = false"""
    try:
        now = datetime.now()
        current_time = now.strftime('%H:%M')
        today = pd.to_datetime('today').date()

        # Определяем смену
        shift_ru = 'День' if 8 <= now.hour < 20 else 'Ночь'
        shift_en = 'day' if 8 <= now.hour < 20 else 'night'

        # Получаем задания на текущее время, дату и смену
        schedule_df = SQL.sql_select('wms', f"""
            SELECT * FROM wms_bot.shift_tasks
            WHERE task_date = '{today}'
              AND shift = '{shift_ru}'
              AND is_constant_task = false
              AND status = 'В ожидании'
              AND merchant_code = '{MERCHANT_ID}'
        """)

        if schedule_df.empty:
            print("📭 Нет заданий по расписанию")
            return

        # Фильтрация по текущему времени
        schedule_df['start_time_short'] = schedule_df['start_time'].apply(
        lambda x: x.strftime('%H:%M') if pd.notnull(x) else None
        )

        due_tasks = schedule_df[schedule_df['start_time_short'] == current_time]

        if due_tasks.empty:
            print("📭 Нет заданий на это время")
            return

        print(f"⏰ Сейчас {current_time}, проверяю задания:\n{due_tasks[['start_time_short','task_name','id']]}")

        for _, task_row in due_tasks.iterrows():
            # Подсчитываем количество дублей этой задачи в shift_tasks
            duplicates_df = SQL.sql_select('wms', f"""
                SELECT COUNT(*) as task_count
                FROM wms_bot.shift_tasks
                WHERE task_date = '{today}'
                AND shift = '{shift_ru}'
                AND is_constant_task = false
                AND status = 'В ожидании'
                AND start_time = '{task_row['start_time']}'
                AND task_name = '{task_row['task_name']}'
                AND merchant_code = '{MERCHANT_ID}'
            """)

            task_count = int(duplicates_df.iloc[0]['task_count'])

            # Подбираем ОПВ на смене
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
            AND st.time_end IS null
            AND st.merchant_code = '{MERCHANT_ID}')
            """)

            # Фильтрация по полу
            if task_row['gender'] in ['M', 'F']:
                opv_df = opv_df[opv_df['gender'].fillna('U').str.upper() == task_row['gender']]

            if opv_df.empty:
                print(f"📭 Нет подходящих ОПВ для задания {task_row['id']}")
                continue

            # Берём task_count ОПВ (или меньше если их не хватает)
            selected_opv = opv_df.head(task_count)

            # Назначаем задачу каждому ОПВ из списка
            for _, opv in selected_opv.iterrows():
                # Заморозка активных заданий ОПВ
                SQL.sql_delete('wms', f"""
                    UPDATE wms_bot.shift_tasks
                    SET status = 'Заморожено'
                    WHERE user_id = '{opv['employee_id']}'
                    AND status IN ('Выполняется')
                    AND time_end IS NULL
                """)

                # Назначаем одну задачу из дубликатов
                task_to_assign_df = SQL.sql_select('wms', f"""
                    SELECT id FROM wms_bot.shift_tasks
                    WHERE task_date = '{today}'
                    AND shift = '{shift_ru}'
                    AND is_constant_task = false
                    AND status = 'В ожидании'
                    AND start_time = '{task_row['start_time']}'
                    AND task_name = '{task_row['task_name']}'
                    AND merchant_code = '{MERCHANT_ID}'
                    ORDER BY id LIMIT 1
                """)

                if task_to_assign_df.empty:
                    print(f"❌ Не осталось свободных дублей для задания {task_row['task_name']}")
                    break

                task_id = int(task_to_assign_df.iloc[0]['id'])

                # Обновляем задание
                SQL.sql_delete('wms', f"""
                    UPDATE wms_bot.shift_tasks
                    SET status = 'Выполняется',
                        user_id = '{opv['employee_id']}',
                        time_begin = '{now}'
                    WHERE id = {task_id}
                """)

                if task_row['task_duration'] is not None:
                    duration = task_row['task_duration'].strftime('%H:%M')
                else:
                    duration = "не указано"
                chat_id = opv['userid']
                if isinstance(chat_id, pd.Series):
                    chat_id = chat_id.values[0]

                # Импортируем клавиатуру
                from ..keyboards.opv_keyboards import get_special_task_keyboard
                
                # Отправляем уведомление с кнопкой завершения
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

                print(f"✅ Назначено задание {task_row['task_name']} ({task_id}) для {opv['fio']}")

    except Exception as e:
        print(f"❌ Ошибка в schedule_tasks_from_rules: {e}")

async def auto_close_expired_tasks(context):
    """Автоматически закрывает задания в статусах 'Выполняется' или 'Заморожено'"""
    try:
        now = datetime.now()
        now_str = now.strftime('%Y-%m-%d %H:%M:%S')

        expired_df = SQL.sql_select('wms', f"""
            SELECT id, user_id, task_name, time_begin, status
            FROM wms_bot.shift_tasks
            WHERE status IN ('Выполняется', 'Заморожено','Ожидает проверки') 
              AND task_date IN (current_date, current_date - 1)
              AND merchant_code = '{MERCHANT_ID}'
        """)

        if expired_df.empty:
            print("✅ Нет просроченных заданий на сейчас")
            return

        print(f"⚠️ Найдено просроченных заданий: {len(expired_df)}")

        for _, row in expired_df.iterrows():
            SQL.sql_delete('wms', f"""
                UPDATE wms_bot.shift_tasks
                SET status = 'Проверено',
                    time_end = '{now_str}',
                    inspector_id = 0
                WHERE id = '{row['id']}'
            """)
            print(f"✔️ Закрыто задание ID={row['id']} у user_id={row['user_id']} ({row['task_name']})")

    except Exception as e:
        print(f"❌ Ошибка в auto_close_expired_tasks: {e}")
