import pandas as pd
from datetime import datetime, timedelta, time
from ..database.sql_client import SQL
from ..config.settings import ZS_GROUP_CHAT_ID, MERCHANT_ID

# Кэш для отслеживания заданий без свободных ОПВ
_no_opv_cache = {}

# Глобальный семафор для предотвращения параллельных запусков планировщика
_scheduler_running = False

async def schedule_tasks_from_rules(context):
    """Проверяет расписание и назначает задания из shift_tasks с is_constant_task = false"""
    global _scheduler_running
    
    # Проверка, не запущен ли уже планировщик
    if _scheduler_running:
        print("⚠️ Планировщик уже выполняется. Пропускаем этот запуск.")
        return
        
    # Устанавливаем флаг, что планировщик запущен
    _scheduler_running = True
    
    try:
        now = datetime.now()
        current_time = now.strftime('%H:%M')
        current_time_full = now.strftime('%H:%M:%S')
        today = pd.to_datetime('today').date()
        
        # Определяем смену
        shift_ru = 'День' if 8 <= now.hour < 20 else 'Ночь'
        shift_en = 'day' if 8 <= now.hour < 20 else 'night'
        
        print(f"🔄 Планировщик запущен в {current_time_full} (смена: {shift_ru}, дата: {today})")

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
            print(f"📭 Нет заданий на смену {shift_ru} за {today}")
            return
        
        # Фильтрация по текущему времени с окном в 5 минут
        # Создаем список времен в диапазоне ±5 минут от текущего
        current_hour = now.hour
        current_minute = now.minute
        
        time_window = []
        for offset in range(-5, 6):  # от -5 до +5 минут
            target_minute = current_minute + offset
            target_hour = current_hour
            
            # Обработка перехода через час
            if target_minute < 0:
                target_minute += 60
                target_hour -= 1
                if target_hour < 0:
                    target_hour = 23
            elif target_minute >= 60:
                target_minute -= 60
                target_hour += 1
                if target_hour >= 24:
                    target_hour = 0
            
            time_window.append(f"{target_hour:02d}:{target_minute:02d}")
        
        # Фильтруем задания в окне времени
        schedule_df['start_time_short'] = schedule_df['start_time'].apply(
            lambda x: x.strftime('%H:%M') if pd.notnull(x) else None
        )
        
        # Показываем, какие времена попадают в окно
        unique_times = schedule_df['start_time_short'].dropna().unique()
        times_in_window = [t for t in unique_times if t in time_window]
        if times_in_window:
            print(f"⏰ Окно: {time_window[0]}-{time_window[-1]} | Времена в окне: {times_in_window}")

        due_tasks = schedule_df[schedule_df['start_time_short'].isin(time_window)]

        if due_tasks.empty:
            return
        
        # Убираем дубликаты заданий по названию и времени
        due_tasks = due_tasks.drop_duplicates(subset=['task_name', 'start_time_short'])
        print(f"📋 Уникальных заданий для обработки: {len(due_tasks)}")

        total_assigned = 0
        assigned_opv_ids = set()  # Отслеживаем ОПВ, которым уже назначили задания в этой итерации
        
        for _, task_row in due_tasks.iterrows():
            # Безопасное получение названия задания
            task_name = task_row.get('task_name', 'Неизвестное задание')
            task_time = task_row.get('start_time_short', 'Неизвестно')
            
            # Создаем ключ для кэша
            cache_key = f"{task_name}_{task_time}_{shift_ru}"
            
            # Проверяем кэш - если недавно не было ОПВ, пропускаем
            if cache_key in _no_opv_cache:
                last_check = _no_opv_cache[cache_key]
                if (now - last_check).total_seconds() < 180:  # 3 минуты
                    print(f"\n⏭️ Пропускаем '{task_name}' - недавно не было ОПВ (кэш)")
                    continue
            
            print(f"\n🎯 Обрабатываем задание: {task_name}")
            
            # Подсчитываем количество дублей этой задачи в shift_tasks
            start_time_str = task_row['start_time'].strftime('%Y-%m-%d %H:%M:%S') if pd.notnull(task_row['start_time']) else None
            task_name_escaped = str(task_name).replace("'", "''")  # Экранируем апострофы
            duplicates_df = SQL.sql_select('wms', f"""
                SELECT COUNT(*) as task_count
                FROM wms_bot.shift_tasks
                WHERE task_date = '{today}'
                AND shift = '{shift_ru}'
                AND is_constant_task = false
                AND status = 'В ожидании'
                AND start_time = '{start_time_str}'
                AND task_name = '{task_name_escaped}'
                AND merchant_code = '{MERCHANT_ID}'
            """)

            task_count = int(duplicates_df.iloc[0]['task_count'])
            print(f"  📊 Найдено {task_count} дублей задания")

            # Подбираем ОПВ на смене
            # Для спец-заданий ищем ОПВ, у которых НЕТ других спец-заданий (priority='111') в статусе 'Выполняется'
            # Обычные задания (is_constant_task = true) НЕ блокируют назначение спец-заданий
            # ИСПРАВЛЕНИЕ: исключаем ОПВ, которым уже назначили задания в текущей итерации
            assigned_opv_filter = ""
            if assigned_opv_ids:
                assigned_ids_str = ','.join([f"'{opv_id}'" for opv_id in assigned_opv_ids])
                assigned_opv_filter = f"AND ss.employee_id NOT IN ({assigned_ids_str})"
            
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
                  {assigned_opv_filter}
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
            
            # Отладочная информация - показываем всех ОПВ на смене (не только свободных)
            all_opv_df = SQL.sql_select('wms', f"""
                SELECT DISTINCT ss.employee_id, bs.gender, concat(bs."name", ' ', bs.surname) AS fio, ba.userid
                FROM wms_bot.shift_sessions1 ss
                JOIN wms_bot.t_staff bs ON bs.id = ss.employee_id::int
                JOIN wms_bot.bot_auth ba ON ba.employee_id = ss.employee_id
                WHERE ss.end_time IS NULL
                  AND ss.start_time::date = current_date
                  AND ss.role = 'opv'
                  AND ss.shift_type = '{shift_en}'
                  AND ss.merchantid = {MERCHANT_ID}
            """)
            
            # Фильтрация по полу
            if task_row['gender'] in ['M', 'F']:
                opv_df = opv_df[opv_df['gender'].fillna('U').str.upper() == task_row['gender']]
                print(f"  👤 После фильтрации по полу ({task_row['gender']}): {len(opv_df)} ОПВ")

            if opv_df.empty:
                # Сохраняем в кэш, чтобы не проверять 5 минут
                _no_opv_cache[cache_key] = now
                continue

            # Берём task_count ОПВ (или меньше если их не хватает)
            selected_opv = opv_df.head(task_count)

            # Назначаем задачу каждому ОПВ из списка
            for idx, opv in selected_opv.iterrows():
                opv_name = opv.get('fio', 'Неизвестный')
                opv_id = opv.get('employee_id', 'Неизвестный')
                
                # Получаем активные задания ОПВ перед заморозкой
                
                # Пробуем оба варианта запросов (строковый и числовой)
                try:
                    # Числовой вариант
                    active_tasks_df = SQL.sql_select('wms', f"""
                        SELECT id, task_name, status FROM wms_bot.shift_tasks
                        WHERE user_id = {int(opv['employee_id'])}
                        AND status IN ('Выполняется')
                        AND time_end IS NULL
                        AND merchant_code = '{MERCHANT_ID}'
                    """)
                except Exception:
                    # Строковый вариант
                    active_tasks_df = SQL.sql_select('wms', f"""
                        SELECT id, task_name, status FROM wms_bot.shift_tasks
                        WHERE user_id = '{opv['employee_id']}'
                        AND status IN ('Выполняется')
                        AND time_end IS NULL
                        AND merchant_code = '{MERCHANT_ID}'
                    """)
                
                # Заморозка активных заданий ОПВ
                if not active_tasks_df.empty:
                    
                    SQL.sql_delete('wms', f"""
                        UPDATE wms_bot.shift_tasks
                        SET status = 'Заморожено'
                        WHERE user_id = '{opv['employee_id']}'
                        AND status IN ('Выполняется')
                        AND time_end IS NULL
                    """)
                
                # Сохраняем информацию о замороженных заданиях для восстановления
                from ..config.settings import frozen_tasks_info
                for _, task_row in active_tasks_df.iterrows():
                    task_id = int(task_row['id'])
                    
                    # Получаем время начала задания для сохранения original_start_time
                    task_time_info = SQL.sql_select('wms', f"""
                        SELECT time_begin, task_duration FROM wms_bot.shift_tasks
                        WHERE id = {task_id}
                    """)
                    
                    if not task_time_info.empty:
                        time_begin = task_time_info.iloc[0]['time_begin']
                        task_duration = task_time_info.iloc[0]['task_duration']
                        
                        # Вычисляем прошедшее и оставшееся время
                        elapsed_seconds = 0
                        original_start_time = now  # По умолчанию текущее время
                        
                        if time_begin:
                            try:
                                if isinstance(time_begin, str):
                                    time_begin = datetime.strptime(time_begin, '%Y-%m-%d %H:%M:%S')
                                elif hasattr(time_begin, 'hour') and not hasattr(time_begin, 'year'):
                                    today = datetime.today().date()
                                    time_begin = datetime.combine(today, time_begin)
                                
                                original_start_time = time_begin
                                elapsed_seconds = int((now - time_begin).total_seconds())
                            except Exception as e:
                                print(f"      ⚠️ Ошибка при вычислении времени: {e}")
                        
                        # Парсим длительность задания
                        try:
                            from ..utils.task_utils import parse_task_duration
                            full_duration = parse_task_duration(task_duration)
                        except Exception:
                            full_duration = 900
                        
                        remaining_seconds = max(0, full_duration - elapsed_seconds)
                        
                        # Сохраняем информацию о замороженном задании
                        frozen_tasks_info[task_id] = {
                            'freeze_time': now,
                            'elapsed_seconds': elapsed_seconds,
                            'remaining_seconds': remaining_seconds,
                            'allocated_seconds': int(elapsed_seconds + remaining_seconds),
                            'original_start_time': original_start_time
                        }
                
                # Останавливаем таймеры и отправляем уведомления о заморозке
                for _, task_row in active_tasks_df.iterrows():
                    task_id = int(task_row['id'])
                    task_name = task_row.get('task_name', 'Неизвестное задание')
                    
                    try:
                        # Проверяем существование таймера для задания
                        from ..config.settings import active_timers
                        if task_id in active_timers:
                            # Останавливаем таймер если он есть
                            from ..handlers.task_handlers import stop_timer_for_task
                            await stop_timer_for_task(task_id, context, "задание заморожено из-за спец-задания")
                            
                        # Отправляем прямое уведомление в любом случае
                        chat_id = opv['userid']
                        if isinstance(chat_id, pd.Series):
                            chat_id = chat_id.values[0]
                            
                        await context.bot.send_message(
                            chat_id=int(chat_id),
                            text=(
                                f"❄️ *Задание заморожено!*\n\n"
                                f"📝 *Наименование:* {task_name}\n"
                                f"📋 *ID задания:* {task_id}\n\n"
                                f"Ваше задание было заморожено из-за назначения спец-задания.\n"
                                f"Вы сможете продолжить его после завершения спец-задания."
                            ),
                            parse_mode='Markdown'
                        )
                    except Exception:
                        pass

                # Назначаем одну задачу из дубликатов с блокировкой строки (FOR UPDATE)
                task_to_assign_df = SQL.sql_select('wms', f"""
                    SELECT id FROM wms_bot.shift_tasks
                    WHERE task_date = '{today}'
                    AND shift = '{shift_ru}'
                    AND is_constant_task = false
                    AND status = 'В ожидании'
                    AND start_time = '{start_time_str}'
                    AND task_name = '{task_name_escaped}'
                    AND merchant_code = '{MERCHANT_ID}'
                    ORDER BY id LIMIT 1
                    FOR UPDATE
                """)

                if task_to_assign_df.empty:
                    print(f"      ⚠️ Все дубли задания '{task_name}' уже назначены")
                    break

                task_id = int(task_to_assign_df.iloc[0]['id'])

                # Обновляем задание
                now_str = now.strftime('%Y-%m-%d %H:%M:%S')
                print(f"  ✅ Назначено: {task_name} → {opv_name}")
                
                SQL.sql_delete('wms', f"""
                    UPDATE wms_bot.shift_tasks
                    SET status = 'Выполняется',
                        user_id = '{opv['employee_id']}',
                        time_begin = '{now_str}'
                    WHERE id = {task_id}
                """)

                # Получаем полную информацию о задании
                task_details_df = SQL.sql_select('wms', f"""
                    SELECT task_name, product_group, slot, task_duration
                    FROM wms_bot.shift_tasks
                    WHERE id = {task_id}
                """)
                
                if not task_details_df.empty:
                    task_details = task_details_df.iloc[0]
                    task_name = task_details.get('task_name', 'Неизвестное задание')
                    product_group = task_details.get('product_group', 'Не указано')
                    slot = task_details.get('slot', 'Не указано')
                    
                    # Безопасная проверка наличия поля task_duration
                    try:
                        if 'task_duration' in task_details and task_details['task_duration'] is not None and pd.notnull(task_details['task_duration']):
                            if hasattr(task_details['task_duration'], 'strftime'):
                                duration = task_details['task_duration'].strftime('%H:%M')
                            else:
                                duration = str(task_details['task_duration'])
                        else:
                            duration = "не указано"
                    except Exception as e:
                        print(f"      ⚠️ Ошибка при получении duration: {e}")
                        duration = "не указано"
                else:
                    task_name = task_row.get('task_name', 'Неизвестное задание')
                    product_group = task_row.get('product_group', 'Не указано')
                    slot = task_row.get('slot', 'Не указано')
                    duration = "не указано"
                    print(f"      ⚠️ Не удалось получить детали задания {task_id}")
                
                chat_id = opv['userid']
                if isinstance(chat_id, pd.Series):
                    chat_id = chat_id.values[0]

                # Импортируем клавиатуру
                from ..keyboards.opv_keyboards import get_special_task_keyboard
                
                try:
                    message_text = (
                        f"📌 *На Вас назначено спец-задание!*\n\n"
                        f"📝 *Наименование:* {task_name}\n"
                        f"📦 *Группа:* {product_group}\n"
                        f"📍 *Слот:* {slot}\n"
                        f"⏰ *Время:* {duration} мин\n\n"
                        f"*Приоритет 111* - задание не требует проверки ЗС"
                    )
                    
                    # Отправляем сообщение
                    keyboard = get_special_task_keyboard()
                    await context.bot.send_message(
                        chat_id=int(chat_id),
                        text=message_text,
                        parse_mode='Markdown',
                        reply_markup=keyboard
                    )
                except Exception:
                    # Пробуем отправить без форматирования и клавиатуры
                    try:
                        simple_text = f"📌 На Вас назначено спец-задание!\n\nНаименование: {task_name}\nГруппа: {product_group}\nСлот: {slot}\nВремя: {duration} мин"
                        await context.bot.send_message(
                            chat_id=int(chat_id),
                            text=simple_text
                        )
                    except Exception:
                        pass
                total_assigned += 1
                
                # ИСПРАВЛЕНИЕ: добавляем ОПВ в список назначенных
                assigned_opv_ids.add(str(opv['employee_id']))
                
                # Удаляем из кэша, так как задание успешно назначено
                if cache_key in _no_opv_cache:
                    del _no_opv_cache[cache_key]
        
        if total_assigned > 0:
            print(f"✅ Назначено заданий: {total_assigned}")

    except Exception as e:
        print(f"❌ Ошибка планировщика: {e}")
        pass
    finally:
        # Сбрасываем флаг планировщика в любом случае
        _scheduler_running = False

async def auto_close_expired_tasks(context):
    """Автоматически закрывает задания в статусах 'Выполняется' или 'Заморожено', если прошло 4 часа с начала выполнения"""
    try:
        now = datetime.now()
        now_str = now.strftime('%Y-%m-%d %H:%M:%S')

        expired_df = SQL.sql_select('wms', f"""
            SELECT id, user_id, task_name, time_begin, status
            FROM wms_bot.shift_tasks
            WHERE status IN ('Выполняется', 'Заморожено','Ожидает проверки') 
              AND task_date IN (current_date, current_date - 1)
              AND merchant_code = '{MERCHANT_ID}'
              AND time_begin IS NOT NULL
              AND time_begin <= NOW() - INTERVAL '4 hours'
        """)

        if expired_df.empty:
            return

        for _, row in expired_df.iterrows():
            SQL.sql_delete('wms', f"""
                UPDATE wms_bot.shift_tasks
                SET status = 'Проверено',
                    time_end = '{now_str}',
                    inspector_id = 0
                WHERE id = '{row['id']}'
            """)

    except Exception as e:
        pass
