import asyncio
from datetime import datetime, timedelta, time
from uuid import uuid4
import pandas as pd
from telegram import (
    Update, InlineKeyboardButton, InlineKeyboardMarkup, 
    KeyboardButton, ReplyKeyboardMarkup, InputMediaPhoto
)
from telegram.ext import (
    Application, CommandHandler, CallbackQueryHandler,
    CallbackContext, MessageHandler, filters
)
from telegram.ext import Application, JobQueue

from process.kidou_opt import SQL_requests
from telegram.error import BadRequest
import datetime as dt

# Инициализация SQL-клиента
SQL = SQL_requests()

# Глобальные переменные (лучше заменить на Redis в будущем)
active_tasks = {}
zav_on_shift = []
task_assignments = {}  # {task_num: кол-во уже выданных}
assignments = []

# Константы
SHIFT_MAP = {'День': 'day', 'Ночь': 'night'}
ZS_GROUP_CHAT_ID = -1002694047317  # Чат для проверки заданий

TOPIC_IDS = {
    'Бакалея': 9,
    'Напитки': 10,
    'Химия': 2,
    'СОФ': 11,
    'Молочка': 12,
    'Гастрономия': 13,
    'Холодная зона': 14,
    'Сухая зона': 15,
    'Заморозка': 16
}


async def debug_chat_id(update: Update, context: CallbackContext):
    print(f"🆔 Chat ID: {update.effective_chat.id}")
    await update.message.reply_text(f"🆔 Chat ID этой группы: `{update.effective_chat.id}`", parse_mode='Markdown')


# ======================
# ВСПОМОГАТЕЛЬНЫЕ ФУНКЦИИ
# ======================

def get_current_slot(shift: str) -> int:
    """Определяет текущий временной слот для смены"""
    now = datetime.now().time()

    if shift == 'night':
        if time(20, 0) <= now or now < time(0, 0):
            return 5
        elif time(0, 0) <= now < time(3, 0):
            return 6
        elif time(3, 0) <= now < time(5, 0):
            return 7
        elif time(5, 0) <= now < time(8, 0):
            return 8
        else:
            return None
    else:
        if time(8, 0) <= now < time(10, 30):
            return 1
        elif time(10, 30) <= now < time(13, 30):
            return 2
        elif time(13, 30) <= now < time(16, 30):
            return 3
        elif time(16, 30) <= now < time(20, 0):
            return 4
        else:
            return None

    
#######РАЗБИВКА секторов
# async def debug_thread_id(update: Update, context: CallbackContext):
#     print(f"🧵 message_thread_id: {update.message.message_thread_id}")
#     await update.message.reply_text(
#         f"🧵 ID этой темы (топика): {update.message.message_thread_id}"
#     )


async def schedule_tasks_from_rules(context: CallbackContext):
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
        """)

        if schedule_df.empty:
            print("📭 Нет заданий по расписанию")
            return

        # Фильтрация по текущему времени
        schedule_df['start_time_short'] = schedule_df['start_time'].apply(lambda x: x.strftime('%H:%M'))
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
      AND ss.merchantid = 6001
      AND NOT EXISTS (
          SELECT 1
          FROM wms_bot.shift_tasks st
          WHERE st.user_id = ss.employee_id::int
            AND st.status = 'Выполняется'
            AND st.is_constant_task = false
            AND st.time_end IS null)
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

                duration = task_row['task_duration'].strftime('%H:%M')
                chat_id = opv['userid']
                if isinstance(chat_id, pd.Series):
                    chat_id = chat_id.values[0]

                # Отправляем уведомление
                await context.bot.send_message(
                    chat_id=int(chat_id),
                    text=(
                        f"📌 *На Вас назначено задание!*\n\n"
                        f"📝 *Наименование:* {task_row['task_name']}\n"
                        f"📦 *Группа:* {task_row['product_group']}\n"
                        f"📍 *Слот:* {task_row['slot']}\n"
                        f"⏰ *Время:* {duration} мин"
                    ),
                    parse_mode='Markdown'
                )

                print(f"✅ Назначено задание {task_row['task_name']} ({task_id}) для {opv['fio']}")


    except Exception as e:
        print(f"❌ Ошибка в schedule_tasks_from_rules: {e}")





async def send_task_to_zs(context: CallbackContext, task: dict, photos: list):
    """Отправляет задание на проверку заведующему"""
    try:
        keyboard = InlineKeyboardMarkup([
            [
                InlineKeyboardButton("✅ Подтвердить", callback_data=f"approve_{task['task_id']}|{context.user_data.get('staff_id')}"),
                InlineKeyboardButton("🔁 Вернуть", callback_data=f"start_reject_{task['task_id']}|{context.user_data.get('staff_id')}")
            ]
        ])
        sector = context.user_data.get('sector', '').strip().capitalize()
        thread_id = TOPIC_IDS.get(sector)

        time_spent = datetime.now() - task['assigned_time']

        message = (
            f"📬 Задание от *{context.user_data.get('staff_name', 'ОПВ')}* завершено\n"
            f"📝 *Наименование:* {task.get('task_name', '—')}\n"
            f"📦 *Группа товаров:* {task.get('product_group', '—')}\n"
            f"📍 *Слот:* {task.get('slot', '—')}\n"
            f"⏱️ Время выполнения: {str(time_spent).split('.')[0]}\n"
            f"⏳ Выделенное время: {task['duration']} мин"
        )

        if photos:
            media_group = []

            for i, photo in enumerate(photos):
                if i == 0:
                    media_group.append(InputMediaPhoto(media=photo, caption=message, parse_mode='Markdown'))
                else:
                    media_group.append(InputMediaPhoto(media=photo))

            messages = await context.bot.send_media_group(
                chat_id=ZS_GROUP_CHAT_ID,
                media=media_group,
                message_thread_id=thread_id
            )
            context.user_data['last_task_message_id'] = messages[0].message_id

            await context.bot.send_message(
                chat_id=ZS_GROUP_CHAT_ID,
                text="🔽 Выберите действие:",
                reply_markup=keyboard,
                message_thread_id=thread_id
            )
        else:
            sent_msg = await context.bot.send_message(
                chat_id=ZS_GROUP_CHAT_ID,
                text=message,
                parse_mode='Markdown',
                reply_markup=keyboard,
                message_thread_id=thread_id
            )
            context.user_data['last_task_message_id'] = sent_msg.message_id

    except Exception as e:
        print(f"❌ Ошибка отправки в ЗС группу: {e}")
        raise

# ======================
# ОСНОВНЫЕ ОБРАБОТЧИКИ
# ======================

async def start(update: Update, context: CallbackContext):
    """Обработчик команды /start - авторизация пользователя"""
    user_id = update.effective_user.id
    
    try:
        auth_check = SQL.sql_select('wms', f"""
            SELECT phone, fio, employee_id
            FROM wms_bot.bot_auth
            WHERE userid = {user_id}
        """)
        
        if not auth_check.empty:
            record = auth_check.iloc[0]
            context.user_data.update({
                'phone': record['phone'],
                'staff_name': record['fio'],
                'staff_id': record['employee_id']
            })
            await update.message.reply_text(f"✅ Добро пожаловать, {record['fio']}!")
            await shift_start(update, context)
        else:
            button = KeyboardButton("Отправить номер телефона 📱", request_contact=True)
            reply_markup = ReplyKeyboardMarkup([[button]], resize_keyboard=True, one_time_keyboard=True)
            await update.message.reply_text("Пожалуйста, авторизуйтесь, отправив свой номер телефона:", reply_markup=reply_markup)

    except Exception as e:
        print(f"❌ Ошибка авторизации: {e}")
        await update.message.reply_text("Произошла ошибка при проверке авторизации. Обратитесь к администратору.")

async def handle_contact(update: Update, context: CallbackContext):
    """Обработчик отправки контакта для авторизации"""
    contact = update.message.contact
    user_phone = contact.phone_number[-7:]  # последние 7 цифр
    
    try:
        staff_df = SQL.sql_select('wms', "select id,cell_phone, concat(bs.name, ' ', bs.surname) fio, gender from wms_bot.t_staff bs")
        match = staff_df[staff_df['cell_phone'].astype(str).str.endswith(user_phone)]

        if not match.empty:
            context.user_data.update({
                'phone': contact.phone_number,
                'staff_id': match.iloc[0]['id'],
                'staff_name': match.iloc[0]['fio']
            })

            # Сохраняем в базу
            auth_data = pd.DataFrame([{
                'userid': update.effective_user.id,
                'phone': contact.phone_number,
                'fio': match.iloc[0]['fio'],
                'employee_id': match.iloc[0]['id']
            }])
            
            SQL.sql_execute_df('wms', auth_data, 'wms_bot.bot_auth(userid, phone, fio, employee_id)')
            await update.message.reply_text(f"✅ Вы авторизованы как: {match.iloc[0]['fio']}")
            await shift_start(update, context)
        else:
            await update.message.reply_text("❌ Вы не авторизованы. Обратитесь к администратору.")
            
    except Exception as e:
        print(f"❌ Ошибка обработки контакта: {e}")
        await update.message.reply_text("Произошла ошибка при обработке вашего номера.")

async def shift_start(update: Update, context: CallbackContext):
    """Начало смены - выбор типа смены"""
    keyboard = [
        [InlineKeyboardButton("День 🌇", callback_data='day')],
        [InlineKeyboardButton("Ночь 🌃", callback_data='night')]
    ]
    reply_markup = InlineKeyboardMarkup(keyboard)
    
    if update.message:
        await update.message.reply_text("Выберите смену:", reply_markup=reply_markup)
    else:
        await update.callback_query.edit_message_text("Выберите смену:", reply_markup=reply_markup)

async def shift_choice(update: Update, context: CallbackContext):
    """Обработчик выбора смены"""
    query = update.callback_query
    await query.answer()
    shift = query.data.lower()
    
    keyboard = [
        [InlineKeyboardButton("ОПВ", callback_data=f'opv_{shift}')],
        [InlineKeyboardButton("ЗС", callback_data=f'zs_{shift}')]
    ]
    reply_markup = InlineKeyboardMarkup(keyboard)
    await query.edit_message_text(f"Вы выбрали смену: {shift}. Теперь выберите роль:", reply_markup=reply_markup)


async def role_choice(update: Update, context: CallbackContext):
    query = update.callback_query
    await query.answer()
    role, shift = query.data.split('_')
    
    context.user_data.update({
        'role': role,
        'shift': shift
    })

    try:
        # Сохраняем старт смены в базу
        session_row = pd.DataFrame([{
            'employee_id': context.user_data['staff_id'],
            'role': role,
            'shift_type': shift,
            'start_time': datetime.now(),
            'end_time': None,
            'load_date': pd.to_datetime('today').date(),
            'merchantid': 6001 
        }])
        SQL.sql_execute_df('wms', session_row, 'wms_bot.shift_sessions1')
    except Exception as e:
        print(f"❌ Ошибка записи начала смены: {e}")

    if role == 'opv':
        keyboard = [
            [InlineKeyboardButton("Основная смена", callback_data='employment_main')],
            [InlineKeyboardButton("Парттайм", callback_data='employment_part_time')]
        ]
        reply_markup = InlineKeyboardMarkup(keyboard)
        await query.edit_message_text("Выберите тип занятости:", reply_markup=reply_markup)
    else:
        # Для ЗС сразу показываем меню
        if update.effective_user.id not in zav_on_shift:
            zav_on_shift.append(update.effective_user.id)

        keyboard = [
            [InlineKeyboardButton("Список ОПВ на смене 📋", callback_data='opv_list_on_shift')],
            [InlineKeyboardButton("Список ОПВ завершивших смену ✅", callback_data='opv_list_completed')]
        ]
        reply_markup = InlineKeyboardMarkup(keyboard)
        await query.edit_message_text(
            f"Вы выбрали роль: {role.upper()} на смене: {shift}.",
            reply_markup=reply_markup
        )

async def employment_type_choice(update: Update, context: CallbackContext):
    """Обработчик выбора типа занятости"""
    query = update.callback_query
    await query.answer()

    employment_type = query.data.replace('employment_', '')
    context.user_data['employment_type'] = employment_type

    try:
        # Сохраняем тип занятости в shift_sessions1
        SQL.sql_delete('wms', f"""
            UPDATE wms_bot.wms_bot.shift_tasks
            SET part_time = '{employment_type}'
            WHERE user_id = '{context.user_data['staff_id']}' 
        """)
    except Exception as e:
        print(f"❌ Ошибка сохранения типа занятости: {e}")

    # Переход к выбору сектора
    shift = context.user_data.get('shift')
    shift_ru = 'День' if shift == 'day' else 'Ночь'
    task_date = get_task_date(shift)

    sectors_df = SQL.sql_select('wms', f"""SELECT DISTINCT sector FROM wms_bot.shift_tasks WHERE task_date =  '{task_date}' AND shift = '{shift_ru}' and merchant_code='6001'""")
    sectors = sectors_df['sector'].dropna().tolist()

    if not sectors:
        await query.edit_message_text("❌ Нет доступных секторов для этой смены.")
        return

    keyboard = [[InlineKeyboardButton(sector, callback_data=f'sectorchoice_{sector}')] for sector in sectors]
    reply_markup = InlineKeyboardMarkup(keyboard)

    await query.edit_message_text("Выберите сектор, с которым будете работать в эту смену:", reply_markup=reply_markup)


async def sector_select_and_confirm(update: Update, context: CallbackContext):
    """Обработчик выбора сектора"""
    query = update.callback_query
    await query.answer()
    
    sector = query.data.replace('sectorchoice_', '')
    context.user_data.update({
        'sector': sector,
        'sector_selected': True
    })

    keyboard = [[InlineKeyboardButton("Получить задание", callback_data='get_task')]]
    reply_markup = InlineKeyboardMarkup(keyboard)

    await query.edit_message_text(
        f"✅ Вы выбрали сектор: *{sector}*\nТеперь можно брать задания.",
        parse_mode='Markdown',
        reply_markup=reply_markup
    )

async def get_task(update: Update, context: CallbackContext):
    """Получение нового задания"""
    query = update.callback_query
    await query.answer()
    
    employee_id = context.user_data.get('staff_id')
    if not employee_id:
        await query.edit_message_text("⚠️ Ваш ID не найден в системе.")
        return

    # Проверяем активные задания
    active_df = SQL.sql_select('wms', f"""
        SELECT id, status
        FROM wms_bot.shift_tasks
        WHERE user_id = '{employee_id}'
          AND status IN ('Выполняется', 'На доработке','Заморожено')
    """)
    
    if not active_df.empty:
        await query.edit_message_text(
            "⚠️ У вас уже есть активное задание.\n"
            "Пожалуйста, завершите его перед тем как брать новое."
        )
        return

    # Проверяем выбор сектора
    if not context.user_data.get('sector_selected'):
        sectors_df = SQL.sql_select('stock', """SELECT DISTINCT "Сектор" FROM public.task_schedule WHERE "Дата" = current_date""")
        sectors = sectors_df['Сектор'].dropna().tolist()
        
        keyboard = [[InlineKeyboardButton(s, callback_data=f'sectorchoice_{s}')] for s in sectors]
        reply_markup = InlineKeyboardMarkup(keyboard)
        await query.edit_message_text("Выберите сектор, с которым будете работать:", reply_markup=reply_markup)
        return

    # Если сектор выбран - назначаем задание
    await assign_task_from_sector(update, context)

def get_task_date(shift: str) -> datetime.date:
    now = datetime.now()
    if shift == 'night':
        if now.hour >= 0 and now.hour < 8:  
            return now.date()               
        else:
            return (now + timedelta(days=1)).date()  
    else:
        return now.date()


async def assign_task_from_sector(update: Update, context: CallbackContext):
    """Назначение задания из выбранного сектора"""
    query = update.callback_query
    await query.answer()

    employee_id = context.user_data.get('staff_id')
    sector = context.user_data.get('sector')
    shift = context.user_data.get('shift')

    if not all([employee_id, sector, shift]):
        await query.edit_message_text("⚠️ Недостаточно данных для назначения.")
        return

    try:
        # Проверяем активные заданияtF
        active_df = SQL.sql_select('wms', f"""
            SELECT id FROM wms_bot.shift_tasks
            WHERE user_id = '{employee_id}'
              AND status IN ('Выполняется','На доработке','Заморожено')
        """)

        if not active_df.empty:
            await query.edit_message_text("⚠️ У вас уже есть активное задание.")
            return

        # Получаем пол
        gender_df = SQL.sql_select('wms', f"SELECT gender FROM wms_bot.t_staff WHERE id = '{employee_id}'")
        opv_gender = gender_df.iloc[0]['gender'].strip().upper() if not gender_df.empty else 'U'

        # Переводим shift в нужный вид
        shift_ru = 'День' if shift == 'day' else 'Ночь'

        task_date = get_task_date(shift)
        current_slot = get_current_slot(shift)
        if current_slot is None:
            await query.edit_message_text("⏰ Сейчас не время активного слота.")
            return

        # Выбираем задание из shift_tasks
        task_df = SQL.sql_select('wms', f"""
            SELECT * FROM wms_bot.shift_tasks
            WHERE task_date = '{task_date}'
              AND shift = '{shift_ru}'
              AND sector = '{sector}'
              AND slot = {current_slot}
              AND is_constant_task = true
              and merchant_code ='6001'
              AND (status IS NULL OR status = 'В ожидании')
        """)

        if task_df.empty:
            await query.edit_message_text("❌ Нет доступных заданий.")
            return

        # Фильтруем по полу
        task_df = task_df[
            (task_df['gender'].isnull()) |
            (task_df['gender'].str.upper() == 'U') |
            (task_df['gender'].str.upper() == opv_gender)
        ]

        if task_df.empty:
            await query.edit_message_text("❌ Нет подходящих по полу заданий.")
            return

        # Берём задание с наивысшим приоритетом
        task_row = task_df.sort_values('priority').iloc[0]
        now = datetime.now()

        # Обновляем статус задания
        employment_type = context.user_data.get('employment_type', 'main')
        SQL.sql_delete('wms', f"""
            UPDATE wms_bot.shift_tasks
            SET status = 'Выполняется',
                user_id = '{employee_id}',
                time_begin = '{now}',
                part_time = '{employment_type}'
            WHERE id = {task_row['id']}
        """)

        # Парсим время выполнения
        try:
            duration_raw = task_row['task_duration']

            if isinstance(duration_raw, dt.time):
                total_seconds = duration_raw.hour * 3600 + duration_raw.minute * 60 + duration_raw.second
            else:
                time_str = str(duration_raw).strip()
                t = datetime.strptime(time_str, '%H:%M:%S')
                total_seconds = t.hour * 3600 + t.minute * 60 + t.second

        except Exception as e:
            print(f"❌ Ошибка парсинга task_duration: {e}")
            total_seconds = 900  # дефолт 15 мин если что-то не так

        # Сохраняем задание в контекст
        task = {
            'task_id': task_row['id'],
            'task_name': task_row['task_name'],
            'product_group': task_row['product_group'],
            'slot': task_row['slot'],
            'duration': task_row['task_duration'],
            'assigned_time': now
        }
        context.user_data['task'] = task
        context.user_data.pop('photos', None)
        context.user_data.pop('photo_request_time', None)
        context.user_data['late_warning_sent'] = False

        # Формируем сообщение с таймером
        message = (
            f"📄 *Номер задания:* {task_row['id']}\n"
            f"✅ *Задание получено!*\n\n"
            f"📝 *Наименование:* {task_row['task_name']}\n"
            f"📦 *Группа товаров:* {task_row.get('product_group', '—')}\n"
            f"📍 *Слот:* {task_row['slot']}\n"
            f"⏱ *Выделенное время:* {str(timedelta(seconds=total_seconds))}\n"
            f"⏳ *Оставшееся время:* {str(timedelta(seconds=total_seconds))}"
        )

        # Если есть комментарий — добавляем в сообщение
        comment = task_row.get('comment')
        if comment and str(comment).strip():
            message += f"\n💬 *Комментарий:* {comment}"

        sent_msg = await query.edit_message_text(message, parse_mode='Markdown')

        # 💥💥💥 Запускаем таймер!
        asyncio.create_task(
            update_timer(context, sent_msg.chat_id, sent_msg.message_id, task, total_seconds)
        )

    except Exception as e:
        print(f"❌ Ошибка в assign_task_from_sector: {e}")
        await query.edit_message_text("⚠️ Произошла ошибка при назначении задания.")



async def update_timer(context, chat_id, message_id, task, total_seconds, comment=None):
    for remaining in range(total_seconds, -1, -15):
        try:
            minutes = remaining // 60
            seconds = remaining % 60
            remaining_str = f"{minutes:02d}:{seconds:02d}"

            message = (
                f"📄 *Номер задания:* {task['task_id']}\n"
                f"✅ *Задание выполняется*\n\n"
                f"📝 *Наименование:* {task['task_name']}\n"
                f"📦 *Группа товаров:* {task.get('product_group', '—')}\n"
                f"📍 *Слот:* {task.get('slot', '—')}\n"
                f"⏱ *Выделенное время:* {task['duration']}\n"
                f"⏳ *Оставшееся время:* {remaining_str}"
            )

            if comment and str(comment).strip():
                message += f"\n💬 *Комментарий:* {comment}"

            await context.bot.edit_message_text(
                chat_id=chat_id,
                message_id=message_id,
                text=message,
                parse_mode='Markdown'
            )

            await asyncio.sleep(15)
        except Exception as e:
            print(f"⚠️ Ошибка обновления таймера: {e}")
            break




async def complete_the_task(update: Update, context: CallbackContext):
    """Завершение текущего задания"""
    user_id = update.effective_user.id
    staff_id = context.user_data.get('staff_id')
    task = context.user_data.get('task')

    if not task:
        staff_id = context.user_data.get('staff_id')
        if not staff_id:
            await update.message.reply_text("⚠️ Ваш ID не найден в системе.")
            return

        # Проверяем активное задание из базы
        task_df = SQL.sql_select('wms', f"""
            SELECT id, task_name, product_group, slot, time_begin, task_duration, comment
            FROM wms_bot.shift_tasks
            WHERE user_id = '{staff_id}' AND status = 'Выполняется'
            AND time_end IS NULL
            ORDER BY time_begin DESC LIMIT 1
        """)

        if task_df.empty:
            await update.message.reply_text("❌ Нет активного задания.")
            return


        row = task_df.iloc[0]
        time_begin_value = row['time_begin']

        # Если это строка — парсим в time
        if isinstance(time_begin_value, str):
            time_begin_value = datetime.strptime(time_begin_value, '%H:%M:%S').time()

        # Если это time — комбинируем с сегодняшней датой
        if isinstance(time_begin_value, dt.time):
            assigned_time = datetime.combine(datetime.today(), time_begin_value)
        else:
            assigned_time = pd.to_datetime(time_begin_value)

        now = datetime.now()

        task = {
            'task_id': row['id'],
            'task_name': row['task_name'],
            'product_group': row['product_group'],
            'slot': row['slot'],
            'assigned_time': assigned_time,
            'duration': row['task_duration'],
            'assigned_time': now
        }
        context.user_data['task'] = task

    # Проверяем фото
    if 'photos' not in context.user_data:
        context.user_data.update({'photo_request_time': datetime.now(), 'photos': []})
        await update.message.reply_text("📸 Сделайте не более 3 фото в течение 5 минут и отправьте боту.")
        return

    if not context.user_data['photos']:
        await update.message.reply_text("⚠️ Необходимо отправить хотя бы одно фото.")
        return

    # Завершаем задание
    now = datetime.now()
    time_spent = now - task['assigned_time']

    try:
        # Обновляем статус и время окончания
        SQL.sql_delete('wms', f"""
            UPDATE wms_bot.shift_tasks
            SET status = 'Ожидает проверки',
                time_end = '{now}'
            WHERE id = {task['task_id']}
        """)

        await update.message.reply_text("✅ Задание отправлено на проверку заведующему.")
        await send_task_to_zs(context, task, context.user_data['photos'])

        # Очищаем контекст
        context.user_data.pop('task', None)
        context.user_data.pop('photos', None)
        context.user_data.pop('photo_request_time', None)
        context.user_data['late_warning_sent'] = False


    except Exception as e:
        print(f"❌ Ошибка завершения задания: {e}")
        await update.message.reply_text("⚠️ Ошибка при завершении задания. Попробуйте позже.")


async def receive_photo(update: Update, context: CallbackContext):
    """Обработчик получения фото для задания"""
    staff_id = context.user_data.get('staff_id')
    now = datetime.now()

    if 'photo_request_time' not in context.user_data:
        await update.message.reply_text("⚠️ Завершите задание и запросите фото заново.")
        return

    # Проверка времени
    time_passed = now - context.user_data['photo_request_time']
    is_late = time_passed > timedelta(minutes=180)

    # Инициализируем список фото если нужно
    if 'photos' not in context.user_data:
        context.user_data['photos'] = []

    # Инициализируем флаг, если его ещё нет
    if 'late_warning_sent' not in context.user_data:
        context.user_data['late_warning_sent'] = False

    # Проверяем лимит фото
    if len(context.user_data['photos']) >= 5:
        await update.message.reply_text("⚠️ Можно загрузить не более 3 фото для одного задания.")
        return

    # Проверка наличия фото
    if not update.message.photo:
        await update.message.reply_text("❌ Фото не обнаружено.")
        return

    # Сохраняем фото
    photo_id = update.message.photo[-1].file_id
    context.user_data['photos'].append(photo_id)

    photo_num = len(context.user_data['photos'])
    await update.message.reply_text(f"📸 Фото {photo_num}/3 получено.")

    # ⚠️ Сообщение об опоздании — только один раз
    if is_late and not context.user_data['late_warning_sent']:
        context.user_data['late_warning_sent'] = True
        await update.message.reply_text(
            "⚠️ Вы отправили фотоотчет с опозданием.\n"
            "Пожалуйста, завершите задание вручную. ЗС проверит и подтвердит выполнение."
        )




async def handle_review(update: Update, context: CallbackContext):
    """Обработчик подтверждения/отклонения задания ЗС (новая таблица)"""
    query = update.callback_query
    await query.answer()

    action, data = query.data.split('_')
    task_id, opv_id = data.split('|')

    now = datetime.now()

    if action == 'approve':
        # Получаем имя инспектора
        inspector_df = SQL.sql_select('wms', f"""
            SELECT fio FROM wms_bot.bot_auth WHERE userid = {update.effective_user.id}
        """)
        inspector_name = inspector_df.iloc[0]['fio'] if not inspector_df.empty else 'Неизвестно'

        # Обновляем статус и инспектора в shift_tasks_test
        SQL.sql_delete('wms', f"""
            UPDATE wms_bot.shift_tasks
            SET status = 'Проверено',
                time_end = '{now}',
                inspector_id = {update.effective_user.id}
            WHERE id = {task_id}
        """)

        # Редактируем сообщение в чате
        first_message_id = context.user_data.get('last_task_message_id')
        if first_message_id:
            try:
                await context.bot.edit_message_caption(
                    chat_id=ZS_GROUP_CHAT_ID,
                    message_id=first_message_id,
                    caption=f"✅ Задание №{task_id} одобрено."
                )
            except:
                pass

        await query.edit_message_text(f"✅ Задание №{task_id} одобрено.")

        # Уведомляем ОПВ
        opv_userid_df = SQL.sql_select('wms', f"""
            SELECT userid FROM wms_bot.bot_auth WHERE employee_id = '{opv_id}'
        """)
        if not opv_userid_df.empty:
            opv_user_id = int(opv_userid_df.iloc[0]['userid'])
            await context.bot.send_message(
                chat_id=opv_user_id,
                text=f"✅ Задание №{task_id} *подтверждено* заведующим. Отличная работа!",
                parse_mode='Markdown'
            )

            keyboard = InlineKeyboardMarkup([
                [InlineKeyboardButton("Получить задание", callback_data='get_task')]
            ])
            await context.bot.send_message(
                chat_id=opv_user_id,
                text="Хотите взять следующее задание? 👇",
                reply_markup=keyboard
            )

    elif action == 'reject':
        # Обновляем статус на доработку
        SQL.sql_delete('wms', f"""
            UPDATE wms_bot.shift_tasks
            SET status = 'На доработке'
            WHERE id = {task_id}
        """)

        try:
            await query.message.edit_caption(f"🔁 Задание №{task_id} возвращено на доработку.")
        except:
            await query.edit_message_text(f"🔁 Задание №{task_id} возвращено на доработку.")

        # Уведомляем ОПВ
        opv_userid_df = SQL.sql_select('wms', f"""
            SELECT userid FROM wms_bot.bot_auth WHERE employee_id = '{opv_id}'
        """)
        if opv_userid_df.empty:
            return

        opv_user_id = int(opv_userid_df.iloc[0]['userid'])

        # Получаем данные задания из новой таблицы
        task_df = SQL.sql_select('wms', f"""
            SELECT id, task_name, slot, status, time_begin, task_duration, product_group
            FROM wms_bot.shift_tasks
            WHERE id = {task_id}
        """)
        if task_df.empty:
            return

        row = task_df.iloc[0]
        assigned_time = pd.to_datetime(row['time_begin'])
        total_duration = (
            row['task_duration'].hour * 3600 + row['task_duration'].minute * 60 + row['task_duration'].second
            if isinstance(row['task_duration'], dt.time)
            else 900
        )
        deadline = assigned_time + timedelta(seconds=total_duration)
        now = datetime.now()
        remaining_seconds = max(0, int((deadline - now).total_seconds()))

        # Формируем сообщение
        message = (
            f"⚠️ Задание №{row['id']} вернули на доработку.\n\n"
            f"📋 *Задание повторно активировано:*\n"
            f"📍 *Слот:* {row['slot']}\n"
            f"📝 *Наименование:* {row['task_name']}\n"
            f"📦 *Группа товаров:* {row.get('product_group', '—')}\n"
            f"⏱ *Выделенное время:* {str(timedelta(seconds=total_duration))}\n"
            f"⏳ *Оставшееся время:* {str(timedelta(seconds=remaining_seconds))}"
        )

        await context.bot.send_message(
            chat_id=opv_user_id,
            text=message,
            parse_mode='Markdown'
        )

        # Обновляем контекст ОПВ
        context.application.user_data[opv_user_id]['task'] = {
            'task_id': row['id'],
            'task_name': row['task_name'],
            'slot': row['slot'],
            'assigned_time': assigned_time,
            'дедлайн': total_duration,
            'status': 'На доработке'
        }

        context.application.user_data[opv_user_id].pop('photo', None)
        context.user_data.pop('last_task_message_id', None)



async def complete_the_extra_task(update: Update, context: CallbackContext):
    """Завершает активное дополнительное задание из расписания (priority = 111)"""
    staff_id = context.user_data.get('staff_id')

    # Проверяем активное доп. задание
    extra_task_df = SQL.sql_select('wms', f"""
        SELECT id, task_name, time_begin
        FROM wms_bot.shift_tasks
        WHERE user_id = '{staff_id}'
          AND status = 'Выполняется'
          AND time_end IS NULL
          AND priority = '111'
        ORDER BY time_begin DESC LIMIT 1
    """)

    if extra_task_df.empty:
        await update.message.reply_text("У вас нет активных дополнительных заданий.")
        return

    task = extra_task_df.iloc[0]
    now = datetime.now()

    # Завершаем задание
    SQL.sql_delete('wms', f"""
        UPDATE wms_bot.shift_tasks
        SET status = 'Проверено',
            time_end = '{now}'
        WHERE id = {task['id']}
    """)

    await update.message.reply_text(
        f"✅ Дополнительное задание №{task['id']} ({task['task_name']}) завершено!"
    )



async def start_reject_reason(update: Update, context: CallbackContext):
    """Начало процесса возврата задания с указанием причины"""
    query = update.callback_query
    await query.answer()

    task_num, opv_id = query.data.replace("start_reject_", "").split("|")
    context.user_data.update({
        'reject_task_id': task_num,
        'reject_opv_id': opv_id
    })

    try:
        await query.edit_message_text("✏️ Пожалуйста, укажите причину возврата задания:")
    except telegram.error.BadRequest:
        try:
            await query.edit_message_caption("✏️ Пожалуйста, укажите причину возврата задания:")
        except Exception as e:
            print(f"❌ Ошибка при смене caption: {e}")

async def receive_reject_reason(update: Update, context: CallbackContext):
    """Обработчик причины возврата задания (shift_tasks_test)"""
    reason = update.message.text
    task_id = context.user_data.get('reject_task_id')

    if not task_id:
        await update.message.reply_text("⚠️ Нет активного задания для возврата.")
        return

    # Обновляем статус
    SQL.sql_delete('wms', f"""
        UPDATE wms_bot.shift_tasks
        SET status = 'На доработке'
        WHERE id = {task_id}
    """)

    # Получаем данные задания
    task_df = SQL.sql_select('wms', f"""
        SELECT user_id, task_name, slot, time_begin, task_duration, product_group
        FROM wms_bot.shift_tasks
        WHERE id = {task_id}
    """)
    if task_df.empty:
        await update.message.reply_text("⚠️ Не найдено задание для возврата.")
        return

    row = task_df.iloc[0]
    opv_employee_id = row['user_id']

    if not opv_employee_id:
        await update.message.reply_text("⚠️ Задание не привязано к сотруднику.")
        return

    # Получаем userid ОПВ
    opv_userid_df = SQL.sql_select('wms', f"""
        SELECT userid FROM wms_bot.bot_auth WHERE employee_id = '{opv_employee_id}'
    """)
    if opv_userid_df.empty:
        await update.message.reply_text("⚠️ У сотрудника не зарегистрирован Telegram ID.")
        return

    opv_user_id = int(opv_userid_df.iloc[0]['userid'])

    if isinstance(row['time_begin'], dt.time):
        assigned_time = datetime.combine(datetime.today(), row['time_begin'])
    else:
        assigned_time = pd.to_datetime(row['time_begin'])

    total_duration = (
        row['task_duration'].hour * 3600 + row['task_duration'].minute * 60 + row['task_duration'].second
        if isinstance(row['task_duration'], dt.time)
        else 900
    )
    deadline = assigned_time + timedelta(seconds=total_duration)
    now = datetime.now()
    remaining_seconds = max(0, int((deadline - now).total_seconds()))

    # Формируем сообщение
    message = (
        f"⚠️ Задание №{task_id} вернули на доработку.\n"
        f"📝 Причина: {reason}\n\n"
        f"📋 *Задание повторно активировано:*\n"
        f"📍 *Слот:* {row['slot']}\n"
        f"📝 *Наименование:* {row['task_name']}\n"
        f"📦 *Группа товаров:* {row.get('product_group', '—')}\n"
        f"⏱ *Выделенное время:* {str(timedelta(seconds=total_duration))}\n"
        f"⏳ *Оставшееся время:* {str(timedelta(seconds=remaining_seconds))}"
    )

    # Отправляем ОПВ
    await context.bot.send_message(
        chat_id=opv_user_id,
        text=message,
        parse_mode='Markdown'
    )

    # Обновляем контекст ОПВ
    context.application.user_data[opv_user_id]['task'] = {
        'task_id': task_id,
        'task_name': row['task_name'],
        'slot': row['slot'],
        'assigned_time': assigned_time,
        'дедлайн': total_duration,
        'status': 'На доработке',
        'duration': int(total_duration // 60)
    }

    # Очищаем фото
    context.application.user_data[opv_user_id].pop('photo', None)
    await update.message.reply_text("📤 Задание возвращено в работу. ОПВ уведомлён.")



async def show_opv_list(update: Update, context: CallbackContext):
    """Показывает меню списка ОПВ"""
    query = update.callback_query
    await query.answer()

    keyboard = [
        [InlineKeyboardButton("✅ Список ОПВ - свободные", callback_data='opv_free')],
        [InlineKeyboardButton("⏳ Список ОПВ - занятые", callback_data='opv_busy')]
    ]
    reply_markup = InlineKeyboardMarkup(keyboard)
    await query.edit_message_text("Выберите категорию ОПВ:", reply_markup=reply_markup)

async def show_opv_free(update: Update, context: CallbackContext):
    """Показывает список свободных ОПВ"""
    query = update.callback_query
    await query.answer()

    df = SQL.sql_select('wms', """
SELECT DISTINCT sh.employee_id, sh.role, sh.shift_type, concat(bs."name", ' ', bs.surname) user_name
        FROM wms_bot.shift_sessions1 sh
        LEFT JOIN (
            SELECT user_id, MAX(time_end) AS task_end
            FROM wms_bot.shift_tasks where task_date =current_date
            GROUP BY user_id
        ) t ON t.user_id = sh.employee_id::int
        left join wms_bot.t_staff bs ON bs.id = sh.employee_id::int
        WHERE sh.end_time IS NULL  
          AND t.task_end IS NOT NULL   and sh.role ='opv'
        ORDER BY user_name
    """)

    if df.empty:
        await query.edit_message_text("Нет свободных ОПВ на смене.")
        return

    keyboard = [
        [InlineKeyboardButton(row['user_name'], callback_data=f"opv_{row['employee_id']}")]
        for _, row in df.iterrows()
    ]
    reply_markup = InlineKeyboardMarkup(keyboard)
    await query.edit_message_text("✅ Свободные ОПВ (задание завершено):", reply_markup=reply_markup)

async def show_opv_busy(update: Update, context: CallbackContext):
    """Показывает список занятых ОПВ"""
    query = update.callback_query
    await query.answer()

    df = SQL.sql_select('wms', """
        SELECT DISTINCT 
            sh.employee_id, 
            sh.role, 
            sh.shift_type, 
            concat(bs."name", ' ', bs.surname) user_name
        FROM wms_bot.shift_sessions1 sh
        LEFT JOIN (
            SELECT 
                user_id, 
                status
            FROM wms_bot.shift_tasks
            GROUP BY user_id,status
        ) t ON t.user_id = sh.employee_id ::int
        left jOIN wms_bot.t_staff bs ON bs.id = sh.employee_id::int
        WHERE sh.end_time IS NULL  
          AND t.status in('Выполняется','На доработке','Заморожено')
          AND sh.role NOT IN ('zs')      
        ORDER BY user_name;
    """)

    if df.empty:
        await query.edit_message_text("Нет занятых ОПВ на смене.")
        return

    keyboard = []
    for _, row in df.iterrows():
        user_name = str(row.get('user_name', '')).strip()
        if user_name:  # Пропускаем пустые имена
            keyboard.append([
                InlineKeyboardButton(text=user_name, callback_data=f"opv_{row['employee_id']}")
            ])

    if not keyboard:
        await query.edit_message_text("⏳ Нет валидных данных для отображения.")
        return

    reply_markup = InlineKeyboardMarkup(keyboard)
    await query.edit_message_text("⏳ Занятые ОПВ (задание выполняется):", reply_markup=reply_markup)

async def show_opv_completed_list(update: Update, context: CallbackContext):
    """Показывает список ОПВ, завершивших смену"""
    query = update.callback_query
    await query.answer()
    shift = context.user_data.get('shift')

    try:
        completed_df = SQL.sql_select('wms', f"""
            SELECT DISTINCT st.user_id,concat(bs."name", ' ', bs.surname)
            FROM wms_bot.shift_tasks st
            left join wms_bot.shift_sessions1 ss on ss.employee_id::int =st.user_id 
            left join wms_bot.t_staff bs on bs.id=st.user_id 
            WHERE shift = '{shift}' AND st.status = 'Проверено' and ss.end_time is not null and ss.end_time ::date=current_date 
        """)

        if completed_df.empty:
            await query.edit_message_text("Пока никто не завершил смену.")
            return

        keyboard = [
            [InlineKeyboardButton(f"{row['user_name']}", callback_data=f"completed_{row['employee_id']}")]
            for _, row in completed_df.iterrows()
        ]
        reply_markup = InlineKeyboardMarkup(keyboard)
        await query.edit_message_text("✅ ОПВ, завершившие смену:", reply_markup=reply_markup)

    except Exception as e:
        print(f"❌ Ошибка при получении завершивших ОПВ: {e}")
        await query.edit_message_text("Ошибка при получении данных.")

async def show_opv_summary(update: Update, context: CallbackContext):
    """Показывает статистику по ОПВ"""
    query = update.callback_query
    await query.answer()
    employee_id = query.data.replace('completed_', '')

    try:
        summary_df = SQL.sql_select('stock', f"""
            SELECT user_id, COUNT(DISTINCT id) AS task_count
            FROM wms_bot.shift_tasks
            WHERE user_id = '{employee_id}' AND status = 'Проверено'
            GROUP BY user_id
        """)

        if summary_df.empty:
            await query.edit_message_text("Нет завершённых заданий у этого ОПВ.")
            return

        row = summary_df.iloc[0]
        message = (
            f"📊 *Данные по смене:*\n"
            f"👤 *ФИО:* {row['user_name']}\n"
            f"✅ *Кол-во выполненных задач:* {row['task_count']}"
        )
        await query.edit_message_text(message, parse_mode='Markdown')

    except Exception as e:
        print(f"❌ Ошибка при выводе данных по ОПВ: {e}")
        await query.edit_message_text("Ошибка при получении данных.")

async def shift_end(update: Update, context: CallbackContext):
    """Завершение смены"""
    staff_id = context.user_data.get('staff_id')
    role = context.user_data.get('role')

    if not staff_id or not role:
        await update.message.reply_text("Сначала начните смену командой /start.")
        return

    # Проверяем активные задания перед завершением смены
    active_tasks_df = SQL.sql_select('wms', f"""
        SELECT id FROM wms_bot.shift_tasks
        WHERE user_id = '{staff_id}'
          AND status IN ('Выполняется', 'На доработке', 'Заморожено')
    """)

    if not active_tasks_df.empty:
        await update.message.reply_text("⚠️ У вас есть незавершённые задания. Завершите их перед завершением смены.")
        return

    now = datetime.now()

    try:
        # Обновляем конец смены
        SQL.sql_delete('wms', f"""
            UPDATE wms_bot.shift_sessions1
            SET end_time = '{now}' 
            WHERE employee_id = '{staff_id}' AND end_time IS NULL
        """)

        # Получаем данные смены
        session_df = SQL.sql_select('wms', f"""
            SELECT start_time
            FROM wms_bot.shift_sessions1
            WHERE employee_id = '{staff_id}'
              AND end_time = '{now}'
            ORDER BY start_time DESC LIMIT 1
        """)

        if session_df.empty:
            await update.message.reply_text("Не удалось найти активную смену.")
            return

        # Рассчитываем продолжительность
        start_time = pd.to_datetime(session_df.iloc[0]['start_time'])
        duration = now - start_time
        duration_hours = round(duration.total_seconds() / 3600, 2)

        await update.message.reply_text(
            f"🕒 Смена завершена!\n"
            f"⌛ Отработано: *{duration_hours} ч*",
            parse_mode='Markdown'
        )

    except Exception as e:
        print(f"❌ Ошибка завершения смены: {e}")
        await update.message.reply_text("Ошибка при завершении смены. Обратитесь к администратору.")



from telegram.ext import ContextTypes

async def auto_close_expired_tasks(context: ContextTypes.DEFAULT_TYPE):
    """Автоматически закрывает задания в статусах 'Выполняется' или 'Заморожено'"""
    try:
        now = datetime.now()
        now_str = now.strftime('%Y-%m-%d %H:%M:%S')

        expired_df = SQL.sql_select('wms', f"""
            SELECT id, user_id, task_name, time_begin, status
            FROM wms_bot.shift_tasks
            WHERE status IN ('Выполняется', 'Заморожено','Ожидает проверки') 
              AND task_date IN (current_date, current_date - 1)
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




async def exit_session(update: Update, context: CallbackContext):
    """Выход из текущей сессии"""
    role = context.user_data.get('role')

    if role == 'opv':
        keyboard = [[InlineKeyboardButton("Получить задание", callback_data='get_task')]]
        reply_markup = InlineKeyboardMarkup(keyboard)
        await update.message.reply_text("Вы завершили текущую сессию. Готовы взять новое задание? 👇", reply_markup=reply_markup)
    elif role == 'zs':
        keyboard = [
            [InlineKeyboardButton("Список ОПВ на смене 📋", callback_data='opv_list_on_shift')],
            [InlineKeyboardButton("Список ОПВ завершивших смену ✅", callback_data='opv_list_completed')]
        ]
        reply_markup = InlineKeyboardMarkup(keyboard)
        await update.message.reply_text("Вы завершили текущую сессию. Что хотите посмотреть?", reply_markup=reply_markup)
    else:
        await update.message.reply_text("Вы ещё не выбрали роль. Введите /start для начала работы.")

async def log_group_id(update: Update, context: CallbackContext):
    """Логирует ID чата (для отладки)"""
    chat_id = update.effective_chat.id
    sender = update.effective_user.full_name
    print(f"👤 {sender} написал в чат ID: {chat_id}")
    await update.message.reply_text(f"Chat ID: {chat_id}")


async def register_topic(update: Update, context: CallbackContext):#######НА УДАЛЕНИЕ
    topic_name = " ".join(context.args) if context.args else "Без названия"
    thread_id = update.message.message_thread_id

    await context.bot.send_message(
        chat_id=update.effective_chat.id,
        text=f"🔍 Регистрирую тему: {topic_name}",
        message_thread_id=thread_id
    )

    await update.message.reply_text(
        f"✅ Тема зарегистрирована: *{topic_name}*\n🧵 message_thread_id: `{thread_id}`",
        parse_mode='Markdown'
    )


# ======================
# ЗАПУСК БОТА
# ======================

def main():
    TOKEN = '8119695965:AAEQpNuryd5Re-CuW4o2RP9L1nZUG8dEtag'
    
    #application = Application.builder().token(TOKEN).build()
    application = Application.builder().token(TOKEN).build()

 

    # Обработчики команд
    application.add_handler(CommandHandler('start', start))
    application.add_handler(CommandHandler('shift_start', shift_start))
    application.add_handler(CommandHandler('shift_end', shift_end))
    application.add_handler(CommandHandler('exit', exit_session))
    application.add_handler(CommandHandler('complete_the_task', complete_the_task))
    application.add_handler(CommandHandler('complete_the_extra_task', complete_the_extra_task))
    application.add_handler(CommandHandler('force_close_tasks', auto_close_expired_tasks))
    # application.add_handler(CommandHandler('register_topic', register_topic))#######РАЗБИВКА




    # Обработчики сообщений
    # application.add_handler(MessageHandler(filters.TEXT, log_group_id))
    application.add_handler(MessageHandler(filters.CONTACT, handle_contact))
    application.add_handler(MessageHandler(filters.PHOTO, receive_photo))
    application.add_handler(MessageHandler(filters.TEXT & ~filters.COMMAND, receive_reject_reason))
    # application.add_handler(MessageHandler(filters.TEXT, debug_thread_id))#######РАЗБИВКА
    

    # Обработчики callback-запросов
    application.add_handler(CallbackQueryHandler(shift_choice, pattern='^(day|night)$'))
    application.add_handler(CallbackQueryHandler(role_choice, pattern='^(opv|zs)_(day|night)$'))
    application.add_handler(CallbackQueryHandler(sector_select_and_confirm, pattern='^sectorchoice_'))
    application.add_handler(CallbackQueryHandler(employment_type_choice, pattern='^employment_'))
    application.add_handler(CallbackQueryHandler(employment_type_choice, pattern='^employment_'))
    application.add_handler(CallbackQueryHandler(get_task, pattern='^get_task$'))
    application.add_handler(CallbackQueryHandler(show_opv_list, pattern='^opv_list_on_shift$'))
    application.add_handler(CallbackQueryHandler(show_opv_completed_list, pattern='^opv_list_completed$'))
    application.add_handler(CallbackQueryHandler(show_opv_summary, pattern='^completed_'))
    application.add_handler(CallbackQueryHandler(show_opv_free, pattern='^opv_free$'))
    application.add_handler(CallbackQueryHandler(show_opv_busy, pattern='^opv_busy$'))
    application.add_handler(CallbackQueryHandler(handle_review, pattern='^(approve|reject)_'))
    # application.add_handler(CallbackQueryHandler(confirm_task_from_zs, pattern='^confirm_'))
    application.add_handler(CallbackQueryHandler(start_reject_reason, pattern='^start_reject_'))
    
    application.job_queue.run_repeating(schedule_tasks_from_rules, interval=60, first=10)
    print("✅ Планировщик запущен")
    # Вечером в 9:00
    application.job_queue.run_daily(auto_close_expired_tasks, time(hour=4, minute=0))

    # Утром в 21:00
    application.job_queue.run_daily(auto_close_expired_tasks, time(hour=16, minute=0))

    application.run_polling()

if __name__ == '__main__':
    main()