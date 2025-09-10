import asyncio
import pandas as pd
from datetime import datetime, timedelta
import datetime as dt
from telegram import Update
from telegram.ext import CallbackContext
from ..database.sql_client import SQL
from ..utils.time_utils import get_current_slot, get_task_date
from ..config.settings import MERCHANT_ID
from ..utils.task_utils import send_task_to_zs, check_user_task_status
from ..keyboards.opv_keyboards import get_next_task_keyboard, get_task_in_progress_keyboard, get_photo_upload_keyboard, get_task_keyboard
from telegram.ext import ContextTypes
import logging
logger = logging.getLogger(__name__)



async def get_task(update: Update, context: CallbackContext):
    """Получение нового задания"""
    query = update.callback_query
    await query.answer()
    
    employee_id = context.user_data.get('staff_id')
    if not employee_id:
        await query.edit_message_text("⚠️ Ваш ID не найден в системе.")
        return

    # Проверяем статус заданий пользователя
    status_check = check_user_task_status(employee_id)
    if status_check['blocked']:
        reply_markup = status_check.get('reply_markup')
        await query.edit_message_text(status_check['message'], reply_markup=reply_markup)
        return

    # Проверяем выбор сектора
    if not context.user_data.get('sector_selected'):
        sectors_df = SQL.sql_select('stock', """SELECT DISTINCT "Сектор" FROM public.task_schedule WHERE "Дата" = current_date""")
        sectors = sectors_df['Сектор'].dropna().tolist()
        
        from ..keyboards.opv_keyboards import get_sector_keyboard
        reply_markup = get_sector_keyboard(sectors)
        await query.edit_message_text("Выберите сектор, с которым будете работать:", reply_markup=reply_markup)
        return

    # Если сектор выбран - назначаем задание
    await assign_task_from_sector(update, context)

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
        print(f"🔎 assign_task_from_sector: staff_id={employee_id}, sector={sector}, shift={shift}")
        # Проверяем статус заданий пользователя
        status_check = check_user_task_status(employee_id)
        if status_check['blocked']:
            reply_markup = status_check.get('reply_markup')
            await query.edit_message_text(status_check['message'], reply_markup=reply_markup)
            return

        # Получаем пол
        gender_df = SQL.sql_select('wms', f"SELECT gender FROM wms_bot.t_staff WHERE id = '{employee_id}'")
        opv_gender = gender_df.iloc[0]['gender'].strip().upper() if not gender_df.empty else 'U'
        
        # Получаем ФИО из контекста
        operator_full_name = context.user_data.get('staff_name', 'ОПВ')

        # Переводим shift в нужный вид
        shift_ru = 'День' if shift == 'day' else 'Ночь'

        task_date = get_task_date(shift)
        current_slot = get_current_slot(shift)
        if current_slot is None:
            await query.edit_message_text("⏰ Сейчас не время активного слота.")
            return

        # Выбираем задание из shift_tasks
        sql_query = f"""
            SELECT * FROM wms_bot.shift_tasks
            WHERE task_date = '{task_date}'
              AND shift = '{shift_ru}'
              AND sector = '{sector}'
              AND slot = {current_slot}
              AND is_constant_task = true
              and merchant_code ='{MERCHANT_ID}'
              AND (status IS NULL OR status = 'В ожидании')
        """
        # print("🔎 SQL (assign_task_from_sector):", sql_query)
        task_df = SQL.sql_select('wms', sql_query)

        if task_df.empty:
            print("🔎 Результат пустой. Параметры:", {
                'task_date': str(task_date),
                'shift_ru': shift_ru,
                'sector': sector,
                'slot': current_slot,
                'merchant': MERCHANT_ID
            })
            # Доп. отладка: какие мерчанты есть под эти условия без фильтра
            dbg_merchants_sql = f"""
                SELECT merchant_code, COUNT(*) AS cnt
                FROM wms_bot.shift_tasks
                WHERE task_date = '{task_date}'
                  AND shift = '{shift_ru}'
                  AND sector = '{sector}'
                  AND slot = {current_slot}
                  AND is_constant_task = true
                GROUP BY merchant_code
                ORDER BY cnt DESC
            """
            try:
                dbg_df = SQL.sql_select('wms', dbg_merchants_sql)
                print("🔎 Merchants under same constraints:", dbg_df.to_dict(orient='records') if hasattr(dbg_df, 'to_dict') else dbg_df)
                # Без фильтра по слоту
                dbg_no_slot = f"""
                    SELECT merchant_code, slot, COUNT(*) cnt
                    FROM wms_bot.shift_tasks
                    WHERE task_date = '{task_date}'
                      AND shift = '{shift_ru}'
                      AND sector = '{sector}'
                      AND is_constant_task = true
                    GROUP BY merchant_code, slot
                    ORDER BY cnt DESC
                """
                dbg_no_slot_df = SQL.sql_select('wms', dbg_no_slot)
                print("🔎 Merchants by slot (no slot filter):", dbg_no_slot_df.to_dict(orient='records') if hasattr(dbg_no_slot_df, 'to_dict') else dbg_no_slot_df)
                # Без фильтра по сектору
                dbg_no_sector = f"""
                    SELECT merchant_code, sector, COUNT(*) cnt
                    FROM wms_bot.shift_tasks
                    WHERE task_date = '{task_date}'
                      AND shift = '{shift_ru}'
                      AND slot = {current_slot}
                      AND is_constant_task = true
                    GROUP BY merchant_code, sector
                    ORDER BY cnt DESC
                """
                dbg_no_sector_df = SQL.sql_select('wms', dbg_no_sector)
                print("🔎 Merchants by sector (no sector filter):", dbg_no_sector_df.to_dict(orient='records') if hasattr(dbg_no_sector_df, 'to_dict') else dbg_no_sector_df)
                # Сводка по доступным сменам для 5001
                dbg_shifts_5001 = f"""
                    SELECT shift, COUNT(*) cnt
                    FROM wms_bot.shift_tasks
                    WHERE task_date = '{task_date}'
                      AND merchant_code = '{MERCHANT_ID}'
                    GROUP BY shift
                """
                dbg_shifts_df = SQL.sql_select('wms', dbg_shifts_5001)
                print("🔎 Shifts present for merchant:", dbg_shifts_df.to_dict(orient='records') if hasattr(dbg_shifts_df, 'to_dict') else dbg_shifts_df)
            except Exception as e:
                print("⚠️ Debug merchants query failed:", e)
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

        # Обновляем статус задания + записываем ФИО оператора
        employment_type = context.user_data.get('employment_type', 'main')
        SQL.sql_delete('wms', f"""
            UPDATE wms_bot.shift_tasks
            SET status = 'Выполняется',
                user_id = '{employee_id}',
                time_begin = '{now}',
                part_time = '{employment_type}',
                operator_name = '{operator_full_name}'
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

        # Добавляем клавиатуру с кнопкой "Завершить задачу"
        reply_markup = get_task_in_progress_keyboard()
        sent_msg = await query.edit_message_text(message, parse_mode='Markdown', reply_markup=reply_markup)

        # 💥💥💥 Запускаем таймер!
        asyncio.create_task(
            update_timer(context, sent_msg.chat_id, sent_msg.message_id, task, total_seconds, reply_markup)
        )

    except Exception as e:
        print(f"❌ Ошибка в assign_task_from_sector: {e}")
        await query.edit_message_text("⚠️ Произошла ошибка при назначении задания.")

async def update_timer(context, chat_id, message_id, task, total_seconds, reply_markup=None, comment=None):
    """Обновление таймера задания"""
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
                parse_mode='Markdown',
                reply_markup=reply_markup
            )

            await asyncio.sleep(15)
        except Exception as e:
            print(f"⚠️ Ошибка обновления таймера: {e}")
            break

async def complete_task_inline(update: Update, context: CallbackContext):
    """Обработчик инлайн кнопки 'Завершить задачу'"""
    query = update.callback_query
    await query.answer()
    
    user_id = update.effective_user.id
    staff_id = context.user_data.get('staff_id')
    task = context.user_data.get('task')

    if not task:
        staff_id = context.user_data.get('staff_id')
        if not staff_id:
            await query.edit_message_text("⚠️ Ваш ID не найден в системе.")
            return

        # Проверяем активное задание из базы
        task_df = SQL.sql_select('wms', f"""
            SELECT id, task_name, product_group, slot, time_begin, task_duration, comment
            FROM wms_bot.shift_tasks
            WHERE user_id = '{staff_id}' AND status IN ('Выполняется', 'На доработке')
            AND merchant_code = '{MERCHANT_ID}'
            ORDER BY time_begin DESC LIMIT 1
        """)

        if task_df.empty:
            await query.edit_message_text("❌ Нет активного задания.")
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
        reply_markup = get_photo_upload_keyboard()
        await query.edit_message_text(
            "📸 Сделайте не более 3 фото в течение 5 минут и отправьте боту.\n\n"
            "После отправки фото нажмите кнопку '✅ Завершить задачу'",
            reply_markup=reply_markup
        )
        return

    if not context.user_data['photos']:
        reply_markup = get_photo_upload_keyboard()
        await query.edit_message_text(
            "⚠️ Необходимо отправить хотя бы одно фото.\n\n"
            "Отправьте фото и нажмите кнопку '✅ Завершить задачу'",
            reply_markup=reply_markup
        )
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

        await query.edit_message_text("✅ Задание отправлено на проверку заведующему.")
        await send_task_to_zs(context, task, context.user_data['photos'])

        # Очищаем контекст
        context.user_data.pop('task', None)
        context.user_data.pop('photos', None)
        context.user_data.pop('photo_request_time', None)
        context.user_data['late_warning_sent'] = False

        # Показываем сообщение о том, что задание отправлено на проверку
        await context.bot.send_message(
            chat_id=query.message.chat_id,
            text="✅ Задание отправлено на проверку заведующему.\n\n⏳ Дождитесь подтверждения перед тем как брать новое задание."
        )

    except Exception as e:
        print(f"❌ Ошибка завершения задания: {e}")
        error_message = "⚠️ Ошибка при завершении задания. Попробуйте позже."
        
        # Добавляем дополнительную информацию для отладки
        if "Message thread not found" in str(e):
            sector = context.user_data.get('sector', 'неизвестен')
            error_message += f"\n\n🔍 Отладочная информация:\nСектор: {sector}\nПроблема: Топик для сектора не найден"
            print(f"🔍 Отладочная информация - Сектор: {sector}")
        
        await query.edit_message_text(error_message)

async def show_task(update: Update, context: CallbackContext):
    """Показывает активную задачу пользователя"""
    query = update.callback_query
    await query.answer()

    staff_id = context.user_data.get('staff_id')

    if not staff_id:
        await query.edit_message_text("⚠️ Ваш ID не найден в системе.")
        return

    # Сначала ищем активные задания (выполняющиеся)
    task_df = SQL.sql_select('wms', f"""
        SELECT id, task_name, product_group, slot, time_begin, task_duration, comment, status
        FROM wms_bot.shift_tasks
        WHERE user_id = '{staff_id}' AND status = 'Выполняется'
        AND merchant_code = '{MERCHANT_ID}'
        ORDER BY time_begin DESC LIMIT 1
    """)

    # Если нет активных, ищем задания на доработке
    if task_df.empty:
        task_df = SQL.sql_select('wms', f"""
            SELECT id, task_name, product_group, slot, time_begin, task_duration, comment, status
            FROM wms_bot.shift_tasks
            WHERE user_id = '{staff_id}' AND status = 'На доработке'
            AND merchant_code = '{MERCHANT_ID}'
            ORDER BY time_begin DESC LIMIT 1
        """)

    # Если нет заданий на доработке, ищем замороженные
    if task_df.empty:
        task_df = SQL.sql_select('wms', f"""
            SELECT id, task_name, product_group, slot, time_begin, task_duration, comment, status
            FROM wms_bot.shift_tasks
            WHERE user_id = '{staff_id}' AND status = 'Заморожено'
            AND merchant_code = '{MERCHANT_ID}'
            ORDER BY time_begin DESC LIMIT 1
        """)

    if task_df.empty:
        await query.edit_message_text("❌ У вас нет активных заданий.")
        return

    row = task_df.iloc[0]

    # Определяем статус задания для отображения
    if row['status'] == 'Выполняется':
        status_emoji = "▶️"
        status_text = "Выполняется"
    elif row['status'] == 'На доработке':
        status_emoji = "🔄"
        status_text = "На доработке"
    else:  # Заморожено
        status_emoji = "❄️"
        status_text = "Заморожено (из-за спец-задания)"

    # Формируем сообщение с задачей
    task_info = (
        f"📋 *Текущее задание*\n\n"
        f"🆔 ID: `{row['id']}`\n"
        f"📌 Название: *{row['task_name']}*\n"
        f"📦 Группа: {row['product_group']}\n"
        f"📍 Слот: {row['slot']}\n"
        f"⏰ Время начала: {row['time_begin']}\n"
        f"⏳ Плановая длительность: {row['task_duration']} мин\n"
        f"{status_emoji} *Статус:* {status_text}"
    )
    if "comment" in row and row["comment"]:
        task_info += f"\n💬 Комментарий: {row['comment']}"

    # Показываем кнопку завершения для активных заданий и заданий на доработке
    if row['status'] in ['Выполняется', 'На доработке']:
        await query.edit_message_text(task_info, parse_mode="Markdown", reply_markup=get_task_keyboard())
    else:
        # Для замороженных заданий показываем только информацию
        task_info += f"\n\n*ℹ️ Задание заморожено. Завершите спец-задание, чтобы продолжить.*"
        await query.edit_message_text(task_info, parse_mode="Markdown")



async def complete_the_task(update: Update, context: CallbackContext):
    """Завершение текущего задания (команда) - оставляем для совместимости"""
    await complete_task_inline(update, context)

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

    # Проверяем лимит фото
    if len(context.user_data['photos']) >= 3:   # лимит 3 фото
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

    # Если это первое фото — отправляем сообщение с прогрессом
    if 'progress_message' not in context.user_data:
        sent = await update.message.reply_text(f"📸 Фото {photo_num}/3 получено.")
        context.user_data['progress_message'] = sent.message_id
    else:
        # Обновляем старое сообщение
        try:
            new_text = (
                f"📸 Фото {photo_num}/3 получено."
                if photo_num < 3 else
                f"📸 Фото {photo_num}/3 получено. ✅ Все фото загружены!"
            )
            new_markup = get_task_keyboard() if photo_num == 3 else None
            
            await context.bot.edit_message_text(
                chat_id=update.effective_chat.id,
                message_id=context.user_data['progress_message'],
                text=new_text,
                reply_markup=new_markup
            )
        except Exception as e:
            if "Message is not modified" not in str(e):
                print(f"⚠️ Ошибка при обновлении сообщения: {e}")
            # Если сообщение не изменилось, просто игнорируем ошибку
   

    # ⚠️ Сообщение об опоздании — только один раз
    if is_late and not context.user_data['late_warning_sent']:
        context.user_data['late_warning_sent'] = True
        await update.message.reply_text(
            "⚠️ Вы отправили фотоотчет с опозданием.\n"
            "Пожалуйста, завершите задание вручную. ЗС проверит и подтвердит выполнение."
        )

async def complete_special_task_inline(update: Update, context: CallbackContext):
    """Завершает спец-задание через инлайн кнопку"""
    query = update.callback_query
    await query.answer()
    
    staff_id = context.user_data.get('staff_id')
    if not staff_id:
        await query.edit_message_text("⚠️ Ваш ID не найден в системе.")
        return

    # Проверяем активное спец-задание
    extra_task_df = SQL.sql_select('wms', f"""
        SELECT id, task_name, time_begin
        FROM wms_bot.shift_tasks
        WHERE user_id = '{staff_id}'
          AND status = 'Выполняется'
          AND time_end IS NULL
          AND priority = '111'
          AND merchant_code = '{MERCHANT_ID}'
        ORDER BY time_begin DESC LIMIT 1
    """)

    if extra_task_df.empty:
        await query.edit_message_text("У вас нет активных спец-заданий.")
        return

    task = extra_task_df.iloc[0]
    now = datetime.now()

    # Получаем время задания для учета в отработанном времени
    from ..utils.task_utils import parse_task_duration
    task_duration_df = SQL.sql_select('wms', f"""
        SELECT task_duration FROM wms_bot.shift_tasks WHERE id = {task['id']} AND merchant_code = '{MERCHANT_ID}'
    """)
    
    if not task_duration_df.empty:
        duration_raw = task_duration_df.iloc[0]['task_duration']
        task_seconds = parse_task_duration(duration_raw)
        
        # Добавляем время к отработанному
        current_worked = context.user_data.get('worked_seconds', 0)
        new_worked = current_worked + task_seconds
        context.user_data['worked_seconds'] = new_worked
        
        print(f"⏰ Обновлено время работы (спец-задание): {current_worked}s + {task_seconds}s = {new_worked}s")

    # Завершаем спец-задание
    SQL.sql_delete('wms', f"""
        UPDATE wms_bot.shift_tasks
        SET status = 'Проверено',
            time_end = '{now}'
        WHERE id = {task['id']}
    """)

    # Проверяем замороженные задания и восстанавливаем их
    frozen_task_df = SQL.sql_select('wms', f"""
        SELECT id, task_name, product_group, slot, task_duration, comment
        FROM wms_bot.shift_tasks
        WHERE user_id = '{staff_id}'
          AND status = 'Заморожено'
          AND time_end IS NULL
          AND merchant_code = '{MERCHANT_ID}'
        ORDER BY time_begin DESC LIMIT 1
    """)

    success_message = f"✅ Спец-задание №{task['id']} ({task['task_name']}) завершено!"

    if not frozen_task_df.empty:
        # Восстанавливаем замороженное задание
        frozen_task = frozen_task_df.iloc[0]
        SQL.sql_delete('wms', f"""
            UPDATE wms_bot.shift_tasks
            SET status = 'Выполняется'
            WHERE id = {frozen_task['id']}
        """)

        # Показываем восстановленное задание с кнопкой завершения
        from ..keyboards.opv_keyboards import get_task_keyboard
        
        task_info = (
            f"{success_message}\n\n"
            f"🔄 *Восстановлено дефолтное задание:*\n\n"
            f"🆔 ID: `{frozen_task['id']}`\n"
            f"📌 Название: *{frozen_task['task_name']}*\n"
            f"📦 Группа: {frozen_task['product_group']}\n"
            f"📍 Слот: {frozen_task['slot']}\n"
            f"⏳ Длительность: {frozen_task['task_duration']} мин"
        )
        
        if frozen_task['comment']:
            task_info += f"\n💬 Комментарий: {frozen_task['comment']}"

        await query.edit_message_text(
            task_info, 
            parse_mode="Markdown", 
            reply_markup=get_task_keyboard()
        )
    else:
        # Просто показываем сообщение об успешном завершении
        await query.edit_message_text(success_message)

async def complete_the_extra_task(update: Update, context: CallbackContext):
    """Завершает активное дополнительное задание из расписания (priority = 111) - команда"""
    staff_id = context.user_data.get('staff_id')

    # Проверяем активное доп. задание
    extra_task_df = SQL.sql_select('wms', f"""
        SELECT id, task_name, time_begin
        FROM wms_bot.shift_tasks
        WHERE user_id = '{staff_id}'
          AND status = 'Выполняется'
          AND time_end IS NULL
          AND priority = '111'
          AND merchant_code = '{MERCHANT_ID}'
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
            WHERE user_id = '{staff_id}' AND status IN ('Выполняется', 'На доработке')
            AND merchant_code = '{MERCHANT_ID}'
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
                        logger.debug(f"Пропущено сообщение {msg_id} (начинается с @)")
                    
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
                            logger.debug(f"Пропущено сообщение {msg_id} (возможно закреплено): {e}")
                        else:
                            errors_count += 1
                            logger.debug(f"Ошибка удаления сообщения {msg_id}: {e}")
            
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
            logger.debug(f"Не удалось удалить команду: {e}")
        
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
    from telegram import InlineKeyboardButton, InlineKeyboardMarkup
    
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


# Callback handler для обработки нажатий на кнопки подтверждения
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
