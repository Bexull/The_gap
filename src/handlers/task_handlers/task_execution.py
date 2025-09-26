import pandas as pd
from datetime import datetime, timedelta
import datetime as dt
from telegram import Update
from telegram.ext import CallbackContext
from ...database.sql_client import SQL
from ...config.settings import MERCHANT_ID
from ...utils.task_utils import send_task_to_zs
from ...keyboards.opv_keyboards import get_photo_upload_keyboard, get_task_keyboard
from .special_task_completion import complete_special_task_directly
from .task_restoration import restore_frozen_task_if_needed


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
        try:
            task_df = SQL.sql_select('wms', f"""
                SELECT id, task_name, product_group, slot, time_begin, task_duration, comment, provider, priority
                FROM wms_bot.shift_tasks
                WHERE user_id = '{staff_id}' AND status IN ('Выполняется', 'На доработке')
                AND merchant_code = '{MERCHANT_ID}'
                ORDER BY time_begin DESC LIMIT 1
            """)
        except Exception as e:
            print(f"❌ Ошибка получения задания: {e}")
            await query.edit_message_text("❌ Ошибка подключения к базе данных. Попробуйте позже.")
            return

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
            'provider': row.get('provider', 'Не указан'),
            'assigned_time': assigned_time,
            'duration': row['task_duration'],
            'assigned_time': now,
            'priority': row.get('priority', '1')
        }
        context.user_data['task'] = task

    # Проверяем приоритет задания
    task_priority = task.get('priority', '1')
    
    # Если это спец-задание (приоритет 111) - завершаем без фото
    if str(task_priority) == '111':
        await complete_special_task_directly(update, context, task)
        return

    # Для обычных заданий проверяем фото
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
        now_str = now.strftime('%Y-%m-%d %H:%M:%S')
        SQL.sql_delete('wms', f"""
            UPDATE wms_bot.shift_tasks
            SET status = 'Ожидает проверки',
                time_end = '{now_str}'
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
        error_message = "⚠️ Ошибка при завершении задания. Попробуйте позже."
        
        await query.edit_message_text(error_message)


async def show_task(update: Update, context: CallbackContext):
    """Показывает активную задачу пользователя"""
    query = update.callback_query
    await query.answer()

    staff_id = context.user_data.get('staff_id')

    if not staff_id:
        await query.edit_message_text("⚠️ Ваш ID не найден в системе.")
        return

    # Проверяем и восстанавливаем замороженные задания если нужно
    await restore_frozen_task_if_needed(staff_id, context)

    # Сначала ищем активные задания (выполняющиеся)
    task_df = SQL.sql_select('wms', f"""
        SELECT id, task_name, product_group, slot, time_begin, task_duration, comment, status, provider
        FROM wms_bot.shift_tasks
        WHERE user_id = '{staff_id}' AND status = 'Выполняется'
        AND merchant_code = '{MERCHANT_ID}'
        ORDER BY time_begin DESC LIMIT 1
    """)

    # Если нет активных, ищем задания на доработке
    if task_df.empty:
        task_df = SQL.sql_select('wms', f"""
            SELECT id, task_name, product_group, slot, time_begin, task_duration, comment, status, provider
            FROM wms_bot.shift_tasks
            WHERE user_id = '{staff_id}' AND status = 'На доработке'
            AND merchant_code = '{MERCHANT_ID}'
            ORDER BY time_begin DESC LIMIT 1
        """)

    # Если нет заданий на доработке, ищем спец-задания с приоритетом 111
    if task_df.empty:
        task_df = SQL.sql_select('wms', f"""
            SELECT id, task_name, product_group, slot, time_begin, task_duration, comment, status, provider, priority
            FROM wms_bot.shift_tasks
            WHERE user_id = '{staff_id}' AND status = 'Выполняется'
            AND priority = '111'
            AND merchant_code = '{MERCHANT_ID}'
            ORDER BY time_begin DESC LIMIT 1
        """)
    
    # Если нет спец-заданий, ищем замороженные
    if task_df.empty:
        task_df = SQL.sql_select('wms', f"""
            SELECT id, task_name, product_group, slot, time_begin, task_duration, comment, status, provider, priority
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
        if row.get('priority') == '111':
            status_emoji = "🔥"
            status_text = "Спец-задание (приоритет 111)"
        else:
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
        f"🏢 Поставщик: {row.get('provider', 'Не указан')}\n"
        f"⏰ Время начала: {row['time_begin']}\n"
        f"⏳ Плановая длительность: {row['task_duration']} мин\n"
        f"{status_emoji} *Статус:* {status_text}"
    )
    if "comment" in row and row["comment"]:
        task_info += f"\n💬 Комментарий: {row['comment']}"

    # Показываем кнопку завершения для активных заданий и заданий на доработке
    if row['status'] in ['Выполняется', 'На доработке']:
        # Для спец-заданий показываем специальную кнопку
        if row.get('priority') == '111':
            from ...keyboards.opv_keyboards import get_special_task_keyboard
            await query.edit_message_text(task_info, parse_mode="Markdown", reply_markup=get_special_task_keyboard())
        else:
            await query.edit_message_text(task_info, parse_mode="Markdown", reply_markup=get_task_keyboard())
    else:
        # Для замороженных заданий показываем только информацию
        task_info += f"\n\n*ℹ️ Задание заморожено. Завершите спец-задание, чтобы продолжить.*"
        await query.edit_message_text(task_info, parse_mode="Markdown")


async def complete_the_task(update: Update, context: CallbackContext):
    """Завершение текущего задания (команда) - оставляем для совместимости"""
    staff_id = context.user_data.get('staff_id')
    
    if not staff_id:
        await update.message.reply_text("⚠️ Ваш ID не найден в системе.")
        return

    # Проверяем активное задание из базы
    task_df = SQL.sql_select('wms', f"""
        SELECT id, task_name, product_group, slot, time_begin, task_duration, comment, provider, priority
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
        'provider': row.get('provider', 'Не указан'),
        'assigned_time': assigned_time,
        'duration': row['task_duration'],
        'assigned_time': now,
        'priority': row.get('priority', '1')
    }
    context.user_data['task'] = task

    # Проверяем приоритет задания
    task_priority = task.get('priority', '1')
    
    # Если это спец-задание (приоритет 111) - завершаем без фото
    if str(task_priority) == '111':
        # Создаем фиктивный callback_query для совместимости
        class FakeCallbackQuery:
            def __init__(self, message):
                self.message = message
                self.data = None
            
            async def answer(self):
                pass
        
        fake_query = FakeCallbackQuery(update.message)
        # Создаем новый объект update с фиктивным callback_query
        fake_update = type('FakeUpdate', (), {
            'callback_query': fake_query,
            'effective_user': update.effective_user
        })()
        
        await complete_special_task_directly(fake_update, context, task)
        return

    # Для обычных заданий проверяем фото
    if 'photos' not in context.user_data:
        context.user_data.update({'photo_request_time': datetime.now(), 'photos': []})
        await update.message.reply_text(
            "📸 Сделайте не более 3 фото в течение 5 минут и отправьте боту.\n\n"
            "После отправки фото нажмите кнопку '✅ Завершить задачу'"
        )
        return

    if not context.user_data['photos']:
        await update.message.reply_text(
            "⚠️ Необходимо отправить хотя бы одно фото.\n\n"
            "Отправьте фото и нажмите кнопку '✅ Завершить задачу'"
        )
        return

    # Завершаем обычное задание
    now = datetime.now()
    time_spent = now - task['assigned_time']

    try:
        # Обновляем статус и время окончания
        now_str = now.strftime('%Y-%m-%d %H:%M:%S')
        SQL.sql_delete('wms', f"""
            UPDATE wms_bot.shift_tasks
            SET status = 'Ожидает проверки',
                time_end = '{now_str}'
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
            # Если сообщение не изменилось, просто игнорируем ошибку
            pass

    # ⚠️ Сообщение об опоздании — только один раз
    if is_late and not context.user_data['late_warning_sent']:
        context.user_data['late_warning_sent'] = True
        await update.message.reply_text(
            "⚠️ Вы отправили фотоотчет с опозданием.\n"
            "Пожалуйста, завершите задание вручную. ЗС проверит и подтвердит выполнение."
        )
