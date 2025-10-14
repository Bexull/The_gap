import asyncio
import pandas as pd
from datetime import datetime, timedelta
import datetime as dt
from telegram import Update
from telegram.ext import CallbackContext
from ..database.sql_client import SQL
from ..keyboards.zs_keyboards import get_zs_main_menu_keyboard, get_opv_list_keyboard, get_opv_names_keyboard
from ..keyboards.opv_keyboards import get_next_task_keyboard, get_task_keyboard
from ..config.settings import ZS_GROUP_CHAT_ID, MERCHANT_ID
from ..utils.freeze_time_utils import read_freeze_time
from telegram import InlineKeyboardButton, InlineKeyboardMarkup, ForceReply


def safe_update_user_data(application, user_id, updates):
    """Безопасно обновляет user_data для предотвращения ошибок mappingproxy"""
    try:
        # Получаем текущий контекст
        current_data = application.user_data.get(user_id, {})
        
        # Создаем новый словарь
        new_data = dict(current_data)
        
        # Применяем обновления
        new_data.update(updates)
        
        # Присваиваем новый словарь
        application.user_data[user_id] = new_data
        
        return True
    except Exception as e:
        return False


async def show_opv_list(update: Update, context: CallbackContext):
    """Показывает меню списка ОПВ"""
    query = update.callback_query
    await query.answer()

    reply_markup = get_opv_list_keyboard()
    await query.edit_message_text("Выберите категорию ОПВ:", reply_markup=reply_markup)

async def show_opv_free(update: Update, context: CallbackContext):
    """Показывает список свободных ОПВ"""
    query = update.callback_query
    await query.answer()

    df = SQL.sql_select('wms', f"""
SELECT DISTINCT sh.employee_id, sh.role, sh.shift_type, concat(bs."name", ' ', bs.surname) user_name
        FROM wms_bot.shift_sessions1 sh
        LEFT JOIN (
            SELECT user_id, MAX(time_end) AS task_end
            FROM wms_bot.shift_tasks where task_date =current_date AND merchant_code = '{MERCHANT_ID}'
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

    reply_markup = get_opv_names_keyboard(df, 'opv')
    await query.edit_message_text("✅ Свободные ОПВ (задание завершено):", reply_markup=reply_markup)

async def show_opv_busy(update: Update, context: CallbackContext):
    """Показывает список занятых ОПВ"""
    query = update.callback_query
    await query.answer()

    df = SQL.sql_select('wms', f"""
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
            WHERE merchant_code = '{MERCHANT_ID}'
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
            AND st.merchant_code = '{MERCHANT_ID}'
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
            WHERE user_id = '{employee_id}' AND status = 'Проверено' AND merchant_code = '{MERCHANT_ID}'
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
        await query.edit_message_text("Ошибка при получении данных.")

async def handle_review(update: Update, context: CallbackContext):
    """Обработчик подтверждения задания ЗС (ТОЛЬКО approve)"""
    query = update.callback_query
    await query.answer()

    action, data = query.data.split('_', 1)  # Используем split с лимитом
    task_id, opv_id = data.split('|')

    now = datetime.now()
    

    if action == 'approve':
        # Получаем имя инспектора
        inspector_df = SQL.sql_select('wms', f"""
            SELECT fio FROM wms_bot.bot_auth WHERE userid = {update.effective_user.id}
        """)
        inspector_name = inspector_df.iloc[0]['fio'] if not inspector_df.empty else 'Неизвестно'

        # Обновляем статус и инспектора
        now_str = now.strftime('%Y-%m-%d %H:%M:%S')
        SQL.sql_delete('wms', f"""
            UPDATE wms_bot.shift_tasks
            SET status = 'Проверено',
                time_end = '{now_str}',
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
            
            # Получаем выделенное время задания и добавляем к отработанному
            task_row = SQL.sql_select('wms', f"SELECT task_duration FROM wms_bot.shift_tasks WHERE id = {task_id}")
            if not task_row.empty:
                from ..utils.task_utils import parse_task_duration, add_worked_time
                duration_raw = task_row.iloc[0]['task_duration']
                task_seconds = parse_task_duration(duration_raw)
                
                # Добавляем время к отработанному (используем правильный контекст)
                # Получаем контекст пользователя через application
                try:
                    user_context = context.application.user_data.get(opv_user_id, {})
                    current_worked = user_context.get('worked_seconds', 0)
                    new_worked = current_worked + task_seconds
                    
                    # Используем безопасную функцию обновления
                    success = safe_update_user_data(
                        context.application, 
                        opv_user_id, 
                        {'worked_seconds': new_worked}
                    )
                    
                    if success:
                        pass
                    else:
                        pass
                except Exception as e:
                    # Если не удалось обновить контекст, просто логируем
                    pass
            
            try:
                await context.bot.send_message(
                    chat_id=opv_user_id,
                    text=f"✅ Задание №{task_id} *подтверждено* заведующим. Отличная работа!",
                    parse_mode='Markdown'
                )

                reply_markup = get_next_task_keyboard()
                await context.bot.send_message(
                    chat_id=opv_user_id,
                    text="Хотите взять следующее задание? 👇",
                    reply_markup=reply_markup
                )
            except Exception as e:
                # Отправляем уведомление в группу ЗС
                await context.bot.send_message(
                    chat_id=ZS_GROUP_CHAT_ID,
                    text=f"⚠️ Не удалось отправить уведомление ОПВ {opv_id} о подтверждении задания {task_id}. Ошибка: {e}"
                )
    else:
        # Если это не approve - значит что-то пошло не так
        await query.edit_message_text("⚠️ Ошибка обработки действия.")


async def start_reject_reason(update: Update, context: CallbackContext):
    """Начало процесса возврата задания с указанием причины"""
    query = update.callback_query
    await query.answer()

    # Правильно парсим callback_data
    callback_data = query.data
    task_num, opv_id = callback_data.replace("start_reject_", "").split("|")
    
    context.user_data.update({
        'reject_task_id': task_num,
        'reject_opv_id': opv_id
    })
    

    # Отправляем отдельное сообщение с ForceReply для запроса причины
    try:
        thread_id = getattr(query.message, 'message_thread_id', None)
        if thread_id is not None:
            await context.bot.send_message(
                chat_id=query.message.chat_id,
                text="✏️ Пожалуйста, укажите причину возврата задания (ответьте на это сообщение):",
                reply_markup=ForceReply(selective=True),
                message_thread_id=thread_id
            )
        else:
            await context.bot.send_message(
                chat_id=query.message.chat_id,
                text="✏️ Пожалуйста, укажите причину возврата задания (ответьте на это сообщение):",
                reply_markup=ForceReply(selective=True)
            )
    except Exception as e:
        pass


async def receive_reject_reason(update: Update, context: CallbackContext):
    """Обработчик причины возврата задания - только для ЗС"""
    
    user_id = update.effective_user.id
    task_id = context.user_data.get('reject_task_id')
    opv_id = context.user_data.get('reject_opv_id')
    
    
    # КРИТИЧЕСКИ ВАЖНО: проверяем, что это именно ЗС, который ждет ввода причины
    if not task_id or not opv_id:
        return  # Просто игнорируем сообщение
    
    # Дополнительная проверка: если сообщение начинается с команды, игнорируем
    message_text = update.message.text.strip() if update.message.text else ""
    if message_text.startswith('/') or len(message_text) < 3:
        return
    
    reason = message_text

    try:
        
        # Сначала отправляем подтверждение ЗС
        await update.message.reply_text(f"⏳ Обрабатываю возврат задания №{task_id}...")
        
        # Проверяем, что задание существует
        task_check_df = SQL.sql_select('wms', f"""
            SELECT id, user_id, status FROM wms_bot.shift_tasks WHERE id = {task_id}
        """)
        
        if task_check_df.empty:
            await update.message.reply_text("⚠️ Задание не найдено или уже обработано.")
            context.user_data.pop('reject_task_id', None)
            context.user_data.pop('reject_opv_id', None)
            return
            
        current_status = task_check_df.iloc[0]['status']
        
        # Разрешаем возврат, когда задача на проверке/ожидает проверки, выполняется или уже помечена на доработку
        if current_status not in ['На проверке', 'Выполняется', 'Ожидает проверки']:
            await update.message.reply_text(f"⚠️ Задание уже имеет статус '{current_status}' и не может быть возвращено.")
            context.user_data.pop('reject_task_id', None)
            context.user_data.pop('reject_opv_id', None)
            return

        # Обновляем статус с экранированием кавычек
        escaped_reason = reason.replace("'", "''")
        now_str = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
        
        SQL.sql_delete('wms', f"""
            UPDATE wms_bot.shift_tasks
            SET status = 'На доработке',
                time_begin = '{now_str}'
            WHERE id = {task_id}
        """)
        

        # Получаем данные задания для уведомления
        task_df = SQL.sql_select('wms', f"""
            SELECT user_id, task_name, slot, time_begin, task_duration, product_group
            FROM wms_bot.shift_tasks
            WHERE id = {task_id}
        """)
        
        if task_df.empty:
            await update.message.reply_text("⚠️ Не найдено задание для возврата.")
            context.user_data.pop('reject_task_id', None)
            context.user_data.pop('reject_opv_id', None)
            return

        row = task_df.iloc[0]
        opv_employee_id = row['user_id']

        # Получаем Telegram ID сотрудника
        opv_userid_df = SQL.sql_select('wms', f"""
            SELECT userid FROM wms_bot.bot_auth WHERE employee_id = '{opv_employee_id}'
        """)
        
        if opv_userid_df.empty:
            await update.message.reply_text("⚠️ У сотрудника не зарегистрирован Telegram ID.")
            context.user_data.pop('reject_task_id', None)
            context.user_data.pop('reject_opv_id', None)
            return

        opv_user_id = int(opv_userid_df.iloc[0]['userid'])

        # Обработка времени
        if isinstance(row['time_begin'], dt.time):
            assigned_time = datetime.combine(datetime.today(), row['time_begin'])
        else:
            assigned_time = pd.to_datetime(row['time_begin'])

        # 1. Читаем elapsed из БД
        elapsed_seconds = read_freeze_time(task_id)
        
        # 2. Вычисляем allocated и remaining
        total_duration = (
            row['task_duration'].hour * 3600 + row['task_duration'].minute * 60 + row['task_duration'].second
            if isinstance(row['task_duration'], dt.time)
            else 900
        )
        remaining_seconds = max(0, total_duration - elapsed_seconds)

        from ..utils.time_utils import seconds_to_hms

        # 3. Формируем сообщение для ОПВ с информацией о времени
        message = (
            f"⚠️ Задание №{task_id} вернули на доработку.\n"
            f"📝 Причина: {reason}\n\n"
            f"📋 *Задание повторно активировано:*\n"
            f"📍 *Слот:* {row['slot']}\n"
            f"📝 *Наименование:* {row['task_name']}\n"
            f"📦 *Группа товаров:* {row.get('product_group', '—')}\n"
            f"⏱ *Выделенное время:* {seconds_to_hms(total_duration)}\n"
            f"⏳ *Оставшееся время:* {seconds_to_hms(remaining_seconds)}"
        )
        
        if elapsed_seconds > 0:
            message += f"\n⏱ *Уже затрачено:* {seconds_to_hms(elapsed_seconds)}"
        


        # Отправляем уведомление ОПВ
        try:
            sent_message = await context.bot.send_message(
                chat_id=opv_user_id,
                text=message,
                parse_mode='Markdown',
                reply_markup=get_task_keyboard()
            )

            from ..config.settings import active_timers
            from ..handlers.task_handlers import update_timer

            # Останавливаем старый таймер если есть
            if task_id in active_timers:
                del active_timers[task_id]

            task_payload = {
                'task_id': task_id,
                'task_name': row['task_name'],
                'product_group': row.get('product_group', '—'),
                'slot': row['slot'],
                'provider': row.get('provider', 'Не указан'),
                'duration': seconds_to_hms(total_duration)
            }

            asyncio.create_task(
                update_timer(
                    context,
                    sent_message.chat_id,
                    sent_message.message_id,
                    task_payload,
                    total_duration,
                    get_task_keyboard()
                )
            )
        except Exception as e:
            # Отправляем уведомление в группу ЗС
            await context.bot.send_message(
                chat_id=ZS_GROUP_CHAT_ID,
                text=f"⚠️ Не удалось отправить уведомление о возврате ОПВ {opv_employee_id} для задания {task_id}. Ошибка: {e}"
            )

        # Обновляем контекст ОПВ
        try:
            # Подготавливаем данные для обновления
            task_data = {
                'task_id': task_id,
                'task_name': row['task_name'],
                'slot': row['slot'],
                'assigned_time': assigned_time,
                'duration': int(total_duration // 60),
                'status': 'На доработке'
            }
            
            # Используем безопасную функцию обновления
            success = safe_update_user_data(
                context.application,
                opv_user_id,
                {
                    'task': task_data,
                    'photos': None,  # Очищаем фото
                    'photo_request_time': None  # Очищаем время запроса фото
                }
            )
            
            if success:
                pass
            else:
                pass
        except Exception as e:
            pass
        
        # Уведомляем ЗС об успехе
        await update.message.reply_text(f"✅ Задание №{task_id} возвращено на доработку. ОПВ уведомлён.")

        # Обновляем сообщение в групповом чате, если есть
        first_message_id = context.user_data.get('last_task_message_id')
        if first_message_id:
            try:
                await context.bot.edit_message_caption(
                    chat_id=ZS_GROUP_CHAT_ID,
                    message_id=first_message_id,
                    caption=f"⚠️ Задание №{task_id} возвращено на доработку.\nПричина: {reason}"
                )
            except Exception as e:
                pass

    except Exception as e:
        import traceback
        traceback.print_exc()
        await update.message.reply_text("⚠️ Произошла ошибка при возврате задания. Попробуйте позже.")

    finally:
        # ВАЖНО: ВСЕГДА очищаем контекст ЗС после обработки
        context.user_data.pop('reject_task_id', None)
        context.user_data.pop('reject_opv_id', None)