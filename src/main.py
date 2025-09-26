import asyncio
from datetime import time

from telegram.ext import (
    Application, CommandHandler, CallbackQueryHandler,
    MessageHandler, filters
)

# Инициализация логирования
from .config.logging_config import setup_logging
setup_logging()

from .config.settings import TOKEN
from .handlers.auth_handlers import start, handle_contact
from .handlers.navigation_handlers import back_to_previous_menu, back_to_start
from .handlers.shift_handlers import (
    shift_start, shift_choice, role_choice, employment_type_choice,
    sector_select_and_confirm, shift_end, exit_session
)
from .handlers.task_handlers import (
    get_task, complete_the_task, receive_photo, complete_the_extra_task,
    complete_task_inline, complete_special_task_inline, show_task, clear_topic_handler, clear_topic_confirm_handler,
    clear_topic_callback_handler, set_special_task
)
from .handlers.zs_handlers import (
    show_opv_list, show_opv_free, show_opv_busy, show_opv_completed_list,
    show_opv_summary, handle_review, start_reject_reason, receive_reject_reason
)
from .handlers.admin_handlers import send_notification, handle_notification_text
from .handlers.see_handlers import see_free_opv, set_push_opv
from .scheduler.task_scheduler import schedule_tasks_from_rules, auto_close_expired_tasks

def main():
    """Главная функция запуска бота"""
    application = Application.builder().token(TOKEN).build()

    # Обработчики команд
    application.add_handler(CommandHandler('start', start))
    application.add_handler(CommandHandler('shift_start', shift_start))
    application.add_handler(CommandHandler('shift_end', shift_end))
    application.add_handler(CommandHandler('exit', exit_session))
    application.add_handler(CommandHandler('complete_the_task', complete_the_task))
    application.add_handler(CommandHandler('complete_the_extra_task', complete_the_extra_task))
    application.add_handler(CommandHandler('force_close_tasks', auto_close_expired_tasks))
    application.add_handler(CommandHandler("clear", clear_topic_handler))  # Простая версия
    application.add_handler(CommandHandler("clear_confirm", clear_topic_confirm_handler))  # С подтверждением
    application.add_handler(CommandHandler("send_notification", send_notification))  # Админ команда
    application.add_handler(CommandHandler("set", set_special_task))  # ТЕСТОВАЯ команда для спец-задания
    application.add_handler(CommandHandler("see", see_free_opv))  # Просмотр свободных ОПВ
    application.add_handler(CommandHandler("set_push_opv", set_push_opv))  # Принудительная раздача заданий


    # Обработчики сообщений
    application.add_handler(MessageHandler(filters.CONTACT, handle_contact))
    application.add_handler(MessageHandler(filters.PHOTO, receive_photo))
    # Текстовый ввод - сначала проверяем возврат заданий, потом уведомления
    application.add_handler(MessageHandler(filters.TEXT & ~filters.COMMAND, receive_reject_reason))
    application.add_handler(MessageHandler(filters.TEXT & ~filters.COMMAND, handle_notification_text))
    

    # Обработчики callback-запросов
    application.add_handler(CallbackQueryHandler(back_to_previous_menu, pattern='^back_to_previous$'))
    application.add_handler(CallbackQueryHandler(shift_choice, pattern='^(day|night)$'))
    application.add_handler(CallbackQueryHandler(role_choice, pattern='^(opv|zs)_(day|night)$'))
    application.add_handler(CallbackQueryHandler(sector_select_and_confirm, pattern='^sectorchoice_'))
    application.add_handler(CallbackQueryHandler(employment_type_choice, pattern='^employment_'))
    application.add_handler(CallbackQueryHandler(get_task, pattern='^get_task$'))
    application.add_handler(CallbackQueryHandler(complete_task_inline, pattern='^complete_task_inline$'))
    application.add_handler(CallbackQueryHandler(complete_special_task_inline, pattern='^complete_special_task$'))
    application.add_handler(CallbackQueryHandler(show_opv_list, pattern='^opv_list_on_shift$'))
    application.add_handler(CallbackQueryHandler(show_opv_completed_list, pattern='^opv_list_completed$'))
    application.add_handler(CallbackQueryHandler(show_opv_summary, pattern='^completed_'))
    application.add_handler(CallbackQueryHandler(show_opv_free, pattern='^opv_free$'))
    application.add_handler(CallbackQueryHandler(show_opv_busy, pattern='^opv_busy$'))
    application.add_handler(CallbackQueryHandler(handle_review, pattern='^(approve|reject)_'))
    application.add_handler(CallbackQueryHandler(start_reject_reason, pattern='^start_reject_'))
    application.add_handler(CallbackQueryHandler(show_task, pattern='^show_task$'))

    # Планировщики
    application.job_queue.run_repeating(schedule_tasks_from_rules, interval=60, first=10)
    print("✅ Планировщик запущен (интервал: 60 секунд)")
    
    # Вечером в 9:00
    application.job_queue.run_daily(auto_close_expired_tasks, time(hour=4, minute=0))

    # Утром в 21:00
    application.job_queue.run_daily(auto_close_expired_tasks, time(hour=16, minute=0))

    application.run_polling()

if __name__ == '__main__':
    main()