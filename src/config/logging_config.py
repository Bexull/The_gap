import logging
import logging.handlers
import os
from datetime import datetime

def setup_logging():
    """Настройка системы логирования для бота"""
    
    # Создаем директорию для логов если её нет
    log_dir = "logs"
    if not os.path.exists(log_dir):
        os.makedirs(log_dir)
    
    # Настройка основного логгера
    logger = logging.getLogger()
    logger.setLevel(logging.DEBUG)  # Максимальный уровень для захвата всех логов
    
    # Очищаем существующие обработчики
    for handler in logger.handlers[:]:
        logger.removeHandler(handler)
    
    # Формат логов
    formatter = logging.Formatter(
        '%(asctime)s - %(name)s - %(levelname)s - %(message)s',
        datefmt='%Y-%m-%d %H:%M:%S'
    )
    
    # 1. Консольный вывод (INFO и выше) с UTF-8 кодировкой
    console_handler = logging.StreamHandler()
    console_handler.setLevel(logging.INFO)
    console_handler.setFormatter(formatter)
    
    # Устанавливаем UTF-8 кодировку для консоли
    import sys
    if hasattr(sys.stdout, 'reconfigure'):
        sys.stdout.reconfigure(encoding='utf-8')
    if hasattr(sys.stderr, 'reconfigure'):
        sys.stderr.reconfigure(encoding='utf-8')
    
    logger.addHandler(console_handler)
    
    # 2. Файл для всех логов (DEBUG и выше) с UTF-8 кодировкой
    all_logs_file = os.path.join(log_dir, f"bot_all_{datetime.now().strftime('%Y%m%d')}.log")
    file_handler = logging.handlers.RotatingFileHandler(
        all_logs_file, 
        maxBytes=10*1024*1024,  # 10MB
        backupCount=5,
        encoding='utf-8'
    )
    file_handler.setLevel(logging.DEBUG)
    file_handler.setFormatter(formatter)
    logger.addHandler(file_handler)
    
    # 3. Файл только для ошибок (ERROR и выше) с UTF-8 кодировкой
    error_logs_file = os.path.join(log_dir, f"bot_errors_{datetime.now().strftime('%Y%m%d')}.log")
    error_handler = logging.handlers.RotatingFileHandler(
        error_logs_file,
        maxBytes=5*1024*1024,   # 5MB
        backupCount=10,
        encoding='utf-8'
    )
    error_handler.setLevel(logging.ERROR)
    error_handler.setFormatter(formatter)
    logger.addHandler(error_handler)
    
    # 4. Файл для планировщика (детальные логи) с UTF-8 кодировкой
    scheduler_logs_file = os.path.join(log_dir, f"scheduler_{datetime.now().strftime('%Y%m%d')}.log")
    scheduler_handler = logging.handlers.RotatingFileHandler(
        scheduler_logs_file,
        maxBytes=20*1024*1024,  # 20MB
        backupCount=3,
        encoding='utf-8'
    )
    scheduler_handler.setLevel(logging.DEBUG)
    scheduler_handler.setFormatter(formatter)
    
    # Добавляем обработчик только для планировщика
    scheduler_logger = logging.getLogger('src.scheduler.task_scheduler')
    scheduler_logger.addHandler(scheduler_handler)
    scheduler_logger.setLevel(logging.DEBUG)
    
    # Настройка уровней для разных модулей
    logging.getLogger('telegram').setLevel(logging.WARNING)  # Уменьшаем шум от telegram
    logging.getLogger('httpx').setLevel(logging.WARNING)     # Уменьшаем шум от httpx
    logging.getLogger('apscheduler').setLevel(logging.ERROR)  # Убираем логи APScheduler
    
    return logger

def get_logger(name):
    """Получить логгер для модуля"""
    return logging.getLogger(name)
