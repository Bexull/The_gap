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
    
    
    # Настройка уровней для разных модулей
    logging.getLogger('telegram').setLevel(logging.WARNING)  # Уменьшаем шум от telegram
    logging.getLogger('httpx').setLevel(logging.WARNING)     # Уменьшаем шум от httpx
    logging.getLogger('apscheduler').setLevel(logging.ERROR)  # Убираем логи APScheduler
    
    return logger

def get_logger(name):
    """Получить логгер для модуля"""
    return logging.getLogger(name)
