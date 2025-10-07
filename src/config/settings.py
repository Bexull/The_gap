# =============================================================================
# РЕЖИМ РАБОТЫ БОТА
# =============================================================================
# Доступные режимы: test, prod_kenmart, prod_bekzhan, prod_city
MODE = 'test'  # Измените на нужный режим

# =============================================================================
# КОНФИГУРАЦИИ ДЛЯ КАЖДОГО РЕЖИМА
# =============================================================================

CONFIGS = {
    'test': {
        'TOKEN': '8390538420:AAGtXHSvHpvGOSeaJ86jOnh7m-ec1uz1j0I',
        'ADMIN_ID': 1015079692,
        'ZS_GROUP_CHAT_ID': -1003089690648,
        'MERCHANT_ID': 6011,
        'TOPIC_IDS': {
            'Бакалея': 65,
            'Напитки': 69,
            'Химия': 67,
            'Соф': 77,
            'Молочка': 79,
            'Гастрономия': 75,
            'Холодная зона': 73,
            'Сухая зона': 71,
            'Заморозка': 81
        }
    },
    'prod_kenmart': {
        'TOKEN': '8119695965:AAEQpNuryd5Re-CuW4o2RP9L1nZUG8dEtag',
        'ADMIN_ID': 1015079692,  # TODO: Указать правильный ADMIN_ID для KenMart
        'ZS_GROUP_CHAT_ID': -1002694047317,
        'MERCHANT_ID': 6001,  # TODO: Указать правильный MERCHANT_ID для KenMart
        'TOPIC_IDS': {
            'Бакалея': 9,
            'Напитки': 10,
            'Химия': 2,
            'Соф': 11,
            'Молочка': 12,
            'Гастрономия': 13,
            'Холодная зона': 14,
            'Сухая зона': 15,
            'Заморозка': 16
        }
    },
    'prod_bekzhan': {
        'TOKEN': '8358310264:AAFXG4TWA6w3xMrIQr-jvmR4RVyrC3KzttI',
        'ADMIN_ID': 1015079692,  # TODO: Указать правильный ADMIN_ID для Bekzhan
        'ZS_GROUP_CHAT_ID': -1003028566040,
        'MERCHANT_ID': 7001,  # TODO: Указать правильный MERCHANT_ID для Bekzhan
        'TOPIC_IDS': {
            'Бакалея': 2,
            'Напитки': 4,
            'Химия': 13,
            'Соф': 8,
            'Молочка': 6,
            'Гастрономия': 11,
            'Холодная зона': 17,
            'Сухая зона': 15,
            'Заморозка': 19
        }
    },
    'prod_city': {
        'TOKEN': '8368983595:AAFxWpXPV2a1XsWXUV4J0fVo2TIaOrD29HA',
        'ADMIN_ID': 1015079692,  # TODO: Указать правильный ADMIN_ID для CITY++
        'ZS_GROUP_CHAT_ID': -1002917837249,
        'MERCHANT_ID': 5001,  # TODO: Указать правильный MERCHANT_ID для CITY++
        'TOPIC_IDS': {
            'Бакалея': 2,
            'Напитки': 8,
            'Химия': 6,
            'Соф': 4,
            'Молочка': 16,
            'Гастрономия': 14,
            'Холодная зона': 12,
            'Сухая зона': 10,
            'Заморозка': 18
        }
    }
}

# =============================================================================
# ФУНКЦИЯ ПОЛУЧЕНИЯ КОНФИГУРАЦИИ ПО РЕЖИМУ
# =============================================================================

def get_config_by_mode(mode: str = None):
    """
    Получает конфигурацию для указанного режима
    
    Args:
        mode: Режим работы ('test', 'prod_kenmart', 'prod_bekzhan', 'prod_city')
              Если не указан, используется глобальная переменная MODE
    
    Returns:
        dict: Конфигурация для указанного режима
        
    Raises:
        ValueError: Если указан неверный режим
    """
    if mode is None:
        mode = MODE
    
    if mode not in CONFIGS:
        available_modes = ', '.join(CONFIGS.keys())
        raise ValueError(f"Неверный режим '{mode}'. Доступные режимы: {available_modes}")
    
    return CONFIGS[mode]

# =============================================================================
# АВТОМАТИЧЕСКОЕ ПОЛУЧЕНИЕ КОНФИГУРАЦИИ ДЛЯ ТЕКУЩЕГО РЕЖИМА
# =============================================================================

def load_current_config():
    """Загружает конфигурацию для текущего режима в глобальные переменные"""
    config = get_config_by_mode()
    
    # Загружаем конфигурацию в глобальные переменные
    globals()['TOKEN'] = config['TOKEN']
    globals()['ADMIN_ID'] = config['ADMIN_ID']
    globals()['ZS_GROUP_CHAT_ID'] = config['ZS_GROUP_CHAT_ID']
    globals()['MERCHANT_ID'] = config['MERCHANT_ID']
    globals()['TOPIC_IDS'] = config['TOPIC_IDS']
    
    return config

# Загружаем конфигурацию при импорте модуля
load_current_config()

# =============================================================================
# ГЛОБАЛЬНЫЕ ПЕРЕМЕННЫЕ (лучше заменить на Redis в будущем)
# =============================================================================
active_tasks = {}
zav_on_shift = []
task_assignments = {}  # {task_num: кол-во уже выданных}
assignments = []
active_timers = {}  # {task_id: {'chat_id': int, 'message_id': int, 'task': dict, 'allocated_seconds': int, 'reply_markup': obj}}
frozen_tasks_info = {}  # {task_id: {'freeze_time': datetime, 'original_start_time': datetime}}
task_time_tracker = {}  # {task_id: {'elapsed_seconds': float, 'allocated_seconds': int, 'last_tick': datetime, 'original_start_time': datetime, 'remaining_seconds': int}}

# =============================================================================
# КОНСТАНТЫ
# =============================================================================
SHIFT_MAP = {'День': 'day', 'Ночь': 'night'}

