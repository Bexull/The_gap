import math
from datetime import datetime, timedelta, time

TIMER_TICK_SECONDS = 15


def seconds_to_hms(total_seconds: int) -> str:
    total_seconds = max(0, int(total_seconds))
    hours = total_seconds // 3600
    minutes = (total_seconds % 3600) // 60
    seconds = total_seconds % 60
    return f"{hours:02d}:{minutes:02d}:{seconds:02d}"


def hms_to_seconds(hms_str: str) -> int:
    """
    Конвертирует строку формата 'HH:MM:SS' в секунды
    
    Args:
        hms_str: Строка в формате "HH:MM:SS" или timedelta
    
    Returns:
        int: Количество секунд
    """
    if isinstance(hms_str, timedelta):
        return int(hms_str.total_seconds())
    
    if not hms_str or not isinstance(hms_str, str):
        return 0
    
    parts = str(hms_str).split(':')
    if len(parts) != 3:
        return 0
    
    try:
        hours = int(parts[0])
        minutes = int(parts[1])
        seconds = int(parts[2])
        return hours * 3600 + minutes * 60 + seconds
    except (ValueError, IndexError):
        return 0


def align_seconds(seconds: float, step: int = TIMER_TICK_SECONDS, mode: str = 'floor') -> int:
    """Выравнивает секунды к заданному шагу (floor/ceil/round)."""

    if step <= 0:
        return int(max(0, seconds))

    seconds = max(0.0, float(seconds))

    if mode == 'ceil':
        return int(math.ceil(seconds / step) * step)
    if mode == 'round':
        return int(round(seconds / step) * step)

    return int(math.floor(seconds / step) * step)

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

def get_task_date(shift: str) -> datetime.date:
    """Определяет дату для задания в зависимости от смены"""
    now = datetime.now()
    if shift == 'night':
        if now.hour >= 0 and now.hour < 8:  
            return now.date()               
        else:
            return (now + timedelta(days=1)).date()  
    else:
        return now.date()
