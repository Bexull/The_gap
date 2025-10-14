"""
Утилиты для работы с time_begin и freeze_time в БД
БД - единственный источник истины для учета времени выполнения заданий
"""

import datetime as dt
from datetime import datetime, timedelta
from ..database.sql_client import SQL
from ..config.settings import MERCHANT_ID


def parse_freeze_time_from_db(freeze_time_raw) -> int:
    """
    Парсит freeze_time из БД в секунды
    
    Args:
        freeze_time_raw: значение из БД (может быть None, time, str, timedelta)
    
    Returns:
        int: количество секунд
    """
    if freeze_time_raw is None:
        return 0
    
    try:
        # Если это уже time объект
        if isinstance(freeze_time_raw, dt.time):
            return freeze_time_raw.hour * 3600 + freeze_time_raw.minute * 60 + freeze_time_raw.second
        
        # Если это timedelta
        if isinstance(freeze_time_raw, timedelta):
            return int(freeze_time_raw.total_seconds())
        
        # Если это строка - парсим
        if isinstance(freeze_time_raw, str):
            freeze_time_raw = freeze_time_raw.strip()
            
            # Формат HH:MM:SS
            if ':' in freeze_time_raw:
                parts = freeze_time_raw.split(':')
                if len(parts) == 3:
                    h, m, s = map(int, parts)
                    return h * 3600 + m * 60 + s
                elif len(parts) == 2:
                    m, s = map(int, parts)
                    return m * 60 + s
        
        return 0
    except Exception as e:
        return 0


def parse_time_begin_from_db(time_begin_raw):
    """
    Парсит time_begin из БД в datetime
    
    Args:
        time_begin_raw: значение из БД (может быть None, datetime, str, time)
    
    Returns:
        datetime или None
    """
    if time_begin_raw is None:
        return None
    
    try:
        # Если это уже datetime объект
        if isinstance(time_begin_raw, datetime):
            return time_begin_raw
        
        # Если это строка - парсим
        if isinstance(time_begin_raw, str):
            time_begin_raw = time_begin_raw.strip()
            
            # Формат YYYY-MM-DD HH:MM:SS
            if ' ' in time_begin_raw:
                return datetime.strptime(time_begin_raw, '%Y-%m-%d %H:%M:%S')
            # Формат HH:MM:SS (добавляем сегодняшнюю дату)
            elif ':' in time_begin_raw:
                time_part = datetime.strptime(time_begin_raw, '%H:%M:%S').time()
                return datetime.combine(datetime.today(), time_part)
        
        # Если это time объект
        if isinstance(time_begin_raw, dt.time):
            return datetime.combine(datetime.today(), time_begin_raw)
        
        return None
    except Exception as e:
        return None


def seconds_to_time_str(seconds: int) -> str:
    """Конвертирует секунды в строку формата HH:MM:SS"""
    hours = int(seconds // 3600)
    minutes = int((seconds % 3600) // 60)
    secs = int(seconds % 60)
    return f"{hours:02d}:{minutes:02d}:{secs:02d}"


def get_task_timing_info(task_id: int) -> dict:
    """
    Получает информацию о времени задания из БД
    
    Args:
        task_id: ID задания
    
    Returns:
        dict: {
            'time_begin': datetime или None,
            'freeze_time': int (секунды),
            'task_duration': int (секунды),
            'status': str
        }
    """
    try:
        df = SQL.sql_select('wms', f"""
            SELECT time_begin, freeze_time, task_duration, status
            FROM wms_bot.shift_tasks
            WHERE id = {task_id}
            AND merchant_code = '{MERCHANT_ID}'
            LIMIT 1
        """)
        
        if df.empty:
            return {
                'time_begin': None,
                'freeze_time': 0,
                'task_duration': 0,
                'status': None
            }
        
        row = df.iloc[0]
        
        # Парсим time_begin
        time_begin = parse_time_begin_from_db(row.get('time_begin'))
        
        # Парсим freeze_time
        freeze_time_seconds = parse_freeze_time_from_db(row.get('freeze_time'))
        
        # Парсим task_duration
        task_duration_raw = row.get('task_duration')
        if isinstance(task_duration_raw, dt.time):
            task_duration_seconds = task_duration_raw.hour * 3600 + task_duration_raw.minute * 60 + task_duration_raw.second
        elif isinstance(task_duration_raw, timedelta):
            task_duration_seconds = int(task_duration_raw.total_seconds())
        elif isinstance(task_duration_raw, (int, float)):
            task_duration_seconds = int(task_duration_raw)
        else:
            task_duration_seconds = 0
        
        return {
            'time_begin': time_begin,
            'freeze_time': freeze_time_seconds,
            'task_duration': task_duration_seconds,
            'status': row.get('status')
        }
        
    except Exception as e:
        return {
            'time_begin': None,
            'freeze_time': 0,
            'task_duration': 0,
            'status': None
        }


def calculate_remaining_time(task_id: int) -> int:
    """
    Вычисляет оставшееся время выполнения задания
    
    Формула: remaining = task_duration - freeze_time - (now - time_begin)
    
    Args:
        task_id: ID задания
    
    Returns:
        int: оставшееся время в секундах (может быть отрицательным если просрочено)
    """
    info = get_task_timing_info(task_id)
    
    task_duration = info['task_duration']
    freeze_time = info['freeze_time']
    time_begin = info['time_begin']
    
    # Вычисляем время текущей сессии
    if time_begin is not None:
        current_session_seconds = int((datetime.now() - time_begin).total_seconds())
    else:
        current_session_seconds = 0
    
    # Вычисляем оставшееся время
    remaining = task_duration - freeze_time - current_session_seconds
    
    return remaining


def update_freeze_time_on_pause(task_id: int):
    """
    Обновляет freeze_time при паузе задания (отправка на проверку, заморозка)
    
    Вычисляет delta = now() - time_begin и прибавляет к freeze_time
    Затем устанавливает time_begin = NULL
    
    Args:
        task_id: ID задания
    """
    try:
        info = get_task_timing_info(task_id)
        
        if info['time_begin'] is None:
            return
        
        # Вычисляем время текущей сессии
        delta_seconds = int((datetime.now() - info['time_begin']).total_seconds())
        
        # Новое накопленное время
        new_freeze_time_seconds = info['freeze_time'] + delta_seconds
        new_freeze_time_str = seconds_to_time_str(new_freeze_time_seconds)
        
        # Обновляем в БД
        SQL.sql_delete('wms', f"""
            UPDATE wms_bot.shift_tasks
            SET freeze_time = '{new_freeze_time_str}',
                time_begin = NULL
            WHERE id = {task_id}
            AND merchant_code = '{MERCHANT_ID}'
        """)
        
        
    except Exception as e:
        pass


def reset_time_begin(task_id: int):
    """
    Устанавливает time_begin = NOW() для возобновления задания
    (возврат на доработку, разморозка, взятие задания)
    
    Args:
        task_id: ID задания
    """
    try:
        now_str = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
        
        SQL.sql_delete('wms', f"""
            UPDATE wms_bot.shift_tasks
            SET time_begin = '{now_str}'
            WHERE id = {task_id}
            AND merchant_code = '{MERCHANT_ID}'
        """)
        
        
    except Exception as e:
        pass


def clear_time_begin(task_id: int):
    """
    Устанавливает time_begin = NULL при паузе задания
    (Используется если freeze_time уже обновлен отдельно)
    
    Args:
        task_id: ID задания
    """
    try:
        SQL.sql_delete('wms', f"""
            UPDATE wms_bot.shift_tasks
            SET time_begin = NULL
            WHERE id = {task_id}
            AND merchant_code = '{MERCHANT_ID}'
        """)
        
        
    except Exception as e:
        pass


def reset_freeze_time(task_id: int):
    """
    Обнуляет freeze_time (при взятии нового задания)
    
    Args:
        task_id: ID задания
    """
    try:
        SQL.sql_delete('wms', f"""
            UPDATE wms_bot.shift_tasks
            SET freeze_time = '00:00:00'
            WHERE id = {task_id}
            AND merchant_code = '{MERCHANT_ID}'
        """)
        
        
    except Exception as e:
        pass


# ============================================================================
# УСТАРЕВШИЕ ФУНКЦИИ (для обратной совместимости, будут удалены позже)
# ============================================================================

def read_freeze_time(task_id: int) -> int:
    """
    УСТАРЕЛО: Используйте get_task_timing_info() вместо этого
    
    Читает freeze_time из БД для задания
    """
    info = get_task_timing_info(task_id)
    freeze_time = info['freeze_time']
    return freeze_time


def save_freeze_time(task_id: int, total_seconds: int):
    """
    УСТАРЕЛО: Используйте update_freeze_time_on_pause() или reset_freeze_time()
    
    Сохраняет накопленное время в БД
    """
    try:
        formatted_time = seconds_to_time_str(total_seconds)
        
        SQL.sql_delete('wms', f"""
            UPDATE wms_bot.shift_tasks
            SET freeze_time = '{formatted_time}'
            WHERE id = {task_id}
            AND merchant_code = '{MERCHANT_ID}'
        """)
        
        
    except Exception as e:
        pass


def accumulate_freeze_time(task_id: int, current_session_seconds: float) -> int:
    """
    УСТАРЕЛО: Используйте update_freeze_time_on_pause()
    
    Накапливает время: читает старое из БД, прибавляет новое, сохраняет
    """
    try:
        # 1. Читаем предыдущее накопленное время из БД
        previous_elapsed = read_freeze_time(task_id)
        
        # 2. Прибавляем время текущей сессии
        total_elapsed = previous_elapsed + int(current_session_seconds)
        
        # 3. Сохраняем обратно в БД
        save_freeze_time(task_id, total_elapsed)
        
        
        return total_elapsed
        
    except Exception as e:
        return 0


def clear_freeze_time(task_id: int):
    """
    УСТАРЕЛО: Используйте reset_freeze_time()
    
    Очищает freeze_time в БД (устанавливает в 00:00:00)
    """
    reset_freeze_time(task_id)
