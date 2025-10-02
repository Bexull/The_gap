"""
Утилиты для работы с freeze_time в БД
Единый источник правды для накопления времени выполнения заданий
"""

import datetime as dt
from datetime import timedelta
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
        print(f"❌ [ERROR] Ошибка парсинга freeze_time: {freeze_time_raw}, ошибка: {e}")
        return 0


def read_freeze_time(task_id: int) -> int:
    """
    Читает freeze_time из БД для задания
    
    Args:
        task_id: ID задания
    
    Returns:
        int: накопленное время в секундах (0 если не найдено)
    """
    try:
        df = SQL.sql_select('wms', f"""
            SELECT freeze_time
            FROM wms_bot.shift_tasks
            WHERE id = {task_id}
            AND merchant_code = '{MERCHANT_ID}'
            LIMIT 1
        """)
        
        if df.empty:
            print(f"ℹ️ [INFO] Задание {task_id} не найдено в БД")
            return 0
        
        freeze_time_raw = df.iloc[0]['freeze_time']
        elapsed_seconds = parse_freeze_time_from_db(freeze_time_raw)
        
        print(f"📖 [READ] task={task_id} freeze_time_db={freeze_time_raw} → {elapsed_seconds}s")
        return elapsed_seconds
        
    except Exception as e:
        print(f"❌ [ERROR] Ошибка чтения freeze_time для задания {task_id}: {e}")
        return 0


def save_freeze_time(task_id: int, total_seconds: int):
    """
    Сохраняет накопленное время в БД
    
    Args:
        task_id: ID задания
        total_seconds: общее накопленное время в секундах
    """
    try:
        # Конвертируем секунды в TIME формат (HH:MM:SS)
        hours = int(total_seconds // 3600)
        minutes = int((total_seconds % 3600) // 60)
        seconds = int(total_seconds % 60)
        
        formatted_time = f"{hours:02d}:{minutes:02d}:{seconds:02d}"
        
        SQL.sql_delete('wms', f"""
            UPDATE wms_bot.shift_tasks
            SET freeze_time = '{formatted_time}'
            WHERE id = {task_id}
            AND merchant_code = '{MERCHANT_ID}'
        """)
        
        print(f"💾 [SAVE] task={task_id} freeze_time={formatted_time} ({total_seconds}s)")
        
    except Exception as e:
        print(f"❌ [ERROR] Ошибка сохранения freeze_time для задания {task_id}: {e}")


def accumulate_freeze_time(task_id: int, current_session_seconds: float) -> int:
    """
    Накапливает время: читает старое из БД, прибавляет новое, сохраняет
    
    Args:
        task_id: ID задания
        current_session_seconds: время текущей сессии в секундах
    
    Returns:
        int: общее накопленное время в секундах
    """
    try:
        # 1. Читаем предыдущее накопленное время из БД
        previous_elapsed = read_freeze_time(task_id)
        
        # 2. Прибавляем время текущей сессии
        total_elapsed = previous_elapsed + int(current_session_seconds)
        
        # 3. Сохраняем обратно в БД
        save_freeze_time(task_id, total_elapsed)
        
        print(f"➕ [ACCUMULATE] task={task_id} previous={previous_elapsed}s + session={int(current_session_seconds)}s = total={total_elapsed}s")
        
        return total_elapsed
        
    except Exception as e:
        print(f"❌ [ERROR] Ошибка накопления freeze_time для задания {task_id}: {e}")
        return 0


def clear_freeze_time(task_id: int):
    """
    Очищает freeze_time в БД (устанавливает в 00:00:00)
    
    Args:
        task_id: ID задания
    """
    try:
        save_freeze_time(task_id, 0)
        print(f"🧹 [CLEAR] task={task_id} freeze_time очищен")
    except Exception as e:
        print(f"❌ [ERROR] Ошибка очистки freeze_time для задания {task_id}: {e}")

