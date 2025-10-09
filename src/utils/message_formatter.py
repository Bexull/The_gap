"""
Централизованное форматирование сообщений о заданиях
"""
from .time_utils import seconds_to_hms, hms_to_seconds
from .freeze_time_utils import calculate_remaining_time


def format_task_message(task, status: str = "Выполняется", show_timer: bool = True) -> str:
    """
    Форматирует сообщение о задании с единообразным стилем
    
    Args:
        task: Объект задания (dict или pandas Series) с полями:
              - id/task_id: ID задания
              - task_name: Название задания
              - product_group: Группа товаров
              - slot: Слот
              - provider: Поставщик (опционально)
              - task_duration: Плановая длительность в формате HH:MM:SS
              - comment: Комментарий (опционально)
              - priority: Приоритет (опционально, 111 для спец-заданий)
        status: Статус задания (по умолчанию "Выполняется")
        show_timer: Показывать ли выделенное и оставшееся время (по умолчанию True)
    
    Returns:
        str: Отформатированное сообщение
    """
    # Извлекаем поля из объекта задачи
    task_id = task.get('id') or task.get('task_id')
    task_name = task.get('task_name', 'Не указано')
    product_group = task.get('product_group', 'Не указана')
    slot = task.get('slot', 'Не указан')
    
    # Обрабатываем поставщика (заменяем None/"None" на "Отсутствует")
    provider = task.get('provider')
    if provider is None or str(provider).strip() in ['None', 'none', '']:
        provider = 'Отсутствует'
    
    comment = task.get('comment')
    priority = task.get('priority')
    
    # Заголовок
    text = f"📄 Номер задания: {task_id}\n"
    
    # Определяем тип задания по приоритету
    is_special = str(priority) == '111'
    
    # Статус
    if is_special:
        text += "🔥 Спец-задание (приоритет 111)\n\n"
    elif status == "Выполняется":
        text += "✅ Задание выполняется\n\n"
    elif status == "Получено":
        text += "✅ Задание получено!\n\n"
    elif status == "На доработке":
        text += "🔄 Задание на доработке\n\n"
    elif status == "Заморожено":
        text += "❄️ Задание заморожено\n\n"
    else:
        text += f"ℹ️ Статус: {status}\n\n"
    
    # Комментарий (если есть, показываем в начале)
    if comment and str(comment).strip():
        text += f"📝 Комментарий: {comment}\n\n"
    
    # Основная информация
    text += f"📝 Наименование: {task_name}\n"
    text += f"📦 Группа товаров: {product_group}\n"
    text += f"📍 Слот: {slot}\n"
    text += f"🏢 Поставщик: {provider}\n"
    
    # Время (если нужно показывать)
    if show_timer:
        # Сначала пробуем вычислить оставшееся время (оно читает из БД task_duration)
        try:
            remaining_seconds = calculate_remaining_time(task_id)
            remaining_str = seconds_to_hms(remaining_seconds)
            
            # Получаем task_duration из БД через get_task_timing_info
            # ВАЖНО: get_task_timing_info возвращает task_duration УЖЕ В СЕКУНДАХ!
            from .freeze_time_utils import get_task_timing_info
            timing_info = get_task_timing_info(task_id)
            
            if timing_info and timing_info.get('task_duration'):
                allocated_seconds = timing_info['task_duration']  # Уже в секундах!
                allocated_str = seconds_to_hms(allocated_seconds)
                text += f"⏱️ Выделенное время: {allocated_str}\n"
                text += f"⏳ Оставшееся время: {remaining_str}"
        except Exception as e:
            # Fallback - пытаемся взять из объекта task
            task_duration = task.get('task_duration')
            if task_duration is not None:
                allocated_seconds = hms_to_seconds(task_duration)
                if allocated_seconds > 0:
                    allocated_str = seconds_to_hms(allocated_seconds)
                    text += f"⏱️ Выделенное время: {allocated_str}\n"
                    text += f"⏳ Оставшееся время: {allocated_str}"
    
    return text

