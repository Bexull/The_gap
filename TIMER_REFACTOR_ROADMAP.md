# 🔄 Roadmap: Полная переработка системы таймеров и заморозки

## 🎯 Цель
Создать простую, понятную и надежную систему отслеживания времени выполнения заданий с поддержкой заморозки для спец-заданий.

---

## 📋 Основные принципы новой системы

### 1. Единый источник правды: База данных
- **Все** накопленное время хранится **только в БД** в поле `freeze_time`
- `frozen_tasks_info` в памяти - только для **UI и состояния**, НЕ для расчетов
- При любом расчете **ВСЕГДА** читаем актуальное значение из БД

### 2. Простая логика накопления времени
```
При заморозке:
  новый_freeze_time = старый_freeze_time_из_БД + время_текущей_сессии

При восстановлении:
  elapsed = freeze_time_из_БД
  remaining = task_duration - elapsed
  allocated = task_duration
```

### 3. Четкие состояния задания
- **Выполняется** - таймер работает, время накапливается в tracker
- **Заморожено** - таймер остановлен, время сохранено в БД
- **На доработке** - задание вернулось, время из БД
- **Ожидает проверки** - задание завершено

---

## 🗂️ Структура данных

### БД (wms_bot.shift_tasks)
```sql
- freeze_time: TIME  -- Накопленное время работы (HH:MM:SS)
- status: VARCHAR    -- Статус задания
- task_duration: TIME -- Выделенное время на задание
```

### Память (Python)
```python
# task_time_tracker - активный трекер времени (только для работающих заданий)
task_time_tracker[task_id] = {
    'elapsed_seconds': float,      # Время текущей сессии (с момента старта/восстановления)
    'allocated_seconds': int,      # Выделенное время
    'last_tick': datetime,         # Последнее обновление
    'original_start_time': datetime # Время начала первого запуска
}

# frozen_tasks_info - только для UI (НЕ для расчетов!)
frozen_tasks_info[task_id] = {
    'freeze_time': datetime,       # Когда заморожено
    'original_start_time': datetime # Для отображения
}

# active_timers - информация для UI
active_timers[task_id] = {
    'chat_id': int,
    'message_id': int,
    'task': dict,
    'allocated_seconds': int,
    'reply_markup': object,
    'last_rendered_remaining': str
}
```

---

## 📝 Пошаговый план переработки

### ✅ Шаг 1: Создать вспомогательные функции для работы с freeze_time
**Файл:** `src/utils/freeze_time_utils.py` (новый)

**Функции:**
1. `parse_freeze_time_from_db(freeze_time_raw) -> int` - парсит freeze_time из БД в секунды
2. `read_freeze_time(task_id) -> int` - читает freeze_time из БД, возвращает секунды
3. `save_freeze_time(task_id, total_seconds)` - сохраняет накопленное время в БД
4. `accumulate_freeze_time(task_id, current_session_seconds) -> int` - читает старое, прибавляет новое, сохраняет

---

### ✅ Шаг 2: Упростить stop_timer_for_task()
**Файл:** `src/handlers/task_handlers/task_timer.py`

**Логика:**
```python
async def stop_timer_for_task(task_id, context, reason):
    tracker_entry = task_time_tracker.get(task_id)
    
    if "заморозка" in reason.lower():
        # 1. Берем время текущей сессии из tracker
        current_session_seconds = tracker_entry.get('elapsed_seconds', 0)
        
        # 2. Накапливаем в БД
        total_elapsed = accumulate_freeze_time(task_id, current_session_seconds)
        
        # 3. Сохраняем minimal info для UI
        frozen_tasks_info[task_id] = {
            'freeze_time': datetime.now(),
            'original_start_time': tracker_entry.get('original_start_time')
        }
        
        keep_frozen = True
    
    # Очистка
    _cleanup_task_tracking(task_id, keep_frozen)
```

---

### ✅ Шаг 3: Упростить восстановление задания
**Файлы:** 
- `special_tasks.py`
- `task_restoration.py`
- `zs_handlers.py` (возврат на доработку)

**Единая логика для всех:**
```python
# 1. Читаем elapsed из БД
elapsed_seconds = read_freeze_time(task_id)

# 2. Вычисляем remaining
allocated_seconds = parse_task_duration(task_duration)
remaining_seconds = max(0, allocated_seconds - elapsed_seconds)

# 3. Отправляем сообщение с правильным временем
message = f"⏳ Оставшееся время: {format_time(remaining_seconds)}"
if elapsed_seconds > 0:
    message += f"\n⏱ Уже затрачено: {format_time(elapsed_seconds)}"

# 4. Запускаем таймер с allocated (НЕ remaining!)
asyncio.create_task(
    update_timer(context, chat_id, message_id, task_data, allocated_seconds, reply_markup)
)
```

---

### ✅ Шаг 4: Упростить update_timer()
**Файл:** `src/handlers/task_handlers/task_timer.py`

**Логика:**
```python
async def update_timer(context, chat_id, message_id, task, allocated_seconds, reply_markup):
    task_id = task['task_id']
    
    # 1. Читаем накопленное время из БД (единый источник правды!)
    previous_elapsed = read_freeze_time(task_id)
    
    # 2. Создаем tracker для текущей сессии (начинается с 0)
    task_time_tracker[task_id] = {
        'elapsed_seconds': 0.0,  # Текущая сессия начинается с 0!
        'allocated_seconds': allocated_seconds,
        'previous_elapsed': previous_elapsed,  # Храним для расчетов
        'last_tick': datetime.now(),
        'original_start_time': datetime.now()
    }
    
    # 3. Запускаем render loop
    asyncio.create_task(_render_timer_loop(context, task_id))
```

---

### ✅ Шаг 5: Упростить _render_timer_loop()
**Файл:** `src/handlers/task_handlers/task_timer.py`

**Логика:**
```python
async def _render_timer_loop(context, task_id):
    while task_id in active_timers:
        tracker = task_time_tracker.get(task_id)
        
        # Общее затраченное = предыдущее + текущая сессия
        total_elapsed = tracker['previous_elapsed'] + tracker['elapsed_seconds']
        remaining = max(0, tracker['allocated_seconds'] - total_elapsed)
        
        # Отображаем remaining
        message = f"⏳ Оставшееся время: {format_time(remaining)}"
        
        # Обновляем UI
        await update_message(...)
        
        await asyncio.sleep(15)
```

---

### ✅ Шаг 6: Упростить _handle_freeze_state()
**Файл:** `src/handlers/task_handlers/task_timer.py`

**Логика:**
```python
async def _handle_freeze_state(context, task_id, tracker_entry):
    # 1. Накапливаем время в БД
    current_session = tracker_entry['elapsed_seconds']
    total_elapsed = accumulate_freeze_time(task_id, current_session)
    
    # 2. Вычисляем remaining
    allocated = tracker_entry['allocated_seconds']
    remaining = max(0, allocated - total_elapsed)
    
    # 3. Minimal info для UI
    frozen_tasks_info[task_id] = {
        'freeze_time': datetime.now(),
        'original_start_time': tracker_entry.get('original_start_time')
    }
    
    # 4. Отображаем
    message = f"❄️ Задание заморожено\n⏳ Оставшееся: {format_time(remaining)}"
```

---

### ✅ Шаг 7: Убрать get_task_remaining_time()
**Файл:** `src/utils/task_utils.py`

**Причина:** Запутанная функция, которая смешивает память и БД

**Заменить на:**
```python
def get_task_time_info(task_id, task_duration):
    """Простая функция для получения времени задания"""
    elapsed_seconds = read_freeze_time(task_id)  # Из БД!
    allocated_seconds = parse_task_duration(task_duration)
    remaining_seconds = max(0, allocated_seconds - elapsed_seconds)
    
    return {
        'elapsed': elapsed_seconds,
        'allocated': allocated_seconds,
        'remaining': remaining_seconds
    }
```

---

### ✅ Шаг 8: Исправить возврат на доработку
**Файл:** `src/handlers/zs_handlers.py`

**Проблема:** Неправильно вычисляет elapsed и remaining

**Решение:**
```python
# Читаем из БД
elapsed_seconds = read_freeze_time(task_id)
total_duration = parse_task_duration(task_duration)
remaining_seconds = max(0, total_duration - elapsed_seconds)

# Создаем frozen_tasks_info для update_timer
frozen_tasks_info[task_id] = {
    'freeze_time': datetime.now(),
    'original_start_time': assigned_time
}

# Запускаем таймер с allocated (не remaining!)
update_timer(context, chat_id, message_id, task_data, total_duration, keyboard)
```

---

### ✅ Шаг 9: Добавить логирование для отладки
**Все файлы**

Единый формат логов:
```python
print(f"⏱️ [TIMER] task={task_id} action={action} elapsed={elapsed}s allocated={allocated}s remaining={remaining}s")
```

---

### ✅ Шаг 10: Тестирование
**Сценарий:**
1. Задание 20 мин (1200s)
2. Работа 4:30 → freeze_time=00:04:30 (270s)
3. Спец-задание 10 мин
4. Восстановление → elapsed=270s, remaining=930s (15:30) ✅
5. Работа еще 3:15 → freeze_time=00:07:45 (465s)
6. Второе спец-задание
7. Восстановление → elapsed=465s, remaining=735s (12:15) ✅
8. Возврат на доработку → elapsed=465s, remaining=735s ✅
9. Работа еще 5:00 → freeze_time=00:12:45 (765s)
10. Завершение → total_time=765s ✅

---

## 🚫 Что удалить после переработки

1. ❌ `TIMER_FREEZE_ANALYSIS.md` - анализ старой системы
2. ❌ Старые комментарии с ИСПРАВЛЕНИЕ/ОТЛАДКА
3. ❌ Дублирующий код чтения freeze_time (использовать utils)
4. ❌ Сложные проверки в `frozen_tasks_info`

---

## ✅ Критерии успеха

- [ ] `freeze_time` в БД **ВСЕГДА** правильно накапливается
- [ ] `elapsed` при восстановлении **ВСЕГДА** берется из БД
- [ ] `remaining = allocated - elapsed` **ВСЕГДА** правильно вычисляется
- [ ] Логика простая и понятная
- [ ] Нет дублирования кода
- [ ] Все функции делают одну вещь
- [ ] Тесты проходят успешно

---

## 🎯 Начинаем с...
**Шаг 1: Создание freeze_time_utils.py**

