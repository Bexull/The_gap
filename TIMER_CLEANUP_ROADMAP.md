# 🧹 Roadmap: Удаление старой системы накопления времени

## ✅ ВЫПОЛНЕНО! Все шаги завершены

## 🎯 Цель
Полностью удалить всю текущую логику накопления `freeze_time` и вернуться к базовому таймеру. После проверки добавим новую простую систему.

---

## 📋 Что нужно удалить/упростить

### ✅ Шаг 1: Упростить frozen_tasks_info (память)
**Файлы:** `src/config/settings.py`, все обработчики

**Было:**
```python
frozen_tasks_info[task_id] = {
    'freeze_time': datetime,
    'elapsed_seconds': int,  # ← УДАЛИТЬ
    'remaining_seconds': int,  # ← УДАЛИТЬ
    'original_start_time': datetime,
    'allocated_seconds': int  # ← УДАЛИТЬ
}
```

**Будет (minimal):**
```python
frozen_tasks_info[task_id] = {
    'freeze_time': datetime,
    'original_start_time': datetime
}
```

**Действия:**
- [x] Найти все места где используется `frozen_tasks_info`
- [x] Удалить обращения к `elapsed_seconds`, `remaining_seconds`, `allocated_seconds`
- [x] Оставить только `freeze_time` и `original_start_time` для UI

**Исправленные файлы:**
- settings.py - обновлен комментарий структуры
- task_assignment.py - убрано чтение allocated_seconds
- task_execution.py - убрано чтение elapsed_seconds (2 места)
- task_utils.py - убрано чтение elapsed_seconds

---

### ✅ Шаг 2: Удалить логику накопления из stop_timer_for_task()
**Файл:** `src/handlers/task_handlers/task_timer.py` (строки 336-420)

**Удалить:**
```python
# ВЕСЬ блок чтения freeze_time из БД и накопления (строки 337-373)
if "заморозка" in reason.lower():
    freeze_df = SQL.sql_select(...)  # ← Удалить
    previous_elapsed = ...  # ← Удалить
    total_accumulated_elapsed = ...  # ← Удалить
    frozen_tasks_info[task_id] = {...}  # ← Упростить
```

**Заменить на:**
```python
if "заморозка" in reason.lower():
    # Minimal info для UI
    frozen_tasks_info[task_id] = {
        'freeze_time': datetime.now(),
        'original_start_time': tracker_entry.get('original_start_time') if tracker_entry else None
    }
    keep_frozen = True
```

**Удалить сохранение freeze_time в БД:**
```python
# Строки 400-415 - УДАЛИТЬ весь блок
if "заморозка" in reason.lower():
    total_elapsed = frozen_tasks_info[task_id]['elapsed_seconds']
    ...
SQL.sql_delete('wms', f"""UPDATE ... SET freeze_time = ...""")  # ← УДАЛИТЬ
```

---

### ✅ Шаг 3: Удалить логику накопления из _handle_freeze_state()
**Файл:** `src/handlers/task_handlers/task_timer.py` (строки 277-350)

**Удалить:**
```python
# Весь блок чтения freeze_time из БД (строки 285-319) ← УДАЛИТЬ
freeze_df = SQL.sql_select(...)
previous_elapsed = ...
total_accumulated_elapsed = ...
```

**Заменить на:**
```python
async def _handle_freeze_state(context, task_id, tracker_entry, timer_info):
    remaining_seconds = max(0, int(tracker_entry.get('remaining_seconds', 0)))
    
    # Minimal info
    frozen_tasks_info[task_id] = {
        'freeze_time': datetime.now(),
        'original_start_time': tracker_entry.get('original_start_time')
    }
    
    # UI
    message = f"❄️ Задание заморожено..."
    await context.bot.edit_message_text(...)
```

---

### ✅ Шаг 4: Удалить накопление из _render_timer_loop() finally
**Файл:** `src/handlers/task_handlers/task_timer.py` (строки 193-235)

**Удалить:**
```python
# Весь блок накопления freeze_time (строки 200-229) ← УДАЛИТЬ
if keep_frozen:
    freeze_df = SQL.sql_select(...)
    previous_elapsed = ...
    total_elapsed = ...
    formatted = seconds_to_hms(total_elapsed)
    ...

SQL.sql_delete('wms', f"""UPDATE ... SET freeze_time = ...""")  # ← УДАЛИТЬ
```

**Оставить только:**
```python
finally:
    active_timers.pop(task_id, None)
    _cleanup_task_tracking(task_id, keep_frozen=keep_frozen)
```

---

### ✅ Шаг 5: Удалить/упростить get_task_remaining_time()
**Файл:** `src/utils/task_utils.py` (строки 297-356)

**Удалить полностью:**
```python
def get_task_remaining_time(task_id, task_duration):  # ← УДАЛИТЬ ВСЮ ФУНКЦИЮ
    ...
```

**Создать простую замену:**
```python
def get_task_allocated_seconds(task_duration):
    """Просто парсит task_duration в секунды"""
    return parse_task_duration(task_duration)
```

---

### ✅ Шаг 6: Упростить update_timer()
**Файл:** `src/handlers/task_handlers/task_timer.py` (строки 37-96)

**Удалить:**
```python
# Весь блок работы с frozen_info (строки 51-82) ← УПРОСТИТЬ
frozen_info = frozen_tasks_info.get(task_id, {})
elapsed_seconds = frozen_info.get('elapsed_seconds', 0)  # ← УДАЛИТЬ
remaining_hint = frozen_info.get('remaining_seconds')  # ← УДАЛИТЬ
allocated_hint = frozen_info.get('allocated_seconds')  # ← УДАЛИТЬ
...
```

**Упростить до:**
```python
async def update_timer(context, chat_id, message_id, task, total_seconds, reply_markup=None, comment=None):
    task_id = task['task_id']
    
    if task_id in active_timers:
        return
    
    _ensure_tracker_loop()
    
    now = datetime.now()
    allocated_seconds = int(total_seconds)
    
    # Простой трекер - начинаем с 0
    task_time_tracker[task_id] = {
        'elapsed_seconds': 0.0,
        'allocated_seconds': allocated_seconds,
        'last_tick': now,
        'original_start_time': now,
        'remaining_seconds': allocated_seconds
    }
    
    active_timers[task_id] = {
        'chat_id': chat_id,
        'message_id': message_id,
        'task': task,
        'allocated_seconds': allocated_seconds,
        'reply_markup': reply_markup,
        'comment': comment,
        'last_rendered_remaining': None
    }
    
    asyncio.create_task(_render_timer_loop(context, task_id))
```

---

### ✅ Шаг 7: Упростить special_tasks.py (восстановление)
**Файл:** `src/handlers/task_handlers/special_tasks.py` (строки 106-162)

**Удалить:**
```python
# Строки 106-111 ← УДАЛИТЬ
from ...utils.task_utils import get_task_remaining_time
allocated_seconds, elapsed_seconds = get_task_remaining_time(...)
remaining_seconds = max(0, allocated_seconds - elapsed_seconds)
```

**Упростить до:**
```python
# Просто берем task_duration
allocated_seconds = parse_task_duration(frozen_task['task_duration'])

# Сообщение без elapsed (пока)
message = (
    f"📄 *Номер задания:* {frozen_task['id']}\n"
    f"🔄 *Задание восстановлено*\n\n"
    f"📝 *Наименование:* {frozen_task['task_name']}\n"
    f"⏱ *Выделенное время:* {frozen_task['task_duration']}"
)

# Запускаем таймер с allocated
update_timer(context, chat_id, message_id, task_data, allocated_seconds, reply_markup)
```

---

### ✅ Шаг 8: Упростить task_restoration.py
**Файл:** `src/handlers/task_handlers/task_restoration.py` (строки 113-136)

**Удалить:**
```python
# Строки 113-125 ← УДАЛИТЬ
from ...utils.task_utils import get_task_remaining_time
allocated_seconds, elapsed_seconds = get_task_remaining_time(...)
remaining_seconds = max(0, allocated_seconds - elapsed_seconds)
```

**Упростить до:**
```python
# Просто берем task_duration
allocated_seconds = parse_task_duration(frozen_task['task_duration'])

# Сообщение без elapsed
message = (
    f"📋 *Текущее задание*\n\n"
    f"🆔 ID: `{frozen_task['id']}`\n"
    f"📌 Название: *{frozen_task['task_name']}*\n"
    f"⏳ Плановая длительность: {frozen_task['task_duration']} мин"
)

# Запускаем таймер
update_timer(context, chat_id, message_id, task_data, allocated_seconds, reply_markup)
```

---

### ✅ Шаг 9: Упростить special_task_completion.py
**Файл:** `src/handlers/task_handlers/special_task_completion.py` (строки 105-179)

**Удалить:**
```python
# Строки 108-117 ← УДАЛИТЬ
if task_id in frozen_tasks_info:
    total_seconds = frozen_tasks_info[task_id].get('remaining_seconds', 0)
    elapsed_seconds = frozen_tasks_info[task_id].get('elapsed_seconds', 0)
    ...
```

**Упростить до:**
```python
# Просто берем task_duration
total_seconds = parse_task_duration(frozen_task['task_duration'])

# Сообщение без elapsed
message = f"Задание восстановлено"

# Запускаем таймер
update_timer(context, chat_id, message_id, task_data, total_seconds, reply_markup)
```

---

### ✅ Шаг 10: Упростить zs_handlers.py (возврат на доработку)
**Файл:** `src/handlers/zs_handlers.py` (строки 420-487)

**Удалить:**
```python
# Весь блок расчета elapsed/remaining ← УДАЛИТЬ
elapsed_seconds = ...
remaining_seconds = ...
frozen_tasks_info[task_id] = {...}
```

**Упростить до:**
```python
# Просто берем task_duration
total_duration = parse_task_duration(row['task_duration'])

# Minimal frozen info
frozen_tasks_info[task_id] = {
    'freeze_time': datetime.now(),
    'original_start_time': assigned_time
}

# Запускаем таймер с allocated
update_timer(context, chat_id, message_id, task_payload, total_duration, keyboard)
```

---

### ✅ Шаг 11: Удалить все логи отладки
**Все файлы**

**Удалить:**
```python
print(f"🔄 [FREEZE] Обновлен frozen_tasks_info...")  # ← УДАЛИТЬ
print(f"🔧 [DEBUG] Восстановление...")  # ← УДАЛИТЬ
print(f"💾 freeze_time НАКОПЛЕН...")  # ← УДАЛИТЬ
```

---

### ✅ Шаг 12: Убрать freeze_time из БД (пока НЕ сохраняем)
**Все файлы**

**Удалить все SQL UPDATE freeze_time:**
```python
SQL.sql_delete('wms', f"""
    UPDATE wms_bot.shift_tasks
    SET freeze_time = '{formatted}'  # ← УДАЛИТЬ весь UPDATE
    WHERE id = {task_id}
""")
```

---

## ✅ Проверка после удаления

После выполнения всех шагов проверяем:

1. [ ] Бот запускается без ошибок
2. [ ] Можно взять задание → таймер работает ✅
3. [ ] Можно завершить задание ✅
4. [ ] Спец-задание назначается → основное замораживается ✅
5. [ ] После спец-задания → основное восстанавливается (но время НЕ накапливается - это нормально, добавим позже) ✅
6. [ ] Никаких ошибок с freeze_time ✅
7. [ ] Нет обращений к несуществующим полям frozen_tasks_info ✅

---

## 🎯 После успешного удаления

Переходим к **TIMER_REFACTOR_ROADMAP.md** и добавляем новую систему с нуля! ✨

---

## 📝 Порядок удаления (важно!)

1. ✅ Шаг 12 (freeze_time БД) - сначала перестаем писать в БД
2. ✅ Шаг 11 (логи) - убираем шум
3. ✅ Шаг 5 (get_task_remaining_time) - удаляем функцию
4. ✅ Шаг 7-10 (восстановление) - упрощаем все места восстановления
5. ✅ Шаг 6 (update_timer) - упрощаем запуск таймера
6. ✅ Шаг 2-4 (stop_timer, handle_freeze) - упрощаем остановку
7. ✅ Шаг 1 (frozen_tasks_info) - финальная очистка структуры

---

## ✅ Выполненные изменения

### 1. Удалено сохранение freeze_time в БД
- task_timer.py: удалены все SQL UPDATE freeze_time
- Перестали писать накопленное время в БД

### 2. Удалены отладочные логи
- Удалены логи `🔄 [FREEZE]`, `🔧 [DEBUG]`, `🕒 [RESTORE]`
- Очищен output от лишнего шума

### 3. Удалена функция get_task_remaining_time()
- Заменена на простую get_task_allocated_seconds()
- Убрана вся сложная логика чтения freeze_time из БД/памяти

### 4. Упрощены все места восстановления
- special_tasks.py: убрана логика с elapsed/remaining
- task_restoration.py: убрана логика с elapsed/remaining
- special_task_completion.py: убрана логика с elapsed/remaining  
- zs_handlers.py: убрана логика с elapsed/remaining

### 5. Упрощен update_timer()
- Удалена вся логика с frozen_info
- Таймер всегда начинается с elapsed=0
- Простой трекер времени

### 6. Упрощен stop_timer_for_task()
- Удалена логика чтения/накопления freeze_time
- Minimal frozen_tasks_info (только freeze_time + original_start_time)

### 7. Упрощен _handle_freeze_state()
- Удалена логика чтения/накопления freeze_time
- Minimal frozen_tasks_info

### 8. Очищена структура frozen_tasks_info (Шаг 1)
- settings.py: обновлен комментарий структуры (только freeze_time + original_start_time)
- task_assignment.py: убрано чтение allocated_seconds из frozen_tasks_info
- task_execution.py: убрано чтение elapsed_seconds из frozen_tasks_info (2 места)
- task_utils.py: убрано чтение elapsed_seconds из frozen_tasks_info
- **Теперь везде используется только freeze_time и original_start_time**

---

## 🎯 Результат

**Сейчас система работает так:**
- ✅ Таймер запускается с allocated_seconds
- ✅ Таймер показывает оставшееся время
- ✅ При заморозке сохраняется minimal info в памяти
- ✅ При восстановлении таймер начинается ЗАНОВО (без накопления)
- ✅ НЕ сохраняется freeze_time в БД
- ✅ Нет накопления времени между заморозками

**Готово к переходу на TIMER_REFACTOR_ROADMAP.md** ✨

