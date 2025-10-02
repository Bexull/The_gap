# 🔍 Анализ проблемы с таймером при заморозке/разморозке

## 📊 Наблюдаемая проблема

**Логи показывают:**
```
🔧 [DEBUG] Восстановление из frozen_tasks_info (память) для задания 64374: allocated=1200s, elapsed=176s
🕒 [TIMER] task_id=64374 remaining_before=17:00 -> 16:45
💾 freeze_time НАКОПЛЕН при stop_timer для задачи 64374: 00:04:30 + 00:03:15 = 00:07:45
⏰ Обновлено время работы (спец-задание): 600s + 600s = 1200s
🔧 [DEBUG] Восстановление из frozen_tasks_info (память) для задания 64374: allocated=1200s, elapsed=22s
```

**Проблема:** 
- Первое восстановление: `elapsed=176s` (2:56)
- Второе восстановление: `elapsed=22s` ❌ (должно быть > 176s!)

## 🎯 Что должно происходить

### Сценарий:
1. Задание выполняется 4:30 (270s)
2. **Заморозка для спец-задания** → freeze_time = 00:04:30
3. Спец-задание выполняется 10 минут (600s)
4. **Восстановление** → elapsed должен быть 270s
5. Задание выполняется еще 3:15 (195s)
6. **Заморозка для 2-го спец-задания** → freeze_time = 00:04:30 + 00:03:15 = 00:07:45 ✅
7. **Восстановление после 2-го спец** → elapsed должен быть 465s (7:45)

### Фактически:
- После 1-го спец: elapsed=176s вместо 270s ❌
- После 2-го спец: elapsed=22s вместо 465s ❌

---

## 🔎 Пункты для проверки

### ✅ 1. Проверить сохранение в frozen_tasks_info при заморозке

**Файл:** `task_timer.py` → `stop_timer_for_task()` (строки 307-317)

**Что проверить:**
- [ ] `elapsed_seconds` правильно берется из `tracker_entry`
- [ ] Значение сохраняется в `frozen_tasks_info[task_id]['elapsed_seconds']`
- [ ] Значение НЕ перезаписывается, если уже существует

**Код:**
```python
if task_id not in frozen_tasks_info:
    frozen_tasks_info[task_id] = {
        'elapsed_seconds': int(elapsed_seconds),  # ← ЭТО ЗНАЧЕНИЕ
        ...
    }
```

**Проблема:** Если `task_id` УЖЕ в `frozen_tasks_info`, то старое `elapsed_seconds` НЕ обновляется!

---

### ✅ 2. Проверить сохранение в БД (freeze_time)

**Файл:** `task_timer.py` → `stop_timer_for_task()` (строки 344-381)

**Что проверить:**
- [ ] Правильно читается старое `freeze_time` из БД
- [ ] Правильно парсится в секунды
- [ ] Правильно суммируется: `previous + current`
- [ ] Правильно сохраняется в БД

**Логи показывают:** ✅ РАБОТАЕТ
```
💾 freeze_time НАКОПЛЕН: 00:04:30 + 00:03:15 = 00:07:45
```

---

### ✅ 3. Проверить восстановление из frozen_tasks_info

**Файлы:** 
- `special_tasks.py` → строка 108
- `task_restoration.py` → строка 119

**Что проверить:**
- [ ] `get_task_remaining_time()` правильно читает `frozen_tasks_info[task_id]['elapsed_seconds']`
- [ ] Если данных нет в памяти, читает `freeze_time` из БД
- [ ] Возвращает правильные значения `(allocated, elapsed)`

**Код в `task_utils.py` (строки 319-323):**
```python
if task_id in frozen_tasks_info:
    allocated_seconds = frozen_tasks_info[task_id].get('allocated_seconds', full_duration)
    elapsed_seconds = frozen_tasks_info[task_id].get('elapsed_seconds', 0)  # ← БЕРЕТСЯ ИЗ ПАМЯТИ
```

**Проблема:** frozen_tasks_info может содержать СТАРОЕ значение elapsed!

---

### ✅ 4. Проверить очистку frozen_tasks_info

**Файл:** `task_timer.py` → `_cleanup_task_tracking()` (строки 21-34)

**Что проверить:**
- [ ] Когда вызывается с `keep_frozen=True` - НЕ удаляет из `frozen_tasks_info`
- [ ] Когда вызывается с `keep_frozen=False` - удаляет из `frozen_tasks_info`

**Код:**
```python
def _cleanup_task_tracking(task_id: int, keep_frozen: bool = False):
    if not keep_frozen:
        frozen_tasks_info.pop(task_id, None)  # ← Удаление
```

**Проблема:** Если `frozen_tasks_info` НЕ удаляется, то содержит СТАРОЕ elapsed!

---

### ✅ 5. Проверить обновление frozen_tasks_info при заморозке

**Файл:** `task_timer.py` → `stop_timer_for_task()` (строка 309)

**Код:**
```python
if task_id not in frozen_tasks_info:  # ← ТОЛЬКО ЕСЛИ НЕТ!
    frozen_tasks_info[task_id] = { ... }
```

**ПРОБЛЕМА НАЙДЕНА!** 🔴
- При первой заморозке: создается `frozen_tasks_info[64374] = {elapsed: 270}`
- При восстановлении: `keep_frozen=True` → НЕ удаляется из памяти
- При второй заморозке: `task_id IN frozen_tasks_info` → **НЕ обновляется!**
- Остается старое `elapsed=270`, но таймер работал еще 195s!

---

### ✅ 6. Проверить _handle_freeze_state

**Файл:** `task_timer.py` → `_handle_freeze_state()` (строки 248-286)

**Что проверить:**
- [ ] Правильно сохраняет `elapsed_seconds` из `tracker_entry`
- [ ] Обновляет или создает `frozen_tasks_info`

**Код (строки 255-261):**
```python
frozen_tasks_info[task_id] = {
    'freeze_time': datetime.now(),
    'elapsed_seconds': int(tracker_entry.get('elapsed_seconds', 0)),
    ...
}
```

**Проблема:** ПЕРЕЗАПИСЫВАЕТ весь объект! Может потерять накопленное время!

---

## 🎯 Корневая причина

### Проблема №1: frozen_tasks_info НЕ обновляется при повторной заморозке

**В `stop_timer_for_task()` строка 309:**
```python
if task_id not in frozen_tasks_info:  # ← Проверка блокирует обновление!
    frozen_tasks_info[task_id] = { 
        'elapsed_seconds': int(elapsed_seconds),  # Новое значение НЕ сохраняется!
        ...
    }
```

**Решение:** Всегда обновлять `elapsed_seconds`:
```python
if task_id not in frozen_tasks_info:
    frozen_tasks_info[task_id] = {}

# Обновляем elapsed_seconds из БД freeze_time
freeze_df = SQL.sql_select(...)
previous_elapsed = parse_freeze_time(freeze_df)
frozen_tasks_info[task_id].update({
    'elapsed_seconds': previous_elapsed,
    'remaining_seconds': int(remaining_seconds),
    'allocated_seconds': tracker_entry.get('allocated_seconds'),
    ...
})
```

### Проблема №2: elapsed_seconds при восстановлении берется из памяти

**В `get_task_remaining_time()` строка 322:**
```python
elapsed_seconds = frozen_tasks_info[task_id].get('elapsed_seconds', 0)
```

**Проблема:** Если frozen_tasks_info содержит СТАРОЕ значение (например, 270s после 1-й заморозки), 
а в БД уже накоплено 465s (после 2-й заморозки), то используется СТАРОЕ!

**Решение:** ВСЕГДА читать актуальное значение из БД, а не из памяти!

---

## 📋 План исправления

### ✅ Шаг 1: Изменить логику в stop_timer_for_task() - ВЫПОЛНЕНО
- ✅ При заморозке ВСЕГДА читать текущее freeze_time из БД
- ✅ ВСЕГДА обновлять frozen_tasks_info актуальным elapsed_seconds
- **Изменения (строки 336-373):**
  - Читаем freeze_time из БД
  - Парсим в секунды
  - Накапливаем: previous + current
  - ВСЕГДА обновляем frozen_tasks_info (убрали проверку `if task_id not in frozen_tasks_info`)
  
### ✅ Шаг 2: Обновить _handle_freeze_state() - ВЫПОЛНЕНО
- ✅ Перед сохранением в frozen_tasks_info читать freeze_time из БД
- ✅ Накапливать elapsed_seconds
- **Изменения (строки 285-319):**
  - Читаем freeze_time из БД
  - Накапливаем с текущим elapsed
  - Сохраняем накопленное значение в frozen_tasks_info

### ⏭️ Шаг 3: Проверить get_task_remaining_time() - НУЖНО ПРОВЕРИТЬ
- Сейчас приоритет отдается frozen_tasks_info (память)
- Теперь frozen_tasks_info всегда актуален, так что должно работать
- Но лучше отдавать приоритет БД для надежности

---

## 🧪 Тестовый сценарий

1. Задание 64374 выполняется 4:30
2. Заморозка → freeze_time в БД = 00:04:30, frozen_tasks_info.elapsed = 270
3. Спец-задание 10 мин
4. Восстановление → должно показать elapsed=270s ✅
5. Задание выполняется еще 3:15  
6. Заморозка → freeze_time в БД = 00:07:45, frozen_tasks_info.elapsed должно быть 465s
7. Спец-задание 10 мин
8. Восстановление → должно показать elapsed=465s, remaining=735s

**Ожидаемый результат:** elapsed всегда растет, never сбрасывается

