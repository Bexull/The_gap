"""
Система навигации для бота
Отслеживает историю меню и позволяет возвращаться к предыдущим меню
"""

class NavigationHistory:
    """Класс для управления историей навигации"""
    
    def __init__(self):
        self.history = {}  # {user_id: [menu_stack]}
    
    def add_menu(self, user_id: int, menu_name: str, menu_data: dict = None):
        """Добавляет меню в историю пользователя"""
        if user_id not in self.history:
            self.history[user_id] = []
        
        menu_info = {
            'name': menu_name,
            'data': menu_data or {}
        }
        
        self.history[user_id].append(menu_info)
    
    def get_previous_menu(self, user_id: int):
        """Возвращает предыдущее меню из истории"""
        if user_id not in self.history or len(self.history[user_id]) < 2:
            return None
        
        # Удаляем текущее меню и возвращаем предыдущее
        self.history[user_id].pop()
        return self.history[user_id][-1] if self.history[user_id] else None
    
    def clear_history(self, user_id: int):
        """Очищает историю пользователя"""
        if user_id in self.history:
            self.history[user_id].clear()
    
    def get_current_menu(self, user_id: int):
        """Возвращает текущее меню"""
        if user_id not in self.history or not self.history[user_id]:
            return None
        return self.history[user_id][-1]

# Глобальный экземпляр истории навигации
navigation_history = NavigationHistory()

# Константы для названий меню
MENU_NAMES = {
    'start': 'Начало',
    'shift_choice': 'Выбор смены',
    'role_choice': 'Выбор роли',
    'employment_choice': 'Тип занятости',
    'sector_choice': 'Выбор сектора',
    'task_confirmation': 'Подтверждение задания',
    'task_in_progress': 'Задание в процессе',
    'photo_upload': 'Загрузка фото',
    'zs_main_menu': 'Главное меню ЗС',
    'opv_list': 'Список ОПВ',
    'opv_names': 'Имена ОПВ',
    'task_review': 'Проверка задания'
}
