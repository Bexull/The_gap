# Импорты из task_assignment.py
from .task_assignment import get_task, assign_task_from_sector

# Импорты из task_execution.py
from .task_execution import (
    complete_task_inline, 
    show_task, 
    complete_the_task, 
    receive_photo
)

# Импорты из task_restoration.py
from .task_restoration import restore_frozen_task_if_needed

# Импорты из special_task_completion.py
from .special_task_completion import complete_special_task_directly

# Импорты из auto_special_task_handler.py
from .auto_special_task_handler import handle_special_task_assignment, auto_assign_special_task

# Импорты из task_timer.py
from .task_timer import update_timer, stop_timer

# Импорты из special_tasks.py
from .special_tasks import (
    complete_special_task_inline,
    complete_the_extra_task,
    set_special_task
)

# Импорты из admin_commands.py
from .admin_commands import (
    clear_topic_handler,
    clear_topic_confirm_handler,
    clear_topic_callback_handler
)

__all__ = [
    # Task assignment
    'get_task',
    'assign_task_from_sector',
    
    # Task execution
    'complete_task_inline',
    'show_task', 
    'complete_the_task',
    'receive_photo',
    
    # Task restoration
    'restore_frozen_task_if_needed',
    
    # Special task completion
    'complete_special_task_directly',
    
    # Auto special task handler
    'handle_special_task_assignment',
    'auto_assign_special_task',
    
    # Task timer
    'update_timer',
    'stop_timer',
    
    # Special tasks
    'complete_special_task_inline',
    'complete_the_extra_task',
    'set_special_task',
    
    # Admin commands
    'clear_topic_handler',
    'clear_topic_confirm_handler',
    'clear_topic_callback_handler'
]
