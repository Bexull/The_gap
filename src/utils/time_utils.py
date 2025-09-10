from datetime import datetime, timedelta, time

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
