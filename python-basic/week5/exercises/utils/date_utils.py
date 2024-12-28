"""
Module chứa các hàm xử lý ngày tháng.
"""

from datetime import datetime, date, timedelta
from typing import Optional, Union, List
from enum import Enum

class DateFormat(Enum):
    """Enum cho các định dạng ngày."""
    ISO = "%Y-%m-%d"
    US = "%m/%d/%Y"
    EU = "%d/%m/%Y"
    FULL = "%Y-%m-%d %H:%M:%S"

def parse_date(
    date_str: str,
    format: Union[str, DateFormat] = DateFormat.ISO
) -> Optional[date]:
    """
    Chuyển chuỗi thành date.
    
    Args:
        date_str (str): Chuỗi ngày
        format (Union[str, DateFormat]): Định dạng
        
    Returns:
        Optional[date]: Date object hoặc None nếu lỗi
    """
    try:
        if isinstance(format, DateFormat):
            format = format.value
        return datetime.strptime(date_str, format).date()
    except ValueError:
        return None

def format_date(
    date_obj: Union[date, datetime],
    format: Union[str, DateFormat] = DateFormat.ISO
) -> str:
    """
    Định dạng date thành chuỗi.
    
    Args:
        date_obj (Union[date, datetime]): Date cần đ��nh dạng
        format (Union[str, DateFormat]): Định dạng
        
    Returns:
        str: Chuỗi đã định dạng
    """
    if isinstance(format, DateFormat):
        format = format.value
    return date_obj.strftime(format)

def add_days(
    date_obj: date,
    days: int
) -> date:
    """Cộng/trừ số ngày vào date."""
    return date_obj + timedelta(days=days)

def add_months(
    date_obj: date,
    months: int
) -> date:
    """
    Cộng/trừ số tháng vào date.
    
    Args:
        date_obj (date): Date gốc
        months (int): Số tháng cần cộng/trừ
        
    Returns:
        date: Date mới
    """
    year = date_obj.year + (date_obj.month + months - 1) // 12
    month = (date_obj.month + months - 1) % 12 + 1
    day = min(
        date_obj.day,
        [31, 29 if year % 4 == 0 and (year % 100 != 0 or year % 400 == 0)
         else 28, 31, 30, 31, 30, 31, 31, 30, 31, 30, 31][month - 1]
    )
    return date(year, month, day)

def add_years(
    date_obj: date,
    years: int
) -> date:
    """Cộng/trừ số năm vào date."""
    try:
        return date(date_obj.year + years, date_obj.month, date_obj.day)
    except ValueError:
        # Xử lý 29/02 năm nhuận
        return date(date_obj.year + years, date_obj.month, 28)

def date_range(
    start_date: date,
    end_date: date
) -> List[date]:
    """
    Tạo danh sách các ngày trong khoảng.
    
    Args:
        start_date (date): Ngày bắt đầu
        end_date (date): Ngày kết thúc
        
    Returns:
        List[date]: Danh sách các ngày
    """
    dates = []
    current = start_date
    while current <= end_date:
        dates.append(current)
        current = add_days(current, 1)
    return dates

def is_weekend(date_obj: date) -> bool:
    """Kiểm tra có phải cuối tuần."""
    return date_obj.weekday() >= 5

def get_age(
    birth_date: date,
    today: Optional[date] = None
) -> int:
    """
    Tính tuổi.
    
    Args:
        birth_date (date): Ngày sinh
        today (Optional[date]): Ngày hiện tại
        
    Returns:
        int: Số tuổi
    """
    if today is None:
        today = date.today()
    return (today.year - birth_date.year -
            ((today.month, today.day) < (birth_date.month, birth_date.day)))

def get_quarter(date_obj: date) -> int:
    """Lấy quý trong năm (1-4)."""
    return (date_obj.month - 1) // 3 + 1

def get_week_number(date_obj: date) -> int:
    """Lấy số tuần trong năm (1-53)."""
    return date_obj.isocalendar()[1] 