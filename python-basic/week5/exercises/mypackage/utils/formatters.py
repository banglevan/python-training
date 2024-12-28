"""
Formatting utilities
"""

from typing import Union, Optional
from datetime import datetime, date
from ..exceptions import FormattingError

def format_number(
    value: Union[int, float],
    precision: Optional[int] = None,
    thousands_sep: str = ",",
    decimal_sep: str = "."
) -> str:
    """
    Định dạng số.
    
    Args:
        value: Số cần định dạng
        precision: Số chữ số thập phân
        thousands_sep: Ký tự phân cách hàng nghìn
        decimal_sep: Ký tự phân cách thập phân
        
    Returns:
        str: Chuỗi đã định dạng
        
    Raises:
        FormattingError: Nếu định dạng lỗi
    """
    try:
        # Split integer and decimal parts
        if precision is not None:
            value = round(value, precision)
        str_value = str(value)
        parts = str_value.split('.')
        
        # Format integer part
        int_part = parts[0]
        int_groups = []
        for i in range(len(int_part), 0, -3):
            start = max(0, i - 3)
            int_groups.insert(0, int_part[start:i])
        formatted_int = thousands_sep.join(int_groups)
        
        # Format decimal part
        if len(parts) > 1:
            dec_part = parts[1]
            if precision is not None:
                dec_part = dec_part[:precision]
            return f"{formatted_int}{decimal_sep}{dec_part}"
            
        return formatted_int
        
    except Exception as e:
        raise FormattingError(f"Number formatting error: {e}")

def format_date(
    value: Union[date, datetime],
    format: str = "%Y-%m-%d",
    locale: Optional[str] = None
) -> str:
    """
    Định dạng ngày tháng.
    
    Args:
        value: Ngày cần định dạng
        format: Định dạng
        locale: Locale code
        
    Returns:
        str: Chuỗi đã định dạng
        
    Raises:
        FormattingError: Nếu định dạng lỗi
    """
    try:
        if locale:
            import locale as loc
            loc.setlocale(loc.LC_TIME, locale)
            
        return value.strftime(format)
        
    except Exception as e:
        raise FormattingError(f"Date formatting error: {e}") 