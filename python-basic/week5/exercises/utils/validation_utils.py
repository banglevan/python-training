"""
Module chứa các hàm validate dữ liệu.
"""

import re
from typing import Any, Optional, Union
from datetime import datetime
from enum import Enum

class ValidationType(Enum):
    """Enum cho các kiểu validate."""
    EMAIL = "email"
    PHONE = "phone"
    URL = "url"
    DATE = "date"
    NUMBER = "number"
    INTEGER = "integer"
    FLOAT = "float"
    BOOLEAN = "boolean"
    
def validate_email(email: str) -> bool:
    """Kiểm tra email hợp lệ."""
    pattern = r'^[\w\.-]+@[\w\.-]+\.\w+$'
    return bool(re.match(pattern, email))

def validate_phone(phone: str) -> bool:
    """Kiểm tra số điện thoại hợp lệ."""
    pattern = r'^\+?[\d\s-]{10,}$'
    return bool(re.match(pattern, phone))

def validate_url(url: str) -> bool:
    """Kiểm tra URL hợp lệ."""
    pattern = r'^https?://(?:[\w-]+\.)+[\w-]+(?:/[\w-./?%&=]*)?$'
    return bool(re.match(pattern, url))

def validate_date(
    date_str: str,
    format: str = "%Y-%m-%d"
) -> bool:
    """Kiểm tra ngày hợp lệ."""
    try:
        datetime.strptime(date_str, format)
        return True
    except ValueError:
        return False

def validate_number(
    value: Any,
    min_value: Optional[Union[int, float]] = None,
    max_value: Optional[Union[int, float]] = None
) -> bool:
    """
    Kiểm tra số hợp lệ.
    
    Args:
        value: Giá trị cần kiểm tra
        min_value: Giá trị tối thiểu
        max_value: Giá trị tối đa
        
    Returns:
        bool: True nếu hợp lệ
    """
    try:
        num = float(value)
        if min_value is not None and num < min_value:
            return False
        if max_value is not None and num > max_value:
            return False
        return True
    except (TypeError, ValueError):
        return False

def validate_integer(
    value: Any,
    min_value: Optional[int] = None,
    max_value: Optional[int] = None
) -> bool:
    """
    Kiểm tra số nguyên hợp lệ.
    
    Args:
        value: Giá trị cần kiểm tra
        min_value: Giá trị tối thiểu
        max_value: Giá trị tối đa
        
    Returns:
        bool: True nếu hợp lệ
    """
    try:
        num = int(float(value))
        if float(value) != num:  # Kiểm tra số thập phân
            return False
        if min_value is not None and num < min_value:
            return False
        if max_value is not None and num > max_value:
            return False
        return True
    except (TypeError, ValueError):
        return False

def validate_float(
    value: Any,
    min_value: Optional[float] = None,
    max_value: Optional[float] = None,
    precision: Optional[int] = None
) -> bool:
    """
    Kiểm tra số thực hợp lệ.
    
    Args:
        value: Giá trị cần kiểm tra
        min_value: Giá trị tối thiểu
        max_value: Giá trị tối đa
        precision: Số chữ số thập phân
        
    Returns:
        bool: True nếu hợp lệ
    """
    try:
        num = float(value)
        if min_value is not None and num < min_value:
            return False
        if max_value is not None and num > max_value:
            return False
        if precision is not None:
            str_num = f"{num:.{precision}f}"
            return float(str_num) == num
        return True
    except (TypeError, ValueError):
        return False

def validate_boolean(value: Any) -> bool:
    """Kiểm tra boolean hợp lệ."""
    if isinstance(value, bool):
        return True
    if isinstance(value, str):
        value = value.lower()
        return value in ('true', 'false', '1', '0')
    return False

def validate(
    value: Any,
    type: ValidationType,
    **kwargs
) -> bool:
    """
    Validate theo kiểu.
    
    Args:
        value: Giá trị cần validate
        type: Kiểu validate
        **kwargs: Tham số bổ sung
        
    Returns:
        bool: True nếu hợp lệ
    """
    if type == ValidationType.EMAIL:
        return validate_email(str(value))
    elif type == ValidationType.PHONE:
        return validate_phone(str(value))
    elif type == ValidationType.URL:
        return validate_url(str(value))
    elif type == ValidationType.DATE:
        format = kwargs.get('format', '%Y-%m-%d')
        return validate_date(str(value), format)
    elif type == ValidationType.NUMBER:
        return validate_number(
            value,
            kwargs.get('min_value'),
            kwargs.get('max_value')
        )
    elif type == ValidationType.INTEGER:
        return validate_integer(
            value,
            kwargs.get('min_value'),
            kwargs.get('max_value')
        )
    elif type == ValidationType.FLOAT:
        return validate_float(
            value,
            kwargs.get('min_value'),
            kwargs.get('max_value'),
            kwargs.get('precision')
        )
    elif type == ValidationType.BOOLEAN:
        return validate_boolean(value)
    return False 