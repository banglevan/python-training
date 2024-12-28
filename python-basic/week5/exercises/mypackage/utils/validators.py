"""
Validation utilities
"""

from typing import Any, Union, Optional
import re
from ..exceptions import ValidationError

def validate_number(
    value: Any,
    min_value: Optional[Union[int, float]] = None,
    max_value: Optional[Union[int, float]] = None,
    integer: bool = False
) -> bool:
    """
    Kiểm tra số hợp lệ.
    
    Args:
        value: Giá trị cần kiểm tra
        min_value: Giá trị tối thiểu
        max_value: Giá trị tối đa
        integer: Yêu cầu số nguyên
        
    Returns:
        bool: True nếu hợp lệ
        
    Raises:
        ValidationError: Nếu giá trị không hợp lệ
    """
    try:
        # Convert to number
        num = int(value) if integer else float(value)
        
        # Check type
        if integer and not isinstance(num, int):
            raise ValidationError("Value must be an integer")
            
        # Check range
        if min_value is not None and num < min_value:
            raise ValidationError(f"Value must be >= {min_value}")
        if max_value is not None and num > max_value:
            raise ValidationError(f"Value must be <= {max_value}")
            
        return True
        
    except (TypeError, ValueError):
        raise ValidationError("Invalid number format")

def validate_string(
    value: str,
    min_length: Optional[int] = None,
    max_length: Optional[int] = None,
    pattern: Optional[str] = None,
    strip: bool = True
) -> bool:
    """
    Kiểm tra chuỗi hợp lệ.
    
    Args:
        value: Chuỗi cần kiểm tra
        min_length: Độ dài tối thiểu
        max_length: Độ dài tối đa
        pattern: Regex pattern
        strip: Cắt khoảng trắng
        
    Returns:
        bool: True nếu hợp lệ
        
    Raises:
        ValidationError: Nếu chuỗi không hợp lệ
    """
    if not isinstance(value, str):
        raise ValidationError("Value must be a string")
        
    # Strip whitespace
    if strip:
        value = value.strip()
        
    # Check length
    if min_length is not None and len(value) < min_length:
        raise ValidationError(f"String length must be >= {min_length}")
    if max_length is not None and len(value) > max_length:
        raise ValidationError(f"String length must be <= {max_length}")
        
    # Check pattern
    if pattern is not None and not re.match(pattern, value):
        raise ValidationError("String does not match pattern")
        
    return True 