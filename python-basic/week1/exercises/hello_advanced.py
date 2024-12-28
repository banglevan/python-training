"""
Bài tập 1: Hello World nâng cao
Yêu cầu:
1. Nhận input tên và tuổi từ người dùng
2. Kiểm tra tính hợp lệ của input
3. Tính năm sinh
4. In ra lời chào và thông tin chi tiết
"""

from datetime import datetime
from typing import Tuple, Optional

def get_user_input() -> Tuple[str, Optional[int]]:
    """
    Nhận và kiểm tra input từ người dùng.
    
    Returns:
        Tuple[str, Optional[int]]: (tên, tuổi)
        Nếu tuổi không hợp lệ, trả về None cho tuổi
    
    Example:
        >>> name, age = get_user_input()  # Nhập: "Alice" và "25"
        >>> print(name, age)
        'Alice' 25
    """
    # TODO: Implement this function
    pass

def calculate_birth_year(age: int) -> int:
    """
    Tính năm sinh dựa trên tuổi.
    
    Args:
        age (int): Tuổi hiện tại
    
    Returns:
        int: Năm sinh
    
    Example:
        >>> calculate_birth_year(25)  # Giả sử năm hiện tại là 2024
        1999
    """
    # TODO: Implement this function
    pass

def generate_greeting(name: str, age: Optional[int]) -> str:
    """
    Tạo lời chào với thông tin người dùng.
    
    Args:
        name (str): Tên người dùng
        age (Optional[int]): Tuổi người dùng
    
    Returns:
        str: Lời chào được định dạng
    
    Example:
        >>> print(generate_greeting("Alice", 25))
        'Xin chào Alice! Bạn 25 tuổi và sinh năm 1999.'
        >>> print(generate_greeting("Bob", None))
        'Xin chào Bob!'
    """
    # TODO: Implement this function
    pass

def main():
    """
    Hàm chính của chương trình.
    """
    try:
        name, age = get_user_input()
        greeting = generate_greeting(name, age)
        print(greeting)
    except Exception as e:
        print(f"Có lỗi xảy ra: {e}")

if __name__ == "__main__":
    main() 