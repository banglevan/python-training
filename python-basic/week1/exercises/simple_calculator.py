"""
Bài tập 2: Máy tính đơn giản
Yêu cầu:
1. Thực hiện các phép tính cơ bản (+, -, *, /)
2. Kiểm tra tính hợp lệ của input
3. Xử lý các trường hợp đặc biệt (chia cho 0)
4. Format kết quả đẹp
"""

from typing import Tuple, Optional

def get_numbers() -> Tuple[Optional[float], Optional[float]]:
    """
    Nhận và kiểm tra input là 2 số từ người dùng.
    
    Returns:
        Tuple[Optional[float], Optional[float]]: (số thứ nhất, số thứ hai)
        Nếu input không hợp lệ, trả về None cho số tương ứng
    
    Example:
        >>> num1, num2 = get_numbers()  # Nhập: "10" và "5"
        >>> print(num1, num2)
        10.0 5.0
    """
    # TODO: Implement this function
    pass

def add(a: float, b: float) -> float:
    """
    Cộng hai số.
    
    Args:
        a (float): Số thứ nhất
        b (float): Số thứ hai
    
    Returns:
        float: Tổng hai số
    
    Example:
        >>> add(10, 5)
        15.0
    """
    # TODO: Implement this function
    pass

def subtract(a: float, b: float) -> float:
    """
    Trừ hai số.
    
    Args:
        a (float): Số thứ nhất
        b (float): Số thứ hai
    
    Returns:
        float: Hiệu hai số
    
    Example:
        >>> subtract(10, 5)
        5.0
    """
    # TODO: Implement this function
    pass

def multiply(a: float, b: float) -> float:
    """
    Nhân hai số.
    
    Args:
        a (float): Số thứ nhất
        b (float): Số thứ hai
    
    Returns:
        float: Tích hai số
    
    Example:
        >>> multiply(10, 5)
        50.0
    """
    # TODO: Implement this function
    pass

def divide(a: float, b: float) -> Optional[float]:
    """
    Chia hai số.
    
    Args:
        a (float): Số thứ nhất
        b (float): Số thứ hai
    
    Returns:
        Optional[float]: Thương hai số, None nếu chia cho 0
    
    Example:
        >>> divide(10, 5)
        2.0
        >>> divide(10, 0)
        None
    """
    # TODO: Implement this function
    pass

def format_result(operation: str, a: float, b: float, result: Optional[float]) -> str:
    """
    Format kết quả phép tính.
    
    Args:
        operation (str): Phép tính (+, -, *, /)
        a (float): Số thứ nhất
        b (float): Số thứ hai
        result (Optional[float]): Kết quả phép tính
    
    Returns:
        str: Chuỗi kết quả được format
    
    Example:
        >>> format_result("+", 10, 5, 15)
        '10 + 5 = 15'
    """
    # TODO: Implement this function
    pass

def main():
    """
    Hàm chính của chương trình.
    """
    operations = {
        '+': add,
        '-': subtract,
        '*': multiply,
        '/': divide
    }
    
    while True:
        print("\nMáy tính đơn giản")
        print("1. Cộng (+)")
        print("2. Trừ (-)")
        print("3. Nhân (*)")
        print("4. Chia (/)")
        print("5. Thoát")
        
        choice = input("Chọn phép tính (1-5): ")
        if choice == '5':
            break
            
        if choice not in ['1', '2', '3', '4']:
            print("Lựa chọn không hợp lệ!")
            continue
            
        op = ['+', '-', '*', '/'][int(choice)-1]
        num1, num2 = get_numbers()
        
        if num1 is None or num2 is None:
            print("Input không hợp lệ!")
            continue
            
        result = operations[op](num1, num2)
        print(format_result(op, num1, num2, result))

if __name__ == "__main__":
    main() 