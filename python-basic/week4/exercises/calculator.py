"""
Bài tập 1: Calculator với functions
Yêu cầu:
1. Các phép tính cơ bản (+, -, *, /, %, **)
2. Các hàm toán học (sqrt, pow, abs, round)
3. Xử lý ngoại lệ và kiểm tra đầu vào
4. Lưu lịch sử tính toán
"""

from typing import Union, List, Tuple, Optional
from dataclasses import dataclass
from datetime import datetime
import math
import re

Number = Union[int, float]  # Type alias cho số

@dataclass
class Calculation:
    """Class lưu thông tin một phép tính."""
    expression: str  # Biểu thức (vd: "2 + 3")
    result: Number  # Kết quả
    timestamp: datetime  # Thời điểm tính

def validate_number(value: str) -> Optional[Number]:
    """
    Kiểm tra và chuyển đổi chuỗi thành số.
    
    Args:
        value (str): Chuỗi cần kiểm tra
        
    Returns:
        Optional[Number]: Số nếu hợp lệ, None nếu không
    """
    try:
        # Thử chuyển thành int
        return int(value)
    except ValueError:
        try:
            # Thử chuyển thành float
            return float(value)
        except ValueError:
            return None

def add(a: Number, b: Number) -> Number:
    """Cộng hai số."""
    return a + b

def subtract(a: Number, b: Number) -> Number:
    """Trừ hai số."""
    return a - b

def multiply(a: Number, b: Number) -> Number:
    """Nhân hai số."""
    return a * b

def divide(a: Number, b: Number) -> Number:
    """
    Chia hai số.
    
    Raises:
        ValueError: Nếu chia cho 0
    """
    if b == 0:
        raise ValueError("Không thể chia cho 0")
    return a / b

def modulo(a: Number, b: Number) -> Number:
    """
    Tính phần dư.
    
    Raises:
        ValueError: Nếu chia cho 0
    """
    if b == 0:
        raise ValueError("Không thể chia cho 0")
    return a % b

def power(a: Number, b: Number) -> Number:
    """
    Lũy thừa.
    
    Raises:
        ValueError: Nếu không tính được
    """
    try:
        return math.pow(a, b)
    except (ValueError, OverflowError) as e:
        raise ValueError(f"Không thể tính lũy thừa: {e}")

def square_root(a: Number) -> Number:
    """
    Căn bậc hai.
    
    Raises:
        ValueError: Nếu số âm
    """
    if a < 0:
        raise ValueError("Không thể tính căn bậc hai của số âm")
    return math.sqrt(a)

def absolute(a: Number) -> Number:
    """Giá trị tuyệt đối."""
    return abs(a)

def round_number(a: Number, decimals: int = 0) -> Number:
    """
    Làm tròn số.
    
    Args:
        a (Number): Số cần làm tròn
        decimals (int): Số chữ số thập phân
        
    Returns:
        Number: Số đã làm tròn
    """
    return round(a, decimals)

class Calculator:
    """Class thực hiện tính toán."""
    
    def __init__(self):
        """Khởi tạo calculator."""
        self.history: List[Calculation] = []
        self.operations = {
            '+': add,
            '-': subtract,
            '*': multiply,
            '/': divide,
            '%': modulo,
            '**': power,
            'sqrt': square_root,
            'abs': absolute,
            'round': round_number
        }

    def calculate(
        self,
        expression: str,
        store_history: bool = True
    ) -> Number:
        """
        Tính toán biểu thức.
        
        Args:
            expression (str): Biểu thức cần tính
            store_history (bool): Có lưu vào history không
            
        Returns:
            Number: Kết quả
            
        Raises:
            ValueError: Nếu biểu thức không hợp lệ
        """
        # Chuẩn hóa biểu thức
        expression = expression.strip().lower()
        
        # Kiểm tra các hàm đặc biệt
        for func in ['sqrt', 'abs', 'round']:
            if expression.startswith(func):
                # Tách tham số
                params = expression[len(func):].strip('() ').split(',')
                params = [p.strip() for p in params]
                
                # Validate và chuyển đổi tham số
                numbers = []
                for p in params:
                    num = validate_number(p)
                    if num is None:
                        raise ValueError(f"Tham số không hợp lệ: {p}")
                    numbers.append(num)
                
                # Tính toán
                result = self.operations[func](*numbers)
                
                if store_history:
                    self.history.append(Calculation(
                        expression=expression,
                        result=result,
                        timestamp=datetime.now()
                    ))
                return result
        
        # Xử lý các phép tính hai ngôi
        match = re.match(
            r'^(-?\d*\.?\d+)\s*([+\-*/%]|\*\*)\s*(-?\d*\.?\d+)$',
            expression
        )
        if not match:
            raise ValueError("Biểu thức không hợp lệ")
            
        a = validate_number(match.group(1))
        op = match.group(2)
        b = validate_number(match.group(3))
        
        if None in (a, b):
            raise ValueError("Số không hợp lệ")
            
        result = self.operations[op](a, b)
        
        if store_history:
            self.history.append(Calculation(
                expression=expression,
                result=result,
                timestamp=datetime.now()
            ))
        return result

    def get_history(
        self,
        limit: Optional[int] = None
    ) -> List[Calculation]:
        """
        Lấy lịch sử tính toán.
        
        Args:
            limit (Optional[int]): Số lượng kết quả tối đa
            
        Returns:
            List[Calculation]: Danh sách phép tính
        """
        if limit is None:
            return self.history
        return self.history[-limit:]

    def clear_history(self) -> None:
        """Xóa lịch sử tính toán."""
        self.history.clear()

def main():
    """Chương trình chính."""
    calc = Calculator()

    while True:
        print("\nCalculator")
        print("1. Tính toán")
        print("2. Xem lịch sử")
        print("3. Xóa lịch sử")
        print("4. Thoát")

        choice = input("\nChọn chức năng (1-4): ")

        if choice == '4':
            break
        elif choice == '1':
            print("\nNhập biểu thức (vd: 2 + 3, sqrt(16), abs(-5)):")
            expression = input()
            
            try:
                result = calc.calculate(expression)
                print(f"Kết quả: {result}")
            except ValueError as e:
                print(f"Lỗi: {e}")
                
        elif choice == '2':
            history = calc.get_history()
            if history:
                print("\nLịch sử tính toán:")
                for calc in history:
                    print(
                        f"{calc.timestamp.strftime('%H:%M:%S')}: "
                        f"{calc.expression} = {calc.result}"
                    )
            else:
                print("Chưa có lịch sử tính toán!")
                
        elif choice == '3':
            calc.clear_history()
            print("Đã xóa lịch sử tính toán!")
            
        else:
            print("Lựa chọn không hợp lệ!")

if __name__ == "__main__":
    main() 