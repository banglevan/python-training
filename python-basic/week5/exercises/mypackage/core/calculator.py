"""
Calculator module
"""

from typing import Union, List
from ..exceptions import CalculationError

Number = Union[int, float]

class Calculator:
    """Basic calculator."""
    
    @staticmethod
    def add(x: Number, y: Number) -> Number:
        """Cộng hai số."""
        return x + y
    
    @staticmethod
    def subtract(x: Number, y: Number) -> Number:
        """Trừ hai số."""
        return x - y
    
    @staticmethod
    def multiply(x: Number, y: Number) -> Number:
        """Nhân hai số."""
        return x * y
    
    @staticmethod
    def divide(x: Number, y: Number) -> float:
        """Chia hai số."""
        if y == 0:
            raise CalculationError("Division by zero")
        return x / y
    
    @staticmethod
    def power(x: Number, y: Number) -> Number:
        """Lũy thừa."""
        try:
            return x ** y
        except Exception as e:
            raise CalculationError(f"Power error: {e}")
    
    @staticmethod
    def average(numbers: List[Number]) -> float:
        """Tính trung bình."""
        if not numbers:
            raise CalculationError("Empty list")
        return sum(numbers) / len(numbers) 