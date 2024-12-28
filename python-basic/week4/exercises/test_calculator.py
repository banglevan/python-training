"""
Unit tests cho calculator.py
Tiêu chí đánh giá:
1. Basic Operations (30%): Các phép tính cơ bản
2. Math Functions (30%): Các hàm toán học
3. Input Validation (30%): Kiểm tra đầu vào
4. History Management (10%): Quản lý lịch sử
"""

import pytest
from datetime import datetime
from calculator import (
    Number,
    Calculation,
    Calculator,
    validate_number,
    add,
    subtract,
    multiply,
    divide,
    modulo,
    power,
    square_root,
    absolute,
    round_number
)

@pytest.fixture
def calculator():
    """Tạo instance Calculator."""
    return Calculator()

class TestBasicOperations:
    """
    Test các phép tính cơ bản (30%)
    Pass: Tính đúng ≥ 95% cases
    Fail: Tính đúng < 95% cases
    """
    
    def test_add(self):
        """Test phép cộng."""
        assert add(2, 3) == 5
        assert add(-1, 1) == 0
        assert add(0.1, 0.2) == pytest.approx(0.3)
        assert add(1000000, 1000000) == 2000000

    def test_subtract(self):
        """Test phép trừ."""
        assert subtract(5, 3) == 2
        assert subtract(-1, -1) == 0
        assert subtract(0.3, 0.1) == pytest.approx(0.2)
        assert subtract(1000000, 1000000) == 0

    def test_multiply(self):
        """Test phép nhân."""
        assert multiply(2, 3) == 6
        assert multiply(-2, 3) == -6
        assert multiply(0.1, 0.2) == pytest.approx(0.02)
        assert multiply(1000, 1000) == 1000000

    def test_divide(self):
        """Test phép chia."""
        assert divide(6, 2) == 3
        assert divide(-6, 2) == -3
        assert divide(0.3, 0.1) == pytest.approx(3)
        
        with pytest.raises(ValueError):
            divide(1, 0)

    def test_modulo(self):
        """Test phép chia lấy dư."""
        assert modulo(7, 3) == 1
        assert modulo(-7, 3) == 2
        assert modulo(10.5, 3) == pytest.approx(1.5)
        
        with pytest.raises(ValueError):
            modulo(1, 0)

    def test_power(self):
        """Test phép lũy thừa."""
        assert power(2, 3) == 8
        assert power(2, -1) == 0.5
        assert power(2, 0.5) == pytest.approx(1.4142135623730951)
        
        with pytest.raises(ValueError):
            power(0, -1)

class TestMathFunctions:
    """
    Test các hàm toán học (30%)
    Pass: Tính đúng ≥ 95% cases
    Fail: Tính đúng < 95% cases
    """
    
    def test_square_root(self):
        """Test căn bậc hai."""
        assert square_root(4) == 2
        assert square_root(2) == pytest.approx(1.4142135623730951)
        assert square_root(0) == 0
        
        with pytest.raises(ValueError):
            square_root(-1)

    def test_absolute(self):
        """Test giá trị tuyệt đối."""
        assert absolute(-1) == 1
        assert absolute(1) == 1
        assert absolute(0) == 0
        assert absolute(-1.5) == 1.5

    def test_round_number(self):
        """Test làm tròn số."""
        assert round_number(1.4) == 1
        assert round_number(1.6) == 2
        assert round_number(1.234, 2) == 1.23
        assert round_number(-1.5) == -2

class TestInputValidation:
    """
    Test kiểm tra đầu vào (30%)
    Pass: Validate đúng ≥ 90% cases
    Fail: Validate đúng < 90% cases
    """
    
    def test_validate_number(self):
        """Test validate số."""
        assert validate_number("123") == 123
        assert validate_number("-123") == -123
        assert validate_number("123.45") == 123.45
        assert validate_number("-123.45") == -123.45
        assert validate_number("abc") is None
        assert validate_number("12.34.56") is None

    def test_calculator_expressions(self, calculator):
        """Test biểu thức tính toán."""
        valid_expressions = [
            ("2 + 3", 5),
            ("5 - 3", 2),
            ("2 * 3", 6),
            ("6 / 2", 3),
            ("7 % 3", 1),
            ("2 ** 3", 8),
            ("sqrt(16)", 4),
            ("abs(-5)", 5),
            ("round(1.234, 2)", 1.23)
        ]
        
        for expr, expected in valid_expressions:
            assert calculator.calculate(expr) == expected

        invalid_expressions = [
            "",  # Rỗng
            "1 + ",  # Thiếu toán hạng
            "1 + 2 + 3",  # Quá nhiều toán hạng
            "1 & 2",  # Toán tử không hợp lệ
            "sqrt",  # Thiếu tham số
            "sqrt()",  # Tham số rỗng
            "sqrt(a)",  # Tham số không hợp lệ
            "unknown(1)"  # Hàm không tồn tại
        ]
        
        for expr in invalid_expressions:
            with pytest.raises(ValueError):
                calculator.calculate(expr)

class TestHistoryManagement:
    """
    Test quản lý lịch sử (10%)
    Pass: Quản lý đúng mọi cases
    Fail: Có case quản lý sai
    """
    
    def test_history_storage(self, calculator):
        """Test lưu lịch sử."""
        # Tính và kiểm tra lịch sử
        calculator.calculate("2 + 3")
        calculator.calculate("5 - 3")
        
        history = calculator.get_history()
        assert len(history) == 2
        assert isinstance(history[0], Calculation)
        assert history[0].expression == "2 + 3"
        assert history[0].result == 5
        assert isinstance(history[0].timestamp, datetime)

    def test_history_limit(self, calculator):
        """Test giới hạn lịch sử."""
        # Tính nhiều lần
        for i in range(5):
            calculator.calculate(f"{i} + 1")
            
        # Kiểm tra limit
        limited = calculator.get_history(3)
        assert len(limited) == 3
        assert limited[-1].expression == "4 + 1"

    def test_clear_history(self, calculator):
        """Test xóa lịch sử."""
        calculator.calculate("1 + 1")
        assert len(calculator.get_history()) == 1
        
        calculator.clear_history()
        assert len(calculator.get_history()) == 0

    def test_history_disabled(self, calculator):
        """Test tắt lưu lịch sử."""
        calculator.calculate("1 + 1", store_history=False)
        assert len(calculator.get_history()) == 0 