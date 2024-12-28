"""
Unit tests cho simple_calculator.py
Tiêu chí đánh giá:
1. Functionality (40%): Các phép tính cơ bản hoạt động đúng
2. Edge Cases (30%): Xử lý các trường hợp đặc biệt
3. Exception Handling (20%): Xử lý lỗi phù hợp
4. Performance (10%): Thời gian thực thi trong giới hạn
"""

import pytest
import time
from simple_calculator import (
    get_numbers, add, subtract, 
    multiply, divide, format_result
)

class TestFunctionality:
    """
    Test chức năng cơ bản (40%)
    Pass: Tất cả phép tính cơ bản đúng, sai số ≤ 0.0001
    Fail: Có phép tính sai hoặc sai số > 0.0001
    """
    
    def test_add_basic(self):
        """Test phép cộng cơ bản"""
        assert add(10, 5) == 15.0
        assert add(-10, 5) == -5.0
        assert add(0.1, 0.2) == pytest.approx(0.3, abs=0.0001)

    def test_subtract_basic(self):
        """Test phép trừ cơ bản"""
        assert subtract(10, 5) == 5.0
        assert subtract(-10, 5) == -15.0
        assert subtract(0.3, 0.1) == pytest.approx(0.2, abs=0.0001)

    def test_multiply_basic(self):
        """Test phép nhân cơ bản"""
        assert multiply(10, 5) == 50.0
        assert multiply(-10, 5) == -50.0
        assert multiply(0.1, 0.2) == pytest.approx(0.02, abs=0.0001)

    def test_divide_basic(self):
        """Test phép chia cơ bản"""
        assert divide(10, 5) == 2.0
        assert divide(-10, 5) == -2.0
        assert divide(0.3, 0.1) == pytest.approx(3.0, abs=0.0001)

class TestEdgeCases:
    """
    Test các trường hợp đặc biệt (30%)
    Pass: Xử lý đúng ≥ 80% edge cases
    Fail: Xử lý đúng < 80% edge cases
    """
    
    def test_operations_with_zero(self):
        """Test các phép tính với số 0"""
        assert add(0, 0) == 0.0
        assert subtract(0, 0) == 0.0
        assert multiply(0, 5) == 0.0
        assert divide(0, 5) == 0.0

    def test_large_numbers(self):
        """Test với số lớn"""
        large_num = 1e15
        assert add(large_num, large_num) == 2e15
        assert multiply(large_num, 2) == 2e15

    def test_small_numbers(self):
        """Test với số rất nhỏ"""
        small_num = 1e-15
        assert add(small_num, small_num) == pytest.approx(2e-15, abs=1e-20)
        assert multiply(small_num, 2) == pytest.approx(2e-15, abs=1e-20)

class TestExceptionHandling:
    """
    Test xử lý ngoại lệ (20%)
    Pass: Xử lý đúng tất cả các loại exception
    Fail: Có exception không được xử lý
    """
    
    def test_divide_by_zero(self):
        """Test chia cho 0"""
        assert divide(10, 0) is None

    def test_invalid_input(self, monkeypatch):
        """Test với input không hợp lệ"""
        inputs = iter(["abc", "5"])
        monkeypatch.setattr('builtins.input', lambda _: next(inputs))
        num1, num2 = get_numbers()
        assert num1 is None
        assert num2 == 5.0

    def test_overflow(self):
        """Test với số quá lớn"""
        huge_num = 1e308
        with pytest.raises(OverflowError):
            multiply(huge_num, huge_num)

class TestPerformance:
    """
    Test hiệu năng (10%)
    Pass: Thời gian thực thi ≤ ngưỡng quy định
    Fail: Thời gian thực thi > ngưỡng quy định
    """
    
    def test_operation_performance(self):
        """Test thời gian thực hiện phép tính (ngưỡng: 0.001s)"""
        start_time = time.time()
        for _ in range(1000):
            add(10.0, 5.0)
            subtract(10.0, 5.0)
            multiply(10.0, 5.0)
            divide(10.0, 5.0)
        end_time = time.time()
        
        # Thời gian trung bình cho mỗi operation
        avg_time = (end_time - start_time) / 4000  # 1000 lần * 4 operations
        assert avg_time <= 0.001

    def test_format_result_performance(self):
        """Test thời gian format kết quả (ngưỡng: 0.0001s)"""
        start_time = time.time()
        for _ in range(1000):
            format_result("+", 10.0, 5.0, 15.0)
        end_time = time.time()
        
        avg_time = (end_time - start_time) / 1000
        assert avg_time <= 0.0001 