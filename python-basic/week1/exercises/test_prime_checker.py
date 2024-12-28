"""
Unit tests cho prime_checker.py
Tiêu chí đánh giá:
1. Functionality (40%): Kiểm tra số nguyên tố chính xác
2. Edge Cases (30%): Xử lý các số đặc biệt
3. Exception Handling (20%): Xử lý input không hợp lệ
4. Performance (10%): Tối ưu cho số lớn
"""

import pytest
import time
from prime_checker import is_prime, find_primes_in_range, get_valid_number

class TestFunctionality:
    """
    Test chức năng cơ bản (40%)
    Pass: Tất cả test cases cơ bản đúng
    Fail: Có ít nhất 1 test case sai
    """
    
    def test_is_prime_basic(self):
        """Test các số nguyên tố cơ bản"""
        assert is_prime(2) is True
        assert is_prime(3) is True
        assert is_prime(17) is True
        assert is_prime(4) is False
        assert is_prime(15) is False

    def test_find_primes_basic(self):
        """Test tìm số nguyên tố trong khoảng nhỏ"""
        assert find_primes_in_range(1, 10) == [2, 3, 5, 7]
        assert find_primes_in_range(10, 20) == [11, 13, 17, 19]
        assert find_primes_in_range(1, 1) == []

class TestEdgeCases:
    """
    Test các trường hợp đặc biệt (30%)
    Pass: Xử lý đúng ≥ 80% edge cases
    Fail: Xử lý đúng < 80% edge cases
    """
    
    def test_edge_numbers(self):
        """Test với các số đặc biệt"""
        assert is_prime(0) is False
        assert is_prime(1) is False
        assert is_prime(2) is True  # Số nguyên tố chẵn duy nhất

    def test_large_numbers(self):
        """Test với số lớn"""
        assert is_prime(104729) is True   # Số nguyên tố thứ 10000
        assert is_prime(104730) is False

    def test_empty_ranges(self):
        """Test với khoảng trống"""
        assert find_primes_in_range(20, 10) == []  # start > end
        assert find_primes_in_range(14, 16) == []  # Không có số nguyên tố

class TestExceptionHandling:
    """
    Test xử lý ngoại lệ (20%)
    Pass: Xử lý đúng tất cả exceptions
    Fail: Có exception không được xử lý
    """
    
    def test_invalid_input(self):
        """Test với input không hợp lệ"""
        with pytest.raises(ValueError):
            is_prime(-1)
        with pytest.raises(ValueError):
            find_primes_in_range(-10, 10)

    def test_get_valid_number(self, monkeypatch):
        """Test validate input người dùng"""
        inputs = iter(["abc", "-5", "23"])
        monkeypatch.setattr('builtins.input', lambda _: next(inputs))
        assert get_valid_number("Test: ") == 23

class TestPerformance:
    """
    Test hiệu năng (10%)
    Pass: Thời gian thực thi ≤ ngưỡng quy định
    Fail: Thời gian thực thi > ngưỡng quy định
    """
    
    def test_is_prime_performance(self):
        """Test hiệu năng kiểm tra số nguyên tố (ngưỡng: 0.1s)"""
        start_time = time.time()
        is_prime(104729)  # Số nguyên tố thứ 10000
        end_time = time.time()
        assert end_time - start_time <= 0.1

    def test_find_primes_performance(self):
        """Test hiệu năng tìm số nguyên tố (ngưỡng: 1s cho khoảng 1000 số)"""
        start_time = time.time()
        find_primes_in_range(1, 1000)
        end_time = time.time()
        assert end_time - start_time <= 1 