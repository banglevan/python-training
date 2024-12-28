"""
Unit tests cho hello_advanced.py
Tiêu chí đánh giá:
1. Functionality (40%): Các hàm hoạt động đúng theo yêu cầu
2. Edge Cases (30%): Xử lý các trường hợp đặc biệt
3. Exception Handling (20%): Xử lý lỗi phù hợp
4. Performance (10%): Thời gian thực thi trong giới hạn
"""

import pytest
import time
from datetime import datetime
from hello_advanced import get_user_input, calculate_birth_year, generate_greeting

class TestFunctionality:
    """
    Test chức năng cơ bản (40%)
    Pass: Tất cả test cases cơ bản đều pass
    Fail: Có ít nhất 1 test case cơ bản fail
    """
    
    def test_get_user_input_basic(self, monkeypatch):
        """Test nhập tên và tuổi hợp lệ"""
        inputs = iter(["Alice", "25"])
        monkeypatch.setattr('builtins.input', lambda _: next(inputs))
        name, age = get_user_input()
        assert name == "Alice"
        assert age == 25

    def test_calculate_birth_year_basic(self):
        """Test tính năm sinh với tuổi hợp lệ"""
        current_year = datetime.now().year
        assert calculate_birth_year(25) == current_year - 25

    def test_generate_greeting_basic(self):
        """Test tạo lời chào cơ bản"""
        greeting = generate_greeting("Alice", 25)
        assert "Alice" in greeting
        assert "25" in greeting
        assert "sinh năm" in greeting

class TestEdgeCases:
    """
    Test các trường hợp đặc biệt (30%)
    Pass: Xử lý đúng ≥ 80% edge cases
    Fail: Xử lý đúng < 80% edge cases
    """
    
    def test_get_user_input_empty(self, monkeypatch):
        """Test với tên rỗng"""
        inputs = iter(["", "25"])
        monkeypatch.setattr('builtins.input', lambda _: next(inputs))
        with pytest.raises(ValueError):
            get_user_input()

    def test_get_user_input_special_chars(self, monkeypatch):
        """Test với tên chứa ký tự đặc biệt"""
        inputs = iter(["Alice@123", "25"])
        monkeypatch.setattr('builtins.input', lambda _: next(inputs))
        name, age = get_user_input()
        assert name == "Alice@123"  # Cho phép ký tự đặc biệt trong tên

    def test_calculate_birth_year_edge(self):
        """Test với các giá trị tuổi đặc biệt"""
        current_year = datetime.now().year
        assert calculate_birth_year(0) == current_year
        assert calculate_birth_year(100) == current_year - 100
        assert calculate_birth_year(1) == current_year - 1

class TestExceptionHandling:
    """
    Test xử lý ngoại lệ (20%)
    Pass: Xử lý đúng tất cả các loại exception
    Fail: Có exception không được xử lý
    """
    
    def test_get_user_input_invalid_age(self, monkeypatch):
        """Test với tuổi không hợp lệ"""
        inputs = iter(["Alice", "abc"])
        monkeypatch.setattr('builtins.input', lambda _: next(inputs))
        name, age = get_user_input()
        assert name == "Alice"
        assert age is None

    def test_calculate_birth_year_negative(self):
        """Test với tuổi âm"""
        with pytest.raises(ValueError):
            calculate_birth_year(-1)

    def test_generate_greeting_none_values(self):
        """Test với giá trị None"""
        with pytest.raises(ValueError):
            generate_greeting(None, 25)

class TestPerformance:
    """
    Test hiệu năng (10%)
    Pass: Thời gian thực thi ≤ ngưỡng quy định
    Fail: Thời gian thực thi > ngưỡng quy định
    """
    
    def test_get_user_input_performance(self, monkeypatch):
        """Test thời gian xử lý input (ngưỡng: 0.1s)"""
        inputs = iter(["Alice", "25"])
        monkeypatch.setattr('builtins.input', lambda _: next(inputs))
        
        start_time = time.time()
        get_user_input()
        end_time = time.time()
        
        assert end_time - start_time <= 0.1

    def test_generate_greeting_performance(self):
        """Test thời gian tạo lời chào (ngưỡng: 0.01s)"""
        start_time = time.time()
        generate_greeting("Alice", 25)
        end_time = time.time()
        
        assert end_time - start_time <= 0.01 