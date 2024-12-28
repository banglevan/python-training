"""
Unit tests cho Temperature Converter
Tiêu chí đánh giá:
1. Functionality (40%): Các phép chuyển đổi chính xác
2. Edge Cases (30%): Xử lý các giá trị đặc biệt
3. Exception Handling (20%): Xử lý lỗi input
4. Performance (10%): Thời gian xử lý và I/O
"""

import pytest
import time
import json
from pathlib import Path
from temperature_converter import TemperatureConverter

class TestFunctionality:
    """
    Test chức năng cơ bản (40%)
    Pass: Sai số ≤ 0.01°C/°F/K
    Fail: Sai số > 0.01°C/°F/K
    """
    
    def test_celsius_to_fahrenheit(self):
        converter = TemperatureConverter()
        assert converter.celsius_to_fahrenheit(0) == pytest.approx(32, abs=0.01)
        assert converter.celsius_to_fahrenheit(100) == pytest.approx(212, abs=0.01)
        assert converter.celsius_to_fahrenheit(-40) == pytest.approx(-40, abs=0.01)

    def test_fahrenheit_to_celsius(self):
        converter = TemperatureConverter()
        assert converter.fahrenheit_to_celsius(32) == pytest.approx(0, abs=0.01)
        assert converter.fahrenheit_to_celsius(212) == pytest.approx(100, abs=0.01)
        assert converter.fahrenheit_to_celsius(-40) == pytest.approx(-40, abs=0.01)

    def test_celsius_to_kelvin(self):
        converter = TemperatureConverter()
        assert converter.celsius_to_kelvin(0) == pytest.approx(273.15, abs=0.01)
        assert converter.celsius_to_kelvin(-273.15) == pytest.approx(0, abs=0.01)
        assert converter.celsius_to_kelvin(100) == pytest.approx(373.15, abs=0.01)

class TestEdgeCases:
    """
    Test các trường hợp đặc biệt (30%)
    Pass: Xử lý đúng ≥ 80% edge cases
    Fail: Xử lý đúng < 80% edge cases
    """
    
    def test_absolute_zero(self):
        """Test với nhiệt độ 0K"""
        converter = TemperatureConverter()
        with pytest.raises(ValueError):
            converter.kelvin_to_celsius(-1)  # Nhiệt độ < 0K

    def test_extreme_temperatures(self):
        """Test với nhiệt độ cực cao/thấp"""
        converter = TemperatureConverter()
        # Nhiệt độ cao nhất trong vũ trụ (~1.416785(71)×10^32 K)
        assert converter.kelvin_to_celsius(1.4e32) > 0
        # Nhiệt độ thấp nhất có thể (0K)
        assert converter.kelvin_to_celsius(0) == pytest.approx(-273.15, abs=0.01)

    def test_input_validation(self, monkeypatch):
        """Test validate input người dùng"""
        converter = TemperatureConverter()
        inputs = iter(["abc", "-300", "25"])
        monkeypatch.setattr('builtins.input', lambda _: next(inputs))
        
        temp = converter.get_valid_temperature("C")
        assert temp == 25.0

class TestExceptionHandling:
    """
    Test xử lý ngoại lệ (20%)
    Pass: Xử lý đúng tất cả exceptions
    Fail: Có exception không được xử lý
    """
    
    def test_invalid_file_handling(self):
        """Test với file lịch sử không hợp lệ"""
        converter = TemperatureConverter("invalid/path/history.json")
        assert converter.history == []  # Khởi tạo list rỗng nếu file không tồn tại

    def test_corrupted_history_file(self, tmp_path):
        """Test với file lịch sử bị hỏng"""
        history_file = tmp_path / "corrupted_history.json"
        history_file.write_text("invalid json content")
        
        converter = TemperatureConverter(str(history_file))
        assert converter.history == []  # Khởi tạo list rỗng nếu file bị hỏng

class TestPerformance:
    """
    Test hiệu năng (10%)
    Pass: Thời gian xử lý ≤ ngưỡng quy định
    Fail: Thời gian xử lý > ngưỡng quy định
    """
    
    def test_conversion_performance(self):
        """Test thời gian chuyển đổi (ngưỡng: 0.001s)"""
        converter = TemperatureConverter()
        start_time = time.time()
        
        for _ in range(1000):
            converter.celsius_to_fahrenheit(20)
            converter.fahrenheit_to_celsius(68)
            converter.celsius_to_kelvin(20)
            converter.kelvin_to_celsius(293.15)
        
        end_time = time.time()
        avg_time = (end_time - start_time) / 4000  # 1000 lần * 4 conversions
        assert avg_time <= 0.001

    def test_history_performance(self, tmp_path):
        """Test thời gian xử lý lịch sử (ngưỡng: 0.1s)"""
        history_file = tmp_path / "test_history.json"
        converter = TemperatureConverter(str(history_file))
        
        # Tạo 1000 bản ghi lịch sử
        for i in range(1000):
            converter.save_conversion(20, "C", 68, "F")
        
        start_time = time.time()
        converter.display_history(limit=100)  # Hiển thị 100 bản ghi gần nhất
        end_time = time.time()
        
        assert end_time - start_time <= 0.1 