"""
Unit tests cho mypackage
Tiêu chí đánh giá:
1. Core (40%): Calculator, Converter, Processor
2. Utils (40%): Validators, Formatters, Helpers
3. Error Handling (20%): Exceptions
"""

import pytest
from datetime import datetime, date
from mypackage.core import Calculator, Converter, Processor
from mypackage.utils import (
    validate_number,
    validate_string,
    format_number,
    format_date,
    retry,
    memoize
)
from mypackage.exceptions import (
    CalculationError,
    ConversionError,
    ProcessingError,
    ValidationError,
    FormattingError,
    RetryError
)

class TestCalculator:
    """
    Test calculator (15%)
    Pass: Tính toán đúng ≥ 95%
    Fail: Tính toán đúng < 95%
    """
    
    def test_basic_operations(self):
        """Test phép tính cơ bản."""
        calc = Calculator()
        
        assert calc.add(2, 3) == 5
        assert calc.subtract(5, 3) == 2
        assert calc.multiply(2, 3) == 6
        assert calc.divide(6, 2) == 3.0
        
        with pytest.raises(CalculationError):
            calc.divide(1, 0)

    def test_power(self):
        """Test lũy thừa."""
        calc = Calculator()
        
        assert calc.power(2, 3) == 8
        assert calc.power(3, 2) == 9
        assert calc.power(2, 0) == 1
        
        with pytest.raises(CalculationError):
            calc.power(0, -1)

    def test_average(self):
        """Test trung bình."""
        calc = Calculator()
        
        assert calc.average([1, 2, 3]) == 2.0
        assert calc.average([0]) == 0.0
        
        with pytest.raises(CalculationError):
            calc.average([])

class TestConverter:
    """
    Test converter (15%)
    Pass: Chuyển đổi đúng ≥ 95%
    Fail: Chuyển đổi đúng < 95%
    """
    
    def test_length_conversion(self):
        """Test chuyển đổi độ dài."""
        conv = Converter()
        
        # m to km
        assert conv.convert_length(1000, "m", "km") == 1
        
        # ft to m
        assert conv.convert_length(1, "ft", "m") == pytest.approx(0.3048)
        
        # Invalid units
        with pytest.raises(ConversionError):
            conv.convert_length(1, "invalid", "m")

    def test_mass_conversion(self):
        """Test chuyển đổi khối lượng."""
        conv = Converter()
        
        # kg to g
        assert conv.convert_mass(1, "kg", "g") == 1000
        
        # lb to kg
        assert conv.convert_mass(1, "lb", "kg") == pytest.approx(0.45359237)
        
        # Invalid units
        with pytest.raises(ConversionError):
            conv.convert_mass(1, "invalid", "kg")

    def test_temperature_conversion(self):
        """Test chuyển đổi nhiệt độ."""
        conv = Converter()
        
        # C to F
        assert conv.convert_temperature(0, "C", "F") == pytest.approx(32)
        assert conv.convert_temperature(100, "C", "F") == pytest.approx(212)
        
        # F to C
        assert conv.convert_temperature(32, "F", "C") == pytest.approx(0)
        assert conv.convert_temperature(212, "F", "C") == pytest.approx(100)
        
        # Invalid units
        with pytest.raises(ConversionError):
            conv.convert_temperature(1, "invalid", "C")

class TestProcessor:
    """
    Test processor (10%)
    Pass: Xử lý đúng ≥ 95%
    Fail: Xử lý đúng < 95%
    """
    
    def test_filter_list(self):
        """Test lọc list."""
        proc = Processor()
        
        # Filter numbers
        data = [1, 2, 3, 4, 5]
        assert proc.filter_list(data, lambda x: x > 3) == [4, 5]
        
        # Filter strings
        data = ["a", "ab", "abc"]
        assert proc.filter_list(data, lambda x: len(x) > 1) == ["ab", "abc"]
        
        # Error
        with pytest.raises(ProcessingError):
            proc.filter_list(data, lambda x: x.invalid())

    def test_map_list(self):
        """Test transform list."""
        proc = Processor()
        
        # Map numbers
        data = [1, 2, 3]
        assert proc.map_list(data, lambda x: x * 2) == [2, 4, 6]
        
        # Map strings
        data = ["a", "b", "c"]
        assert proc.map_list(data, str.upper) == ["A", "B", "C"]
        
        # Error
        with pytest.raises(ProcessingError):
            proc.map_list(data, lambda x: x.invalid())

    def test_group_sort(self):
        """Test nhóm và sắp xếp."""
        proc = Processor()
        data = [
            {"id": 1, "group": "A"},
            {"id": 2, "group": "B"},
            {"id": 3, "group": "A"}
        ]
        
        # Group by
        groups = proc.group_by(data, "group")
        assert len(groups["A"]) == 2
        assert len(groups["B"]) == 1
        
        # Sort by
        sorted_data = proc.sort_by(data, "id", reverse=True)
        assert sorted_data[0]["id"] == 3

class TestValidators:
    """
    Test validators (15%)
    Pass: Validate đúng ≥ 95%
    Fail: Validate đúng < 95%
    """
    
    def test_number_validation(self):
        """Test validate số."""
        # Valid numbers
        assert validate_number(123) is True
        assert validate_number("123") is True
        assert validate_number(123.45) is True
        
        # Invalid numbers
        with pytest.raises(ValidationError):
            validate_number("abc")
        
        # Range check
        assert validate_number(5, min_value=0, max_value=10) is True
        with pytest.raises(ValidationError):
            validate_number(15, min_value=0, max_value=10)
            
        # Integer check
        assert validate_number(123, integer=True) is True
        with pytest.raises(ValidationError):
            validate_number(123.45, integer=True)

    def test_string_validation(self):
        """Test validate chuỗi."""
        # Valid strings
        assert validate_string("test") is True
        assert validate_string("  test  ", strip=True) is True
        
        # Length check
        assert validate_string("test", min_length=2, max_length=6) is True
        with pytest.raises(ValidationError):
            validate_string("test", max_length=3)
            
        # Pattern check
        assert validate_string(
            "abc123",
            pattern=r'^[a-z0-9]+$'
        ) is True
        with pytest.raises(ValidationError):
            validate_string(
                "ABC",
                pattern=r'^[a-z]+$'
            )

class TestFormatters:
    """
    Test formatters (15%)
    Pass: Format đúng ≥ 95%
    Fail: Format đúng < 95%
    """
    
    def test_number_formatting(self):
        """Test format số."""
        # Integer
        assert format_number(1234) == "1,234"
        assert format_number(1234, thousands_sep=".") == "1.234"
        
        # Decimal
        assert format_number(1234.5678, precision=2) == "1,234.57"
        assert format_number(
            1234.5678,
            precision=2,
            decimal_sep=","
        ) == "1.234,57"
        
        # Error
        with pytest.raises(FormattingError):
            format_number("invalid")

    def test_date_formatting(self):
        """Test format ngày."""
        d = date(2024, 3, 15)
        
        # Basic format
        assert format_date(d) == "2024-03-15"
        assert format_date(d, format="%d/%m/%Y") == "15/03/2024"
        
        # With locale
        assert format_date(
            d,
            format="%B %d, %Y",
            locale="en_US"
        ) == "March 15, 2024"
        
        # Error
        with pytest.raises(FormattingError):
            format_date("invalid")

class TestHelpers:
    """
    Test helpers (10%)
    Pass: Hoạt động đúng ≥ 95%
    Fail: Hoạt động đúng < 95%
    """
    
    def test_retry_decorator(self):
        """Test retry decorator."""
        attempts = 0
        
        @retry(max_tries=3, delay=0.1)
        def failing_function():
            nonlocal attempts
            attempts += 1
            if attempts < 3:
                raise ValueError("Test error")
            return "success"
        
        # Should succeed after 3 attempts
        assert failing_function() == "success"
        assert attempts == 3
        
        # Should fail after max tries
        attempts = 0
        with pytest.raises(RetryError):
            @retry(max_tries=2, delay=0.1)
            def always_fails():
                raise ValueError("Always fails")
            always_fails()

    def test_memoize_decorator(self):
        """Test memoize decorator."""
        calls = 0
        
        @memoize
        def expensive_function(x, y):
            nonlocal calls
            calls += 1
            return x + y
        
        # First call
        assert expensive_function(2, 3) == 5
        assert calls == 1
        
        # Cached call
        assert expensive_function(2, 3) == 5
        assert calls == 1
        
        # Different args
        assert expensive_function(3, 4) == 7
        assert calls == 2
        
        # Clear cache
        expensive_function.clear_cache()
        assert expensive_function(2, 3) == 5
        assert calls == 3

class TestErrorHandling:
    """
    Test error handling (20%)
    Pass: Xử lý lỗi đúng ≥ 95%
    Fail: Xử lý lỗi đúng < 95%
    """
    
    def test_calculation_errors(self):
        """Test lỗi tính toán."""
        calc = Calculator()
        
        with pytest.raises(CalculationError) as exc:
            calc.divide(1, 0)
        assert "Division by zero" in str(exc.value)
        
        with pytest.raises(CalculationError) as exc:
            calc.power(0, -1)
        assert "Power error" in str(exc.value)

    def test_conversion_errors(self):
        """Test lỗi chuyển đổi."""
        conv = Converter()
        
        with pytest.raises(ConversionError) as exc:
            conv.convert_length(1, "invalid", "m")
        assert "Invalid unit" in str(exc.value)

    def test_processing_errors(self):
        """Test lỗi xử lý."""
        proc = Processor()
        
        with pytest.raises(ProcessingError) as exc:
            proc.filter_list([1, 2, 3], lambda x: x.invalid())
        assert "Filter error" in str(exc.value)

    def test_validation_errors(self):
        """Test lỗi validate."""
        with pytest.raises(ValidationError) as exc:
            validate_number("abc")
        assert "Invalid number format" in str(exc.value)
        
        with pytest.raises(ValidationError) as exc:
            validate_string(123)
        assert "must be a string" in str(exc.value)

    def test_formatting_errors(self):
        """Test lỗi format."""
        with pytest.raises(FormattingError) as exc:
            format_number("invalid")
        assert "Number formatting error" in str(exc.value)
        
        with pytest.raises(FormattingError) as exc:
            format_date("invalid")
        assert "Date formatting error" in str(exc.value) 