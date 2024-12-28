"""
Unit tests cho data_validator.py
Tiêu chí đánh giá:
1. Type Validation (30%): Kiểm tra kiểu dữ liệu
2. Value Validation (30%): Kiểm tra giá trị
3. Format Validation (30%): Kiểm tra định dạng
4. Custom Validation (10%): Kiểm tra tùy chỉnh
"""

import pytest
from datetime import datetime
from data_validator import (
    DataType,
    ValidationRule,
    DataValidator
)

@pytest.fixture
def sample_rules():
    """Tạo rules mẫu."""
    return {
        'name': ValidationRule(
            data_type=DataType.STRING,
            required=True,
            min_length=2,
            max_length=50
        ),
        'age': ValidationRule(
            data_type=DataType.INTEGER,
            required=True,
            min_value=0,
            max_value=150
        ),
        'email': ValidationRule(
            data_type=DataType.EMAIL,
            required=True
        ),
        'phone': ValidationRule(
            data_type=DataType.PHONE,
            required=False
        ),
        'website': ValidationRule(
            data_type=DataType.URL,
            required=False
        ),
        'birth_date': ValidationRule(
            data_type=DataType.DATE,
            required=True
        ),
        'gender': ValidationRule(
            data_type=DataType.STRING,
            required=True,
            allowed_values=['male', 'female', 'other']
        ),
        'score': ValidationRule(
            data_type=DataType.FLOAT,
            required=False,
            min_value=0.0,
            max_value=10.0
        )
    }

@pytest.fixture
def validator(sample_rules):
    """Tạo validator với rules mẫu."""
    return DataValidator(sample_rules)

class TestTypeValidation:
    """
    Test kiểm tra kiểu dữ liệu (30%)
    Pass: Validate đúng ≥ 95% cases
    Fail: Validate đúng < 95% cases
    """
    
    def test_string_type(self, validator):
        """Test kiểu string."""
        data = {'name': 'John Doe'}
        errors = validator.validate(data)
        assert 'name' not in errors
        
        data = {'name': 123}  # Số -> string được
        errors = validator.validate(data)
        assert 'name' not in errors
        
        data = {'name': None}
        errors = validator.validate(data)
        assert 'name' in errors

    def test_integer_type(self, validator):
        """Test kiểu integer."""
        data = {'age': 25}
        errors = validator.validate(data)
        assert 'age' not in errors
        
        data = {'age': '25'}  # String số -> int được
        errors = validator.validate(data)
        assert 'age' not in errors
        
        data = {'age': 'abc'}
        errors = validator.validate(data)
        assert 'age' in errors

    def test_float_type(self, validator):
        """Test kiểu float."""
        data = {'score': 7.5}
        errors = validator.validate(data)
        assert 'score' not in errors
        
        data = {'score': '7.5'}  # String số -> float được
        errors = validator.validate(data)
        assert 'score' not in errors
        
        data = {'score': 'abc'}
        errors = validator.validate(data)
        assert 'score' in errors

    def test_date_type(self, validator):
        """Test kiểu date."""
        data = {'birth_date': '2000-01-01'}
        errors = validator.validate(data)
        assert 'birth_date' not in errors
        
        data = {'birth_date': '2000/01/01'}  # Sai format
        errors = validator.validate(data)
        assert 'birth_date' in errors
        
        data = {'birth_date': 'invalid'}
        errors = validator.validate(data)
        assert 'birth_date' in errors

class TestValueValidation:
    """
    Test ki��m tra giá trị (30%)
    Pass: Validate đúng ≥ 95% cases
    Fail: Validate đúng < 95% cases
    """
    
    def test_required_values(self, validator):
        """Test giá trị bắt buộc."""
        # Thiếu required fields
        data = {'age': 25, 'email': 'test@test.com'}
        errors = validator.validate(data)
        assert 'name' in errors
        assert 'birth_date' in errors
        assert 'gender' in errors
        
        # Optional fields có thể bỏ qua
        assert 'phone' not in errors
        assert 'website' not in errors
        assert 'score' not in errors

    def test_length_limits(self, validator):
        """Test giới hạn độ dài."""
        data = {'name': 'A'}  # Quá ngắn
        errors = validator.validate(data)
        assert 'name' in errors
        
        data = {'name': 'A' * 51}  # Quá dài
        errors = validator.validate(data)
        assert 'name' in errors
        
        data = {'name': 'John Doe'}  # OK
        errors = validator.validate(data)
        assert 'name' not in errors

    def test_value_limits(self, validator):
        """Test giới hạn giá trị."""
        data = {'age': -1}  # Nhỏ hơn min
        errors = validator.validate(data)
        assert 'age' in errors
        
        data = {'age': 151}  # Lớn hơn max
        errors = validator.validate(data)
        assert 'age' in errors
        
        data = {'age': 25}  # OK
        errors = validator.validate(data)
        assert 'age' not in errors

    def test_allowed_values(self, validator):
        """Test giá trị cho phép."""
        data = {'gender': 'male'}  # OK
        errors = validator.validate(data)
        assert 'gender' not in errors
        
        data = {'gender': 'invalid'}  # Không trong danh sách
        errors = validator.validate(data)
        assert 'gender' in errors

class TestFormatValidation:
    """
    Test kiểm tra định dạng (30%)
    Pass: Validate đúng ≥ 95% cases
    Fail: Validate đúng < 95% cases
    """
    
    def test_email_format(self, validator):
        """Test định dạng email."""
        valid_emails = [
            "test@example.com",
            "user.name@domain.com",
            "user-name@domain.co.uk"
        ]
        
        invalid_emails = [
            "invalid.email",
            "@domain.com",
            "user@.com",
            "user@domain",
            "user name@domain.com"
        ]
        
        for email in valid_emails:
            errors = validator.validate({'email': email})
            assert 'email' not in errors
            
        for email in invalid_emails:
            errors = validator.validate({'email': email})
            assert 'email' in errors

    def test_phone_format(self, validator):
        """Test định dạng số điện thoại."""
        valid_phones = [
            "1234567890",
            "+1234567890",
            "123-456-7890",
            "123 456 7890"
        ]
        
        invalid_phones = [
            "123",  # Quá ngắn
            "abcdefghij",  # Không phải số
            "123.456.7890"  # Sai định dạng
        ]
        
        for phone in valid_phones:
            errors = validator.validate({'phone': phone})
            assert 'phone' not in errors
            
        for phone in invalid_phones:
            errors = validator.validate({'phone': phone})
            assert 'phone' in errors

    def test_url_format(self, validator):
        """Test định dạng URL."""
        valid_urls = [
            "http://example.com",
            "https://example.com",
            "http://sub.example.com",
            "http://example.com/path"
        ]
        
        invalid_urls = [
            "example.com",
            "ftp://example.com",
            "http:/example.com",
            "http://",
            "http://invalid domain"
        ]
        
        for url in valid_urls:
            errors = validator.validate({'website': url})
            assert 'website' not in errors
            
        for url in invalid_urls:
            errors = validator.validate({'website': url})
            assert 'website' in errors

class TestCustomValidation:
    """
    Test kiểm tra tùy chỉnh (10%)
    Pass: Custom validator hoạt động đúng
    Fail: Custom validator có lỗi
    """
    
    def test_custom_validator(self):
        """Test custom validation function."""
        def validate_even(value):
            return isinstance(value, int) and value % 2 == 0
            
        rule = ValidationRule(
            data_type=DataType.INTEGER,
            custom_validator=validate_even
        )
        
        is_valid, _ = rule.validate(2)
        assert is_valid is True
        
        is_valid, _ = rule.validate(3)
        assert is_valid is False
        
        is_valid, _ = rule.validate("2")
        assert is_valid is False

    def test_custom_validator_error(self):
        """Test xử lý lỗi trong custom validator."""
        def buggy_validator(value):
            raise Exception("Test error")
            
        rule = ValidationRule(
            data_type=DataType.STRING,
            custom_validator=buggy_validator
        )
        
        is_valid, error = rule.validate("test")
        assert is_valid is False
        assert "Test error" in error 