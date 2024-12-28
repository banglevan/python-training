"""
Unit tests cho utils package
Tiêu chí đánh giá:
1. File Utils (25%): Xử lý file
2. String Utils (25%): Xử lý chuỗi
3. Date Utils (25%): Xử lý ngày tháng
4. Validation Utils (25%): Kiểm tra dữ liệu
"""

import pytest
from pathlib import Path
import json
import csv
import yaml
from datetime import date, datetime, timedelta

from utils.file_utils import (
    read_text, write_text,
    read_json, write_json,
    read_csv, write_csv,
    read_yaml, write_yaml,
    copy_file, move_file,
    delete_file,
    create_dir, delete_dir,
    list_files
)
from utils.string_utils import (
    TextCase,
    normalize_text,
    change_case,
    slugify,
    word_count,
    char_count,
    truncate,
    extract_emails,
    extract_phones,
    extract_urls
)
from utils.date_utils import (
    DateFormat,
    parse_date,
    format_date,
    add_days,
    add_months,
    add_years,
    date_range,
    is_weekend,
    get_age,
    get_quarter,
    get_week_number
)
from utils.validation_utils import (
    ValidationType,
    validate_email,
    validate_phone,
    validate_url,
    validate_date,
    validate_number,
    validate_integer,
    validate_float,
    validate_boolean,
    validate
)

@pytest.fixture
def temp_dir(tmp_path):
    """Tạo thư mục tạm cho testing."""
    return tmp_path

class TestFileUtils:
    """
    Test xử lý file (25%)
    Pass: Xử lý đúng ≥ 95% cases
    Fail: Xử lý đúng < 95% cases
    """
    
    def test_text_file(self, temp_dir):
        """Test xử lý text file."""
        file_path = temp_dir / "test.txt"
        content = "Hello\nWorld"
        
        # Write
        assert write_text(file_path, content) is True
        
        # Read
        assert read_text(file_path) == content
        
        # Append
        assert write_text(file_path, "!", append=True) is True
        assert read_text(file_path) == content + "!"

    def test_json_file(self, temp_dir):
        """Test xử lý JSON file."""
        file_path = temp_dir / "test.json"
        data = {"name": "Test", "value": 123}
        
        # Write
        assert write_json(file_path, data) is True
        
        # Read
        assert read_json(file_path) == data

    def test_csv_file(self, temp_dir):
        """Test xử lý CSV file."""
        file_path = temp_dir / "test.csv"
        header = ["name", "age"]
        data = [["John", "30"], ["Jane", "25"]]
        
        # Write with header
        assert write_csv(file_path, data, header) is True
        
        # Read without header
        assert read_csv(file_path, has_header=True) == data

    def test_yaml_file(self, temp_dir):
        """Test xử lý YAML file."""
        file_path = temp_dir / "test.yaml"
        data = {"name": "Test", "value": 123}
        
        # Write
        assert write_yaml(file_path, data) is True
        
        # Read
        assert read_yaml(file_path) == data

    def test_file_operations(self, temp_dir):
        """Test các thao tác file."""
        src = temp_dir / "source.txt"
        dst = temp_dir / "dest.txt"
        
        # Create file
        write_text(src, "test")
        
        # Copy
        assert copy_file(src, dst) is True
        assert dst.exists()
        
        # Move
        new_dst = temp_dir / "new_dest.txt"
        assert move_file(dst, new_dst) is True
        assert not dst.exists()
        assert new_dst.exists()
        
        # Delete
        assert delete_file(src) is True
        assert not src.exists()

    def test_directory_operations(self, temp_dir):
        """Test các thao tác thư mục."""
        dir_path = temp_dir / "test_dir"
        
        # Create
        assert create_dir(dir_path) is True
        assert dir_path.exists()
        
        # Create files
        write_text(dir_path / "1.txt", "test1")
        write_text(dir_path / "2.txt", "test2")
        write_text(dir_path / "test.json", "{}")
        
        # List files
        txt_files = list_files(dir_path, "*.txt")
        assert len(txt_files) == 2
        
        # Delete
        assert delete_dir(dir_path) is True
        assert not dir_path.exists()

class TestStringUtils:
    """
    Test xử lý chuỗi (25%)
    Pass: Xử lý đúng ≥ 95% cases
    Fail: Xử lý đúng < 95% cases
    """
    
    def test_normalize_text(self):
        """Test chuẩn hóa văn bản."""
        text = "  Hello   world  !  "
        assert normalize_text(text) == "Hello world!"
        
        text = "Hello,world!How are you?"
        assert normalize_text(text) == "Hello, world! How are you?"
        
        text = "This is ( a test ) text"
        assert normalize_text(text) == "This is (a test) text"

    def test_change_case(self):
        """Test chuyển đổi kiểu chữ."""
        text = "Hello, World! This is a TEST."
        
        assert change_case(text, TextCase.LOWER) == "hello, world! this is a test."
        assert change_case(text, TextCase.UPPER) == "HELLO, WORLD! THIS IS A TEST."
        assert change_case(text, TextCase.TITLE) == "Hello, World! This Is A Test."
        assert change_case(
            text, TextCase.SENTENCE
        ) == "Hello, world! This is a test."

    def test_slugify(self):
        """Test tạo slug."""
        assert slugify("Hello World!") == "hello-world"
        assert slugify("This & That") == "this-that"
        assert slugify("100% Pure") == "100-pure"

    def test_text_metrics(self):
        """Test đếm từ và ký tự."""
        text = "Hello, World!"
        
        assert word_count(text) == 2
        assert char_count(text, count_spaces=False) == 10
        assert char_count(text, count_spaces=True) == 12

    def test_truncate(self):
        """Test cắt text."""
        text = "Hello, World!"
        
        assert truncate(text, 5) == "He..."
        assert truncate(text, 20) == text
        assert truncate(text, 8, "...more") == "Hel...more"

    def test_extract_patterns(self):
        """Test trích xuất patterns."""
        text = """
        Contact us at:
        - Email: test@example.com, support@test.com
        - Phone: +1234567890, 098-765-4321
        - Web: https://example.com, http://test.com/page
        """
        
        assert len(extract_emails(text)) == 2
        assert len(extract_phones(text)) == 2
        assert len(extract_urls(text)) == 2

class TestDateUtils:
    """
    Test xử lý ngày tháng (25%)
    Pass: Xử lý đúng ≥ 95% cases
    Fail: Xử lý đúng < 95% cases
    """
    
    def test_parse_format_date(self):
        """Test parse và format date."""
        # ISO format
        date_str = "2024-03-15"
        date_obj = parse_date(date_str)
        assert date_obj is not None
        assert format_date(date_obj) == date_str
        
        # US format
        date_str = "03/15/2024"
        date_obj = parse_date(date_str, DateFormat.US)
        assert date_obj is not None
        assert format_date(date_obj, DateFormat.US) == date_str
        
        # Invalid date
        assert parse_date("invalid") is None

    def test_date_arithmetic(self):
        """Test tính toán ngày."""
        date_obj = date(2024, 3, 15)
        
        # Add days
        assert add_days(date_obj, 5) == date(2024, 3, 20)
        assert add_days(date_obj, -5) == date(2024, 3, 10)
        
        # Add months
        assert add_months(date_obj, 2) == date(2024, 5, 15)
        assert add_months(date_obj, -2) == date(2024, 1, 15)
        
        # Add years
        assert add_years(date_obj, 1) == date(2025, 3, 15)
        assert add_years(date_obj, -1) == date(2023, 3, 15)

    def test_date_range(self):
        """Test tạo range ngày."""
        start = date(2024, 3, 1)
        end = date(2024, 3, 5)
        
        dates = date_range(start, end)
        assert len(dates) == 5
        assert dates[0] == start
        assert dates[-1] == end

    def test_date_info(self):
        """Test thông tin ngày."""
        date_obj = date(2024, 3, 15)
        
        assert is_weekend(date(2024, 3, 16))  # Saturday
        assert not is_weekend(date_obj)  # Friday
        
        assert get_age(date(2000, 1, 1), date(2024, 3, 15)) == 24
        assert get_quarter(date_obj) == 1
        assert 1 <= get_week_number(date_obj) <= 53

class TestValidationUtils:
    """
    Test kiểm tra dữ liệu (25%)
    Pass: Validate đúng ≥ 95% cases
    Fail: Validate đúng < 95% cases
    """
    
    def test_email_validation(self):
        """Test validate email."""
        valid_emails = [
            "test@example.com",
            "user.name@domain.com",
            "user-name@domain.co.uk"
        ]
        
        invalid_emails = [
            "",
            "invalid.email",
            "@domain.com",
            "user@.com",
            "user@domain",
            "user name@domain.com"
        ]
        
        for email in valid_emails:
            assert validate_email(email) is True
            assert validate(email, ValidationType.EMAIL) is True
            
        for email in invalid_emails:
            assert validate_email(email) is False
            assert validate(email, ValidationType.EMAIL) is False

    def test_phone_validation(self):
        """Test validate phone."""
        valid_phones = [
            "1234567890",
            "+1234567890",
            "123-456-7890",
            "123 456 7890"
        ]
        
        invalid_phones = [
            "",
            "123",
            "abcdefghij",
            "123.456.7890"
        ]
        
        for phone in valid_phones:
            assert validate_phone(phone) is True
            assert validate(phone, ValidationType.PHONE) is True
            
        for phone in invalid_phones:
            assert validate_phone(phone) is False
            assert validate(phone, ValidationType.PHONE) is False

    def test_url_validation(self):
        """Test validate URL."""
        valid_urls = [
            "http://example.com",
            "https://example.com",
            "http://sub.example.com",
            "http://example.com/path"
        ]
        
        invalid_urls = [
            "",
            "example.com",
            "ftp://example.com",
            "http:/example.com",
            "http://",
            "http://invalid domain"
        ]
        
        for url in valid_urls:
            assert validate_url(url) is True
            assert validate(url, ValidationType.URL) is True
            
        for url in invalid_urls:
            assert validate_url(url) is False
            assert validate(url, ValidationType.URL) is False

    def test_number_validation(self):
        """Test validate số."""
        # Integer
        assert validate_integer(123) is True
        assert validate_integer("123") is True
        assert validate_integer(123.0) is True
        assert validate_integer(123.5) is False
        assert validate_integer("abc") is False
        
        # Float
        assert validate_float(123.45) is True
        assert validate_float("123.45") is True
        assert validate_float(123) is True
        assert validate_float("abc") is False
        
        # Range
        assert validate_number(5, min_value=0, max_value=10) is True
        assert validate_number(15, min_value=0, max_value=10) is False
        
        # Precision
        assert validate_float(1.23, precision=2) is True
        assert validate_float(1.234, precision=2) is False

    def test_boolean_validation(self):
        """Test validate boolean."""
        valid_values = [
            True,
            False,
            "true",
            "false",
            "1",
            "0"
        ]
        
        invalid_values = [
            "",
            "yes",
            "no",
            "2",
            123
        ]
        
        for value in valid_values:
            assert validate_boolean(value) is True
            assert validate(value, ValidationType.BOOLEAN) is True
            
        for value in invalid_values:
            assert validate_boolean(value) is False
            assert validate(value, ValidationType.BOOLEAN) is False 