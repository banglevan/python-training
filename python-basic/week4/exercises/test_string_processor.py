"""
Unit tests cho string_processor.py
Tiêu chí đánh giá:
1. Text Processing (40%): Xử lý văn bản cơ bản
2. Search & Replace (30%): Tìm kiếm và thay thế
3. Text Analysis (20%): Phân tích văn bản
4. Validation (10%): Kiểm tra định dạng
"""

import pytest
from string_processor import (
    TextCase,
    TextStats,
    normalize_text,
    change_case,
    find_replace,
    get_statistics,
    validate_email,
    validate_phone,
    validate_url
)

class TestTextProcessing:
    """
    Test xử lý văn bản (40%)
    Pass: Xử lý đúng ≥ 95% cases
    Fail: Xử lý đúng < 95% cases
    """
    
    def test_normalize_text(self):
        """Test chuẩn hóa văn bản."""
        test_cases = [
            # Khoảng trắng thừa
            (
                "  Hello   world  !  ",
                "Hello world!"
            ),
            # Dấu câu
            (
                "Hello,world!How are you?",
                "Hello, world! How are you?"
            ),
            # Ngoặc
            (
                "This is ( a test ) text",
                "This is (a test) text"
            ),
            # Nhiều dòng
            (
                "Line 1\n  Line 2  \nLine 3",
                "Line 1 Line 2 Line 3"
            )
        ]
        
        for input_text, expected in test_cases:
            assert normalize_text(input_text) == expected

    def test_change_case(self):
        """Test chuyển đổi kiểu chữ."""
        text = "Hello, World! This is a TEST."
        
        assert change_case(text, TextCase.LOWER) == "hello, world! this is a test."
        assert change_case(text, TextCase.UPPER) == "HELLO, WORLD! THIS IS A TEST."
        assert change_case(text, TextCase.TITLE) == "Hello, World! This Is A Test."
        assert change_case(
            text, TextCase.SENTENCE
        ) == "Hello, world! This is a test."

class TestSearchReplace:
    """
    Test tìm kiếm và thay thế (30%)
    Pass: Thay thế đúng ≥ 95% cases
    Fail: Thay thế đúng < 95% cases
    """
    
    def test_find_replace_basic(self):
        """Test tìm và thay thế cơ bản."""
        text = "Hello world! Hello Python!"
        result, count = find_replace(text, "Hello", "Hi")
        assert result == "Hi world! Hi Python!"
        assert count == 2

    def test_find_replace_case_sensitive(self):
        """Test phân biệt hoa thường."""
        text = "Hello HELLO hello"
        
        # Có phân biệt
        result, count = find_replace(text, "Hello", "Hi", case_sensitive=True)
        assert result == "Hi HELLO hello"
        assert count == 1
        
        # Không phân biệt
        result, count = find_replace(text, "Hello", "Hi", case_sensitive=False)
        assert result == "Hi Hi Hi"
        assert count == 3

    def test_find_replace_whole_word(self):
        """Test thay thế từ hoàn chỉnh."""
        text = "Hello HelloWorld WorldHello"
        
        # Từ hoàn chỉnh
        result, count = find_replace(
            text, "Hello", "Hi", whole_word=True
        )
        assert result == "Hi HelloWorld WorldHello"
        assert count == 1
        
        # Không yêu cầu từ hoàn chỉnh
        result, count = find_replace(
            text, "Hello", "Hi", whole_word=False
        )
        assert result == "Hi HiWorld WorldHi"
        assert count == 3

class TestTextAnalysis:
    """
    Test phân tích văn bản (20%)
    Pass: Phân tích đúng ≥ 90% metrics
    Fail: Phân tích đúng < 90% metrics
    """
    
    def test_get_statistics(self):
        """Test thống kê văn bản."""
        text = "Hello world! This is a test.\nSecond line. Third line!"
        stats = get_statistics(text)
        
        assert isinstance(stats, TextStats)
        assert stats.char_count == 45  # Không tính khoảng trắng
        assert stats.word_count == 11
        assert stats.line_count == 2
        assert stats.sentence_count == 4
        assert stats.avg_word_length == pytest.approx(4.09, 0.01)
        assert stats.avg_sentence_length == pytest.approx(2.75, 0.01)

    def test_empty_text(self):
        """Test văn bản rỗng."""
        stats = get_statistics("")
        
        assert stats.char_count == 0
        assert stats.word_count == 0
        assert stats.line_count == 1
        assert stats.sentence_count == 0
        assert stats.avg_word_length == 0
        assert stats.avg_sentence_length == 0

class TestValidation:
    """
    Test kiểm tra định dạng (10%)
    Pass: Validate đúng ≥ 90% cases
    Fail: Validate đúng < 90% cases
    """
    
    def test_validate_email(self):
        """Test kiểm tra email."""
        valid_emails = [
            "test@example.com",
            "user.name@domain.com",
            "user-name@domain.co.uk",
            "user123@domain.net"
        ]
        
        invalid_emails = [
            "",  # Rỗng
            "invalid.email",  # Thiếu @
            "@domain.com",  # Thiếu username
            "user@.com",  # Thiếu domain
            "user@domain",  # Thiếu TLD
            "user name@domain.com"  # Có khoảng trắng
        ]
        
        for email in valid_emails:
            assert validate_email(email) is True
            
        for email in invalid_emails:
            assert validate_email(email) is False

    def test_validate_phone(self):
        """Test kiểm tra số điện thoại."""
        valid_phones = [
            "1234567890",
            "+1234567890",
            "123-456-7890",
            "123 456 7890",
            "+1 234-567-8900"
        ]
        
        invalid_phones = [
            "",  # Rỗng
            "123",  # Quá ngắn
            "abcdefghij",  # Không phải số
            "123.456.7890"  # Sai định dạng
        ]
        
        for phone in valid_phones:
            assert validate_phone(phone) is True
            
        for phone in invalid_phones:
            assert validate_phone(phone) is False

    def test_validate_url(self):
        """Test kiểm tra URL."""
        valid_urls = [
            "http://example.com",
            "https://example.com",
            "http://sub.example.com",
            "http://example.com/path",
            "http://example.com:8080",
            "http://example.com/path?param=value",
            "http://localhost",
            "http://127.0.0.1"
        ]
        
        invalid_urls = [
            "",  # Rỗng
            "example.com",  # Thiếu protocol
            "ftp://example.com",  # Sai protocol
            "http:/example.com",  # Thiếu slash
            "http://",  # Thiếu domain
            "http://invalid domain"  # Có khoảng trắng
        ]
        
        for url in valid_urls:
            assert validate_url(url) is True
            
        for url in invalid_urls:
            assert validate_url(url) is False 