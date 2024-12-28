"""
Unit tests cho sequence_sum.py
Tiêu chí đánh giá:
1. Functionality (40%): Tính tổng chính xác
2. Edge Cases (30%): Xử lý các trường hợp đặc biệt
3. Exception Handling (20%): Xử lý input không hợp lệ
4. Performance (10%): Tối ưu cho dãy lớn
"""

import pytest
from sequence_sum import sum_divisible_by, sum_not_exceeding, get_valid_numbers

class TestFunctionality:
    """
    Test chức năng cơ bản (40%)
    Pass: Tất cả phép tính đúng
    Fail: Có ít nhất 1 phép tính sai
    """
    
    def test_sum_divisible_basic(self):
        """Test tổng các số chia hết"""
        total, nums = sum_divisible_by([1, 2, 3, 4, 5, 6], 2)
        assert total == 12
        assert nums == [2, 4, 6]

        total, nums = sum_divisible_by([1, 2, 3, 4, 5, 6], 3)
        assert total == 9
        assert nums == [3, 6]

    def test_sum_not_exceeding_basic(self):
        """Test tổng các số không vượt quá n"""
        total, nums = sum_not_exceeding([1, 2, 3, 4, 5, 6], 4)
        assert total == 10
        assert nums == [1, 2, 3, 4]

class TestEdgeCases:
    """
    Test các trường hợp đặc biệt (30%)
    Pass: Xử lý đúng ≥ 80% edge cases
    Fail: Xử lý đúng < 80% edge cases
    """
    
    def test_empty_list(self):
        """Test với list rỗng"""
        total, nums = sum_divisible_by([], 2)
        assert total == 0
        assert nums == []

        total, nums = sum_not_exceeding([], 10)
        assert total == 0
        assert nums == []

    def test_no_matches(self):
        """Test khi không có số thỏa mãn"""
        total, nums = sum_divisible_by([1, 3, 5, 7], 2)
        assert total == 0
        assert nums == []

        total, nums = sum_not_exceeding([10, 20, 30], 5)
        assert total == 0
        assert nums == []

    def test_negative_numbers(self):
        """Test với số âm"""
        total, nums = sum_divisible_by([-4, -2, 0, 2, 4], 2)
        assert total == 0
        assert nums == [-4, -2, 0, 2, 4]

class TestExceptionHandling:
    """
    Test xử lý ngoại lệ (20%)
    Pass: Xử lý đúng tất cả exceptions
    Fail: Có exception không được xử lý
    """
    
    def test_invalid_divisor(self):
        """Test với số chia không hợp lệ"""
        with pytest.raises(ValueError):
            sum_divisible_by([1, 2, 3], 0)

    def test_invalid_limit(self):
        """Test với giới hạn không hợp lệ"""
        with pytest.raises(ValueError):
            sum_not_exceeding([1, 2, 3], -1)

    def test_get_valid_numbers(self, monkeypatch):
        """Test validate input dãy số"""
        inputs = iter(["1 2 3", "abc", "1 2 x", "4 5 6"])
        monkeypatch.setattr('builtins.input', lambda _: next(inputs))
        assert get_valid_numbers() == [4, 5, 6]

class TestPerformance:
    """
    Test hiệu năng (10%)
    Pass: Thời gian xử lý ≤ ngưỡng quy định
    Fail: Thời gian xử lý > ngưỡng quy định
    """
    
    def test_large_sequence(self):
        """Test với dãy lớn (1000 phần tử, ngưỡng: 0.1s)"""
        import time
        large_sequence = list(range(1000))
        
        start_time = time.time()
        sum_divisible_by(large_sequence, 3)
        end_time = time.time()
        assert end_time - start_time <= 0.1

        start_time = time.time()
        sum_not_exceeding(large_sequence, 500)
        end_time = time.time()
        assert end_time - start_time <= 0.1 