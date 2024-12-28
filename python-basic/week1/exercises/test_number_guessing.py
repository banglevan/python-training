"""
Unit tests cho number_guessing.py
Tiêu chí đánh giá:
1. Functionality (40%): Game logic hoạt động đúng
2. Edge Cases (30%): Xử lý các trường hợp đặc biệt
3. Exception Handling (20%): Xử lý input không hợp lệ
4. Performance (10%): Phản hồi nhanh
"""

import pytest
import time
from number_guessing import NumberGuessingGame

class TestFunctionality:
    """
    Test chức năng cơ bản (40%)
    Pass: Game logic hoạt động đúng
    Fail: Game logic có lỗi
    """
    
    def test_generate_number(self):
        """Test sinh số ngẫu nhiên"""
        game = NumberGuessingGame(1, 10)
        game.generate_number()
        assert 1 <= game.target_number <= 10

        # Test nhiều lần để đảm bảo tính ngẫu nhiên
        numbers = set()
        for _ in range(50):
            game.generate_number()
            numbers.add(game.target_number)
        assert len(numbers) > 1  # Số phải ngẫu nhiên

    def test_provide_hint(self):
        """Test gợi ý"""
        game = NumberGuessingGame()
        game.target_number = 50
        
        assert "lớn hơn" in game.provide_hint(30).lower()
        assert "nhỏ hơn" in game.provide_hint(70).lower()
        assert "chính xác" in game.provide_hint(50).lower()

class TestEdgeCases:
    """
    Test các trường hợp đặc biệt (30%)
    Pass: Xử lý đúng ≥ 80% edge cases
    Fail: Xử lý đúng < 80% edge cases
    """
    
    def test_boundary_numbers(self):
        """Test với số biên"""
        game = NumberGuessingGame(1, 10)
        
        # Test với số nhỏ nhất
        game.target_number = 1
        assert "chính xác" in game.provide_hint(1).lower()
        assert "lớn hơn" not in game.provide_hint(1).lower()
        
        # Test với số lớn nhất
        game.target_number = 10
        assert "chính xác" in game.provide_hint(10).lower()
        assert "nhỏ hơn" not in game.provide_hint(10).lower()

    def test_score_calculation(self):
        """Test tính điểm các trường hợp"""
        game = NumberGuessingGame()
        
        # Thắng nhanh
        game.attempts = 1
        game.start_time = time.time() - 5  # 5 seconds ago
        game.end_time = time.time()
        high_score = game.calculate_score()
        
        # Thắng chậm
        game.attempts = game.max_attempts - 1
        game.start_time = time.time() - 30  # 30 seconds ago
        game.end_time = time.time()
        low_score = game.calculate_score()
        
        assert high_score > low_score

class TestExceptionHandling:
    """
    Test xử lý ngoại lệ (20%)
    Pass: Xử lý đúng tất cả exceptions
    Fail: Có exception không được xử lý
    """
    
    def test_invalid_guess(self, monkeypatch):
        """Test với đoán số không hợp lệ"""
        game = NumberGuessingGame()
        
        # Test với input không phải số
        inputs = iter(["abc", "xyz", "50"])
        monkeypatch.setattr('builtins.input', lambda _: next(inputs))
        assert game.get_valid_guess() == 50

        # Test với số ngoài khoảng
        inputs = iter(["0", "101", "50"])
        monkeypatch.setattr('builtins.input', lambda _: next(inputs))
        assert game.get_valid_guess() == 50

    def test_invalid_game_setup(self):
        """Test với thiết lập game không hợp lệ"""
        with pytest.raises(ValueError):
            NumberGuessingGame(10, 1)  # min > max
        with pytest.raises(ValueError):
            NumberGuessingGame(1, 10, 0)  # max_attempts = 0

class TestPerformance:
    """
    Test hiệu năng (10%)
    Pass: Thời gian phản hồi ≤ ngưỡng quy định
    Fail: Thời gian phản hồi > ngưỡng quy định
    """
    
    def test_game_response_time(self):
        """Test thời gian phản hồi game (ngưỡng: 0.1s)"""
        game = NumberGuessingGame()
        
        start_time = time.time()
        game.generate_number()
        game.provide_hint(50)
        game.calculate_score()
        end_time = time.time()
        
        assert end_time - start_time <= 0.1

    def test_multiple_games(self):
        """Test hiệu năng chơi nhiều ván (ngưỡng: 1s cho 100 ván)"""
        start_time = time.time()
        
        for _ in range(100):
            game = NumberGuessingGame()
            game.generate_number()
            game.provide_hint(50)
            game.calculate_score()
            
        end_time = time.time()
        assert end_time - start_time <= 1 