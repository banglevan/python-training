"""
Bài tập 3: Game đoán số
Yêu cầu:
1. Sinh số ngẫu nhiên trong khoảng cho trước
2. Cho phép người chơi đoán với số lần giới hạn
3. Hiển thị gợi ý sau mỗi lần đoán
4. Tính điểm dựa trên số lần đoán
"""

from typing import Optional, Tuple
import random
import time

class NumberGuessingGame:
    def __init__(self, min_num: int = 1, max_num: int = 100, max_attempts: int = 7):
        """
        Khởi tạo game với các thông số.

        Args:
            min_num (int): Số nhỏ nhất
            max_num (int): Số lớn nhất
            max_attempts (int): Số lần đoán tối đa
        """
        self.min_num = min_num
        self.max_num = max_num
        self.max_attempts = max_attempts
        self.target_number = 0
        self.attempts = 0
        self.start_time = 0
        self.end_time = 0

    def generate_number(self) -> None:
        """
        Sinh số ngẫu nhiên mới.
        """
        # TODO: Implement this function
        pass

    def get_valid_guess(self) -> Optional[int]:
        """
        Nhận và validate số đoán từ người chơi.

        Returns:
            Optional[int]: Số hợp lệ hoặc None nếu muốn thoát

        Examples:
            >>> game = NumberGuessingGame()
            >>> guess = game.get_valid_guess()  # Nhập: "50"
            >>> print(guess)
            50
        """
        # TODO: Implement this function
        pass

    def provide_hint(self, guess: int) -> str:
        """
        Tạo gợi ý dựa trên số đoán.

        Args:
            guess (int): Số người chơi đoán

        Returns:
            str: Gợi ý cho người chơi

        Examples:
            >>> game = NumberGuessingGame()
            >>> game.target_number = 50
            >>> print(game.provide_hint(30))
            'Số cần tìm lớn hơn!'
        """
        # TODO: Implement this function
        pass

    def calculate_score(self) -> int:
        """
        Tính điểm dựa trên số lần đoán và thời gian.

        Returns:
            int: Điểm số của người chơi

        Examples:
            >>> game = NumberGuessingGame()
            >>> game.attempts = 3
            >>> game.start_time = time.time() - 10  # 10 seconds ago
            >>> game.end_time = time.time()
            >>> print(game.calculate_score())
            70  # (max_attempts - attempts) * 10
        """
        # TODO: Implement this function
        pass

    def play(self) -> Tuple[bool, int]:
        """
        Chơi một ván game.

        Returns:
            Tuple[bool, int]: (thắng/thua, điểm số)
        """
        self.generate_number()
        self.attempts = 0
        self.start_time = time.time()

        print(f"\nĐoán số trong khoảng [{self.min_num}, {self.max_num}]")
        print(f"Bạn có {self.max_attempts} lần đoán")

        while self.attempts < self.max_attempts:
            self.attempts += 1
            print(f"\nLần đoán thứ {self.attempts}/{self.max_attempts}")
            
            guess = self.get_valid_guess()
            if guess is None:
                return False, 0

            if guess == self.target_number:
                self.end_time = time.time()
                print(f"\nChúc mừng! Bạn đã đoán đúng số {self.target_number}")
                score = self.calculate_score()
                return True, score

            hint = self.provide_hint(guess)
            print(hint)

        print(f"\nGame Over! Số cần tìm là {self.target_number}")
        return False, 0

def main():
    """Chương trình chính."""
    while True:
        print("\nGame Đoán Số")
        print("1. Chơi game")
        print("2. Thay đổi độ khó")
        print("3. Thoát")

        choice = input("\nChọn chức năng (1-3): ")

        if choice == '3':
            break
        elif choice == '1':
            game = NumberGuessingGame()
            won, score = game.play()
            if won:
                print(f"Điểm của bạn: {score}")
        elif choice == '2':
            try:
                min_num = int(input("Nhập số nhỏ nhất: "))
                max_num = int(input("Nhập số lớn nhất: "))
                max_attempts = int(input("Nhập số lần đoán tối đa: "))
                if min_num >= max_num or max_attempts <= 0:
                    raise ValueError
                game = NumberGuessingGame(min_num, max_num, max_attempts)
                won, score = game.play()
                if won:
                    print(f"Điểm của bạn: {score}")
            except ValueError:
                print("Thông số không hợp lệ!")
        else:
            print("Lựa chọn không hợp lệ!")

if __name__ == "__main__":
    main() 