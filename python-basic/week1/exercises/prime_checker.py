"""
Bài tập 1: Kiểm tra số nguyên tố
Yêu cầu:
1. Kiểm tra một số có phải số nguyên tố
2. Tìm các số nguyên tố trong một khoảng
3. Tối ưu hiệu năng cho số lớn
"""

from typing import List, Optional
import math
import time

def is_prime(n: int) -> bool:
    """
    Kiểm tra số nguyên tố.

    Args:
        n (int): Số cần kiểm tra

    Returns:
        bool: True nếu là số nguyên tố, False nếu không

    Examples:
        >>> is_prime(7)
        True
        >>> is_prime(10)
        False
    """
    # TODO: Implement this function
    pass

def find_primes_in_range(start: int, end: int) -> List[int]:
    """
    Tìm tất cả số nguyên tố trong khoảng [start, end].

    Args:
        start (int): Số bắt đầu
        end (int): Số kết thúc

    Returns:
        List[int]: Danh sách các số nguyên tố

    Raises:
        ValueError: Nếu start > end hoặc start < 0

    Examples:
        >>> find_primes_in_range(1, 10)
        [2, 3, 5, 7]
    """
    # TODO: Implement this function
    pass

def get_valid_number(prompt: str) -> Optional[int]:
    """
    Nhận và validate input số từ người dùng.

    Args:
        prompt (str): Thông báo nhập

    Returns:
        Optional[int]: Số hợp lệ hoặc None nếu input không hợp lệ

    Examples:
        >>> n = get_valid_number("Nhập số: ")
        Nhập số: 23
        >>> print(n)
        23
    """
    # TODO: Implement this function
    pass

def main():
    """Chương trình chính."""
    while True:
        print("\nChương trình kiểm tra số nguyên tố")
        print("1. Kiểm tra một số")
        print("2. Tìm số nguyên tố trong khoảng")
        print("3. Thoát")

        choice = input("\nChọn chức năng (1-3): ")

        if choice == '3':
            break
        elif choice == '1':
            n = get_valid_number("Nhập số cần kiểm tra: ")
            if n is not None:
                start_time = time.time()
                result = is_prime(n)
                end_time = time.time()
                print(f"{n} {'là' if result else 'không phải là'} số nguyên tố")
                print(f"Thời gian thực thi: {(end_time - start_time)*1000:.2f}ms")
        elif choice == '2':
            start = get_valid_number("Nhập số bắt đầu: ")
            end = get_valid_number("Nhập số kết thúc: ")
            if start is not None and end is not None:
                try:
                    start_time = time.time()
                    primes = find_primes_in_range(start, end)
                    end_time = time.time()
                    print(f"Các số nguyên tố trong khoảng [{start}, {end}]:")
                    print(primes)
                    print(f"Thời gian thực thi: {(end_time - start_time)*1000:.2f}ms")
                except ValueError as e:
                    print(f"Lỗi: {e}")
        else:
            print("Lựa chọn không hợp lệ!")

if __name__ == "__main__":
    main() 