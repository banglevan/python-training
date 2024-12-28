"""
Bài tập 2: Tính tổng dãy số với điều kiện
Yêu cầu:
1. Tính tổng các số chia hết cho x trong dãy
2. Tính tổng các số không vượt quá n
3. Tính trung bình của dãy con thỏa điều kiện
"""

from typing import List, Tuple, Optional
import statistics

def sum_divisible_by(numbers: List[int], x: int) -> Tuple[int, List[int]]:
    """
    Tính tổng các số chia hết cho x trong dãy.

    Args:
        numbers (List[int]): Dãy số
        x (int): Số chia

    Returns:
        Tuple[int, List[int]]: (tổng, danh sách các số thỏa mãn)

    Examples:
        >>> sum_divisible_by([1, 2, 3, 4, 5, 6], 2)
        (12, [2, 4, 6])
    """
    # TODO: Implement this function
    pass

def sum_not_exceeding(numbers: List[int], n: int) -> Tuple[int, List[int]]:
    """
    Tính tổng các số không vượt quá n.

    Args:
        numbers (List[int]): Dãy số
        n (int): Giới hạn trên

    Returns:
        Tuple[int, List[int]]: (tổng, danh sách các số thỏa mãn)

    Examples:
        >>> sum_not_exceeding([1, 2, 3, 4, 5, 6], 4)
        (10, [1, 2, 3, 4])
    """
    # TODO: Implement this function
    pass

def get_valid_numbers() -> Optional[List[int]]:
    """
    Nhận và validate dãy số từ người dùng.

    Returns:
        Optional[List[int]]: Dãy số hợp lệ hoặc None nếu input không hợp lệ

    Examples:
        >>> numbers = get_valid_numbers()  # Nhập: "1 2 3 4 5"
        >>> print(numbers)
        [1, 2, 3, 4, 5]
    """
    # TODO: Implement this function
    pass

def main():
    """Chương trình chính."""
    while True:
        print("\nChương trình tính tổng dãy số")
        print("1. Tổng các số chia hết cho x")
        print("2. Tổng các số không vượt quá n")
        print("3. Thoát")

        choice = input("\nChọn chức năng (1-3): ")

        if choice == '3':
            break

        numbers = get_valid_numbers()
        if numbers is None:
            continue

        if choice == '1':
            x = input("Nhập số chia x: ")
            try:
                x = int(x)
                if x == 0:
                    print("Số chia không thể bằng 0!")
                    continue
                total, valid_numbers = sum_divisible_by(numbers, x)
                print(f"Các số chia hết cho {x}: {valid_numbers}")
                print(f"Tổng: {total}")
            except ValueError:
                print("Số chia không hợp lệ!")
        elif choice == '2':
            n = input("Nhập giới hạn n: ")
            try:
                n = int(n)
                total, valid_numbers = sum_not_exceeding(numbers, n)
                print(f"Các số không vượt quá {n}: {valid_numbers}")
                print(f"Tổng: {total}")
            except ValueError:
                print("Giới hạn không hợp lệ!")
        else:
            print("Lựa chọn không hợp lệ!")

if __name__ == "__main__":
    main() 