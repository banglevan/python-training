"""
Bài tập 2: Sắp xếp và tìm kiếm
Yêu cầu:
1. Implement các thuật toán sắp xếp (Bubble, Selection, Insertion, Quick, Merge)
2. Implement các thuật toán tìm kiếm (Linear, Binary)
3. So sánh hiệu năng các thuật toán
4. Áp dụng với nhiều kiểu dữ liệu
"""

from typing import List, TypeVar, Callable, Optional
import time
from dataclasses import dataclass
from enum import Enum
from typing import Tuple

T = TypeVar('T')  # Generic type for sorting/searching

class SortOrder(Enum):
    """Enum for sort order."""
    ASCENDING = 'asc'
    DESCENDING = 'desc'

@dataclass
class PerformanceMetrics:
    """Class để lưu metrics hiệu năng."""
    algorithm_name: str
    execution_time: float
    comparisons: int
    swaps: int

class Sorter:
    """Class chứa các thuật toán sắp xếp."""
    
    @staticmethod
    def bubble_sort(arr: List[T], order: SortOrder = SortOrder.ASCENDING) -> PerformanceMetrics:
        """
        Bubble Sort algorithm.
        
        Args:
            arr (List[T]): List cần sắp xếp
            order (SortOrder): Thứ tự sắp xếp
            
        Returns:
            PerformanceMetrics: Metrics hiệu năng
            
        Examples:
            >>> arr = [64, 34, 25, 12, 22, 11, 90]
            >>> metrics = Sorter.bubble_sort(arr)
            >>> print(arr)
            [11, 12, 22, 25, 34, 64, 90]
        """
        # TODO: Implement this function
        pass

    @staticmethod
    def quick_sort(arr: List[T], order: SortOrder = SortOrder.ASCENDING) -> PerformanceMetrics:
        """
        Quick Sort algorithm.
        
        Args:
            arr (List[T]): List cần sắp xếp
            order (SortOrder): Thứ tự sắp xếp
            
        Returns:
            PerformanceMetrics: Metrics hiệu năng
        """
        # TODO: Implement this function
        pass

    @staticmethod
    def merge_sort(arr: List[T], order: SortOrder = SortOrder.ASCENDING) -> PerformanceMetrics:
        """
        Merge Sort algorithm.
        
        Args:
            arr (List[T]): List cần sắp xếp
            order (SortOrder): Thứ tự sắp xếp
            
        Returns:
            PerformanceMetrics: Metrics hiệu năng
        """
        # TODO: Implement this function
        pass

class Searcher:
    """Class chứa các thuật toán tìm kiếm."""
    
    @staticmethod
    def linear_search(arr: List[T], target: T) -> Tuple[Optional[int], PerformanceMetrics]:
        """
        Linear Search algorithm.
        
        Args:
            arr (List[T]): List cần tìm kiếm
            target (T): Giá trị cần tìm
            
        Returns:
            Tuple[Optional[int], PerformanceMetrics]: (index nếu tìm thấy hoặc None, metrics)
            
        Examples:
            >>> arr = [64, 34, 25, 12, 22, 11, 90]
            >>> index, metrics = Searcher.linear_search(arr, 25)
            >>> print(index)
            2
        """
        # TODO: Implement this function
        pass

    @staticmethod
    def binary_search(arr: List[T], target: T) -> Tuple[Optional[int], PerformanceMetrics]:
        """
        Binary Search algorithm (yêu cầu list đã sắp xếp).
        
        Args:
            arr (List[T]): List đã sắp xếp cần tìm kiếm
            target (T): Giá trị cần tìm
            
        Returns:
            Tuple[Optional[int], PerformanceMetrics]: (index nếu tìm thấy hoặc None, metrics)
        """
        # TODO: Implement this function
        pass

class AlgorithmBenchmark:
    """Class để benchmark các thuật toán."""
    
    @staticmethod
    def compare_sorting_algorithms(
        data: List[T],
        algorithms: List[Callable[[List[T], SortOrder], PerformanceMetrics]]
    ) -> List[PerformanceMetrics]:
        """
        So sánh hiệu năng các thuật toán sắp xếp.
        
        Args:
            data (List[T]): Dữ liệu test
            algorithms (List[Callable]): Danh sách các thuật toán
            
        Returns:
            List[PerformanceMetrics]: Metrics của các thuật toán
        """
        results = []
        for algo in algorithms:
            # Tạo bản sao để không ảnh hưởng đến dữ liệu gốc
            test_data = data.copy()
            metrics = algo(test_data, SortOrder.ASCENDING)
            results.append(metrics)
        return results

    @staticmethod
    def compare_searching_algorithms(
        data: List[T],
        target: T,
        algorithms: List[Callable[[List[T], T], Tuple[Optional[int], PerformanceMetrics]]]
    ) -> List[PerformanceMetrics]:
        """
        So sánh hiệu năng các thuật toán tìm kiếm.
        
        Args:
            data (List[T]): Dữ liệu test
            target (T): Giá trị cần tìm
            algorithms (List[Callable]): Danh sách các thuật toán
            
        Returns:
            List[PerformanceMetrics]: Metrics của các thuật toán
        """
        results = []
        for algo in algorithms:
            _, metrics = algo(data, target)
            results.append(metrics)
        return results

def main():
    """Chương trình chính."""
    while True:
        print("\nChương Trình Sắp Xếp và Tìm Kiếm")
        print("1. Demo các thuật toán sắp xếp")
        print("2. Demo các thuật toán tìm kiếm")
        print("3. So sánh hiệu năng thuật toán")
        print("4. Thoát")

        choice = input("\nChọn chức năng (1-4): ")

        if choice == '4':
            break
        elif choice == '1':
            # TODO: Implement sorting demo
            pass
        elif choice == '2':
            # TODO: Implement searching demo
            pass
        elif choice == '3':
            # TODO: Implement benchmark demo
            pass
        else:
            print("Lựa chọn không hợp lệ!")

if __name__ == "__main__":
    main() 