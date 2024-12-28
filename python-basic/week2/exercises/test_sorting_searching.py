"""
Unit tests cho sorting_searching.py
Tiêu chí đánh giá:
1. Functionality (40%): Các thuật toán hoạt động đúng
2. Edge Cases (30%): Xử lý các trường hợp đặc biệt
3. Exception Handling (20%): Xử lý input không hợp lệ
4. Performance (10%): So sánh hiệu năng các thuật toán
"""

import pytest
import random
import time
from typing import List, TypeVar
from sorting_searching import (
    Sorter, Searcher, AlgorithmBenchmark,
    SortOrder, PerformanceMetrics
)

T = TypeVar('T')

@pytest.fixture
def sample_numeric_data():
    """Tạo dữ liệu số ngẫu nhiên."""
    return [64, 34, 25, 12, 22, 11, 90]

@pytest.fixture
def sample_string_data():
    """Tạo dữ liệu chuỗi."""
    return ["banana", "apple", "orange", "grape", "kiwi"]

@pytest.fixture
def large_dataset():
    """Tạo dataset lớn cho performance testing."""
    return random.sample(range(1000), 1000)

class TestSorting:
    """Test các thuật toán sắp xếp."""
    
    class TestFunctionality:
        """
        Test chức năng cơ bản (40%)
        Pass: Sắp xếp đúng thứ tự
        Fail: Sắp xếp sai thứ tự
        """
        
        def test_bubble_sort(self, sample_numeric_data):
            """Test Bubble Sort với số."""
            data = sample_numeric_data.copy()
            metrics = Sorter.bubble_sort(data)
            assert data == sorted(sample_numeric_data)
            assert isinstance(metrics, PerformanceMetrics)

        def test_quick_sort(self, sample_numeric_data):
            """Test Quick Sort với số."""
            data = sample_numeric_data.copy()
            metrics = Sorter.quick_sort(data)
            assert data == sorted(sample_numeric_data)
            assert isinstance(metrics, PerformanceMetrics)

        def test_merge_sort(self, sample_numeric_data):
            """Test Merge Sort với số."""
            data = sample_numeric_data.copy()
            metrics = Sorter.merge_sort(data)
            assert data == sorted(sample_numeric_data)
            assert isinstance(metrics, PerformanceMetrics)

        def test_sort_strings(self, sample_string_data):
            """Test sắp xếp chuỗi."""
            for sort_func in [Sorter.bubble_sort, Sorter.quick_sort, Sorter.merge_sort]:
                data = sample_string_data.copy()
                sort_func(data)
                assert data == sorted(sample_string_data)

    class TestEdgeCases:
        """
        Test các trường hợp đặc biệt (30%)
        Pass: Xử lý đúng ≥ 80% edge cases
        Fail: Xử lý đúng < 80% edge cases
        """
        
        def test_empty_list(self):
            """Test với list rỗng."""
            empty_list = []
            for sort_func in [Sorter.bubble_sort, Sorter.quick_sort, Sorter.merge_sort]:
                data = empty_list.copy()
                sort_func(data)
                assert data == []

        def test_single_element(self):
            """Test với list 1 phần tử."""
            single_element = [42]
            for sort_func in [Sorter.bubble_sort, Sorter.quick_sort, Sorter.merge_sort]:
                data = single_element.copy()
                sort_func(data)
                assert data == [42]

        def test_duplicate_elements(self):
            """Test với phần tử trùng lặp."""
            duplicates = [3, 1, 4, 1, 5, 9, 2, 6, 5, 3, 5]
            for sort_func in [Sorter.bubble_sort, Sorter.quick_sort, Sorter.merge_sort]:
                data = duplicates.copy()
                sort_func(data)
                assert data == sorted(duplicates)

        def test_descending_order(self):
            """Test sắp xếp giảm dần."""
            data = [5, 4, 3, 2, 1]
            for sort_func in [Sorter.bubble_sort, Sorter.quick_sort, Sorter.merge_sort]:
                test_data = data.copy()
                sort_func(test_data, SortOrder.DESCENDING)
                assert test_data == [5, 4, 3, 2, 1]

class TestSearching:
    """Test các thuật toán tìm kiếm."""
    
    class TestFunctionality:
        """
        Test chức năng cơ bản (40%)
        Pass: Tìm kiếm đúng vị trí
        Fail: Tìm kiếm sai vị trí
        """
        
        def test_linear_search(self, sample_numeric_data):
            """Test Linear Search."""
            target = 25
            index, metrics = Searcher.linear_search(sample_numeric_data, target)
            assert index == sample_numeric_data.index(target)
            assert isinstance(metrics, PerformanceMetrics)

        def test_binary_search(self, sample_numeric_data):
            """Test Binary Search."""
            sorted_data = sorted(sample_numeric_data)
            target = 25
            index, metrics = Searcher.binary_search(sorted_data, target)
            assert index == sorted_data.index(target)
            assert isinstance(metrics, PerformanceMetrics)

    class TestEdgeCases:
        """
        Test các trường hợp đ���c biệt (30%)
        Pass: Xử lý đúng ≥ 80% edge cases
        Fail: Xử lý đúng < 80% edge cases
        """
        
        def test_search_empty_list(self):
            """Test tìm kiếm trong list rỗng."""
            empty_list = []
            for search_func in [Searcher.linear_search, Searcher.binary_search]:
                index, _ = search_func(empty_list, 42)
                assert index is None

        def test_search_not_found(self, sample_numeric_data):
            """Test tìm kiếm phần tử không tồn tại."""
            target = 999
            for search_func in [Searcher.linear_search, Searcher.binary_search]:
                index, _ = search_func(sample_numeric_data, target)
                assert index is None

class TestPerformance:
    """
    Test hiệu năng (10%)
    Pass: Thời gian thực thi ≤ ngưỡng quy định
    Fail: Thời gian thực thi > ngưỡng quy định
    """
    
    def test_sorting_performance(self, large_dataset):
        """Test hiệu năng sắp xếp (ngưỡng: Quick/Merge ≤ 0.1s, Bubble ≤ 1s)."""
        # Test Quick Sort
        start_time = time.time()
        data = large_dataset.copy()
        Sorter.quick_sort(data)
        quick_time = time.time() - start_time
        assert quick_time <= 0.1

        # Test Merge Sort
        start_time = time.time()
        data = large_dataset.copy()
        Sorter.merge_sort(data)
        merge_time = time.time() - start_time
        assert merge_time <= 0.1

        # Test Bubble Sort
        start_time = time.time()
        data = large_dataset.copy()
        Sorter.bubble_sort(data)
        bubble_time = time.time() - start_time
        assert bubble_time <= 1.0

    def test_searching_performance(self, large_dataset):
        """Test hiệu năng tìm kiếm (ngưỡng: Binary ≤ 0.001s, Linear ≤ 0.01s)."""
        sorted_data = sorted(large_dataset)
        target = sorted_data[len(sorted_data)//2]  # Middle element

        # Test Binary Search
        start_time = time.time()
        Searcher.binary_search(sorted_data, target)
        binary_time = time.time() - start_time
        assert binary_time <= 0.001

        # Test Linear Search
        start_time = time.time()
        Searcher.linear_search(sorted_data, target)
        linear_time = time.time() - start_time
        assert linear_time <= 0.01

class TestBenchmark:
    """Test class AlgorithmBenchmark."""
    
    def test_sorting_benchmark(self, large_dataset):
        """Test benchmark sắp xếp."""
        algorithms = [
            Sorter.bubble_sort,
            Sorter.quick_sort,
            Sorter.merge_sort
        ]
        
        results = AlgorithmBenchmark.compare_sorting_algorithms(
            large_dataset, algorithms
        )
        
        assert len(results) == len(algorithms)
        for metrics in results:
            assert isinstance(metrics, PerformanceMetrics)
            assert metrics.execution_time > 0
            assert metrics.comparisons > 0

    def test_searching_benchmark(self, large_dataset):
        """Test benchmark tìm kiếm."""
        algorithms = [
            Searcher.linear_search,
            Searcher.binary_search
        ]
        
        target = large_dataset[len(large_dataset)//2]
        results = AlgorithmBenchmark.compare_searching_algorithms(
            sorted(large_dataset), target, algorithms
        )
        
        assert len(results) == len(algorithms)
        for metrics in results:
            assert isinstance(metrics, PerformanceMetrics)
            assert metrics.execution_time > 0
            assert metrics.comparisons > 0 