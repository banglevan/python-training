"""
Unit tests cho matrix_operations.py
Tiêu chí đánh giá:
1. Functionality (40%): Các phép toán ma trận chính xác
2. Edge Cases (30%): Xử lý các ma trận đặc biệt
3. Exception Handling (20%): Xử lý input không hợp lệ
4. Performance (10%): So sánh với NumPy
"""

import pytest
import numpy as np
import time
from matrix_operations import (
    MatrixOperations,
    MatrixError,
    Matrix,
    MatrixSize
)

@pytest.fixture
def sample_matrices():
    """Tạo các ma trận mẫu cho testing."""
    return {
        "2x2": [[1, 2], [3, 4]],
        "2x2_other": [[5, 6], [7, 8]],
        "2x3": [[1, 2, 3], [4, 5, 6]],
        "3x2": [[1, 2], [3, 4], [5, 6]],
        "singular": [[1, 1], [1, 1]],  # Ma trận suy biến
        "identity": [[1, 0], [0, 1]]   # Ma trận đơn vị
    }

@pytest.fixture
def large_matrix():
    """Tạo ma trận lớn cho performance testing."""
    return [[i + j for j in range(50)] for i in range(50)]

class TestMatrixValidation:
    """Test kiểm tra tính hợp lệ của ma trận."""
    
    def test_valid_matrix(self, sample_matrices):
        """Test với ma trận hợp lệ."""
        size = MatrixOperations.validate_matrix(sample_matrices["2x2"])
        assert size.rows == 2
        assert size.cols == 2

    def test_empty_matrix(self):
        """Test với ma trận rỗng."""
        with pytest.raises(MatrixError):
            MatrixOperations.validate_matrix([])
        with pytest.raises(MatrixError):
            MatrixOperations.validate_matrix([[]])

    def test_irregular_matrix(self):
        """Test với ma trận không đều."""
        irregular = [[1, 2, 3], [4, 5]]
        with pytest.raises(MatrixError):
            MatrixOperations.validate_matrix(irregular)

    def test_invalid_elements(self):
        """Test với phần tử không hợp lệ."""
        invalid = [[1, "2"], [3, 4]]
        with pytest.raises(MatrixError):
            MatrixOperations.validate_matrix(invalid)

class TestBasicOperations:
    """Test các phép toán cơ bản."""
    
    def test_addition(self, sample_matrices):
        """Test cộng ma trận."""
        result = MatrixOperations.add(
            sample_matrices["2x2"],
            sample_matrices["2x2_other"]
        )
        expected = [[6, 8], [10, 12]]
        assert result == expected

        # Test với kích thước không khớp
        with pytest.raises(MatrixError):
            MatrixOperations.add(
                sample_matrices["2x2"],
                sample_matrices["2x3"]
            )

    def test_multiplication(self, sample_matrices):
        """Test nhân ma trận."""
        result = MatrixOperations.multiply(
            sample_matrices["2x3"],
            sample_matrices["3x2"]
        )
        expected = [[22, 28], [49, 64]]
        assert result == expected

        # Test với kích thước không khớp
        with pytest.raises(MatrixError):
            MatrixOperations.multiply(
                sample_matrices["2x2"],
                sample_matrices["2x3"]
            )

    def test_transpose(self, sample_matrices):
        """Test chuyển vị ma trận."""
        result = MatrixOperations.transpose(sample_matrices["2x3"])
        expected = [[1, 4], [2, 5], [3, 6]]
        assert result == expected

class TestAdvancedOperations:
    """Test các phép toán nâng cao."""
    
    def test_determinant(self, sample_matrices):
        """Test tính định thức."""
        # Test ma trận 2x2
        det = MatrixOperations.determinant(sample_matrices["2x2"])
        assert det == -2  # (1*4 - 2*3)

        # Test ma trận suy biến
        det = MatrixOperations.determinant(sample_matrices["singular"])
        assert det == 0

        # Test với ma trận không vuông
        with pytest.raises(MatrixError):
            MatrixOperations.determinant(sample_matrices["2x3"])

    def test_inverse(self, sample_matrices):
        """Test tính ma trận nghịch đảo."""
        # Test ma trận khả nghịch
        result = MatrixOperations.inverse(sample_matrices["2x2"])
        expected = [[-2.0, 1.0], [1.5, -0.5]]
        assert all(
            abs(result[i][j] - expected[i][j]) < 1e-10
            for i in range(2)
            for j in range(2)
        )

        # Test ma trận không khả nghịch
        result = MatrixOperations.inverse(sample_matrices["singular"])
        assert result is None

        # Test với ma trận không vuông
        with pytest.raises(MatrixError):
            MatrixOperations.inverse(sample_matrices["2x3"])

class TestEdgeCases:
    """
    Test các trường hợp đặc biệt (30%)
    Pass: Xử lý đúng ≥ 80% edge cases
    Fail: Xử lý đúng < 80% edge cases
    """
    
    def test_zero_matrix(self):
        """Test với ma trận 0."""
        zero = [[0, 0], [0, 0]]
        
        # Cộng với ma trận 0
        result = MatrixOperations.add(zero, zero)
        assert all(all(x == 0 for x in row) for row in result)
        
        # Nhân với ma trận 0
        result = MatrixOperations.multiply(zero, zero)
        assert all(all(x == 0 for x in row) for row in result)

    def test_identity_matrix(self, sample_matrices):
        """Test với ma trận đơn vị."""
        identity = sample_matrices["identity"]
        matrix = sample_matrices["2x2"]
        
        # Nhân với ma trận đơn vị
        result = MatrixOperations.multiply(matrix, identity)
        assert result == matrix
        
        # Nghịch đảo ma trận đơn vị
        result = MatrixOperations.inverse(identity)
        assert result == identity

    def test_single_element(self):
        """Test với ma trận 1x1."""
        single = [[5]]
        
        # Các phép toán với ma trận 1x1
        assert MatrixOperations.determinant(single) == 5
        assert MatrixOperations.transpose(single) == single
        assert MatrixOperations.inverse(single) == [[0.2]]

class TestPerformance:
    """
    Test hiệu năng (10%)
    Pass: Thời gian thực thi ≤ 2x NumPy
    Fail: Thời gian thực thi > 2x NumPy
    """
    
    def test_large_matrix_operations(self, large_matrix):
        """Test hiệu năng với ma trận lớn."""
        # Test c��ng ma trận
        custom_time, numpy_time = MatrixOperations.compare_with_numpy(
            "add", large_matrix, large_matrix
        )
        assert custom_time <= 2 * numpy_time

        # Test nhân ma trận
        custom_time, numpy_time = MatrixOperations.compare_with_numpy(
            "multiply", large_matrix, large_matrix
        )
        assert custom_time <= 2 * numpy_time

    def test_advanced_operations(self, large_matrix):
        """Test hiệu năng các phép toán phức tạp."""
        # Test định thức
        custom_time, numpy_time = MatrixOperations.compare_with_numpy(
            "determinant", large_matrix
        )
        assert custom_time <= 2 * numpy_time

        # Test chuyển vị
        custom_time, numpy_time = MatrixOperations.compare_with_numpy(
            "transpose", large_matrix
        )
        assert custom_time <= 2 * numpy_time

class TestNumpyComparison:
    """Test so sánh kết quả với NumPy."""
    
    def test_results_match_numpy(self, sample_matrices):
        """Test độ chính xác so với NumPy."""
        matrix1 = np.array(sample_matrices["2x2"])
        matrix2 = np.array(sample_matrices["2x2_other"])
        
        # So sánh kết quả cộng
        custom_result = MatrixOperations.add(
            sample_matrices["2x2"],
            sample_matrices["2x2_other"]
        )
        numpy_result = (matrix1 + matrix2).tolist()
        assert custom_result == numpy_result
        
        # So sánh kết quả nhân
        custom_result = MatrixOperations.multiply(
            sample_matrices["2x2"],
            sample_matrices["2x2_other"]
        )
        numpy_result = (matrix1 @ matrix2).tolist()
        assert custom_result == numpy_result 