"""
Bài tập 3: Matrix Operations
Yêu cầu:
1. Implement các phép toán ma trận cơ bản
2. Kiểm tra tính hợp lệ của ma trận
3. Tối ưu hiệu năng cho ma trận lớn
4. Hỗ trợ nhiều kiểu dữ liệu (int, float)
"""

from typing import List, Union, Tuple, Optional
import numpy as np  # for performance comparison
from dataclasses import dataclass

# Type alias for matrix
Matrix = List[List[Union[int, float]]]

@dataclass
class MatrixSize:
    """Class lưu kích thước ma trận."""
    rows: int
    cols: int

class MatrixError(Exception):
    """Custom exception cho các lỗi ma trận."""
    pass

class MatrixOperations:
    """Class chứa các phép toán ma trận."""
    
    @staticmethod
    def validate_matrix(matrix: Matrix) -> MatrixSize:
        """
        Kiểm tra tính hợp lệ của ma trận.
        
        Args:
            matrix (Matrix): Ma trận cần kiểm tra
            
        Returns:
            MatrixSize: Kích thước ma trận
            
        Raises:
            MatrixError: Nếu ma trận không hợp lệ
            
        Examples:
            >>> matrix = [[1, 2], [3, 4]]
            >>> size = MatrixOperations.validate_matrix(matrix)
            >>> print(f"{size.rows}x{size.cols}")
            2x2
        """
        if not matrix or not matrix[0]:
            raise MatrixError("Ma trận rỗng")
            
        rows = len(matrix)
        cols = len(matrix[0])
        
        # Kiểm tra số cột của các hàng
        for row in matrix:
            if len(row) != cols:
                raise MatrixError("Số cột không đồng đều")
                
        # Kiểm tra kiểu dữ liệu
        for row in matrix:
            for element in row:
                if not isinstance(element, (int, float)):
                    raise MatrixError("Phần tử không hợp lệ")
                    
        return MatrixSize(rows, cols)

    @staticmethod
    def add(matrix1: Matrix, matrix2: Matrix) -> Matrix:
        """
        Cộng hai ma trận.
        
        Args:
            matrix1 (Matrix): Ma trận thứ nhất
            matrix2 (Matrix): Ma trận thứ hai
            
        Returns:
            Matrix: Ma trận kết quả
            
        Raises:
            MatrixError: Nếu kích thước không khớp
            
        Examples:
            >>> m1 = [[1, 2], [3, 4]]
            >>> m2 = [[5, 6], [7, 8]]
            >>> result = MatrixOperations.add(m1, m2)
            >>> print(result)
            [[6, 8], [10, 12]]
        """
        # TODO: Implement this function
        pass

    @staticmethod
    def multiply(matrix1: Matrix, matrix2: Matrix) -> Matrix:
        """
        Nhân hai ma trận.
        
        Args:
            matrix1 (Matrix): Ma trận thứ nhất
            matrix2 (Matrix): Ma trận thứ hai
            
        Returns:
            Matrix: Ma trận kết quả
            
        Raises:
            MatrixError: Nếu kích thước không khớp
        """
        # TODO: Implement this function
        pass

    @staticmethod
    def transpose(matrix: Matrix) -> Matrix:
        """
        Chuyển vị ma trận.
        
        Args:
            matrix (Matrix): Ma trận đầu vào
            
        Returns:
            Matrix: Ma trận chuyển vị
        """
        # TODO: Implement this function
        pass

    @staticmethod
    def determinant(matrix: Matrix) -> float:
        """
        Tính định thức ma trận vuông.
        
        Args:
            matrix (Matrix): Ma trận vuông
            
        Returns:
            float: Định thức ma trận
            
        Raises:
            MatrixError: Nếu không phải ma trận vuông
        """
        # TODO: Implement this function
        pass

    @staticmethod
    def inverse(matrix: Matrix) -> Optional[Matrix]:
        """
        Tính ma trận nghịch đảo.
        
        Args:
            matrix (Matrix): Ma trận đầu vào
            
        Returns:
            Optional[Matrix]: Ma trận nghịch đảo hoặc None nếu không tồn tại
            
        Raises:
            MatrixError: Nếu ma trận không khả nghịch
        """
        # TODO: Implement this function
        pass

    @staticmethod
    def compare_with_numpy(
        operation: str,
        matrix1: Matrix,
        matrix2: Optional[Matrix] = None
    ) -> Tuple[float, float]:
        """
        So sánh hiệu năng với NumPy.
        
        Args:
            operation (str): Tên phép toán
            matrix1 (Matrix): Ma trận thứ nhất
            matrix2 (Optional[Matrix]): Ma trận thứ hai (nếu cần)
            
        Returns:
            Tuple[float, float]: (thời gian custom, thời gian numpy)
        """
        # TODO: Implement this function
        pass

def main():
    """Chương trình chính."""
    while True:
        print("\nChương Trình Tính Toán Ma Trận")
        print("1. Cộng ma trận")
        print("2. Nhân ma trận")
        print("3. Chuyển vị ma trận")
        print("4. Tính định thức")
        print("5. Tính ma trận nghịch đảo")
        print("6. So sánh với NumPy")
        print("7. Thoát")

        choice = input("\nChọn chức năng (1-7): ")

        if choice == '7':
            break
        elif choice == '1':
            # TODO: Implement matrix addition interface
            pass
        elif choice == '2':
            # TODO: Implement matrix multiplication interface
            pass
        elif choice == '3':
            # TODO: Implement matrix transpose interface
            pass
        elif choice == '4':
            # TODO: Implement determinant calculation interface
            pass
        elif choice == '5':
            # TODO: Implement matrix inversion interface
            pass
        elif choice == '6':
            # TODO: Implement NumPy comparison interface
            pass
        else:
            print("Lựa chọn không hợp lệ!")

if __name__ == "__main__":
    main() 