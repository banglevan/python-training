"""
Shape hierarchy exercise
Minh họa inheritance và polymorphism với các hình học
"""

from abc import ABC, abstractmethod
import math
from typing import Optional, Tuple, List

class Shape(ABC):
    """Abstract base class cho các hình."""
    
    def __init__(self, color: str = "black"):
        """
        Khởi tạo hình.
        
        Args:
            color: Màu sắc
        """
        self.color = color
    
    @abstractmethod
    def area(self) -> float:
        """Tính diện tích."""
        pass
    
    @abstractmethod
    def perimeter(self) -> float:
        """Tính chu vi."""
        pass
    
    def __str__(self) -> str:
        """String representation."""
        return f"{self.__class__.__name__}(color={self.color})"

class Circle(Shape):
    """Hình tròn."""
    
    def __init__(
        self,
        radius: float,
        color: str = "black"
    ):
        """
        Khởi tạo hình tròn.
        
        Args:
            radius: Bán kính
            color: Màu sắc
        """
        super().__init__(color)
        if radius <= 0:
            raise ValueError("Radius must be positive")
        self.radius = radius
    
    def area(self) -> float:
        """Tính diện tích."""
        return math.pi * self.radius ** 2
    
    def perimeter(self) -> float:
        """Tính chu vi."""
        return 2 * math.pi * self.radius
    
    def __str__(self) -> str:
        """String representation."""
        return (
            f"Circle(radius={self.radius}, "
            f"color={self.color})"
        )

class Rectangle(Shape):
    """Hình chữ nhật."""
    
    def __init__(
        self,
        width: float,
        height: float,
        color: str = "black"
    ):
        """
        Khởi tạo hình chữ nhật.
        
        Args:
            width: Chiều rộng
            height: Chiều cao
            color: Màu sắc
        """
        super().__init__(color)
        if width <= 0 or height <= 0:
            raise ValueError(
                "Width and height must be positive"
            )
        self.width = width
        self.height = height
    
    def area(self) -> float:
        """Tính diện tích."""
        return self.width * self.height
    
    def perimeter(self) -> float:
        """Tính chu vi."""
        return 2 * (self.width + self.height)
    
    def __str__(self) -> str:
        """String representation."""
        return (
            f"Rectangle(width={self.width}, "
            f"height={self.height}, color={self.color})"
        )

class Square(Rectangle):
    """Hình vuông."""
    
    def __init__(
        self,
        side: float,
        color: str = "black"
    ):
        """
        Khởi tạo hình vuông.
        
        Args:
            side: Độ dài cạnh
            color: Màu sắc
        """
        super().__init__(side, side, color)
        self.side = side
    
    def __str__(self) -> str:
        """String representation."""
        return f"Square(side={self.side}, color={self.color})"

class Triangle(Shape):
    """Hình tam giác."""
    
    def __init__(
        self,
        a: float,
        b: float,
        c: float,
        color: str = "black"
    ):
        """
        Khởi tạo tam giác.
        
        Args:
            a: Cạnh a
            b: Cạnh b
            c: Cạnh c
            color: Màu sắc
        """
        super().__init__(color)
        if not self._is_valid(a, b, c):
            raise ValueError("Invalid triangle sides")
        self.a = a
        self.b = b
        self.c = c
    
    def _is_valid(self, a: float, b: float, c: float) -> bool:
        """Kiểm tra tam giác hợp lệ."""
        if a <= 0 or b <= 0 or c <= 0:
            return False
        return (
            a + b > c and
            b + c > a and
            c + a > b
        )
    
    def area(self) -> float:
        """Tính diện tích theo công thức Heron."""
        s = self.perimeter() / 2
        return math.sqrt(
            s *
            (s - self.a) *
            (s - self.b) *
            (s - self.c)
        )
    
    def perimeter(self) -> float:
        """Tính chu vi."""
        return self.a + self.b + self.c
    
    def __str__(self) -> str:
        """String representation."""
        return (
            f"Triangle(a={self.a}, b={self.b}, "
            f"c={self.c}, color={self.color})"
        )

class ShapeCollection:
    """Tập hợp các hình."""
    
    def __init__(self):
        """Khởi tạo collection."""
        self.shapes: List[Shape] = []
    
    def add_shape(self, shape: Shape):
        """Thêm hình."""
        self.shapes.append(shape)
    
    def total_area(self) -> float:
        """Tính tổng diện tích."""
        return sum(shape.area() for shape in self.shapes)
    
    def total_perimeter(self) -> float:
        """Tính tổng chu vi."""
        return sum(shape.perimeter() for shape in self.shapes)
    
    def get_shapes_by_type(
        self,
        shape_type: type
    ) -> List[Shape]:
        """Lấy các hình theo loại."""
        return [
            shape for shape in self.shapes
            if isinstance(shape, shape_type)
        ]
    
    def get_shapes_by_color(
        self,
        color: str
    ) -> List[Shape]:
        """Lấy các hình theo màu."""
        return [
            shape for shape in self.shapes
            if shape.color == color
        ]
    
    def __str__(self) -> str:
        """String representation."""
        return "\n".join(str(shape) for shape in self.shapes) 