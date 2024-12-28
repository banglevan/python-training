"""
Unit tests cho shape hierarchy
Tiêu chí đánh giá:
1. Shape Classes (40%): Circle, Rectangle, Square, Triangle
2. Inheritance (20%): Kế thừa đúng
3. Polymorphism (20%): Đa hình đúng
4. Collection (20%): ShapeCollection hoạt động đúng
"""

import pytest
import math
from shapes import (
    Shape,
    Circle,
    Rectangle,
    Square,
    Triangle,
    ShapeCollection
)

class TestShapeClasses:
    """
    Test shape classes (40%)
    Pass: Tính toán đúng ≥ 95%
    Fail: Tính toán đúng < 95%
    """
    
    def test_circle(self):
        """Test Circle class."""
        circle = Circle(radius=5, color="red")
        
        # Properties
        assert circle.radius == 5
        assert circle.color == "red"
        
        # Area
        assert circle.area() == pytest.approx(
            math.pi * 25
        )
        
        # Perimeter
        assert circle.perimeter() == pytest.approx(
            2 * math.pi * 5
        )
        
        # Invalid radius
        with pytest.raises(ValueError):
            Circle(radius=-1)
        
        # String representation
        assert str(circle) == "Circle(radius=5, color=red)"

    def test_rectangle(self):
        """Test Rectangle class."""
        rect = Rectangle(width=4, height=3, color="blue")
        
        # Properties
        assert rect.width == 4
        assert rect.height == 3
        assert rect.color == "blue"
        
        # Area
        assert rect.area() == 12
        
        # Perimeter
        assert rect.perimeter() == 14
        
        # Invalid dimensions
        with pytest.raises(ValueError):
            Rectangle(width=-1, height=3)
        with pytest.raises(ValueError):
            Rectangle(width=4, height=-1)
        
        # String representation
        assert str(rect) == (
            "Rectangle(width=4, height=3, color=blue)"
        )

    def test_square(self):
        """Test Square class."""
        square = Square(side=5, color="green")
        
        # Properties
        assert square.side == 5
        assert square.width == 5
        assert square.height == 5
        assert square.color == "green"
        
        # Area
        assert square.area() == 25
        
        # Perimeter
        assert square.perimeter() == 20
        
        # Invalid side
        with pytest.raises(ValueError):
            Square(side=-1)
        
        # String representation
        assert str(square) == "Square(side=5, color=green)"

    def test_triangle(self):
        """Test Triangle class."""
        triangle = Triangle(a=3, b=4, c=5, color="yellow")
        
        # Properties
        assert triangle.a == 3
        assert triangle.b == 4
        assert triangle.c == 5
        assert triangle.color == "yellow"
        
        # Area (using Heron's formula)
        assert triangle.area() == 6
        
        # Perimeter
        assert triangle.perimeter() == 12
        
        # Invalid sides
        with pytest.raises(ValueError):
            Triangle(a=-1, b=4, c=5)
        with pytest.raises(ValueError):
            Triangle(a=1, b=2, c=5)  # Invalid triangle
        
        # String representation
        assert str(triangle) == (
            "Triangle(a=3, b=4, c=5, color=yellow)"
        )

class TestInheritance:
    """
    Test inheritance (20%)
    Pass: Kế thừa đúng ≥ 95%
    Fail: Kế thừa đúng < 95%
    """
    
    def test_inheritance_chain(self):
        """Test inheritance chain."""
        # Circle inherits from Shape
        assert issubclass(Circle, Shape)
        
        # Rectangle inherits from Shape
        assert issubclass(Rectangle, Shape)
        
        # Square inherits from Rectangle
        assert issubclass(Square, Rectangle)
        
        # Triangle inherits from Shape
        assert issubclass(Triangle, Shape)

    def test_abstract_methods(self):
        """Test abstract methods."""
        # Cannot instantiate Shape
        with pytest.raises(TypeError):
            Shape()
        
        # Must implement area() and perimeter()
        class InvalidShape(Shape):
            pass
        
        with pytest.raises(TypeError):
            InvalidShape()

class TestPolymorphism:
    """
    Test polymorphism (20%)
    Pass: Đa hình đúng ≥ 95%
    Fail: Đa hình đúng < 95%
    """
    
    def test_polymorphic_behavior(self):
        """Test polymorphic behavior."""
        shapes = [
            Circle(5),
            Rectangle(4, 3),
            Square(5),
            Triangle(3, 4, 5)
        ]
        
        # All shapes have area
        areas = [shape.area() for shape in shapes]
        assert len(areas) == 4
        
        # All shapes have perimeter
        perimeters = [shape.perimeter() for shape in shapes]
        assert len(perimeters) == 4
        
        # All shapes have string representation
        strings = [str(shape) for shape in shapes]
        assert len(strings) == 4

class TestShapeCollection:
    """
    Test shape collection (20%)
    Pass: Collection hoạt động đúng ≥ 95%
    Fail: Collection hoạt động đúng < 95%
    """
    
    def test_collection_operations(self):
        """Test collection operations."""
        collection = ShapeCollection()
        
        # Add shapes
        collection.add_shape(Circle(5, "red"))
        collection.add_shape(Rectangle(4, 3, "blue"))
        collection.add_shape(Square(5, "green"))
        collection.add_shape(Triangle(3, 4, 5, "yellow"))
        
        # Total area
        total_area = collection.total_area()
        assert total_area == pytest.approx(
            math.pi * 25 + 12 + 25 + 6
        )
        
        # Total perimeter
        total_perimeter = collection.total_perimeter()
        assert total_perimeter == pytest.approx(
            2 * math.pi * 5 + 14 + 20 + 12
        )

    def test_collection_filters(self):
        """Test collection filters."""
        collection = ShapeCollection()
        
        # Add shapes
        collection.add_shape(Circle(5, "red"))
        collection.add_shape(Rectangle(4, 3, "blue"))
        collection.add_shape(Square(5, "red"))
        collection.add_shape(Triangle(3, 4, 5, "blue"))
        
        # Filter by type
        circles = collection.get_shapes_by_type(Circle)
        assert len(circles) == 1
        assert isinstance(circles[0], Circle)
        
        rectangles = collection.get_shapes_by_type(Rectangle)
        assert len(rectangles) == 2  # Including Square
        
        # Filter by color
        red_shapes = collection.get_shapes_by_color("red")
        assert len(red_shapes) == 2
        
        blue_shapes = collection.get_shapes_by_color("blue")
        assert len(blue_shapes) == 2 