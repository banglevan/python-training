"""
Unit tests cho math_toolkit.py
Tiêu chí đánh giá:
1. Unit Conversion (40%): Chuyển đổi đơn vị
2. Geometric Calculations (40%): Tính toán hình học
3. Input Validation (20%): Kiểm tra đầu vào
"""

import pytest
import math
from math_toolkit import (
    Number,
    AngleUnit,
    LengthUnit,
    AreaUnit,
    VolumeUnit,
    Point2D,
    Point3D,
    MathToolkit
)

@pytest.fixture
def toolkit():
    """Tạo instance MathToolkit."""
    return MathToolkit()

class TestUnitConversion:
    """
    Test chuyển đổi đơn vị (40%)
    Pass: Chuyển đổi chính xác ≥ 99%
    Fail: Chuyển đổi chính xác < 99%
    """
    
    def test_angle_conversion(self, toolkit):
        """Test chuyển đổi góc."""
        # Degree -> Radian
        assert toolkit.convert_angle(
            180,
            AngleUnit.DEGREE,
            AngleUnit.RADIAN
        ) == pytest.approx(math.pi)
        
        # Radian -> Degree
        assert toolkit.convert_angle(
            math.pi,
            AngleUnit.RADIAN,
            AngleUnit.DEGREE
        ) == pytest.approx(180)
        
        # Cùng đơn vị
        assert toolkit.convert_angle(
            90,
            AngleUnit.DEGREE,
            AngleUnit.DEGREE
        ) == 90

    def test_length_conversion(self, toolkit):
        """Test chuyển đổi độ dài."""
        test_cases = [
            # m -> km
            (1000, LengthUnit.METER, LengthUnit.KILOMETER, 1),
            # km -> m
            (1, LengthUnit.KILOMETER, LengthUnit.METER, 1000),
            # m -> cm
            (1, LengthUnit.METER, LengthUnit.CENTIMETER, 100),
            # ft -> m
            (1, LengthUnit.FOOT, LengthUnit.METER, 0.3048),
            # mi -> km
            (1, LengthUnit.MILE, LengthUnit.KILOMETER, 1.609344)
        ]
        
        for value, from_unit, to_unit, expected in test_cases:
            assert toolkit.convert_length(
                value, from_unit, to_unit
            ) == pytest.approx(expected)

    def test_area_conversion(self, toolkit):
        """Test chuyển đổi diện tích."""
        test_cases = [
            # m² -> km²
            (1_000_000, AreaUnit.SQUARE_METER, AreaUnit.SQUARE_KILOMETER, 1),
            # ha -> m²
            (1, AreaUnit.HECTARE, AreaUnit.SQUARE_METER, 10_000),
            # ac -> m²
            (1, AreaUnit.ACRE, AreaUnit.SQUARE_METER, 4046.86),
            # ft² -> m²
            (1, AreaUnit.SQUARE_FOOT, AreaUnit.SQUARE_METER, 0.092903)
        ]
        
        for value, from_unit, to_unit, expected in test_cases:
            assert toolkit.convert_area(
                value, from_unit, to_unit
            ) == pytest.approx(expected)

    def test_volume_conversion(self, toolkit):
        """Test chuyển đổi thể tích."""
        test_cases = [
            # m³ -> L
            (1, VolumeUnit.CUBIC_METER, VolumeUnit.LITER, 1000),
            # L -> mL
            (1, VolumeUnit.LITER, VolumeUnit.MILLILITER, 1000),
            # gal -> L
            (1, VolumeUnit.GALLON, VolumeUnit.LITER, 3.78541),
            # ft³ -> m³
            (1, VolumeUnit.CUBIC_FOOT, VolumeUnit.CUBIC_METER, 0.0283168)
        ]
        
        for value, from_unit, to_unit, expected in test_cases:
            assert toolkit.convert_volume(
                value, from_unit, to_unit
            ) == pytest.approx(expected)

class TestGeometricCalculations:
    """
    Test tính toán hình học (40%)
    Pass: Tính toán chính xác ≥ 99%
    Fail: Tính toán chính xác < 99%
    """
    
    def test_distance_2d(self, toolkit):
        """Test khoảng cách 2D."""
        p1 = Point2D(0, 0)
        p2 = Point2D(3, 4)
        assert toolkit.distance_2d(p1, p2) == 5
        
        p1 = Point2D(-1, -1)
        p2 = Point2D(2, 3)
        assert toolkit.distance_2d(p1, p2) == 5

    def test_distance_3d(self, toolkit):
        """Test khoảng cách 3D."""
        p1 = Point3D(0, 0, 0)
        p2 = Point3D(1, 2, 2)
        assert toolkit.distance_3d(p1, p2) == 3
        
        p1 = Point3D(-1, -1, -1)
        p2 = Point3D(2, 2, 2)
        assert toolkit.distance_3d(p1, p2) == pytest.approx(5.196152)

    def test_triangle_area(self, toolkit):
        """Test diện tích tam giác."""
        # Tam giác vuông 3-4-5
        assert toolkit.triangle_area(3, 4, 5) == 6
        
        # Tam giác đều cạnh 2
        assert toolkit.triangle_area(2, 2, 2) == pytest.approx(1.732051)
        
        # Tam giác không hợp lệ
        with pytest.raises(ValueError):
            toolkit.triangle_area(1, 1, 3)

    def test_circle_calculations(self, toolkit):
        """Test tính toán hình tròn."""
        # Diện tích
        assert toolkit.circle_area(1) == pytest.approx(math.pi)
        assert toolkit.circle_area(2) == pytest.approx(4 * math.pi)
        
        with pytest.raises(ValueError):
            toolkit.circle_area(-1)

    def test_sphere_calculations(self, toolkit):
        """Test tính toán hình cầu."""
        # Diện tích
        assert toolkit.sphere_area(1) == pytest.approx(4 * math.pi)
        assert toolkit.sphere_area(2) == pytest.approx(16 * math.pi)
        
        # Thể tích
        assert toolkit.sphere_volume(1) == pytest.approx(4/3 * math.pi)
        assert toolkit.sphere_volume(2) == pytest.approx(32/3 * math.pi)
        
        with pytest.raises(ValueError):
            toolkit.sphere_area(-1)
        with pytest.raises(ValueError):
            toolkit.sphere_volume(-1)

    def test_cylinder_calculations(self, toolkit):
        """Test tính toán hình trụ."""
        # Diện tích
        assert toolkit.cylinder_area(1, 1) == pytest.approx(4 * math.pi)
        assert toolkit.cylinder_area(2, 3) == pytest.approx(20 * math.pi)
        
        # Thể tích
        assert toolkit.cylinder_volume(1, 1) == pytest.approx(math.pi)
        assert toolkit.cylinder_volume(2, 3) == pytest.approx(12 * math.pi)
        
        with pytest.raises(ValueError):
            toolkit.cylinder_area(-1, 1)
        with pytest.raises(ValueError):
            toolkit.cylinder_area(1, -1)
        with pytest.raises(ValueError):
            toolkit.cylinder_volume(-1, 1)
        with pytest.raises(ValueError):
            toolkit.cylinder_volume(1, -1)

class TestInputValidation:
    """
    Test kiểm tra đầu vào (20%)
    Pass: Validate đúng ≥ 95% cases
    Fail: Validate đúng < 95% cases
    """
    
    def test_negative_values(self, toolkit):
        """Test giá trị âm."""
        with pytest.raises(ValueError):
            toolkit.circle_area(-1)
            
        with pytest.raises(ValueError):
            toolkit.sphere_area(-1)
            
        with pytest.raises(ValueError):
            toolkit.sphere_volume(-1)
            
        with pytest.raises(ValueError):
            toolkit.cylinder_area(-1, 1)
            
        with pytest.raises(ValueError):
            toolkit.cylinder_volume(1, -1)

    def test_invalid_triangle(self, toolkit):
        """Test tam giác không hợp lệ."""
        invalid_cases = [
            (1, 1, 3),  # Tổng 2 cạnh <= cạnh còn lại
            (3, 1, 1),
            (1, 3, 1),
            (0, 1, 1),  # Có cạnh = 0
            (-1, 2, 2)  # Có cạnh âm
        ]
        
        for a, b, c in invalid_cases:
            with pytest.raises(ValueError):
                toolkit.triangle_area(a, b, c)

    def test_zero_values(self, toolkit):
        """Test giá trị 0."""
        assert toolkit.circle_area(0) == 0
        assert toolkit.sphere_area(0) == 0
        assert toolkit.sphere_volume(0) == 0
        assert toolkit.cylinder_area(0, 1) == 0
        assert toolkit.cylinder_volume(1, 0) == 0 