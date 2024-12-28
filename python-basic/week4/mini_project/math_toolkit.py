"""
Mini Project: Math Toolkit
Yêu cầu:
1. Các hàm toán học
2. Unit conversion
3. Geometric calculations
"""

from typing import Union, List, Dict, Optional, Tuple
from dataclasses import dataclass
import math
from enum import Enum

Number = Union[int, float]  # Type alias cho số

class AngleUnit(Enum):
    """Đơn vị góc."""
    DEGREE = "degree"
    RADIAN = "radian"

class LengthUnit(Enum):
    """Đơn vị độ dài."""
    METER = "m"
    KILOMETER = "km"
    CENTIMETER = "cm"
    MILLIMETER = "mm"
    INCH = "in"
    FOOT = "ft"
    YARD = "yd"
    MILE = "mi"

class AreaUnit(Enum):
    """Đơn vị diện tích."""
    SQUARE_METER = "m²"
    SQUARE_KILOMETER = "km²"
    SQUARE_CENTIMETER = "cm²"
    SQUARE_MILLIMETER = "mm²"
    SQUARE_INCH = "in²"
    SQUARE_FOOT = "ft²"
    SQUARE_YARD = "yd²"
    SQUARE_MILE = "mi²"
    HECTARE = "ha"
    ACRE = "ac"

class VolumeUnit(Enum):
    """Đơn vị thể tích."""
    CUBIC_METER = "m³"
    CUBIC_KILOMETER = "km³"
    CUBIC_CENTIMETER = "cm³"
    CUBIC_MILLIMETER = "mm³"
    CUBIC_INCH = "in³"
    CUBIC_FOOT = "ft³"
    CUBIC_YARD = "yd³"
    LITER = "L"
    MILLILITER = "mL"
    GALLON = "gal"

@dataclass
class Point2D:
    """Điểm 2D."""
    x: Number
    y: Number

@dataclass
class Point3D:
    """Điểm 3D."""
    x: Number
    y: Number
    z: Number

class MathToolkit:
    """Class chứa các công cụ toán học."""
    
    # Conversion factors
    LENGTH_FACTORS = {
        LengthUnit.METER: 1,
        LengthUnit.KILOMETER: 1000,
        LengthUnit.CENTIMETER: 0.01,
        LengthUnit.MILLIMETER: 0.001,
        LengthUnit.INCH: 0.0254,
        LengthUnit.FOOT: 0.3048,
        LengthUnit.YARD: 0.9144,
        LengthUnit.MILE: 1609.344
    }
    
    AREA_FACTORS = {
        AreaUnit.SQUARE_METER: 1,
        AreaUnit.SQUARE_KILOMETER: 1_000_000,
        AreaUnit.SQUARE_CENTIMETER: 0.0001,
        AreaUnit.SQUARE_MILLIMETER: 0.000001,
        AreaUnit.SQUARE_INCH: 0.00064516,
        AreaUnit.SQUARE_FOOT: 0.092903,
        AreaUnit.SQUARE_YARD: 0.836127,
        AreaUnit.SQUARE_MILE: 2_589_988.11,
        AreaUnit.HECTARE: 10_000,
        AreaUnit.ACRE: 4046.86
    }
    
    VOLUME_FACTORS = {
        VolumeUnit.CUBIC_METER: 1,
        VolumeUnit.CUBIC_KILOMETER: 1_000_000_000,
        VolumeUnit.CUBIC_CENTIMETER: 0.000001,
        VolumeUnit.CUBIC_MILLIMETER: 1e-9,
        VolumeUnit.CUBIC_INCH: 0.0000163871,
        VolumeUnit.CUBIC_FOOT: 0.0283168,
        VolumeUnit.CUBIC_YARD: 0.764555,
        VolumeUnit.LITER: 0.001,
        VolumeUnit.MILLILITER: 0.000001,
        VolumeUnit.GALLON: 0.00378541
    }
    
    @staticmethod
    def convert_angle(
        value: Number,
        from_unit: AngleUnit,
        to_unit: AngleUnit
    ) -> Number:
        """
        Chuyển đổi đơn vị góc.
        
        Args:
            value: Giá trị góc
            from_unit: Đơn vị gốc
            to_unit: Đơn vị đích
            
        Returns:
            Number: Giá trị sau chuyển đổi
        """
        if from_unit == to_unit:
            return value
            
        if from_unit == AngleUnit.DEGREE:
            return math.radians(value)
        return math.degrees(value)

    @staticmethod
    def convert_length(
        value: Number,
        from_unit: LengthUnit,
        to_unit: LengthUnit
    ) -> Number:
        """
        Chuyển đổi đơn vị độ dài.
        
        Args:
            value: Giá trị độ dài
            from_unit: Đơn vị gốc
            to_unit: Đơn vị đích
            
        Returns:
            Number: Gi�� trị sau chuyển đổi
        """
        if from_unit == to_unit:
            return value
            
        # Chuyển về mét
        meters = value * MathToolkit.LENGTH_FACTORS[from_unit]
        # Chuyển sang đơn vị đích
        return meters / MathToolkit.LENGTH_FACTORS[to_unit]

    @staticmethod
    def convert_area(
        value: Number,
        from_unit: AreaUnit,
        to_unit: AreaUnit
    ) -> Number:
        """
        Chuyển đổi đơn vị diện tích.
        
        Args:
            value: Giá trị diện tích
            from_unit: Đơn vị gốc
            to_unit: Đơn vị đích
            
        Returns:
            Number: Giá trị sau chuyển đổi
        """
        if from_unit == to_unit:
            return value
            
        # Chuyển về mét vuông
        sq_meters = value * MathToolkit.AREA_FACTORS[from_unit]
        # Chuyển sang đơn vị đích
        return sq_meters / MathToolkit.AREA_FACTORS[to_unit]

    @staticmethod
    def convert_volume(
        value: Number,
        from_unit: VolumeUnit,
        to_unit: VolumeUnit
    ) -> Number:
        """
        Chuyển đổi đơn vị thể tích.
        
        Args:
            value: Giá trị thể tích
            from_unit: Đơn vị gốc
            to_unit: Đơn vị đích
            
        Returns:
            Number: Giá trị sau chuyển đổi
        """
        if from_unit == to_unit:
            return value
            
        # Chuyển về mét khối
        cu_meters = value * MathToolkit.VOLUME_FACTORS[from_unit]
        # Chuyển sang đơn vị đích
        return cu_meters / MathToolkit.VOLUME_FACTORS[to_unit]

    @staticmethod
    def distance_2d(p1: Point2D, p2: Point2D) -> Number:
        """Tính khoảng cách giữa 2 điểm 2D."""
        return math.sqrt(
            (p2.x - p1.x) ** 2 +
            (p2.y - p1.y) ** 2
        )

    @staticmethod
    def distance_3d(p1: Point3D, p2: Point3D) -> Number:
        """Tính khoảng cách giữa 2 điểm 3D."""
        return math.sqrt(
            (p2.x - p1.x) ** 2 +
            (p2.y - p1.y) ** 2 +
            (p2.z - p1.z) ** 2
        )

    @staticmethod
    def triangle_area(a: Number, b: Number, c: Number) -> Number:
        """
        Tính diện tích tam giác từ 3 cạnh.
        
        Args:
            a, b, c: Độ dài 3 cạnh
            
        Returns:
            Number: Diện tích tam giác
            
        Raises:
            ValueError: Nếu 3 cạnh không tạo thành tam giác
        """
        # Kiểm tra điều kiện tam giác
        if a + b <= c or b + c <= a or c + a <= b:
            raise ValueError("3 cạnh không tạo thành tam giác")
            
        # Công thức Heron
        s = (a + b + c) / 2  # Nửa chu vi
        return math.sqrt(
            s * (s - a) * (s - b) * (s - c)
        )

    @staticmethod
    def circle_area(radius: Number) -> Number:
        """Tính diện tích hình tròn."""
        if radius < 0:
            raise ValueError("Bán kính phải không âm")
        return math.pi * radius ** 2

    @staticmethod
    def sphere_area(radius: Number) -> Number:
        """Tính diện tích mặt cầu."""
        if radius < 0:
            raise ValueError("Bán kính phải không âm")
        return 4 * math.pi * radius ** 2

    @staticmethod
    def sphere_volume(radius: Number) -> Number:
        """Tính thể tích hình cầu."""
        if radius < 0:
            raise ValueError("Bán kính phải không âm")
        return 4/3 * math.pi * radius ** 3

    @staticmethod
    def cylinder_area(radius: Number, height: Number) -> Number:
        """Tính diện tích toàn phần hình trụ."""
        if radius < 0 or height < 0:
            raise ValueError("Kích thước phải không âm")
        return 2 * math.pi * radius * (radius + height)

    @staticmethod
    def cylinder_volume(radius: Number, height: Number) -> Number:
        """Tính thể tích hình trụ."""
        if radius < 0 or height < 0:
            raise ValueError("Kích thước phải không âm")
        return math.pi * radius ** 2 * height

def main():
    """Chương trình chính."""
    toolkit = MathToolkit()

    while True:
        print("\nMath Toolkit")
        print("1. Chuyển đổi đơn vị")
        print("2. Tính toán hình học")
        print("3. Thoát")

        choice = input("\nChọn chức năng (1-3): ")

        if choice == '3':
            break
        elif choice == '1':
            print("\nChọn loại đơn vị:")
            print("1. Góc")
            print("2. Độ dài")
            print("3. Diện tích")
            print("4. Thể tích")
            
            unit_choice = input("\nChọn loại (1-4): ")
            
            try:
                value = float(input("Nhập giá trị: "))
                
                if unit_choice == '1':
                    print("\nĐơn vị góc:")
                    for unit in AngleUnit:
                        print(f"{unit.value}")
                    
                    from_unit = AngleUnit(input("Đơn vị gốc: "))
                    to_unit = AngleUnit(input("Đơn vị đích: "))
                    
                    result = toolkit.convert_angle(value, from_unit, to_unit)
                    
                elif unit_choice == '2':
                    print("\nĐơn vị độ dài:")
                    for unit in LengthUnit:
                        print(f"{unit.value}")
                    
                    from_unit = LengthUnit(input("Đơn vị gốc: "))
                    to_unit = LengthUnit(input("Đơn vị đích: "))
                    
                    result = toolkit.convert_length(value, from_unit, to_unit)
                    
                elif unit_choice == '3':
                    print("\nĐơn vị diện tích:")
                    for unit in AreaUnit:
                        print(f"{unit.value}")
                    
                    from_unit = AreaUnit(input("Đơn vị gốc: "))
                    to_unit = AreaUnit(input("Đơn vị đích: "))
                    
                    result = toolkit.convert_area(value, from_unit, to_unit)
                    
                elif unit_choice == '4':
                    print("\nĐơn vị thể tích:")
                    for unit in VolumeUnit:
                        print(f"{unit.value}")
                    
                    from_unit = VolumeUnit(input("Đơn vị gốc: "))
                    to_unit = VolumeUnit(input("Đơn vị đích: "))
                    
                    result = toolkit.convert_volume(value, from_unit, to_unit)
                    
                else:
                    print("Lựa chọn không hợp lệ!")
                    continue
                    
                print(f"\nKết quả: {result}")
                
            except ValueError as e:
                print(f"Lỗi: {e}")
                
        elif choice == '2':
            print("\nChọn phép tính:")
            print("1. Khoảng cách 2D")
            print("2. Khoảng cách 3D")
            print("3. Diện tích tam giác")
            print("4. Diện tích hình tròn")
            print("5. Diện tích mặt cầu")
            print("6. Thể tích hình cầu")
            print("7. Diện tích hình trụ")
            print("8. Thể tích hình trụ")
            
            calc_choice = input("\nChọn phép tính (1-8): ")
            
            try:
                if calc_choice == '1':
                    x1 = float(input("x1: "))
                    y1 = float(input("y1: "))
                    x2 = float(input("x2: "))
                    y2 = float(input("y2: "))
                    
                    p1 = Point2D(x1, y1)
                    p2 = Point2D(x2, y2)
                    result = toolkit.distance_2d(p1, p2)
                    
                elif calc_choice == '2':
                    x1 = float(input("x1: "))
                    y1 = float(input("y1: "))
                    z1 = float(input("z1: "))
                    x2 = float(input("x2: "))
                    y2 = float(input("y2: "))
                    z2 = float(input("z2: "))
                    
                    p1 = Point3D(x1, y1, z1)
                    p2 = Point3D(x2, y2, z2)
                    result = toolkit.distance_3d(p1, p2)
                    
                elif calc_choice == '3':
                    a = float(input("Cạnh a: "))
                    b = float(input("Cạnh b: "))
                    c = float(input("Cạnh c: "))
                    
                    result = toolkit.triangle_area(a, b, c)
                    
                elif calc_choice == '4':
                    radius = float(input("Bán kính: "))
                    result = toolkit.circle_area(radius)
                    
                elif calc_choice == '5':
                    radius = float(input("Bán kính: "))
                    result = toolkit.sphere_area(radius)
                    
                elif calc_choice == '6':
                    radius = float(input("Bán kính: "))
                    result = toolkit.sphere_volume(radius)
                    
                elif calc_choice == '7':
                    radius = float(input("Bán kính: "))
                    height = float(input("Chiều cao: "))
                    result = toolkit.cylinder_area(radius, height)
                    
                elif calc_choice == '8':
                    radius = float(input("Bán kính: "))
                    height = float(input("Chiều cao: "))
                    result = toolkit.cylinder_volume(radius, height)
                    
                else:
                    print("Lựa chọn không hợp lệ!")
                    continue
                    
                print(f"\nKết quả: {result}")
                
            except ValueError as e:
                print(f"Lỗi: {e}")
                
        else:
            print("Lựa chọn không hợp lệ!")

if __name__ == "__main__":
    main() 