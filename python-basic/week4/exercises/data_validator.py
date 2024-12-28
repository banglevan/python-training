"""
Bài tập 3: Data validation functions
Yêu cầu:
1. Kiểm tra kiểu dữ liệu
2. Kiểm tra giá trị và phạm vi
3. Kiểm tra định dạng
4. Kiểm tra ràng buộc
"""

from typing import Any, List, Dict, Optional, Union, Type
from dataclasses import dataclass
from datetime import datetime
import re
from enum import Enum

class DataType(Enum):
    """Enum cho các kiểu dữ liệu."""
    STRING = "string"
    INTEGER = "integer"
    FLOAT = "float"
    BOOLEAN = "boolean"
    DATE = "date"
    EMAIL = "email"
    PHONE = "phone"
    URL = "url"

@dataclass
class ValidationRule:
    """Class định nghĩa rule kiểm tra."""
    data_type: DataType
    required: bool = True
    min_length: Optional[int] = None
    max_length: Optional[int] = None
    min_value: Optional[Union[int, float]] = None
    max_value: Optional[Union[int, float]] = None
    pattern: Optional[str] = None
    allowed_values: Optional[List[Any]] = None
    custom_validator: Optional[callable] = None
    
    def validate(self, value: Any) -> tuple[bool, Optional[str]]:
        """
        Kiểm tra giá trị theo rule.
        
        Args:
            value: Giá trị cần kiểm tra
            
        Returns:
            tuple[bool, Optional[str]]: (Kết quả, Thông báo lỗi)
        """
        # Kiểm tra required
        if value is None or value == "":
            if self.required:
                return False, "Giá trị bắt buộc"
            return True, None
            
        # Kiểm tra kiểu dữ liệu
        try:
            if self.data_type == DataType.STRING:
                value = str(value)
            elif self.data_type == DataType.INTEGER:
                value = int(value)
            elif self.data_type == DataType.FLOAT:
                value = float(value)
            elif self.data_type == DataType.BOOLEAN:
                if isinstance(value, str):
                    value = value.lower()
                    if value not in ('true', 'false', '1', '0'):
                        return False, "Giá trị boolean không hợp lệ"
                    value = value in ('true', '1')
            elif self.data_type == DataType.DATE:
                if isinstance(value, str):
                    try:
                        value = datetime.strptime(value, "%Y-%m-%d")
                    except ValueError:
                        return False, "Đ���nh dạng ngày không hợp lệ (YYYY-MM-DD)"
            elif self.data_type == DataType.EMAIL:
                if not re.match(r'^[\w\.-]+@[\w\.-]+\.\w+$', str(value)):
                    return False, "Email không hợp lệ"
            elif self.data_type == DataType.PHONE:
                if not re.match(r'^\+?[\d\s-]{10,}$', str(value)):
                    return False, "Số điện thoại không hợp lệ"
            elif self.data_type == DataType.URL:
                if not re.match(
                    r'^https?://(?:[\w-]+\.)+[\w-]+(?:/[\w-./?%&=]*)?$',
                    str(value)
                ):
                    return False, "URL không hợp lệ"
        except (ValueError, TypeError):
            return False, f"Không thể chuyển đổi sang {self.data_type.value}"
            
        # Kiểm tra độ dài
        if isinstance(value, (str, list, dict)):
            if (
                self.min_length is not None and
                len(value) < self.min_length
            ):
                return False, f"Độ dài tối thiểu: {self.min_length}"
            if (
                self.max_length is not None and
                len(value) > self.max_length
            ):
                return False, f"Độ dài tối đa: {self.max_length}"
                
        # Kiểm tra giá trị số
        if isinstance(value, (int, float)):
            if (
                self.min_value is not None and
                value < self.min_value
            ):
                return False, f"Giá trị tối thiểu: {self.min_value}"
            if (
                self.max_value is not None and
                value > self.max_value
            ):
                return False, f"Giá trị tối đa: {self.max_value}"
                
        # Kiểm tra pattern
        if self.pattern and isinstance(value, str):
            if not re.match(self.pattern, value):
                return False, f"Không khớp định dạng: {self.pattern}"
                
        # Kiểm tra giá trị cho phép
        if self.allowed_values is not None:
            if value not in self.allowed_values:
                return False, f"Giá trị không hợp lệ. Cho phép: {self.allowed_values}"
                
        # Kiểm tra custom
        if self.custom_validator:
            try:
                result = self.custom_validator(value)
                if not result:
                    return False, "Không pass custom validation"
            except Exception as e:
                return False, f"Lỗi custom validation: {str(e)}"
                
        return True, None

class DataValidator:
    """Class thực hiện validate dữ liệu."""
    
    def __init__(self, rules: Dict[str, ValidationRule]):
        """
        Khởi tạo validator với rules.
        
        Args:
            rules: Dict[str, ValidationRule]: Rules cho từng trường
        """
        self.rules = rules

    def validate(self, data: Dict[str, Any]) -> Dict[str, List[str]]:
        """
        Kiểm tra dữ liệu theo rules.
        
        Args:
            data: Dict[str, Any]: Dữ liệu cần kiểm tra
            
        Returns:
            Dict[str, List[str]]: Danh sách lỗi cho từng trường
        """
        errors = {}
        
        # Kiểm tra từng trường
        for field, rule in self.rules.items():
            field_errors = []
            value = data.get(field)
            
            # Validate theo rule
            is_valid, error = rule.validate(value)
            if not is_valid:
                field_errors.append(error)
                
            if field_errors:
                errors[field] = field_errors
                
        return errors

def main():
    """Chương trình chính."""
    # Định nghĩa rules
    rules = {
        'name': ValidationRule(
            data_type=DataType.STRING,
            required=True,
            min_length=2,
            max_length=50
        ),
        'age': ValidationRule(
            data_type=DataType.INTEGER,
            required=True,
            min_value=0,
            max_value=150
        ),
        'email': ValidationRule(
            data_type=DataType.EMAIL,
            required=True
        ),
        'phone': ValidationRule(
            data_type=DataType.PHONE,
            required=False
        ),
        'website': ValidationRule(
            data_type=DataType.URL,
            required=False
        ),
        'birth_date': ValidationRule(
            data_type=DataType.DATE,
            required=True
        ),
        'gender': ValidationRule(
            data_type=DataType.STRING,
            required=True,
            allowed_values=['male', 'female', 'other']
        )
    }
    
    # Tạo validator
    validator = DataValidator(rules)
    
    while True:
        print("\nData Validator")
        print("1. Kiểm tra dữ liệu")
        print("2. Xem rules")
        print("3. Thoát")
        
        choice = input("\nChọn chức năng (1-3): ")
        
        if choice == '3':
            break
        elif choice == '1':
            # Nhập dữ liệu
            print("\nNhập dữ liệu (Enter để bỏ qua):")
            data = {}
            
            for field in rules:
                value = input(f"{field}: ")
                if value:
                    data[field] = value
                    
            # Validate và hiển thị kết quả
            errors = validator.validate(data)
            
            if errors:
                print("\nLỗi:")
                for field, field_errors in errors.items():
                    print(f"{field}:")
                    for error in field_errors:
                        print(f"  - {error}")
            else:
                print("\nDữ liệu hợp lệ!")
                
        elif choice == '2':
            print("\nRules:")
            for field, rule in rules.items():
                print(f"\n{field}:")
                print(f"  Kiểu dữ liệu: {rule.data_type.value}")
                print(f"  Bắt buộc: {rule.required}")
                
                if rule.min_length is not None:
                    print(f"  Độ dài tối thiểu: {rule.min_length}")
                if rule.max_length is not None:
                    print(f"  Độ dài tối đa: {rule.max_length}")
                    
                if rule.min_value is not None:
                    print(f"  Giá trị tối thiểu: {rule.min_value}")
                if rule.max_value is not None:
                    print(f"  Giá trị tối đa: {rule.max_value}")
                    
                if rule.pattern is not None:
                    print(f"  Pattern: {rule.pattern}")
                    
                if rule.allowed_values is not None:
                    print(f"  Giá trị cho phép: {rule.allowed_values}")
                    
        else:
            print("Lựa chọn không hợp lệ!")

if __name__ == "__main__":
    main() 