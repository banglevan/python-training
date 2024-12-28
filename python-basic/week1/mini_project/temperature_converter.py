"""
Mini Project: Temperature Converter
Yêu cầu:
1. Chuyển đổi giữa các đơn vị nhiệt độ (Celsius, Fahrenheit, Kelvin)
2. Lưu và hiển thị lịch sử chuyển đổi
3. Giao diện console thân thiện
4. Xử lý lỗi input
"""

from typing import Optional, List, Dict, Tuple
from datetime import datetime
import json
from pathlib import Path

class TemperatureConverter:
    def __init__(self, history_file: str = "conversion_history.json"):
        """
        Khởi tạo converter với file lưu lịch sử.

        Args:
            history_file (str): Đường dẫn file lưu lịch sử
        """
        self.history_file = Path(history_file)
        self.history: List[Dict] = self.load_history()

    def celsius_to_fahrenheit(self, celsius: float) -> float:
        """
        Chuyển đổi từ Celsius sang Fahrenheit.
        
        Args:
            celsius (float): Nhiệt độ Celsius

        Returns:
            float: Nhiệt độ Fahrenheit
        """
        # TODO: Implement this function
        pass

    def fahrenheit_to_celsius(self, fahrenheit: float) -> float:
        """
        Chuyển đổi từ Fahrenheit sang Celsius.
        
        Args:
            fahrenheit (float): Nhiệt độ Fahrenheit

        Returns:
            float: Nhiệt độ Celsius
        """
        # TODO: Implement this function
        pass

    def celsius_to_kelvin(self, celsius: float) -> float:
        """
        Chuyển đổi từ Celsius sang Kelvin.
        
        Args:
            celsius (float): Nhiệt độ Celsius

        Returns:
            float: Nhiệt độ Kelvin
        """
        # TODO: Implement this function
        pass

    def kelvin_to_celsius(self, kelvin: float) -> float:
        """
        Chuyển đổi từ Kelvin sang Celsius.
        
        Args:
            kelvin (float): Nhiệt độ Kelvin

        Returns:
            float: Nhiệt độ Celsius
        """
        # TODO: Implement this function
        pass

    def get_valid_temperature(self, unit: str) -> Optional[float]:
        """
        Nhận và validate input nhiệt độ từ người dùng.
        
        Args:
            unit (str): Đơn vị nhiệt độ (C/F/K)

        Returns:
            Optional[float]: Nhiệt độ hợp lệ hoặc None nếu input không hợp lệ
        """
        # TODO: Implement this function
        pass

    def save_conversion(self, from_value: float, from_unit: str, 
                       to_value: float, to_unit: str) -> None:
        """
        Lưu kết quả chuyển đổi vào lịch sử.
        
        Args:
            from_value (float): Giá trị gốc
            from_unit (str): Đơn vị gốc
            to_value (float): Giá trị đích
            to_unit (str): Đơn vị đích
        """
        # TODO: Implement this function
        pass

    def load_history(self) -> List[Dict]:
        """
        Đọc lịch sử chuyển đổi từ file.
        
        Returns:
            List[Dict]: Danh sách các chuyển đổi đã thực hiện
        """
        # TODO: Implement this function
        pass

    def display_history(self, limit: int = 5) -> None:
        """
        Hiển thị lịch sử chuyển đổi.
        
        Args:
            limit (int): Số lượng kết quả hiển thị
        """
        # TODO: Implement this function
        pass

    def run(self) -> None:
        """Chạy chương trình chính."""
        while True:
            print("\nTemperature Converter")
            print("1. Celsius → Fahrenheit")
            print("2. Fahrenheit → Celsius")
            print("3. Celsius → Kelvin")
            print("4. Kelvin → Celsius")
            print("5. Xem lịch sử chuyển đổi")
            print("6. Thoát")

            choice = input("\nChọn chức năng (1-6): ")

            if choice == '6':
                break
            elif choice == '5':
                self.display_history()
                continue

            # TODO: Implement the conversion logic based on user choice

if __name__ == "__main__":
    converter = TemperatureConverter()
    converter.run() 