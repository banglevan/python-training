"""
Bài tập 1: Quản lý danh sách sinh viên
Yêu cầu:
1. Tạo class Student với các thuộc tính: id, name, age, grades
2. CRUD operations cho danh sách sinh viên
3. Tính điểm trung bình, xếp loại
4. Lưu và đọc từ file
"""

from typing import List, Dict, Optional
from dataclasses import dataclass
import json
from pathlib import Path

@dataclass
class Student:
    """
    Class Student với các thuộc tính cơ bản.
    """
    id: str
    name: str
    age: int
    grades: Dict[str, float]  # subject: grade

    def calculate_gpa(self) -> float:
        """
        Tính điểm trung bình.
        
        Returns:
            float: Điểm trung bình
        
        Examples:
            >>> student = Student("001", "Alice", 20, {"Math": 8.5, "Physics": 7.5})
            >>> student.calculate_gpa()
            8.0
        """
        # TODO: Implement this function
        pass

    def get_grade_classification(self) -> str:
        """
        Xếp loại học lực dựa trên GPA.
        
        Returns:
            str: Xếp loại (Xuất sắc, Giỏi, Khá, Trung bình, Yếu)
        
        Examples:
            >>> student = Student("001", "Alice", 20, {"Math": 8.5, "Physics": 7.5})
            >>> student.get_grade_classification()
            'Giỏi'
        """
        # TODO: Implement this function
        pass

class StudentManager:
    def __init__(self, data_file: str = "students.json"):
        """
        Khởi tạo manager với file dữ liệu.
        
        Args:
            data_file (str): Đường dẫn file JSON
        """
        self.data_file = Path(data_file)
        self.students: List[Student] = self.load_data()

    def add_student(self, student: Student) -> bool:
        """
        Thêm sinh viên mới.
        
        Args:
            student (Student): Sinh viên cần thêm
            
        Returns:
            bool: True nếu thêm thành công, False nếu id đã tồn tại
        """
        # TODO: Implement this function
        pass

    def update_student(self, student_id: str, new_data: Dict) -> bool:
        """
        Cập nhật thông tin sinh viên.
        
        Args:
            student_id (str): ID sinh viên
            new_data (Dict): Thông tin cần cập nhật
            
        Returns:
            bool: True nếu cập nhật thành công, False nếu không tìm thấy
        """
        # TODO: Implement this function
        pass

    def delete_student(self, student_id: str) -> bool:
        """
        Xóa sinh viên.
        
        Args:
            student_id (str): ID sinh viên cần xóa
            
        Returns:
            bool: True nếu xóa thành công, False nếu không tìm thấy
        """
        # TODO: Implement this function
        pass

    def find_student(self, student_id: str) -> Optional[Student]:
        """
        Tìm sinh viên theo ID.
        
        Args:
            student_id (str): ID sinh viên cần tìm
            
        Returns:
            Optional[Student]: Student object nếu tìm thấy, None nếu không
        """
        # TODO: Implement this function
        pass

    def get_top_students(self, n: int = 5) -> List[Student]:
        """
        Lấy danh sách n sinh viên có GPA cao nhất.
        
        Args:
            n (int): Số lượng sinh viên cần lấy
            
        Returns:
            List[Student]: Danh sách sinh viên
        """
        # TODO: Implement this function
        pass

    def load_data(self) -> List[Student]:
        """
        Đọc dữ liệu từ file JSON.
        
        Returns:
            List[Student]: Danh sách sinh viên
        """
        # TODO: Implement this function
        pass

    def save_data(self) -> None:
        """Lưu dữ liệu vào file JSON."""
        # TODO: Implement this function
        pass

def main():
    """Chương trình chính."""
    manager = StudentManager()

    while True:
        print("\nQuản Lý Sinh Viên")
        print("1. Thêm sinh viên")
        print("2. Cập nhật sinh viên")
        print("3. Xóa sinh viên")
        print("4. Tìm sinh viên")
        print("5. Xem top sinh viên")
        print("6. Thoát")

        choice = input("\nChọn chức năng (1-6): ")

        if choice == '6':
            manager.save_data()
            break
        elif choice == '1':
            # TODO: Implement add student logic
            pass
        elif choice == '2':
            # TODO: Implement update student logic
            pass
        elif choice == '3':
            # TODO: Implement delete student logic
            pass
        elif choice == '4':
            # TODO: Implement find student logic
            pass
        elif choice == '5':
            # TODO: Implement view top students logic
            pass
        else:
            print("Lựa chọn không hợp lệ!")

if __name__ == "__main__":
    main() 