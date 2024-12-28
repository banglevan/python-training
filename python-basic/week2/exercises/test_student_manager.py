"""
Unit tests cho student_manager.py
Tiêu chí đánh giá:
1. Functionality (40%): CRUD operations hoạt động đúng
2. Edge Cases (30%): Xử lý các trường hợp đặc biệt
3. Exception Handling (20%): Xử lý lỗi input và file I/O
4. Performance (10%): Tối ưu cho dữ liệu lớn
"""

import pytest
import json
import time
from pathlib import Path
from student_manager import Student, StudentManager

@pytest.fixture
def sample_students():
    """Tạo dữ liệu test mẫu."""
    return [
        Student("001", "Alice", 20, {"Math": 8.5, "Physics": 7.5}),
        Student("002", "Bob", 21, {"Math": 7.0, "Physics": 8.0}),
        Student("003", "Charlie", 19, {"Math": 9.0, "Physics": 9.0})
    ]

@pytest.fixture
def temp_file(tmp_path):
    """Tạo file tạm cho testing."""
    return tmp_path / "test_students.json"

class TestStudent:
    """Test class Student."""
    
    def test_calculate_gpa(self):
        """Test tính GPA."""
        student = Student("001", "Alice", 20, {"Math": 8.5, "Physics": 7.5})
        assert student.calculate_gpa() == pytest.approx(8.0)
        
        student = Student("002", "Bob", 21, {"Math": 9.0, "Physics": 9.0})
        assert student.calculate_gpa() == pytest.approx(9.0)

    def test_grade_classification(self):
        """Test xếp loại học lực."""
        # Test các ngưỡng điểm khác nhau
        classifications = [
            ({"Math": 9.0, "Physics": 9.0}, "Xuất sắc"),
            ({"Math": 8.0, "Physics": 8.0}, "Giỏi"),
            ({"Math": 7.0, "Physics": 7.0}, "Khá"),
            ({"Math": 6.0, "Physics": 6.0}, "Trung bình"),
            ({"Math": 4.0, "Physics": 4.0}, "Yếu")
        ]
        
        for grades, expected in classifications:
            student = Student("test", "Test", 20, grades)
            assert student.get_grade_classification() == expected

class TestStudentManager:
    """Test class StudentManager."""
    
    class TestFunctionality:
        """
        Test chức năng cơ bản (40%)
        Pass: Tất cả CRUD operations hoạt động đúng
        Fail: Có operation không hoạt động đúng
        """
        
        def test_add_student(self, temp_file, sample_students):
            """Test thêm sinh viên."""
            manager = StudentManager(temp_file)
            
            # Test thêm sinh viên mới
            assert manager.add_student(sample_students[0]) is True
            assert manager.find_student("001") is not None
            
            # Test thêm sinh viên đã tồn tại
            assert manager.add_student(sample_students[0]) is False

        def test_update_student(self, temp_file, sample_students):
            """Test cập nhật sinh viên."""
            manager = StudentManager(temp_file)
            manager.add_student(sample_students[0])
            
            # Test cập nhật thành công
            new_data = {"name": "Alice Updated", "age": 21}
            assert manager.update_student("001", new_data) is True
            updated = manager.find_student("001")
            assert updated.name == "Alice Updated"
            assert updated.age == 21
            
            # Test cập nhật sinh viên không tồn tại
            assert manager.update_student("999", new_data) is False

    class TestEdgeCases:
        """
        Test các trường hợp đặc biệt (30%)
        Pass: Xử lý đúng ≥ 80% edge cases
        Fail: Xử lý đúng < 80% edge cases
        """
        
        def test_empty_database(self, temp_file):
            """Test với database rỗng."""
            manager = StudentManager(temp_file)
            assert len(manager.students) == 0
            assert manager.get_top_students() == []
            assert manager.find_student("001") is None

        def test_duplicate_operations(self, temp_file, sample_students):
            """Test các operation trùng lặp."""
            manager = StudentManager(temp_file)
            
            # Thêm sinh viên
            assert manager.add_student(sample_students[0]) is True
            # Thêm lại sinh viên đó
            assert manager.add_student(sample_students[0]) is False
            # Xóa sinh viên
            assert manager.delete_student("001") is True
            # Xóa lại sinh viên đã xóa
            assert manager.delete_student("001") is False

        def test_special_characters(self, temp_file):
            """Test với dữ liệu đặc biệt."""
            student = Student("001", "Test!@#$%", 20, {"Math!": 8.5})
            manager = StudentManager(temp_file)
            assert manager.add_student(student) is True
            found = manager.find_student("001")
            assert found.name == "Test!@#$%"

    class TestExceptionHandling:
        """
        Test xử lý ngoại lệ (20%)
        Pass: Xử lý đúng tất cả exceptions
        Fail: Có exception không đư���c xử lý
        """
        
        def test_invalid_file_operations(self, tmp_path):
            """Test với file không hợp lệ."""
            # File trong thư mục không tồn tại
            invalid_path = tmp_path / "nonexistent" / "students.json"
            manager = StudentManager(invalid_path)
            assert len(manager.students) == 0
            
            # File không có quyền ghi
            readonly_file = tmp_path / "readonly.json"
            readonly_file.touch()
            readonly_file.chmod(0o444)  # Read-only
            manager = StudentManager(readonly_file)
            manager.add_student(Student("001", "Test", 20, {}))
            # Không raise exception khi save thất bại
            manager.save_data()

        def test_invalid_input(self, temp_file):
            """Test với input không hợp lệ."""
            manager = StudentManager(temp_file)
            
            # ID không hợp lệ
            with pytest.raises(ValueError):
                Student("", "Test", 20, {})
            
            # Tuổi không hợp lệ
            with pytest.raises(ValueError):
                Student("001", "Test", -1, {})
            
            # Điểm không hợp lệ
            with pytest.raises(ValueError):
                Student("001", "Test", 20, {"Math": -1})

    class TestPerformance:
        """
        Test hiệu năng (10%)
        Pass: Thời gian xử lý ≤ ngưỡng quy định
        Fail: Thời gian xử lý > ngưỡng quy định
        """
        
        def test_large_dataset(self, temp_file):
            """Test với dữ liệu lớn (1000 sinh viên, ngưỡng: 1s)."""
            manager = StudentManager(temp_file)
            
            # Tạo 1000 sinh viên
            start_time = time.time()
            for i in range(1000):
                student = Student(
                    f"{i:03d}",
                    f"Student {i}",
                    20,
                    {"Math": 8.0, "Physics": 8.0}
                )
                manager.add_student(student)
            end_time = time.time()
            
            assert end_time - start_time <= 1.0
            
            # Test tìm kiếm
            start_time = time.time()
            for i in range(1000):
                manager.find_student(f"{i:03d}")
            end_time = time.time()
            
            assert end_time - start_time <= 1.0

        def test_file_operations(self, temp_file, sample_students):
            """Test hiệu năng đọc/ghi file (ngưỡng: 0.1s)."""
            manager = StudentManager(temp_file)
            
            # Test ghi file
            start_time = time.time()
            for student in sample_students:
                manager.add_student(student)
            manager.save_data()
            end_time = time.time()
            
            assert end_time - start_time <= 0.1
            
            # Test đọc file
            start_time = time.time()
            manager = StudentManager(temp_file)
            end_time = time.time()
            
            assert end_time - start_time <= 0.1 