"""
Unit tests cho grade_manager.py
Tiêu chí đánh giá:
1. Functionality (40%): Các chức năng hoạt động đúng
2. Data Validation (30%): Kiểm tra dữ liệu chặt chẽ
3. Exception Handling (20%): Xử lý lỗi tốt
4. File I/O (10%): Đọc/ghi file ổn định
"""

import pytest
from pathlib import Path
import json
from grade_manager import Subject, Student, GradeManager

@pytest.fixture
def temp_file(tmp_path):
    """Tạo file tạm cho testing."""
    return tmp_path / "test_grades.json"

@pytest.fixture
def sample_subjects():
    """Tạo dữ liệu môn học mẫu."""
    return {
        "Math": Subject("Math", 3, 8.5),
        "Physics": Subject("Physics", 4, 7.5),
        "Chemistry": Subject("Chemistry", 3, 9.0)
    }

@pytest.fixture
def sample_students(sample_subjects):
    """Tạo dữ liệu sinh viên mẫu."""
    return {
        "001": Student("001", "Alice", {
            "Math": sample_subjects["Math"],
            "Physics": sample_subjects["Physics"]
        }),
        "002": Student("002", "Bob", {
            "Math": sample_subjects["Math"],
            "Chemistry": sample_subjects["Chemistry"]
        })
    }

class TestSubject:
    """Test class Subject."""
    
    def test_valid_subject(self):
        """Test khởi tạo môn học hợp lệ."""
        subject = Subject("Math", 3, 8.5)
        subject.validate()  # Không raise exception

    def test_invalid_subject(self):
        """Test với dữ liệu không hợp lệ."""
        # Tên không hợp lệ
        with pytest.raises(ValueError):
            Subject("", 3, 8.5).validate()
        
        # Số tín chỉ không hợp lệ
        with pytest.raises(ValueError):
            Subject("Math", 0, 8.5).validate()
        with pytest.raises(ValueError):
            Subject("Math", -1, 8.5).validate()
        
        # Điểm không hợp lệ
        with pytest.raises(ValueError):
            Subject("Math", 3, -1).validate()
        with pytest.raises(ValueError):
            Subject("Math", 3, 11).validate()

class TestStudent:
    """Test class Student."""
    
    def test_valid_student(self, sample_subjects):
        """Test khởi tạo sinh viên hợp lệ."""
        student = Student("001", "Alice", {
            "Math": sample_subjects["Math"]
        })
        student.validate()  # Không raise exception

    def test_invalid_student(self, sample_subjects):
        """Test với dữ liệu không hợp lệ."""
        # ID không hợp lệ
        with pytest.raises(ValueError):
            Student("", "Alice", {}).validate()
        
        # Tên không hợp lệ
        with pytest.raises(ValueError):
            Student("001", "", {}).validate()

    def test_gpa_calculation(self, sample_subjects):
        """Test tính GPA."""
        student = Student("001", "Alice", {
            "Math": Subject("Math", 3, 8.0),
            "Physics": Subject("Physics", 2, 7.0)
        })
        # GPA = (8.0 * 3 + 7.0 * 2) / (3 + 2) = 7.6
        assert student.calculate_gpa() == 7.6

    def test_grade_classification(self, sample_subjects):
        """Test xếp loại học lực."""
        classifications = [
            (9.0, "Xuất sắc"),
            (8.0, "Giỏi"),
            (7.0, "Khá"),
            (5.0, "Trung bình"),
            (4.0, "Yếu")
        ]
        
        for grade, expected in classifications:
            student = Student("test", "Test", {
                "Math": Subject("Math", 3, grade)
            })
            assert student.get_grade_classification() == expected

class TestGradeManager:
    """Test class GradeManager."""
    
    class TestFunctionality:
        """
        Test chức năng cơ bản (40%)
        Pass: Tất cả operations hoạt động đúng
        Fail: Có operation không hoạt động đúng
        """
        
        def test_add_student(self, temp_file, sample_students):
            """Test thêm sinh viên."""
            manager = GradeManager(temp_file)
            
            # Thêm sinh viên mới
            student = sample_students["001"]
            assert manager.add_student(student) is True
            assert "001" in manager.students
            
            # Thêm sinh viên đã tồn tại
            assert manager.add_student(student) is False

        def test_add_grade(self, temp_file, sample_students, sample_subjects):
            """Test thêm/cập nhật điểm."""
            manager = GradeManager(temp_file)
            manager.add_student(sample_students["001"])
            
            # Thêm điểm mới
            assert manager.add_grade("001", sample_subjects["Chemistry"]) is True
            
            # Cập nhật điểm
            new_math = Subject("Math", 3, 9.0)
            assert manager.add_grade("001", new_math) is True
            assert manager.students["001"].subjects["Math"].grade == 9.0

        def test_get_student_report(self, temp_file, sample_students):
            """Test báo cáo sinh viên."""
            manager = GradeManager(temp_file)
            manager.add_student(sample_students["001"])
            
            report = manager.get_student_report("001")
            assert report is not None
            assert report["id"] == "001"
            assert "gpa" in report
            assert "classification" in report

        def test_get_class_statistics(self, temp_file, sample_students):
            """Test thống kê lớp."""
            manager = GradeManager(temp_file)
            for student in sample_students.values():
                manager.add_student(student)
            
            stats = manager.get_class_statistics()
            assert stats["total_students"] == len(sample_students)
            assert "average_gpa" in stats
            assert "classifications" in stats
            assert "subject_statistics" in stats

    class TestDataValidation:
        """
        Test kiểm tra dữ liệu (30%)
        Pass: Validate đúng ≥ 90% cases
        Fail: Validate đúng < 90% cases
        """
        
        def test_student_validation(self, temp_file):
            """Test validate dữ liệu sinh viên."""
            manager = GradeManager(temp_file)
            
            # Test với dữ liệu không hợp lệ
            invalid_cases = [
                Student("", "Test", {}),  # ID trống
                Student("001", "", {}),   # Tên trống
                Student("001", "Test", {  # Điểm không hợp lệ
                    "Math": Subject("Math", 3, -1)
                })
            ]
            
            for student in invalid_cases:
                assert manager.add_student(student) is False

        def test_grade_validation(self, temp_file, sample_students):
            """Test validate điểm số."""
            manager = GradeManager(temp_file)
            manager.add_student(sample_students["001"])
            
            # Test với điểm không hợp lệ
            invalid_grades = [
                Subject("Math", 3, -1),    # Điểm âm
                Subject("Math", 3, 11),    # Điểm > 10
                Subject("Math", -1, 8.5),  # Tín chỉ âm
                Subject("", 3, 8.5)        # Tên môn trống
            ]
            
            for subject in invalid_grades:
                assert manager.add_grade("001", subject) is False

    class TestExceptionHandling:
        """
        Test xử lý ngoại lệ (20%)
        Pass: Xử lý đúng tất cả exceptions
        Fail: Có exception không được xử lý
        """
        
        def test_file_operations(self, tmp_path):
            """Test xử lý lỗi file."""
            # File trong thư mục không tồn tại
            invalid_path = tmp_path / "nonexistent" / "grades.json"
            manager = GradeManager(invalid_path)
            assert len(manager.students) == 0
            
            # File không có quyền ghi
            readonly_file = tmp_path / "readonly.json"
            readonly_file.touch()
            readonly_file.chmod(0o444)  # Read-only
            manager = GradeManager(readonly_file)
            manager.add_student(Student("001", "Test", {}))
            manager.save_data()  # Không raise exception

        def test_corrupted_data(self, temp_file):
            """Test với file dữ liệu hỏng."""
            # Tạo file JSON không hợp lệ
            temp_file.write_text("invalid json")
            manager = GradeManager(temp_file)
            assert len(manager.students) == 0

    class TestFileIO:
        """
        Test đọc/ghi file (10%)
        Pass: Dữ liệu được lưu và đọc chính xác
        Fail: Dữ liệu bị mất hoặc sai lệch
        """
        
        def test_data_persistence(self, temp_file, sample_students):
            """Test lưu và đọc dữ liệu."""
            # Lưu dữ liệu
            manager1 = GradeManager(temp_file)
            for student in sample_students.values():
                manager1.add_student(student)
            manager1.save_data()
            
            # Đọc và kiểm tra dữ liệu
            manager2 = GradeManager(temp_file)
            assert len(manager2.students) == len(sample_students)
            for student_id, student in sample_students.items():
                assert student_id in manager2.students
                assert manager2.students[student_id].name == student.name
                assert len(manager2.students[student_id].subjects) == len(student.subjects)

        def test_file_format(self, temp_file, sample_students):
            """Test định dạng file JSON."""
            manager = GradeManager(temp_file)
            manager.add_student(sample_students["001"])
            manager.save_data()
            
            # Kiểm tra cấu trúc JSON
            data = json.loads(temp_file.read_text())
            assert isinstance(data, dict)
            assert "001" in data
            assert "name" in data["001"]
            assert "subjects" in data["001"]
            assert isinstance(data["001"]["subjects"], dict) 