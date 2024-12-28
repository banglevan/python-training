"""
Mini Project: Grade Management
Yêu cầu:
1. Quản lý điểm nhiều môn học
2. Tính điểm trung bình (có trọng số)
3. Xếp loại học lực
4. Lưu và đọc từ file
"""

from typing import Dict, List, Optional, Tuple
from dataclasses import dataclass
import json
from pathlib import Path
from datetime import datetime

@dataclass
class Subject:
    """Class đại diện cho một môn học."""
    name: str
    credits: int
    grade: float
    
    def validate(self) -> None:
        """
        Kiểm tra tính hợp lệ của dữ liệu môn học.
        
        Raises:
            ValueError: Nếu dữ liệu không hợp lệ
        """
        if not self.name or not isinstance(self.name, str):
            raise ValueError("Tên môn học không hợp lệ")
        if not isinstance(self.credits, int) or self.credits <= 0:
            raise ValueError("Số tín chỉ phải là số nguyên dương")
        if not isinstance(self.grade, (int, float)) or not 0 <= self.grade <= 10:
            raise ValueError("Điểm phải từ 0 đến 10")

@dataclass
class Student:
    """Class đại diện cho một sinh viên."""
    id: str
    name: str
    subjects: Dict[str, Subject]  # key: subject name
    
    def validate(self) -> None:
        """
        Kiểm tra tính hợp lệ của dữ liệu sinh viên.
        
        Raises:
            ValueError: Nếu dữ liệu không hợp lệ
        """
        if not self.id or not isinstance(self.id, str):
            raise ValueError("Mã sinh viên không hợp lệ")
        if not self.name or not isinstance(self.name, str):
            raise ValueError("Tên sinh viên không hợp lệ")
        for subject in self.subjects.values():
            subject.validate()

    def calculate_gpa(self) -> float:
        """
        Tính điểm trung bình có trọng số.
        
        Returns:
            float: Điểm trung bình
        """
        if not self.subjects:
            return 0.0
            
        total_weighted_grade = 0
        total_credits = 0
        
        for subject in self.subjects.values():
            total_weighted_grade += subject.grade * subject.credits
            total_credits += subject.credits
            
        return round(total_weighted_grade / total_credits, 2)

    def get_grade_classification(self) -> str:
        """
        Xếp loại học lực dựa trên GPA.
        
        Returns:
            str: Xếp loại (Xuất sắc, Giỏi, Khá, Trung bình, Yếu)
        """
        gpa = self.calculate_gpa()
        
        if gpa >= 9.0:
            return "Xuất sắc"
        elif gpa >= 8.0:
            return "Giỏi"
        elif gpa >= 7.0:
            return "Khá"
        elif gpa >= 5.0:
            return "Trung bình"
        else:
            return "Yếu"

class GradeManager:
    """Class quản lý điểm sinh viên."""
    
    def __init__(self, data_file: str = "grades.json"):
        """
        Khởi tạo manager với file dữ liệu.
        
        Args:
            data_file (str): Đường dẫn file JSON
        """
        self.data_file = Path(data_file)
        self.students: Dict[str, Student] = {}  # key: student id
        self.load_data()

    def add_student(self, student: Student) -> bool:
        """
        Thêm sinh viên mới.
        
        Args:
            student (Student): Sinh viên cần thêm
            
        Returns:
            bool: True nếu thêm thành công, False nếu id đã tồn tại
        """
        try:
            student.validate()
            if student.id in self.students:
                return False
            self.students[student.id] = student
            self.save_data()
            return True
        except ValueError as e:
            print(f"Lỗi: {e}")
            return False

    def add_grade(self, student_id: str, subject: Subject) -> bool:
        """
        Thêm/cập nhật điểm môn học.
        
        Args:
            student_id (str): Mã sinh viên
            subject (Subject): Môn học và điểm
            
        Returns:
            bool: True nếu thành công, False nếu thất bại
        """
        try:
            subject.validate()
            if student_id not in self.students:
                return False
            self.students[student_id].subjects[subject.name] = subject
            self.save_data()
            return True
        except ValueError as e:
            print(f"Lỗi: {e}")
            return False

    def get_student_report(self, student_id: str) -> Optional[Dict]:
        """
        Lấy báo cáo kết quả học tập của sinh viên.
        
        Args:
            student_id (str): Mã sinh viên
            
        Returns:
            Optional[Dict]: Báo cáo hoặc None nếu không tìm thấy
        """
        student = self.students.get(student_id)
        if not student:
            return None
            
        return {
            "id": student.id,
            "name": student.name,
            "subjects": {
                name: {
                    "credits": subject.credits,
                    "grade": subject.grade
                }
                for name, subject in student.subjects.items()
            },
            "gpa": student.calculate_gpa(),
            "classification": student.get_grade_classification()
        }

    def get_class_statistics(self) -> Dict:
        """
        Tính toán thống kê lớp học.
        
        Returns:
            Dict: Các chỉ số thống kê
        """
        if not self.students:
            return {
                "total_students": 0,
                "average_gpa": 0,
                "classifications": {},
                "subject_statistics": {}
            }
            
        # Tính GPA trung bình
        gpas = [student.calculate_gpa() for student in self.students.values()]
        avg_gpa = sum(gpas) / len(gpas)
        
        # Thống kê xếp loại
        classifications = {}
        for student in self.students.values():
            grade = student.get_grade_classification()
            classifications[grade] = classifications.get(grade, 0) + 1
            
        # Thống kê theo môn học
        subject_stats = {}
        for student in self.students.values():
            for subject in student.subjects.values():
                if subject.name not in subject_stats:
                    subject_stats[subject.name] = {
                        "total": 0,
                        "count": 0
                    }
                subject_stats[subject.name]["total"] += subject.grade
                subject_stats[subject.name]["count"] += 1
                
        # Tính điểm trung bình môn học
        for stats in subject_stats.values():
            stats["average"] = round(stats["total"] / stats["count"], 2)
            del stats["total"]
            
        return {
            "total_students": len(self.students),
            "average_gpa": round(avg_gpa, 2),
            "classifications": classifications,
            "subject_statistics": subject_stats
        }

    def load_data(self) -> None:
        """Đọc dữ liệu từ file JSON."""
        if not self.data_file.exists():
            return
            
        try:
            data = json.loads(self.data_file.read_text())
            self.students = {
                student_id: Student(
                    id=student_id,
                    name=student_data["name"],
                    subjects={
                        subject_name: Subject(
                            name=subject_name,
                            credits=subject_data["credits"],
                            grade=subject_data["grade"]
                        )
                        for subject_name, subject_data
                        in student_data["subjects"].items()
                    }
                )
                for student_id, student_data in data.items()
            }
        except (json.JSONDecodeError, KeyError) as e:
            print(f"Lỗi đọc file: {e}")
            self.students = {}

    def save_data(self) -> None:
        """Lưu dữ liệu vào file JSON."""
        try:
            data = {
                student.id: {
                    "name": student.name,
                    "subjects": {
                        subject.name: {
                            "credits": subject.credits,
                            "grade": subject.grade
                        }
                        for subject in student.subjects.values()
                    }
                }
                for student in self.students.values()
            }
            self.data_file.write_text(json.dumps(data, indent=4))
        except Exception as e:
            print(f"Lỗi lưu file: {e}")

def main():
    """Chương trình chính."""
    manager = GradeManager()

    while True:
        print("\nQuản Lý Điểm Sinh Viên")
        print("1. Thêm sinh viên mới")
        print("2. Nhập điểm môn học")
        print("3. Xem báo cáo sinh viên")
        print("4. Xem thống kê lớp")
        print("5. Thoát")

        choice = input("\nChọn chức năng (1-5): ")

        if choice == '5':
            break
        elif choice == '1':
            student_id = input("Nhập mã sinh viên: ")
            name = input("Nhập tên sinh viên: ")
            student = Student(student_id, name, {})
            if manager.add_student(student):
                print("Thêm sinh viên thành công!")
            else:
                print("Thêm sinh viên thất bại!")
        elif choice == '2':
            student_id = input("Nhập mã sinh viên: ")
            subject_name = input("Nhập tên môn học: ")
            try:
                credits = int(input("Nhập số tín chỉ: "))
                grade = float(input("Nhập điểm: "))
                subject = Subject(subject_name, credits, grade)
                if manager.add_grade(student_id, subject):
                    print("Nhập điểm thành công!")
                else:
                    print("Nhập điểm thất bại!")
            except ValueError:
                print("Dữ liệu không hợp lệ!")
        elif choice == '3':
            student_id = input("Nhập mã sinh viên: ")
            report = manager.get_student_report(student_id)
            if report:
                print("\nBáo Cáo Kết Quả Học Tập")
                print(f"Mã số: {report['id']}")
                print(f"Họ tên: {report['name']}")
                print("\nBảng điểm:")
                for subject, data in report['subjects'].items():
                    print(f"- {subject}: {data['grade']} ({data['credits']} tín chỉ)")
                print(f"\nĐiểm trung bình: {report['gpa']}")
                print(f"Xếp loại: {report['classification']}")
            else:
                print("Không tìm thấy sinh viên!")
        elif choice == '4':
            stats = manager.get_class_statistics()
            print("\nThống Kê Lớp Học")
            print(f"Tổng số sinh viên: {stats['total_students']}")
            print(f"GPA trung bình: {stats['average_gpa']}")
            print("\nPhân bố xếp loại:")
            for grade, count in stats['classifications'].items():
                print(f"- {grade}: {count} sinh viên")
            print("\nĐiểm trung bình theo môn:")
            for subject, data in stats['subject_statistics'].items():
                print(f"- {subject}: {data['average']} ({data['count']} sinh viên)")
        else:
            print("Lựa chọn không hợp lệ!")

if __name__ == "__main__":
    main() 