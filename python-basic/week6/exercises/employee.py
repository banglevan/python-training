"""
Employee management exercise
Minh họa inheritance và polymorphism với quản lý nhân viên
"""

from abc import ABC, abstractmethod
from dataclasses import dataclass
from datetime import date
from enum import Enum
from typing import List, Dict, Optional

class EmployeeLevel(Enum):
    """Level của nhân viên."""
    INTERN = "intern"
    JUNIOR = "junior"
    SENIOR = "senior"
    LEAD = "lead"
    MANAGER = "manager"

class Department(Enum):
    """Phòng ban."""
    IT = "it"
    HR = "hr"
    FINANCE = "finance"
    SALES = "sales"
    MARKETING = "marketing"

@dataclass
class Address:
    """Địa chỉ."""
    street: str
    city: str
    country: str
    postal_code: Optional[str] = None

class Employee(ABC):
    """Abstract base class cho nhân viên."""
    
    def __init__(
        self,
        id: int,
        name: str,
        birth_date: date,
        address: Address,
        level: EmployeeLevel,
        department: Department
    ):
        """
        Khởi tạo nhân viên.
        
        Args:
            id: ID nh��n viên
            name: Tên
            birth_date: Ngày sinh
            address: Địa chỉ
            level: Level
            department: Phòng ban
        """
        self.id = id
        self.name = name
        self.birth_date = birth_date
        self.address = address
        self.level = level
        self.department = department
    
    @abstractmethod
    def calculate_salary(self) -> float:
        """Tính lương."""
        pass
    
    def get_age(self) -> int:
        """Tính tuổi."""
        today = date.today()
        return (
            today.year - self.birth_date.year -
            ((today.month, today.day) <
             (self.birth_date.month, self.birth_date.day))
        )
    
    def __str__(self) -> str:
        """String representation."""
        return (
            f"{self.__class__.__name__}("
            f"id={self.id}, "
            f"name={self.name}, "
            f"level={self.level.value}, "
            f"department={self.department.value})"
        )

class FulltimeEmployee(Employee):
    """Nhân viên fulltime."""
    
    def __init__(
        self,
        id: int,
        name: str,
        birth_date: date,
        address: Address,
        level: EmployeeLevel,
        department: Department,
        base_salary: float,
        bonus_rate: float = 0
    ):
        """
        Khởi tạo nhân viên fulltime.
        
        Args:
            base_salary: Lương cơ bản
            bonus_rate: Tỷ lệ thưởng (0-1)
        """
        super().__init__(
            id, name, birth_date,
            address, level, department
        )
        self.base_salary = base_salary
        self.bonus_rate = bonus_rate
    
    def calculate_salary(self) -> float:
        """Tính lương = lương cơ bản + thưởng."""
        return self.base_salary * (1 + self.bonus_rate)

class ContractEmployee(Employee):
    """Nhân viên theo hợp đồng."""
    
    def __init__(
        self,
        id: int,
        name: str,
        birth_date: date,
        address: Address,
        level: EmployeeLevel,
        department: Department,
        hourly_rate: float,
        hours_worked: float
    ):
        """
        Khởi tạo nhân viên hợp đồng.
        
        Args:
            hourly_rate: Lương theo giờ
            hours_worked: Số giờ làm việc
        """
        super().__init__(
            id, name, birth_date,
            address, level, department
        )
        self.hourly_rate = hourly_rate
        self.hours_worked = hours_worked
    
    def calculate_salary(self) -> float:
        """Tính lương = số giờ * lương giờ."""
        return self.hourly_rate * self.hours_worked

class InternEmployee(Employee):
    """Nhân viên thực tập."""
    
    def __init__(
        self,
        id: int,
        name: str,
        birth_date: date,
        address: Address,
        department: Department,
        stipend: float,
        mentor_id: int
    ):
        """
        Khởi tạo nhân viên thực tập.
        
        Args:
            stipend: Trợ cấp
            mentor_id: ID mentor
        """
        super().__init__(
            id, name, birth_date,
            address, EmployeeLevel.INTERN, department
        )
        self.stipend = stipend
        self.mentor_id = mentor_id
    
    def calculate_salary(self) -> float:
        """Trả về trợ cấp."""
        return self.stipend

class EmployeeManager:
    """Quản lý nhân viên."""
    
    def __init__(self):
        """Khởi tạo manager."""
        self.employees: Dict[int, Employee] = {}
    
    def add_employee(self, employee: Employee) -> bool:
        """Thêm nhân viên."""
        if employee.id in self.employees:
            return False
        self.employees[employee.id] = employee
        return True
    
    def remove_employee(self, id: int) -> bool:
        """Xóa nhân viên."""
        if id not in self.employees:
            return False
        del self.employees[id]
        return True
    
    def get_employee(self, id: int) -> Optional[Employee]:
        """Lấy thông tin nhân viên."""
        return self.employees.get(id)
    
    def get_employees_by_department(
        self,
        department: Department
    ) -> List[Employee]:
        """Lấy nhân viên theo phòng ban."""
        return [
            emp for emp in self.employees.values()
            if emp.department == department
        ]
    
    def get_employees_by_level(
        self,
        level: EmployeeLevel
    ) -> List[Employee]:
        """Lấy nhân viên theo level."""
        return [
            emp for emp in self.employees.values()
            if emp.level == level
        ]
    
    def calculate_payroll(self) -> Dict[int, float]:
        """Tính lương cho tất cả nhân viên."""
        return {
            id: emp.calculate_salary()
            for id, emp in self.employees.items()
        }
    
    def get_interns_with_mentors(self) -> List[tuple]:
        """Lấy danh sách intern và mentor."""
        interns = []
        for emp in self.employees.values():
            if isinstance(emp, InternEmployee):
                mentor = self.employees.get(emp.mentor_id)
                interns.append((emp, mentor))
        return interns
    
    def __str__(self) -> str:
        """String representation."""
        return "\n".join(
            str(emp) for emp in self.employees.values()
        ) 