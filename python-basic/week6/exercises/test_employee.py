"""
Unit tests cho employee management
Tiêu chí đánh giá:
1. Employee Classes (40%): Fulltime, Contract, Intern
2. Inheritance (20%): Kế thừa đúng
3. Polymorphism (20%): Đa hình đúng
4. Manager (20%): EmployeeManager hoạt động đúng
"""

import pytest
from datetime import date
from employee import (
    Employee,
    FulltimeEmployee,
    ContractEmployee,
    InternEmployee,
    EmployeeManager,
    EmployeeLevel,
    Department,
    Address
)

@pytest.fixture
def sample_address():
    """Fixture cho Address."""
    return Address(
        street="123 Main St",
        city="City",
        country="Country"
    )

@pytest.fixture
def sample_employees(sample_address):
    """Fixture cho employees."""
    fulltime = FulltimeEmployee(
        id=1,
        name="John Doe",
        birth_date=date(1990, 1, 1),
        address=sample_address,
        level=EmployeeLevel.SENIOR,
        department=Department.IT,
        base_salary=5000,
        bonus_rate=0.2
    )
    
    contract = ContractEmployee(
        id=2,
        name="Jane Smith",
        birth_date=date(1995, 5, 5),
        address=sample_address,
        level=EmployeeLevel.JUNIOR,
        department=Department.HR,
        hourly_rate=20,
        hours_worked=160
    )
    
    intern = InternEmployee(
        id=3,
        name="Bob Wilson",
        birth_date=date(2000, 10, 10),
        address=sample_address,
        department=Department.IT,
        stipend=1000,
        mentor_id=1
    )
    
    return {
        "fulltime": fulltime,
        "contract": contract,
        "intern": intern
    }

class TestEmployeeClasses:
    """
    Test employee classes (40%)
    Pass: Tính toán đúng ≥ 95%
    Fail: Tính toán đúng < 95%
    """
    
    def test_fulltime_employee(self, sample_employees):
        """Test FulltimeEmployee."""
        emp = sample_employees["fulltime"]
        
        # Properties
        assert emp.id == 1
        assert emp.name == "John Doe"
        assert emp.level == EmployeeLevel.SENIOR
        assert emp.department == Department.IT
        
        # Salary calculation
        assert emp.calculate_salary() == 6000  # 5000 * (1 + 0.2)
        
        # Age calculation
        age = emp.get_age()
        assert isinstance(age, int)
        assert age > 0
        
        # String representation
        assert "FulltimeEmployee" in str(emp)
        assert "John Doe" in str(emp)

    def test_contract_employee(self, sample_employees):
        """Test ContractEmployee."""
        emp = sample_employees["contract"]
        
        # Properties
        assert emp.id == 2
        assert emp.name == "Jane Smith"
        assert emp.level == EmployeeLevel.JUNIOR
        assert emp.department == Department.HR
        
        # Salary calculation
        assert emp.calculate_salary() == 3200  # 20 * 160
        
        # Age calculation
        age = emp.get_age()
        assert isinstance(age, int)
        assert age > 0
        
        # String representation
        assert "ContractEmployee" in str(emp)
        assert "Jane Smith" in str(emp)

    def test_intern_employee(self, sample_employees):
        """Test InternEmployee."""
        emp = sample_employees["intern"]
        
        # Properties
        assert emp.id == 3
        assert emp.name == "Bob Wilson"
        assert emp.level == EmployeeLevel.INTERN
        assert emp.department == Department.IT
        assert emp.mentor_id == 1
        
        # Salary calculation
        assert emp.calculate_salary() == 1000  # stipend
        
        # Age calculation
        age = emp.get_age()
        assert isinstance(age, int)
        assert age > 0
        
        # String representation
        assert "InternEmployee" in str(emp)
        assert "Bob Wilson" in str(emp)

class TestInheritance:
    """
    Test inheritance (20%)
    Pass: Kế thừa đúng ≥ 95%
    Fail: Kế thừa đúng < 95%
    """
    
    def test_inheritance_chain(self):
        """Test inheritance chain."""
        # All inherit from Employee
        assert issubclass(FulltimeEmployee, Employee)
        assert issubclass(ContractEmployee, Employee)
        assert issubclass(InternEmployee, Employee)

    def test_abstract_methods(self):
        """Test abstract methods."""
        # Cannot instantiate Employee
        with pytest.raises(TypeError):
            Employee(
                id=1,
                name="Test",
                birth_date=date.today(),
                address=Address("", "", ""),
                level=EmployeeLevel.JUNIOR,
                department=Department.IT
            )

class TestPolymorphism:
    """
    Test polymorphism (20%)
    Pass: Đa hình đúng ≥ 95%
    Fail: Đa hình đúng < 95%
    """
    
    def test_polymorphic_behavior(self, sample_employees):
        """Test polymorphic behavior."""
        employees = list(sample_employees.values())
        
        # All can calculate salary
        salaries = [emp.calculate_salary() for emp in employees]
        assert len(salaries) == 3
        assert all(isinstance(s, (int, float)) for s in salaries)
        
        # All can calculate age
        ages = [emp.get_age() for emp in employees]
        assert len(ages) == 3
        assert all(isinstance(a, int) for a in ages)
        
        # All have string representation
        strings = [str(emp) for emp in employees]
        assert len(strings) == 3
        assert all(isinstance(s, str) for s in strings)

class TestEmployeeManager:
    """
    Test employee manager (20%)
    Pass: Manager hoạt động đúng ≥ 95%
    Fail: Manager hoạt động đúng < 95%
    """
    
    def test_manager_operations(self, sample_employees):
        """Test manager operations."""
        manager = EmployeeManager()
        
        # Add employees
        for emp in sample_employees.values():
            assert manager.add_employee(emp) is True
        
        # Cannot add duplicate
        assert manager.add_employee(sample_employees["fulltime"]) is False
        
        # Get employee
        emp = manager.get_employee(1)
        assert emp is not None
        assert emp.name == "John Doe"
        
        # Get by department
        it_emps = manager.get_employees_by_department(Department.IT)
        assert len(it_emps) == 2
        
        hr_emps = manager.get_employees_by_department(Department.HR)
        assert len(hr_emps) == 1
        
        # Get by level
        senior_emps = manager.get_employees_by_level(EmployeeLevel.SENIOR)
        assert len(senior_emps) == 1
        
        intern_emps = manager.get_employees_by_level(EmployeeLevel.INTERN)
        assert len(intern_emps) == 1
        
        # Calculate payroll
        payroll = manager.calculate_payroll()
        assert len(payroll) == 3
        assert payroll[1] == 6000  # Fulltime
        assert payroll[2] == 3200  # Contract
        assert payroll[3] == 1000  # Intern
        
        # Get interns with mentors
        interns = manager.get_interns_with_mentors()
        assert len(interns) == 1
        intern, mentor = interns[0]
        assert isinstance(intern, InternEmployee)
        assert isinstance(mentor, FulltimeEmployee)
        
        # Remove employee
        assert manager.remove_employee(1) is True
        assert manager.get_employee(1) is None
        
        # Cannot remove non-existent
        assert manager.remove_employee(99) is False 