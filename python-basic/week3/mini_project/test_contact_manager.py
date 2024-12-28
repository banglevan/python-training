"""
Unit tests cho contact_manager.py
Tiêu chí đánh giá:
1. CRUD Operations (40%): Thêm/sửa/xóa/xem contact
2. Search & Filter (30%): Tìm kiếm và lọc contacts
3. Data Validation (20%): Kiểm tra dữ liệu
4. Import/Export (10%): Xử lý file CSV/JSON
"""

import pytest
from pathlib import Path
import json
import csv
from contact_manager import Contact, ContactManager

@pytest.fixture
def temp_dir(tmp_path):
    """Tạo thư mục tạm cho testing."""
    return tmp_path

@pytest.fixture
def valid_contact():
    """Tạo contact hợp lệ."""
    return Contact(
        name="John Doe",
        email="john@example.com",
        phone="+1234567890",
        address="123 Main St",
        birthday="1990-01-01",
        groups={"Friends", "Work"},
        notes="Test contact"
    )

@pytest.fixture
def contact_manager(temp_dir):
    """Tạo instance ContactManager với file tạm."""
    return ContactManager(str(temp_dir / "contacts.json"))

@pytest.fixture
def sample_contacts():
    """Tạo danh sách contacts mẫu."""
    return [
        Contact(
            name="John Doe",
            email="john@example.com",
            phone="+1234567890",
            address="123 Main St",
            birthday="1990-01-01",
            groups={"Friends", "Work"},
            notes="Note 1"
        ),
        Contact(
            name="Jane Smith",
            email="jane@example.com",
            phone="+0987654321",
            address="456 Oak St",
            birthday="1992-02-02",
            groups={"Family"},
            notes="Note 2"
        ),
        Contact(
            name="Bob Wilson",
            email="bob@example.com",
            phone="+1122334455",
            address="789 Pine St",
            birthday="1985-03-03",
            groups={"Work"},
            notes="Note 3"
        )
    ]

class TestContact:
    """Test class Contact."""
    
    def test_valid_contact(self, valid_contact):
        """Test contact hợp lệ."""
        valid_contact.validate()  # Không raise exception

    def test_invalid_contact(self):
        """Test với contact không hợp lệ."""
        invalid_cases = [
            # Tên trống
            Contact(
                name="",
                email="test@test.com",
                phone="1234567890",
                address="Test",
                birthday="1990-01-01",
                groups=set(),
                notes=""
            ),
            # Email không hợp lệ
            Contact(
                name="Test",
                email="invalid-email",
                phone="1234567890",
                address="Test",
                birthday="1990-01-01",
                groups=set(),
                notes=""
            ),
            # Số điện thoại không hợp lệ
            Contact(
                name="Test",
                email="test@test.com",
                phone="abc",
                address="Test",
                birthday="1990-01-01",
                groups=set(),
                notes=""
            ),
            # Ngày sinh sai định dạng
            Contact(
                name="Test",
                email="test@test.com",
                phone="1234567890",
                address="Test",
                birthday="1990/01/01",
                groups=set(),
                notes=""
            )
        ]
        
        for contact in invalid_cases:
            with pytest.raises(ValueError):
                contact.validate()

class TestContactManager:
    """Test class ContactManager."""
    
    class TestCRUD:
        """
        Test CRUD operations (40%)
        Pass: Thêm/sửa/xóa/xem đúng
        Fail: Có operation không đúng
        """
        
        def test_add_contact(self, contact_manager, valid_contact):
            """Test thêm contact."""
            assert contact_manager.add_contact(valid_contact) is True
            stored = contact_manager.get_contact(valid_contact.email)
            assert stored is not None
            assert stored.name == valid_contact.name
            assert stored.email == valid_contact.email

        def test_update_contact(self, contact_manager, valid_contact):
            """Test cập nhật contact."""
            contact_manager.add_contact(valid_contact)
            
            updated = Contact(
                name="Jane Doe",
                email=valid_contact.email,
                phone=valid_contact.phone,
                address=valid_contact.address,
                birthday=valid_contact.birthday,
                groups=valid_contact.groups,
                notes=valid_contact.notes
            )
            
            assert contact_manager.update_contact(
                valid_contact.email, updated
            ) is True
            stored = contact_manager.get_contact(valid_contact.email)
            assert stored.name == "Jane Doe"

        def test_delete_contact(self, contact_manager, valid_contact):
            """Test xóa contact."""
            contact_manager.add_contact(valid_contact)
            assert contact_manager.delete_contact(valid_contact.email) is True
            assert contact_manager.get_contact(valid_contact.email) is None

    class TestSearch:
        """
        Test tìm kiếm và lọc (30%)
        Pass: Tìm kiếm chính xác ≥ 95% cases
        Fail: Tìm kiếm chính xác < 95% cases
        """
        
        def test_search_by_name(self, contact_manager, sample_contacts):
            """Test tìm kiếm theo tên."""
            for contact in sample_contacts:
                contact_manager.add_contact(contact)
                
            results = contact_manager.search_contacts(name="John")
            assert len(results) == 1
            assert results[0].name == "John Doe"
            
            results = contact_manager.search_contacts(name="xyz")
            assert len(results) == 0

        def test_search_by_group(self, contact_manager, sample_contacts):
            """Test tìm kiếm theo nhóm."""
            for contact in sample_contacts:
                contact_manager.add_contact(contact)
                
            results = contact_manager.get_contacts_by_group("Work")
            assert len(results) == 2
            assert all("Work" in c.groups for c in results)
            
            results = contact_manager.get_contacts_by_group("Family")
            assert len(results) == 1
            assert results[0].name == "Jane Smith"

    class TestDataValidation:
        """
        Test kiểm tra dữ liệu (20%)
        Pass: Validate đúng ≥ 90% cases
        Fail: Validate đúng < 90% cases
        """
        
        def test_duplicate_email(self, contact_manager, valid_contact):
            """Test trùng email."""
            assert contact_manager.add_contact(valid_contact) is True
            assert contact_manager.add_contact(valid_contact) is False

        def test_invalid_updates(self, contact_manager, valid_contact):
            """Test cập nhật không hợp lệ."""
            contact_manager.add_contact(valid_contact)
            
            invalid = Contact(
                name="Test",
                email="invalid-email",
                phone=valid_contact.phone,
                address=valid_contact.address,
                birthday=valid_contact.birthday,
                groups=valid_contact.groups,
                notes=valid_contact.notes
            )
            
            assert contact_manager.update_contact(
                valid_contact.email, invalid
            ) is False

    class TestImportExport:
        """
        Test Import/Export (10%)
        Pass: Xử lý file đúng định dạng
        Fail: Có lỗi khi xử lý file
        """
        
        def test_csv_import_export(
            self, contact_manager, sample_contacts, temp_dir
        ):
            """Test import/export CSV."""
            # Tạo file CSV
            csv_file = temp_dir / "contacts.csv"
            with csv_file.open('w', newline='') as f:
                writer = csv.writer(f)
                writer.writerow([
                    "Name", "Email", "Phone", "Address",
                    "Birthday", "Groups", "Notes"
                ])
                for contact in sample_contacts:
                    writer.writerow([
                        contact.name,
                        contact.email,
                        contact.phone,
                        contact.address,
                        contact.birthday,
                        "|".join(contact.groups),
                        contact.notes
                    ])
            
            # Test import
            assert contact_manager.import_csv(str(csv_file)) is True
            assert len(contact_manager.contacts) == len(sample_contacts)
            
            # Test export
            export_file = temp_dir / "export.csv"
            assert contact_manager.export_csv(str(export_file)) is True
            assert export_file.exists()
            
            # Đọc file export
            with export_file.open() as f:
                reader = csv.reader(f)
                next(reader)  # Skip header
                exported = list(reader)
                assert len(exported) == len(sample_contacts)

        def test_json_persistence(
            self, contact_manager, sample_contacts
        ):
            """Test lưu và đọc JSON."""
            # Lưu contacts
            for contact in sample_contacts:
                contact_manager.add_contact(contact)
            contact_manager.save_data()
            
            # Tạo manager mới và đọc data
            new_manager = ContactManager(contact_manager.data_file)
            assert len(new_manager.contacts) == len(sample_contacts)
            for contact in sample_contacts:
                stored = new_manager.get_contact(contact.email)
                assert stored is not None
                assert stored.name == contact.name
                assert stored.email == contact.email 