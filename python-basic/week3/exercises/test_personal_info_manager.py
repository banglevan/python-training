"""
Unit tests cho personal_info_manager.py
Tiêu chí đánh giá:
1. Data Management (40%): CRUD operations hoạt động đúng
2. Security (30%): Mã hóa và phân quyền chính xác
3. Data Validation (20%): Kiểm tra dữ liệu chặt chẽ
4. File I/O (10%): Import/Export ổn định
"""

import pytest
from pathlib import Path
import json
from cryptography.fernet import Fernet
from personal_info_manager import (
    PersonalInfo,
    SensitiveInfo,
    InfoManager
)

@pytest.fixture
def temp_dir(tmp_path):
    """Tạo thư mục tạm cho testing."""
    return tmp_path

@pytest.fixture
def valid_personal_info():
    """Tạo thông tin cá nhân hợp lệ."""
    return PersonalInfo(
        full_name="John Doe",
        dob="1990-01-01",
        email="john@example.com",
        phone="+1234567890",
        address="123 Main St"
    )

@pytest.fixture
def valid_sensitive_info():
    """Tạo thông tin nhạy cảm hợp lệ."""
    return SensitiveInfo(
        ssn="123456789",
        passport="AB123456",
        bank_accounts=["1234567890", "0987654321"]
    )

@pytest.fixture
def info_manager(temp_dir):
    """Tạo instance InfoManager với file tạm."""
    return InfoManager(
        data_file=str(temp_dir / "info.json"),
        key_file=str(temp_dir / "key.key")
    )

class TestPersonalInfo:
    """Test class PersonalInfo."""
    
    def test_valid_info(self, valid_personal_info):
        """Test thông tin hợp lệ."""
        valid_personal_info.validate()  # Không raise exception

    def test_invalid_info(self):
        """Test với thông tin không hợp lệ."""
        invalid_cases = [
            # Tên trống
            PersonalInfo("", "1990-01-01", "test@test.com", "1234567890", "Test"),
            # Email không hợp lệ
            PersonalInfo("Test", "1990-01-01", "invalid-email", "1234567890", "Test"),
            # Số điện thoại không hợp lệ
            PersonalInfo("Test", "1990-01-01", "test@test.com", "abc", "Test"),
            # Ngày sinh sai định dạng
            PersonalInfo("Test", "1990/01/01", "test@test.com", "1234567890", "Test")
        ]
        
        for info in invalid_cases:
            with pytest.raises(ValueError):
                info.validate()

class TestSensitiveInfo:
    """Test class SensitiveInfo."""
    
    def test_valid_info(self, valid_sensitive_info):
        """Test thông tin hợp lệ."""
        valid_sensitive_info.validate()  # Không raise exception

    def test_invalid_info(self):
        """Test với thông tin không hợp lệ."""
        invalid_cases = [
            # SSN không hợp lệ
            SensitiveInfo("12345", "AB123456", ["1234567890"]),
            # Số hộ chiếu không hợp lệ
            SensitiveInfo("123456789", "123", ["1234567890"]),
            # Không có tài khoản ngân hàng
            SensitiveInfo("123456789", "AB123456", []),
            # Số tài khoản không hợp lệ
            SensitiveInfo("123456789", "AB123456", ["123"])
        ]
        
        for info in invalid_cases:
            with pytest.raises(ValueError):
                info.validate()

class TestInfoManager:
    """Test class InfoManager."""
    
    class TestDataManagement:
        """
        Test quản lý dữ liệu (40%)
        Pass: CRUD operations hoạt động đúng
        Fail: Có operation không hoạt động đúng
        """
        
        def test_personal_info_crud(self, info_manager, valid_personal_info):
            """Test CRUD thông tin cá nhân."""
            # Create
            assert info_manager.set_personal_info(valid_personal_info) is True
            
            # Read
            stored_info = info_manager.get_personal_info()
            assert stored_info is not None
            assert stored_info.full_name == valid_personal_info.full_name
            assert stored_info.email == valid_personal_info.email
            
            # Update
            updated_info = PersonalInfo(
                full_name="Jane Doe",
                dob=valid_personal_info.dob,
                email=valid_personal_info.email,
                phone=valid_personal_info.phone,
                address=valid_personal_info.address
            )
            assert info_manager.set_personal_info(updated_info) is True
            assert info_manager.get_personal_info().full_name == "Jane Doe"

        def test_sensitive_info_crud(
            self, info_manager, valid_sensitive_info
        ):
            """Test CRUD thông tin nhạy cảm."""
            password = "password"  # Default test password
            
            # Create
            assert info_manager.set_sensitive_info(
                valid_sensitive_info, password
            ) is True
            
            # Read
            stored_info = info_manager.get_sensitive_info(password)
            assert stored_info is not None
            assert stored_info.ssn == valid_sensitive_info.ssn
            assert stored_info.passport == valid_sensitive_info.passport
            
            # Wrong password
            assert info_manager.get_sensitive_info("wrong") is None

    class TestSecurity:
        """
        Test bảo mật (30%)
        Pass: Mã hóa và phân quyền chính xác
        Fail: Có lỗ hổng bảo mật
        """
        
        def test_encryption(self, info_manager, valid_sensitive_info):
            """Test mã hóa dữ liệu."""
            password = "password"
            
            # Lưu thông tin nhạy cảm
            info_manager.set_sensitive_info(valid_sensitive_info, password)
            
            # Kiểm tra file dữ liệu
            data = json.loads(Path(info_manager.data_file).read_text())
            assert "sensitive" in data
            encrypted = data["sensitive"]
            
            # Thông tin được mã hóa
            assert encrypted != json.dumps(valid_sensitive_info.__dict__)
            
            # Có thể giải mã với key đúng
            fernet = info_manager._load_or_create_key()
            decrypted = json.loads(fernet.decrypt(encrypted.encode()).decode())
            assert decrypted["ssn"] == valid_sensitive_info.ssn

        def test_access_control(self, info_manager, valid_sensitive_info):
            """Test phân quyền truy cập."""
            password = "password"
            wrong_password = "wrong"
            
            info_manager.set_sensitive_info(valid_sensitive_info, password)
            
            # Truy cập với mật khẩu đúng
            assert info_manager.get_sensitive_info(password) is not None
            
            # Truy cập với mật khẩu sai
            assert info_manager.get_sensitive_info(wrong_password) is None
            
            # Export với mật khẩu sai
            assert info_manager.export_data(
                "test.json", True, wrong_password
            ) is False

    class TestDataValidation:
        """
        Test kiểm tra dữ liệu (20%)
        Pass: Validate đúng ≥ 90% cases
        Fail: Validate đúng < 90% cases
        """
        
        def test_personal_info_validation(self, info_manager):
            """Test validate thông tin cá nhân."""
            invalid_cases = [
                # Email không hợp lệ
                PersonalInfo(
                    "Test", "1990-01-01", "invalid",
                    "1234567890", "Test"
                ),
                # Số điện thoại không hợp lệ
                PersonalInfo(
                    "Test", "1990-01-01", "test@test.com",
                    "abc", "Test"
                )
            ]
            
            for info in invalid_cases:
                assert info_manager.set_personal_info(info) is False

        def test_sensitive_info_validation(self, info_manager):
            """Test validate thông tin nhạy cảm."""
            password = "password"
            invalid_cases = [
                # SSN không hợp lệ
                SensitiveInfo("123", "AB123456", ["1234567890"]),
                # Số hộ chiếu không hợp lệ
                SensitiveInfo("123456789", "123", ["1234567890"])
            ]
            
            for info in invalid_cases:
                assert info_manager.set_sensitive_info(
                    info, password
                ) is False

    class TestFileIO:
        """
        Test Import/Export (10%)
        Pass: Xử lý file ổn định
        Fail: Có lỗi khi xử lý file
        """
        
        def test_import_export(
            self, info_manager, valid_personal_info,
            valid_sensitive_info, temp_dir
        ):
            """Test import/export dữ liệu."""
            password = "password"
            export_file = temp_dir / "export.json"
            
            # Lưu dữ liệu
            info_manager.set_personal_info(valid_personal_info)
            info_manager.set_sensitive_info(valid_sensitive_info, password)
            
            # Export
            assert info_manager.export_data(
                str(export_file), True, password
            ) is True
            
            # Import vào manager mới
            new_manager = InfoManager(
                str(temp_dir / "new.json"),
                str(temp_dir / "new.key")
            )
            assert new_manager.import_data(str(export_file), password) is True
            
            # Kiểm tra dữ liệu
            imported_personal = new_manager.get_personal_info()
            imported_sensitive = new_manager.get_sensitive_info(password)
            
            assert imported_personal.full_name == valid_personal_info.full_name
            assert imported_sensitive.ssn == valid_sensitive_info.ssn

        def test_file_errors(self, info_manager, temp_dir):
            """Test xử lý lỗi file."""
            # File không tồn tại
            assert info_manager.import_data(
                "nonexistent.json", "password"
            ) is False
            
            # File không có quyền ghi
            readonly_file = temp_dir / "readonly.json"
            readonly_file.touch()
            readonly_file.chmod(0o444)  # Read-only
            
            assert info_manager.export_data(
                str(readonly_file), False
            ) is False 