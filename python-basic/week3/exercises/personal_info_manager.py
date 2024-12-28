"""
Bài tập 3: Quản lý thông tin cá nhân
Yêu cầu:
1. Lưu trữ và quản lý thông tin cá nhân
2. Mã hóa thông tin nhạy cảm
3. Phân quyền truy cập
4. Import/Export dữ liệu
"""

from typing import Dict, List, Optional, Set
from dataclasses import dataclass
from pathlib import Path
import json
import hashlib
import base64
from datetime import datetime
from cryptography.fernet import Fernet
import re

@dataclass
class PersonalInfo:
    """Class chứa thông tin cá nhân."""
    full_name: str
    dob: str  # YYYY-MM-DD
    email: str
    phone: str
    address: str
    
    def validate(self) -> None:
        """
        Kiểm tra tính hợp lệ của thông tin.
        
        Raises:
            ValueError: Nếu thông tin không hợp lệ
        """
        if not self.full_name or not isinstance(self.full_name, str):
            raise ValueError("Tên không hợp lệ")
            
        # Kiểm tra định dạng ngày sinh
        if not re.match(r'^\d{4}-\d{2}-\d{2}$', self.dob):
            raise ValueError("Ngày sinh không đúng định d���ng YYYY-MM-DD")
            
        # Kiểm tra email
        if not re.match(r'^[\w\.-]+@[\w\.-]+\.\w+$', self.email):
            raise ValueError("Email không hợp lệ")
            
        # Kiểm tra số điện thoại
        if not re.match(r'^\+?[\d\s-]{10,}$', self.phone):
            raise ValueError("Số điện thoại không hợp lệ")
            
        if not self.address or not isinstance(self.address, str):
            raise ValueError("Địa chỉ không hợp lệ")

@dataclass
class SensitiveInfo:
    """Class chứa thông tin nhạy cảm."""
    ssn: str  # Social Security Number
    passport: str
    bank_accounts: List[str]
    
    def validate(self) -> None:
        """
        Kiểm tra tính hợp lệ của thông tin.
        
        Raises:
            ValueError: Nếu thông tin không hợp lệ
        """
        if not re.match(r'^\d{9}$', self.ssn):
            raise ValueError("SSN không hợp lệ")
            
        if not re.match(r'^[A-Z0-9]{8,}$', self.passport):
            raise ValueError("Số hộ chiếu không hợp lệ")
            
        if not self.bank_accounts:
            raise ValueError("Cần ít nhất một tài khoản ngân hàng")
        for account in self.bank_accounts:
            if not re.match(r'^\d{10,}$', account):
                raise ValueError(f"Số tài khoản không hợp lệ: {account}")

class InfoManager:
    """Class quản lý thông tin."""
    
    def __init__(
        self,
        data_file: str = "personal_info.json",
        key_file: str = "encryption.key"
    ):
        """
        Khởi tạo manager với file dữ liệu.
        
        Args:
            data_file (str): File JSON chứa dữ liệu
            key_file (str): File chứa key mã hóa
        """
        self.data_file = Path(data_file)
        self.key_file = Path(key_file)
        self.personal_info: Optional[PersonalInfo] = None
        self.sensitive_info: Optional[SensitiveInfo] = None
        self.fernet = self._load_or_create_key()
        self.load_data()

    def set_personal_info(self, info: PersonalInfo) -> bool:
        """
        Cập nhật thông tin cá nhân.
        
        Args:
            info (PersonalInfo): Thông tin mới
            
        Returns:
            bool: True nếu cập nhật thành công
        """
        try:
            info.validate()
            self.personal_info = info
            self.save_data()
            return True
        except ValueError as e:
            print(f"Lỗi: {e}")
            return False

    def set_sensitive_info(
        self,
        info: SensitiveInfo,
        password: str
    ) -> bool:
        """
        Cập nhật thông tin nhạy cảm.
        
        Args:
            info (SensitiveInfo): Thông tin mới
            password (str): Mật khẩu để xác thực
            
        Returns:
            bool: True nếu cập nhật thành công
        """
        if not self._verify_password(password):
            print("Mật khẩu không đúng!")
            return False
            
        try:
            info.validate()
            self.sensitive_info = info
            self.save_data()
            return True
        except ValueError as e:
            print(f"Lỗi: {e}")
            return False

    def get_personal_info(self) -> Optional[PersonalInfo]:
        """
        Lấy thông tin cá nhân.
        
        Returns:
            Optional[PersonalInfo]: Thông tin cá nhân hoặc None
        """
        return self.personal_info

    def get_sensitive_info(self, password: str) -> Optional[SensitiveInfo]:
        """
        Lấy thông tin nhạy cảm.
        
        Args:
            password (str): Mật khẩu để xác thực
            
        Returns:
            Optional[SensitiveInfo]: Thông tin nhạy cảm hoặc None
        """
        if not self._verify_password(password):
            print("Mật khẩu không đúng!")
            return None
        return self.sensitive_info

    def export_data(
        self,
        file_path: str,
        include_sensitive: bool = False,
        password: str = None
    ) -> bool:
        """
        Export dữ liệu ra file.
        
        Args:
            file_path (str): Đường dẫn file
            include_sensitive (bool): Có export thông tin nhạy cảm
            password (str): Mật khẩu nếu export thông tin nhạy cảm
            
        Returns:
            bool: True nếu export thành công
        """
        try:
            data = {}
            
            if self.personal_info:
                data["personal"] = {
                    "full_name": self.personal_info.full_name,
                    "dob": self.personal_info.dob,
                    "email": self.personal_info.email,
                    "phone": self.personal_info.phone,
                    "address": self.personal_info.address
                }
                
            if include_sensitive and self.sensitive_info:
                if not password or not self._verify_password(password):
                    print("Cần mật khẩu đúng để export thông tin nh���y cảm!")
                    return False
                    
                data["sensitive"] = {
                    "ssn": self.sensitive_info.ssn,
                    "passport": self.sensitive_info.passport,
                    "bank_accounts": self.sensitive_info.bank_accounts
                }
                
            Path(file_path).write_text(
                json.dumps(data, indent=4),
                encoding='utf-8'
            )
            return True
        except Exception as e:
            print(f"Lỗi export: {e}")
            return False

    def import_data(
        self,
        file_path: str,
        password: str = None
    ) -> bool:
        """
        Import dữ liệu từ file.
        
        Args:
            file_path (str): Đường dẫn file
            password (str): Mật khẩu nếu import thông tin nhạy cảm
            
        Returns:
            bool: True nếu import thành công
        """
        try:
            data = json.loads(Path(file_path).read_text(encoding='utf-8'))
            
            if "personal" in data:
                personal = data["personal"]
                self.personal_info = PersonalInfo(
                    full_name=personal["full_name"],
                    dob=personal["dob"],
                    email=personal["email"],
                    phone=personal["phone"],
                    address=personal["address"]
                )
                
            if "sensitive" in data:
                if not password or not self._verify_password(password):
                    print("Cần mật khẩu đúng để import thông tin nhạy cảm!")
                    return False
                    
                sensitive = data["sensitive"]
                self.sensitive_info = SensitiveInfo(
                    ssn=sensitive["ssn"],
                    passport=sensitive["passport"],
                    bank_accounts=sensitive["bank_accounts"]
                )
                
            self.save_data()
            return True
        except Exception as e:
            print(f"Lỗi import: {e}")
            return False

    def _load_or_create_key(self) -> Fernet:
        """
        Tạo hoặc đọc key mã hóa.
        
        Returns:
            Fernet: Instance để mã hóa/giải mã
        """
        if self.key_file.exists():
            key = self.key_file.read_bytes()
        else:
            key = Fernet.generate_key()
            self.key_file.write_bytes(key)
        return Fernet(key)

    def _verify_password(self, password: str) -> bool:
        """
        Xác thực mật khẩu.
        
        Args:
            password (str): Mật khẩu cần kiểm tra
            
        Returns:
            bool: True nếu mật khẩu đúng
        """
        # TODO: Implement proper password verification
        return hashlib.sha256(password.encode()).hexdigest() == (
            "5e884898da28047151d0e56f8dc6292773603d0d6aabbdd62a11ef721d1542d8"
        )  # This is 'password'

    def load_data(self) -> None:
        """Đọc dữ liệu từ file."""
        try:
            if self.data_file.exists():
                data = json.loads(self.data_file.read_text(encoding='utf-8'))
                
                if "personal" in data:
                    personal = data["personal"]
                    self.personal_info = PersonalInfo(
                        full_name=personal["full_name"],
                        dob=personal["dob"],
                        email=personal["email"],
                        phone=personal["phone"],
                        address=personal["address"]
                    )
                    
                if "sensitive" in data:
                    encrypted = data["sensitive"]
                    decrypted = json.loads(
                        self.fernet.decrypt(
                            encrypted.encode()
                        ).decode()
                    )
                    self.sensitive_info = SensitiveInfo(
                        ssn=decrypted["ssn"],
                        passport=decrypted["passport"],
                        bank_accounts=decrypted["bank_accounts"]
                    )
        except Exception as e:
            print(f"Lỗi đọc file: {e}")
            self.personal_info = None
            self.sensitive_info = None

    def save_data(self) -> None:
        """Lưu dữ liệu vào file."""
        try:
            data = {}
            
            if self.personal_info:
                data["personal"] = {
                    "full_name": self.personal_info.full_name,
                    "dob": self.personal_info.dob,
                    "email": self.personal_info.email,
                    "phone": self.personal_info.phone,
                    "address": self.personal_info.address
                }
                
            if self.sensitive_info:
                sensitive = {
                    "ssn": self.sensitive_info.ssn,
                    "passport": self.sensitive_info.passport,
                    "bank_accounts": self.sensitive_info.bank_accounts
                }
                data["sensitive"] = self.fernet.encrypt(
                    json.dumps(sensitive).encode()
                ).decode()
                
            self.data_file.write_text(
                json.dumps(data, indent=4),
                encoding='utf-8'
            )
        except Exception as e:
            print(f"Lỗi lưu file: {e}")

def main():
    """Chương trình chính."""
    manager = InfoManager()

    while True:
        print("\nQuản Lý Thông Tin Cá Nhân")
        print("1. Xem thông tin cá nhân")
        print("2. Cập nhật thông tin cá nhân")
        print("3. Xem thông tin nhạy cảm")
        print("4. Cập nhật thông tin nhạy cảm")
        print("5. Export dữ liệu")
        print("6. Import dữ liệu")
        print("7. Thoát")

        choice = input("\nChọn chức năng (1-7): ")

        if choice == '7':
            break
        elif choice == '1':
            info = manager.get_personal_info()
            if info:
                print("\nThông tin cá nhân:")
                print(f"Họ tên: {info.full_name}")
                print(f"Ngày sinh: {info.dob}")
                print(f"Email: {info.email}")
                print(f"Điện thoại: {info.phone}")
                print(f"Địa chỉ: {info.address}")
            else:
                print("Chưa có thông tin!")
                
        elif choice == '2':
            try:
                full_name = input("Họ tên: ")
                dob = input("Ngày sinh (YYYY-MM-DD): ")
                email = input("Email: ")
                phone = input("Điện thoại: ")
                address = input("Địa chỉ: ")
                
                info = PersonalInfo(
                    full_name=full_name,
                    dob=dob,
                    email=email,
                    phone=phone,
                    address=address
                )
                
                if manager.set_personal_info(info):
                    print("Cập nhật thành công!")
                else:
                    print("Cập nhật thất bại!")
            except ValueError as e:
                print(f"Lỗi: {e}")
                
        elif choice == '3':
            password = input("Nhập mật khẩu: ")
            info = manager.get_sensitive_info(password)
            if info:
                print("\nThông tin nhạy cảm:")
                print(f"SSN: {info.ssn}")
                print(f"Hộ chiếu: {info.passport}")
                print("Tài khoản ngân hàng:")
                for account in info.bank_accounts:
                    print(f"- {account}")
            else:
                print("Không thể truy cập thông tin!")
                
        elif choice == '4':
            password = input("Nhập mật khẩu: ")
            try:
                ssn = input("SSN: ")
                passport = input("Số hộ chiếu: ")
                
                bank_accounts = []
                while True:
                    account = input("Số tài khoản (Enter để kết thúc): ")
                    if not account:
                        break
                    bank_accounts.append(account)
                
                info = SensitiveInfo(
                    ssn=ssn,
                    passport=passport,
                    bank_accounts=bank_accounts
                )
                
                if manager.set_sensitive_info(info, password):
                    print("Cập nhật thành công!")
                else:
                    print("Cập nhật thất bại!")
            except ValueError as e:
                print(f"Lỗi: {e}")
                
        elif choice == '5':
            file_path = input("Nhập đường dẫn file export: ")
            include_sensitive = input("Export cả thông tin nhạy cảm? (y/n): ").lower() == 'y'
            
            if include_sensitive:
                password = input("Nhập mật khẩu: ")
            else:
                password = None
                
            if manager.export_data(file_path, include_sensitive, password):
                print("Export thành công!")
            else:
                print("Export thất bại!")
                
        elif choice == '6':
            file_path = input("Nhập đường dẫn file import: ")
            password = input("Nhập mật khẩu (nếu có thông tin nhạy cảm): ")
            
            if manager.import_data(file_path, password):
                print("Import thành công!")
            else:
                print("Import thất bại!")
                
        else:
            print("Lựa chọn không hợp lệ!")

if __name__ == "__main__":
    main() 