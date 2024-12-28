"""
Mini Project: Contact Manager
Yêu cầu:
1. CRUD operations cho contacts
2. Tìm kiếm theo nhiều tiêu chí
3. Phân loại contacts
4. Import/Export data (CSV, JSON)
"""

from typing import Dict, List, Optional, Set
from dataclasses import dataclass
from pathlib import Path
import json
import csv
from datetime import datetime
import re

@dataclass
class Contact:
    """Class đại diện cho một contact."""
    name: str
    email: str
    phone: str
    address: str
    birthday: str  # YYYY-MM-DD
    groups: Set[str]
    notes: str
    
    def validate(self) -> None:
        """
        Kiểm tra tính hợp lệ của contact.
        
        Raises:
            ValueError: Nếu thông tin không hợp lệ
        """
        if not self.name or not isinstance(self.name, str):
            raise ValueError("Tên không hợp lệ")
            
        if self.email and not re.match(r'^[\w\.-]+@[\w\.-]+\.\w+$', self.email):
            raise ValueError("Email không hợp lệ")
            
        if self.phone and not re.match(r'^\+?[\d\s-]{10,}$', self.phone):
            raise ValueError("Số điện thoại không hợp lệ")
            
        if self.birthday and not re.match(r'^\d{4}-\d{2}-\d{2}$', self.birthday):
            raise ValueError("Ngày sinh không đúng định dạng YYYY-MM-DD")
            
        if not isinstance(self.groups, set):
            raise ValueError("Nhóm phải là tập hợp")

class ContactManager:
    """Class quản lý contacts."""
    
    def __init__(self, data_file: str = "contacts.json"):
        """
        Khởi tạo manager với file dữ liệu.
        
        Args:
            data_file (str): File JSON chứa dữ liệu
        """
        self.data_file = Path(data_file)
        self.contacts: Dict[str, Contact] = {}  # key: email
        self.groups: Set[str] = set()
        self.load_data()

    def add_contact(self, contact: Contact) -> bool:
        """
        Thêm contact mới.
        
        Args:
            contact (Contact): Contact cần thêm
            
        Returns:
            bool: True nếu thêm thành công
        """
        try:
            contact.validate()
            if contact.email in self.contacts:
                print("Email đã tồn tại!")
                return False
                
            self.contacts[contact.email] = contact
            self.groups.update(contact.groups)
            self.save_data()
            return True
        except ValueError as e:
            print(f"Lỗi: {e}")
            return False

    def update_contact(self, email: str, contact: Contact) -> bool:
        """
        Cập nhật contact.
        
        Args:
            email (str): Email của contact cần cập nhật
            contact (Contact): Thông tin mới
            
        Returns:
            bool: True nếu cập nhật thành công
        """
        if email not in self.contacts:
            print("Không tìm thấy contact!")
            return False
            
        try:
            contact.validate()
            if contact.email != email and contact.email in self.contacts:
                print("Email mới đã tồn tại!")
                return False
                
            del self.contacts[email]
            self.contacts[contact.email] = contact
            self.update_groups()
            self.save_data()
            return True
        except ValueError as e:
            print(f"Lỗi: {e}")
            return False

    def delete_contact(self, email: str) -> bool:
        """
        Xóa contact.
        
        Args:
            email (str): Email của contact cần xóa
            
        Returns:
            bool: True nếu xóa thành công
        """
        if email not in self.contacts:
            print("Không tìm thấy contact!")
            return False
            
        del self.contacts[email]
        self.update_groups()
        self.save_data()
        return True

    def get_contact(self, email: str) -> Optional[Contact]:
        """
        Lấy thông tin contact.
        
        Args:
            email (str): Email của contact
            
        Returns:
            Optional[Contact]: Thông tin contact hoặc None
        """
        return self.contacts.get(email)

    def search_contacts(
        self,
        name: str = "",
        email: str = "",
        phone: str = "",
        group: str = ""
    ) -> List[Contact]:
        """
        Tìm kiếm contacts theo tiêu chí.
        
        Args:
            name (str): Tên cần tìm
            email (str): Email cần tìm
            phone (str): Số điện thoại cần tìm
            group (str): Nhóm cần tìm
            
        Returns:
            List[Contact]: Danh sách contacts thỏa mãn
        """
        results = []
        
        for contact in self.contacts.values():
            if name and name.lower() not in contact.name.lower():
                continue
            if email and email.lower() not in contact.email.lower():
                continue
            if phone and phone not in contact.phone:
                continue
            if group and group not in contact.groups:
                continue
            results.append(contact)
            
        return results

    def get_contacts_by_group(self, group: str) -> List[Contact]:
        """
        Lấy danh sách contacts theo nhóm.
        
        Args:
            group (str): Tên nhóm
            
        Returns:
            List[Contact]: Danh sách contacts thuộc nhóm
        """
        return [
            contact for contact in self.contacts.values()
            if group in contact.groups
        ]

    def import_csv(self, file_path: str) -> bool:
        """
        Import contacts từ file CSV.
        
        Args:
            file_path (str): Đường dẫn file CSV
            
        Returns:
            bool: True nếu import thành công
        """
        try:
            path = Path(file_path)
            if not path.exists():
                print("File không tồn tại!")
                return False
                
            with path.open('r', encoding='utf-8') as f:
                reader = csv.DictReader(f)
                for row in reader:
                    contact = Contact(
                        name=row['name'],
                        email=row['email'],
                        phone=row['phone'],
                        address=row['address'],
                        birthday=row['birthday'],
                        groups=set(row['groups'].split(',')),
                        notes=row['notes']
                    )
                    self.add_contact(contact)
            return True
        except Exception as e:
            print(f"Lỗi import CSV: {e}")
            return False

    def export_csv(self, file_path: str) -> bool:
        """
        Export contacts ra file CSV.
        
        Args:
            file_path (str): Đường dẫn file CSV
            
        Returns:
            bool: True nếu export thành công
        """
        try:
            fieldnames = [
                'name', 'email', 'phone', 'address',
                'birthday', 'groups', 'notes'
            ]
            
            with Path(file_path).open('w', encoding='utf-8', newline='') as f:
                writer = csv.DictWriter(f, fieldnames=fieldnames)
                writer.writeheader()
                
                for contact in self.contacts.values():
                    writer.writerow({
                        'name': contact.name,
                        'email': contact.email,
                        'phone': contact.phone,
                        'address': contact.address,
                        'birthday': contact.birthday,
                        'groups': ','.join(contact.groups),
                        'notes': contact.notes
                    })
            return True
        except Exception as e:
            print(f"Lỗi export CSV: {e}")
            return False

    def load_data(self) -> None:
        """Đọc dữ liệu từ file."""
        try:
            if self.data_file.exists():
                data = json.loads(self.data_file.read_text(encoding='utf-8'))
                
                for item in data['contacts']:
                    contact = Contact(
                        name=item['name'],
                        email=item['email'],
                        phone=item['phone'],
                        address=item['address'],
                        birthday=item['birthday'],
                        groups=set(item['groups']),
                        notes=item['notes']
                    )
                    self.contacts[contact.email] = contact
                    
                self.groups = set(data['groups'])
        except Exception as e:
            print(f"Lỗi đọc file: {e}")
            self.contacts = {}
            self.groups = set()

    def save_data(self) -> None:
        """Lưu dữ liệu vào file."""
        try:
            data = {
                'contacts': [
                    {
                        'name': c.name,
                        'email': c.email,
                        'phone': c.phone,
                        'address': c.address,
                        'birthday': c.birthday,
                        'groups': list(c.groups),
                        'notes': c.notes
                    }
                    for c in self.contacts.values()
                ],
                'groups': list(self.groups)
            }
            
            self.data_file.write_text(
                json.dumps(data, indent=4),
                encoding='utf-8'
            )
        except Exception as e:
            print(f"Lỗi lưu file: {e}")

    def update_groups(self) -> None:
        """Cập nhật danh sách nhóm."""
        groups = set()
        for contact in self.contacts.values():
            groups.update(contact.groups)
        self.groups = groups

def main():
    """Chương trình chính."""
    manager = ContactManager()

    while True:
        print("\nQuản Lý Danh Bạ")
        print("1. Xem danh sách contacts")
        print("2. Thêm contact mới")
        print("3. Cập nhật contact")
        print("4. Xóa contact")
        print("5. Tìm kiếm contacts")
        print("6. Xem theo nhóm")
        print("7. Import từ CSV")
        print("8. Export ra CSV")
        print("9. Thoát")

        choice = input("\nChọn chức năng (1-9): ")

        if choice == '9':
            break
        elif choice == '1':
            if not manager.contacts:
                print("Chưa có contact nào!")
                continue
                
            print("\nDanh sách contacts:")
            for contact in manager.contacts.values():
                print(f"\nTên: {contact.name}")
                print(f"Email: {contact.email}")
                print(f"Điện thoại: {contact.phone}")
                print(f"Địa chỉ: {contact.address}")
                print(f"Sinh nhật: {contact.birthday}")
                print(f"Nhóm: {', '.join(contact.groups)}")
                if contact.notes:
                    print(f"Ghi chú: {contact.notes}")
                    
        elif choice == '2':
            try:
                name = input("Tên: ")
                email = input("Email: ")
                phone = input("Điện thoại: ")
                address = input("Địa chỉ: ")
                birthday = input("Sinh nhật (YYYY-MM-DD): ")
                
                groups = set()
                while True:
                    group = input("Nhóm (Enter để kết thúc): ")
                    if not group:
                        break
                    groups.add(group)
                    
                notes = input("Ghi chú: ")
                
                contact = Contact(
                    name=name,
                    email=email,
                    phone=phone,
                    address=address,
                    birthday=birthday,
                    groups=groups,
                    notes=notes
                )
                
                if manager.add_contact(contact):
                    print("Thêm contact thành công!")
                else:
                    print("Thêm contact thất bại!")
            except ValueError as e:
                print(f"Lỗi: {e}")
                
        elif choice == '3':
            email = input("Nhập email contact cần cập nhật: ")
            old_contact = manager.get_contact(email)
            if not old_contact:
                print("Không tìm thấy contact!")
                continue
                
            try:
                print("\nNhập thông tin mới (Enter để giữ nguyên):")
                name = input(f"Tên [{old_contact.name}]: ")
                new_email = input(f"Email [{old_contact.email}]: ")
                phone = input(f"Điện thoại [{old_contact.phone}]: ")
                address = input(f"Địa chỉ [{old_contact.address}]: ")
                birthday = input(f"Sinh nhật [{old_contact.birthday}]: ")
                
                groups = old_contact.groups.copy()
                print("\nNhóm hiện tại:", ', '.join(groups))
                while True:
                    action = input("Thêm/Xóa nhóm (+/-/Enter để kết thúc): ")
                    if not action:
                        break
                    if action == '+':
                        group = input("Tên nhóm: ")
                        groups.add(group)
                    elif action == '-':
                        group = input("Tên nhóm: ")
                        groups.discard(group)
                        
                notes = input(f"Ghi chú [{old_contact.notes}]: ")
                
                contact = Contact(
                    name=name or old_contact.name,
                    email=new_email or old_contact.email,
                    phone=phone or old_contact.phone,
                    address=address or old_contact.address,
                    birthday=birthday or old_contact.birthday,
                    groups=groups,
                    notes=notes or old_contact.notes
                )
                
                if manager.update_contact(email, contact):
                    print("Cập nhật thành công!")
                else:
                    print("Cập nhật thất bại!")
            except ValueError as e:
                print(f"Lỗi: {e}")
                
        elif choice == '4':
            email = input("Nhập email contact cần xóa: ")
            if manager.delete_contact(email):
                print("Xóa contact thành công!")
            else:
                print("Xóa contact thất bại!")
                
        elif choice == '5':
            print("\nNhập tiêu chí tìm kiếm (Enter để bỏ qua):")
            name = input("Tên: ")
            email = input("Email: ")
            phone = input("Điện thoại: ")
            group = input("Nhóm: ")
            
            results = manager.search_contacts(name, email, phone, group)
            if results:
                print(f"\nTìm thấy {len(results)} kết quả:")
                for contact in results:
                    print(f"\nTên: {contact.name}")
                    print(f"Email: {contact.email}")
                    print(f"Điện thoại: {contact.phone}")
                    print(f"Nhóm: {', '.join(contact.groups)}")
            else:
                print("Không tìm thấy kết quả!")
                
        elif choice == '6':
            if not manager.groups:
                print("Chưa có nhóm nào!")
                continue
                
            print("\nDanh sách nhóm:")
            for group in sorted(manager.groups):
                contacts = manager.get_contacts_by_group(group)
                print(f"\n{group} ({len(contacts)} contacts):")
                for contact in contacts:
                    print(f"- {contact.name} ({contact.email})")
                    
        elif choice == '7':
            file_path = input("Nhập đường dẫn file CSV: ")
            if manager.import_csv(file_path):
                print("Import thành công!")
            else:
                print("Import thất bại!")
                
        elif choice == '8':
            file_path = input("Nhập đường dẫn file CSV: ")
            if manager.export_csv(file_path):
                print("Export thành công!")
            else:
                print("Export thất bại!")
                
        else:
            print("Lựa chọn không hợp lệ!")

if __name__ == "__main__":
    main() 