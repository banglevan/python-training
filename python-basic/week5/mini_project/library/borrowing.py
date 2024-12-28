"""
Borrowing system module
"""

from datetime import date, datetime, timedelta
from typing import Dict, List, Optional, Tuple
from enum import Enum

from .models import Book, BookStatus

class BorrowingStatus(Enum):
    """Trạng thái mượn sách."""
    BORROWED = "borrowed"      # Đang mượn
    RETURNED = "returned"      # Đã trả
    OVERDUE = "overdue"       # Quá hạn
    LOST = "lost"             # Mất sách

class BorrowingRecord:
    """Record mượn sách."""
    
    def __init__(
        self,
        id: int,
        book_id: int,
        user_id: int,
        borrowed_date: date,
        due_date: date
    ):
        """
        Khởi tạo record.
        
        Args:
            id: ID record
            book_id: ID sách
            user_id: ID người mượn
            borrowed_date: Ngày mượn
            due_date: Ngày phải trả
        """
        self.id = id
        self.book_id = book_id
        self.user_id = user_id
        self.borrowed_date = borrowed_date
        self.due_date = due_date
        self.returned_date: Optional[date] = None
        self.status = BorrowingStatus.BORROWED
        self.fine_paid = False
    
    def return_book(self, returned_date: date):
        """Trả sách."""
        self.returned_date = returned_date
        if returned_date > self.due_date:
            self.status = BorrowingStatus.OVERDUE
        else:
            self.status = BorrowingStatus.RETURNED
    
    def mark_as_lost(self):
        """Đánh dấu mất sách."""
        self.status = BorrowingStatus.LOST
    
    def is_overdue(self, current_date: date) -> bool:
        """Kiểm tra quá hạn."""
        if self.status == BorrowingStatus.BORROWED:
            return current_date > self.due_date
        return self.status == BorrowingStatus.OVERDUE
    
    def calculate_fine(self, current_date: date) -> float:
        """Tính tiền phạt."""
        if self.fine_paid:
            return 0
            
        if self.status == BorrowingStatus.LOST:
            return 100  # Phạt mất sách
            
        if not self.is_overdue(current_date):
            return 0
            
        # Tính số ngày quá hạn
        end_date = self.returned_date or current_date
        overdue_days = (end_date - self.due_date).days
        
        # Tính tiền phạt (2$ mỗi ngày)
        return max(0, overdue_days * 2)
    
    def pay_fine(self):
        """Đóng tiền phạt."""
        self.fine_paid = True

class BorrowingSystem:
    """Hệ thống mượn sách."""
    
    def __init__(self):
        """Khởi tạo hệ thống."""
        self.records: Dict[int, BorrowingRecord] = {}
        self.user_records: Dict[int, List[int]] = {}  # user_id -> [record_id]
        self.book_records: Dict[int, List[int]] = {}  # book_id -> [record_id]
        self._next_record_id = 1
    
    def borrow_book(
        self,
        book: Book,
        user_id: int,
        borrowed_date: date,
        duration: int = 14
    ) -> Optional[BorrowingRecord]:
        """
        Mượn sách.
        
        Args:
            book: Sách muốn mượn
            user_id: ID người mượn
            borrowed_date: Ngày mượn
            duration: Số ngày được mượn
            
        Returns:
            Record nếu thành công, None nếu thất bại
        """
        # Check book status
        if book.status != BookStatus.AVAILABLE:
            return None
        
        # Create record
        record = BorrowingRecord(
            id=self._next_record_id,
            book_id=book.id,
            user_id=user_id,
            borrowed_date=borrowed_date,
            due_date=borrowed_date + timedelta(days=duration)
        )
        
        # Update book status
        book.status = BookStatus.BORROWED
        
        # Save record
        self.records[record.id] = record
        
        # Update indices
        if user_id not in self.user_records:
            self.user_records[user_id] = []
        self.user_records[user_id].append(record.id)
        
        if book.id not in self.book_records:
            self.book_records[book.id] = []
        self.book_records[book.id].append(record.id)
        
        # Increment record ID
        self._next_record_id += 1
        
        return record
    
    def return_book(
        self,
        book: Book,
        user_id: int,
        returned_date: date
    ) -> Optional[BorrowingRecord]:
        """
        Trả sách.
        
        Args:
            book: Sách muốn trả
            user_id: ID người trả
            returned_date: Ngày trả
            
        Returns:
            Record nếu thành công, None nếu thất bại
        """
        # Find active borrowing record
        record = self._find_active_record(book.id, user_id)
        if not record:
            return None
        
        # Return book
        record.return_book(returned_date)
        
        # Update book status
        book.status = BookStatus.AVAILABLE
        
        return record
    
    def report_lost(
        self,
        book: Book,
        user_id: int
    ) -> Optional[BorrowingRecord]:
        """
        Báo mất sách.
        
        Args:
            book: Sách bị mất
            user_id: ID người báo mất
            
        Returns:
            Record nếu thành công, None nếu thất bại
        """
        # Find active borrowing record
        record = self._find_active_record(book.id, user_id)
        if not record:
            return None
        
        # Mark as lost
        record.mark_as_lost()
        
        # Update book status
        book.status = BookStatus.LOST
        
        return record
    
    def get_user_records(
        self,
        user_id: int,
        status: Optional[BorrowingStatus] = None
    ) -> List[BorrowingRecord]:
        """
        Lấy records của user.
        
        Args:
            user_id: ID user
            status: Lọc theo trạng thái
        """
        if user_id not in self.user_records:
            return []
            
        records = [
            self.records[record_id]
            for record_id in self.user_records[user_id]
        ]
        
        if status:
            records = [
                record for record in records
                if record.status == status
            ]
            
        return records
    
    def get_book_records(
        self,
        book_id: int,
        status: Optional[BorrowingStatus] = None
    ) -> List[BorrowingRecord]:
        """
        Lấy records của sách.
        
        Args:
            book_id: ID sách
            status: Lọc theo trạng thái
        """
        if book_id not in self.book_records:
            return []
            
        records = [
            self.records[record_id]
            for record_id in self.book_records[book_id]
        ]
        
        if status:
            records = [
                record for record in records
                if record.status == status
            ]
            
        return records
    
    def get_overdue_records(
        self,
        current_date: date
    ) -> List[BorrowingRecord]:
        """Lấy các records quá hạn."""
        return [
            record for record in self.records.values()
            if record.is_overdue(current_date)
        ]
    
    def calculate_total_fines(
        self,
        user_id: int,
        current_date: date
    ) -> float:
        """Tính tổng tiền phạt của user."""
        records = self.get_user_records(user_id)
        return sum(
            record.calculate_fine(current_date)
            for record in records
            if not record.fine_paid
        )
    
    def pay_fines(
        self,
        user_id: int,
        current_date: date
    ) -> float:
        """
        Đóng tiền phạt.
        
        Returns:
            Số tiền phải đóng
        """
        total = self.calculate_total_fines(user_id, current_date)
        if total > 0:
            records = self.get_user_records(user_id)
            for record in records:
                if not record.fine_paid:
                    record.pay_fine()
        return total
    
    def _find_active_record(
        self,
        book_id: int,
        user_id: int
    ) -> Optional[BorrowingRecord]:
        """Tìm record đang mượn."""
        if user_id not in self.user_records:
            return None
            
        for record_id in self.user_records[user_id]:
            record = self.records[record_id]
            if (record.book_id == book_id and
                record.status == BorrowingStatus.BORROWED):
                return record
        
        return None 