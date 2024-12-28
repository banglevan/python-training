"""
Unit tests cho borrowing system
Tiêu chí đánh giá:
1. Borrowing Records (30%): Record management
2. Borrowing Operations (40%): Borrow/Return/Lost
3. Fine Calculation (30%): Overdue/Lost fines
"""

import pytest
from datetime import date, timedelta

from library.models import Book, Author, Category, BookStatus
from library.borrowing import (
    BorrowingSystem,
    BorrowingRecord,
    BorrowingStatus
)

@pytest.fixture
def sample_book():
    """Fixture cho book."""
    author = Author(
        id=1,
        name="John Doe",
        birth_date=date(1980, 1, 1),
        nationality="US"
    )
    
    category = Category(
        id=1,
        name="Fiction",
        description="Fiction books"
    )
    
    return Book(
        id=1,
        title="Sample Book",
        author=author,
        category=category,
        published_date=date(2020, 1, 1),
        isbn="1234567890"
    )

@pytest.fixture
def borrowing_system():
    """Fixture cho borrowing system."""
    return BorrowingSystem()

class TestBorrowingRecords:
    """
    Test borrowing records (30%)
    Pass: Record management đúng ≥ 95%
    Fail: Record management đúng < 95%
    """
    
    def test_record_creation(self, sample_book):
        """Test record creation."""
        record = BorrowingRecord(
            id=1,
            book_id=sample_book.id,
            user_id=1,
            borrowed_date=date(2024, 1, 1),
            due_date=date(2024, 1, 15)
        )
        
        # Properties
        assert record.id == 1
        assert record.book_id == sample_book.id
        assert record.user_id == 1
        assert record.borrowed_date == date(2024, 1, 1)
        assert record.due_date == date(2024, 1, 15)
        assert record.returned_date is None
        assert record.status == BorrowingStatus.BORROWED
        assert record.fine_paid is False

    def test_record_status(self, sample_book):
        """Test record status changes."""
        record = BorrowingRecord(
            id=1,
            book_id=sample_book.id,
            user_id=1,
            borrowed_date=date(2024, 1, 1),
            due_date=date(2024, 1, 15)
        )
        
        # Return on time
        record.return_book(date(2024, 1, 14))
        assert record.status == BorrowingStatus.RETURNED
        
        # Return late
        record = BorrowingRecord(
            id=2,
            book_id=sample_book.id,
            user_id=1,
            borrowed_date=date(2024, 1, 1),
            due_date=date(2024, 1, 15)
        )
        record.return_book(date(2024, 1, 16))
        assert record.status == BorrowingStatus.OVERDUE
        
        # Mark as lost
        record = BorrowingRecord(
            id=3,
            book_id=sample_book.id,
            user_id=1,
            borrowed_date=date(2024, 1, 1),
            due_date=date(2024, 1, 15)
        )
        record.mark_as_lost()
        assert record.status == BorrowingStatus.LOST

class TestBorrowingOperations:
    """
    Test borrowing operations (40%)
    Pass: Operations đúng ≥ 95%
    Fail: Operations đúng < 95%
    """
    
    def test_borrow_book(
        self,
        sample_book,
        borrowing_system
    ):
        """Test borrowing book."""
        # Borrow book
        record = borrowing_system.borrow_book(
            book=sample_book,
            user_id=1,
            borrowed_date=date(2024, 1, 1)
        )
        
        assert record is not None
        assert record.status == BorrowingStatus.BORROWED
        assert sample_book.status == BookStatus.BORROWED
        
        # Cannot borrow borrowed book
        record2 = borrowing_system.borrow_book(
            book=sample_book,
            user_id=2,
            borrowed_date=date(2024, 1, 1)
        )
        assert record2 is None

    def test_return_book(
        self,
        sample_book,
        borrowing_system
    ):
        """Test returning book."""
        # Borrow and return
        record = borrowing_system.borrow_book(
            book=sample_book,
            user_id=1,
            borrowed_date=date(2024, 1, 1)
        )
        
        returned = borrowing_system.return_book(
            book=sample_book,
            user_id=1,
            returned_date=date(2024, 1, 14)
        )
        
        assert returned is not None
        assert returned.status == BorrowingStatus.RETURNED
        assert sample_book.status == BookStatus.AVAILABLE
        
        # Cannot return again
        returned2 = borrowing_system.return_book(
            book=sample_book,
            user_id=1,
            returned_date=date(2024, 1, 15)
        )
        assert returned2 is None

    def test_report_lost(
        self,
        sample_book,
        borrowing_system
    ):
        """Test reporting lost book."""
        # Borrow and report lost
        record = borrowing_system.borrow_book(
            book=sample_book,
            user_id=1,
            borrowed_date=date(2024, 1, 1)
        )
        
        lost = borrowing_system.report_lost(
            book=sample_book,
            user_id=1
        )
        
        assert lost is not None
        assert lost.status == BorrowingStatus.LOST
        assert sample_book.status == BookStatus.LOST

class TestFineCalculation:
    """
    Test fine calculation (30%)
    Pass: Fine calculation đúng ≥ 95%
    Fail: Fine calculation đúng < 95%
    """
    
    def test_overdue_fine(
        self,
        sample_book,
        borrowing_system
    ):
        """Test overdue fine calculation."""
        # Borrow and return late
        record = borrowing_system.borrow_book(
            book=sample_book,
            user_id=1,
            borrowed_date=date(2024, 1, 1),
            duration=7  # 1 week
        )
        
        # Return 3 days late
        borrowing_system.return_book(
            book=sample_book,
            user_id=1,
            returned_date=date(2024, 1, 11)
        )
        
        # Calculate fine
        fine = borrowing_system.calculate_total_fines(
            user_id=1,
            current_date=date(2024, 1, 11)
        )
        assert fine == 6  # 3 days * $2
        
        # Pay fine
        paid = borrowing_system.pay_fines(
            user_id=1,
            current_date=date(2024, 1, 11)
        )
        assert paid == 6
        
        # No more fines
        fine = borrowing_system.calculate_total_fines(
            user_id=1,
            current_date=date(2024, 1, 11)
        )
        assert fine == 0

    def test_lost_fine(
        self,
        sample_book,
        borrowing_system
    ):
        """Test lost book fine."""
        # Borrow and report lost
        record = borrowing_system.borrow_book(
            book=sample_book,
            user_id=1,
            borrowed_date=date(2024, 1, 1)
        )
        
        borrowing_system.report_lost(
            book=sample_book,
            user_id=1
        )
        
        # Calculate fine
        fine = borrowing_system.calculate_total_fines(
            user_id=1,
            current_date=date(2024, 1, 11)
        )
        assert fine == 100  # Lost book fee
        
        # Pay fine
        paid = borrowing_system.pay_fines(
            user_id=1,
            current_date=date(2024, 1, 11)
        )
        assert paid == 100

    def test_multiple_fines(
        self,
        sample_book,
        borrowing_system
    ):
        """Test multiple fines."""
        # Create another book
        book2 = Book(
            id=2,
            title="Another Book",
            author=sample_book.author,
            category=sample_book.category,
            published_date=date(2020, 1, 1),
            isbn="0987654321"
        )
        
        # Borrow and return late
        borrowing_system.borrow_book(
            book=sample_book,
            user_id=1,
            borrowed_date=date(2024, 1, 1),
            duration=7
        )
        borrowing_system.return_book(
            book=sample_book,
            user_id=1,
            returned_date=date(2024, 1, 11)
        )
        
        # Borrow and lose another
        borrowing_system.borrow_book(
            book=book2,
            user_id=1,
            borrowed_date=date(2024, 1, 1)
        )
        borrowing_system.report_lost(
            book=book2,
            user_id=1
        )
        
        # Calculate total fines
        fine = borrowing_system.calculate_total_fines(
            user_id=1,
            current_date=date(2024, 1, 11)
        )
        assert fine == 106  # $6 overdue + $100 lost 