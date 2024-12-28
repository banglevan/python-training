"""
Unit tests cho Finance Manager
Tiêu chí đánh giá:
1. Transaction Management (35%): Transaction CRUD
2. Budget Management (35%): Budget tracking
3. Financial Reports (30%): Report generation
"""

import pytest
from datetime import datetime, timedelta
from pathlib import Path

from finance_manager.models import (
    Transaction,
    Budget,
    TransactionType,
    Category
)
from finance_manager.storage import FinanceStorage

@pytest.fixture
def temp_file(tmp_path):
    """Fixture cho temporary file."""
    return tmp_path / "finance.json"

@pytest.fixture
def storage(temp_file):
    """Fixture cho storage."""
    return FinanceStorage(str(temp_file))

@pytest.fixture
def sample_transaction():
    """Fixture cho sample transaction."""
    return {
        "type": "income",
        "category": "salary",
        "amount": 5000,
        "description": "Monthly salary",
        "date": datetime.now().isoformat(),
        "tags": ["work", "monthly"]
    }

class TestTransactionManagement:
    """Test transaction management (35%)."""
    
    def test_add_transaction(
        self,
        storage,
        sample_transaction
    ):
        """Test adding transaction."""
        transaction = storage.add_transaction(
            sample_transaction
        )
        
        assert transaction.id == 1
        assert transaction.type == TransactionType.INCOME
        assert transaction.category == Category.SALARY
        assert transaction.amount == 5000
        
        # Verify persistence
        loaded = storage.get_transaction(1)
        assert loaded is not None
        assert loaded.amount == transaction.amount
    
    def test_update_transaction(
        self,
        storage,
        sample_transaction
    ):
        """Test updating transaction."""
        # Add transaction
        transaction = storage.add_transaction(
            sample_transaction
        )
        
        # Update transaction
        updated = storage.update_transaction(
            transaction.id,
            {
                "amount": 5500,
                "description": "Updated salary"
            }
        )
        
        assert updated is not None
        assert updated.amount == 5500
        assert updated.description == "Updated salary"
        
        # Verify persistence
        loaded = storage.get_transaction(transaction.id)
        assert loaded.amount == 5500
    
    def test_delete_transaction(
        self,
        storage,
        sample_transaction
    ):
        """Test deleting transaction."""
        # Add transaction
        transaction = storage.add_transaction(
            sample_transaction
        )
        
        # Delete transaction
        assert storage.delete_transaction(transaction.id)
        assert storage.get_transaction(transaction.id) is None

class TestBudgetManagement:
    """Test budget management (35%)."""
    
    def test_budget_tracking(self, storage):
        """Test budget tracking."""
        # Set budget
        budget = storage.set_budget({
            "category": "food",
            "amount": 500,
            "period": "monthly",
            "start_date": datetime.now().isoformat()
        })
        
        assert budget.category == Category.FOOD
        assert budget.amount == 500
        
        # Add expenses
        storage.add_transaction({
            "type": "expense",
            "category": "food",
            "amount": 200,
            "description": "Groceries",
            "date": datetime.now().isoformat()
        })
        
        storage.add_transaction({
            "type": "expense",
            "category": "food",
            "amount": 150,
            "description": "Restaurant",
            "date": datetime.now().isoformat()
        })
        
        # Check remaining budget
        transactions = storage.get_transactions(
            transaction_type=TransactionType.EXPENSE,
            category=Category.FOOD
        )
        remaining = budget.calculate_remaining(transactions)
        assert remaining == 150  # 500 - 200 - 150
    
    def test_budget_limits(self, storage):
        """Test budget limits."""
        # Set budget
        budget = storage.set_budget({
            "category": "entertainment",
            "amount": 100,
            "period": "monthly",
            "start_date": datetime.now().isoformat()
        })
        
        # Add expense within budget
        t1 = storage.add_transaction({
            "type": "expense",
            "category": "entertainment",
            "amount": 50,
            "description": "Movies",
            "date": datetime.now().isoformat()
        })
        
        transactions = [t1]
        assert budget.calculate_remaining(transactions) == 50
        
        # Add expense exceeding budget
        t2 = storage.add_transaction({
            "type": "expense",
            "category": "entertainment",
            "amount": 60,
            "description": "Concert",
            "date": datetime.now().isoformat()
        })
        
        transactions.append(t2)
        assert budget.calculate_remaining(transactions) == -10

class TestFinancialReports:
    """Test financial reports (30%)."""
    
    def test_report_generation(
        self,
        storage,
        sample_transaction
    ):
        """Test report generation."""
        # Add income
        storage.add_transaction(sample_transaction)  # 5000
        
        # Add expenses
        storage.add_transaction({
            "type": "expense",
            "category": "food",
            "amount": 500,
            "description": "Groceries",
            "date": datetime.now().isoformat()
        })
        
        storage.add_transaction({
            "type": "expense",
            "category": "transport",
            "amount": 200,
            "description": "Gas",
            "date": datetime.now().isoformat()
        })
        
        # Generate report
        start_date = datetime.now() - timedelta(days=1)
        end_date = datetime.now() + timedelta(days=1)
        report = storage.generate_report(
            start_date,
            end_date
        )
        
        # Verify totals
        assert report.total_income() == 5000
        assert report.total_expense() == 700
        assert report.net_savings() == 4300
        
        # Verify categories
        expenses = report.expenses_by_category()
        assert expenses[Category.FOOD] == 500
        assert expenses[Category.TRANSPORT] == 200
        
        income = report.income_by_category()
        assert income[Category.SALARY] == 5000
    
    def test_date_filtering(
        self,
        storage,
        sample_transaction
    ):
        """Test report date filtering."""
        # Add old transaction
        old_date = datetime.now() - timedelta(days=10)
        storage.add_transaction({
            **sample_transaction,
            "date": old_date.isoformat()
        })
        
        # Add recent transaction
        storage.add_transaction(sample_transaction)
        
        # Generate report for recent transactions
        start_date = datetime.now() - timedelta(days=1)
        end_date = datetime.now() + timedelta(days=1)
        report = storage.generate_report(
            start_date,
            end_date
        )
        
        assert len(report.transactions) == 1
        assert report.total_income() == 5000 