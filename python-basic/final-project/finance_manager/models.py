"""
Personal finance models
"""

from dataclasses import dataclass
from datetime import datetime
from enum import Enum
from typing import List, Dict, Optional

class TransactionType(Enum):
    """Transaction type."""
    INCOME = "income"
    EXPENSE = "expense"

class Category(Enum):
    """Transaction categories."""
    # Income categories
    SALARY = "salary"
    INVESTMENT = "investment"
    BUSINESS = "business"
    OTHER_INCOME = "other_income"
    
    # Expense categories
    FOOD = "food"
    TRANSPORT = "transport"
    HOUSING = "housing"
    UTILITIES = "utilities"
    HEALTHCARE = "healthcare"
    ENTERTAINMENT = "entertainment"
    SHOPPING = "shopping"
    EDUCATION = "education"
    OTHER_EXPENSE = "other_expense"

@dataclass
class Transaction:
    """Transaction model."""
    id: int
    type: TransactionType
    category: Category
    amount: float
    description: str
    date: datetime
    tags: List[str]
    recurring: bool = False
    recurring_period: Optional[str] = None  # daily, weekly, monthly, yearly

@dataclass
class Budget:
    """Budget model."""
    category: Category
    amount: float
    period: str  # monthly, yearly
    start_date: datetime
    
    def calculate_remaining(
        self,
        transactions: List[Transaction]
    ) -> float:
        """Calculate remaining budget."""
        spent = sum(
            t.amount for t in transactions
            if (t.category == self.category and
                t.type == TransactionType.EXPENSE)
        )
        return self.amount - spent

@dataclass
class Report:
    """Report model."""
    start_date: datetime
    end_date: datetime
    transactions: List[Transaction]
    budgets: Optional[List[Budget]] = None
    
    def total_income(self) -> float:
        """Calculate total income."""
        return sum(
            t.amount for t in self.transactions
            if t.type == TransactionType.INCOME
        )
    
    def total_expense(self) -> float:
        """Calculate total expense."""
        return sum(
            t.amount for t in self.transactions
            if t.type == TransactionType.EXPENSE
        )
    
    def net_savings(self) -> float:
        """Calculate net savings."""
        return self.total_income() - self.total_expense()
    
    def expenses_by_category(self) -> Dict[Category, float]:
        """Group expenses by category."""
        result = {}
        for t in self.transactions:
            if t.type == TransactionType.EXPENSE:
                if t.category not in result:
                    result[t.category] = 0
                result[t.category] += t.amount
        return result
    
    def income_by_category(self) -> Dict[Category, float]:
        """Group income by category."""
        result = {}
        for t in self.transactions:
            if t.type == TransactionType.INCOME:
                if t.category not in result:
                    result[t.category] = 0
                result[t.category] += t.amount
        return result
    
    def budget_status(self) -> Dict[Category, Dict]:
        """Calculate budget status."""
        if not self.budgets:
            return {}
            
        result = {}
        for budget in self.budgets:
            spent = sum(
                t.amount for t in self.transactions
                if (t.category == budget.category and
                    t.type == TransactionType.EXPENSE)
            )
            result[budget.category] = {
                "budget": budget.amount,
                "spent": spent,
                "remaining": budget.amount - spent,
                "percentage": (spent / budget.amount * 100)
                if budget.amount > 0 else 0
            }
        return result 