"""
Finance storage module
"""

import json
from datetime import datetime
from pathlib import Path
from typing import Dict, List, Optional

from .models import (
    Transaction,
    Budget,
    Report,
    TransactionType,
    Category
)

class FinanceStorage:
    """Finance storage."""
    
    def __init__(self, file_path: str):
        """Initialize storage."""
        self.file_path = Path(file_path)
        self.transactions: Dict[int, Transaction] = {}
        self.budgets: Dict[Category, Budget] = {}
        self._next_transaction_id = 1
        self._load()
    
    def add_transaction(
        self,
        transaction_data: dict
    ) -> Transaction:
        """Add new transaction."""
        transaction = Transaction(
            id=self._next_transaction_id,
            type=TransactionType(transaction_data["type"]),
            category=Category(transaction_data["category"]),
            amount=float(transaction_data["amount"]),
            description=transaction_data["description"],
            date=datetime.fromisoformat(
                transaction_data["date"]
            ),
            tags=transaction_data.get("tags", []),
            recurring=transaction_data.get("recurring", False),
            recurring_period=transaction_data.get(
                "recurring_period"
            )
        )
        
        self.transactions[transaction.id] = transaction
        self._next_transaction_id += 1
        self._save()
        
        return transaction
    
    def get_transaction(
        self,
        transaction_id: int
    ) -> Optional[Transaction]:
        """Get transaction by ID."""
        return self.transactions.get(transaction_id)
    
    def update_transaction(
        self,
        transaction_id: int,
        transaction_data: dict
    ) -> Optional[Transaction]:
        """Update transaction."""
        transaction = self.get_transaction(transaction_id)
        if not transaction:
            return None
            
        # Update fields
        if "type" in transaction_data:
            transaction.type = TransactionType(
                transaction_data["type"]
            )
        if "category" in transaction_data:
            transaction.category = Category(
                transaction_data["category"]
            )
        if "amount" in transaction_data:
            transaction.amount = float(
                transaction_data["amount"]
            )
        if "description" in transaction_data:
            transaction.description = (
                transaction_data["description"]
            )
        if "date" in transaction_data:
            transaction.date = datetime.fromisoformat(
                transaction_data["date"]
            )
        if "tags" in transaction_data:
            transaction.tags = transaction_data["tags"]
        if "recurring" in transaction_data:
            transaction.recurring = (
                transaction_data["recurring"]
            )
        if "recurring_period" in transaction_data:
            transaction.recurring_period = (
                transaction_data["recurring_period"]
            )
        
        self._save()
        return transaction
    
    def delete_transaction(
        self,
        transaction_id: int
    ) -> bool:
        """Delete transaction."""
        if transaction_id not in self.transactions:
            return False
            
        del self.transactions[transaction_id]
        self._save()
        return True
    
    def set_budget(self, budget_data: dict) -> Budget:
        """Set budget for category."""
        budget = Budget(
            category=Category(budget_data["category"]),
            amount=float(budget_data["amount"]),
            period=budget_data["period"],
            start_date=datetime.fromisoformat(
                budget_data["start_date"]
            )
        )
        
        self.budgets[budget.category] = budget
        self._save()
        
        return budget
    
    def get_budget(
        self,
        category: Category
    ) -> Optional[Budget]:
        """Get budget for category."""
        return self.budgets.get(category)
    
    def delete_budget(self, category: Category) -> bool:
        """Delete budget."""
        if category not in self.budgets:
            return False
            
        del self.budgets[category]
        self._save()
        return True
    
    def get_transactions(
        self,
        start_date: Optional[datetime] = None,
        end_date: Optional[datetime] = None,
        transaction_type: Optional[TransactionType] = None,
        category: Optional[Category] = None
    ) -> List[Transaction]:
        """Get filtered transactions."""
        transactions = self.transactions.values()
        
        if start_date:
            transactions = [
                t for t in transactions
                if t.date >= start_date
            ]
        
        if end_date:
            transactions = [
                t for t in transactions
                if t.date <= end_date
            ]
        
        if transaction_type:
            transactions = [
                t for t in transactions
                if t.type == transaction_type
            ]
        
        if category:
            transactions = [
                t for t in transactions
                if t.category == category
            ]
        
        return sorted(
            transactions,
            key=lambda t: t.date
        )
    
    def generate_report(
        self,
        start_date: datetime,
        end_date: datetime
    ) -> Report:
        """Generate financial report."""
        transactions = self.get_transactions(
            start_date=start_date,
            end_date=end_date
        )
        
        return Report(
            start_date=start_date,
            end_date=end_date,
            transactions=transactions,
            budgets=list(self.budgets.values())
        )
    
    def _load(self):
        """Load from file."""
        if not self.file_path.exists():
            return
            
        with open(self.file_path, "r") as f:
            data = json.load(f)
            
        # Load transactions
        self._next_transaction_id = data["next_transaction_id"]
        for t_data in data["transactions"]:
            transaction = Transaction(
                id=t_data["id"],
                type=TransactionType(t_data["type"]),
                category=Category(t_data["category"]),
                amount=float(t_data["amount"]),
                description=t_data["description"],
                date=datetime.fromisoformat(t_data["date"]),
                tags=t_data.get("tags", []),
                recurring=t_data.get("recurring", False),
                recurring_period=t_data.get(
                    "recurring_period"
                )
            )
            self.transactions[transaction.id] = transaction
        
        # Load budgets
        for b_data in data["budgets"]:
            budget = Budget(
                category=Category(b_data["category"]),
                amount=float(b_data["amount"]),
                period=b_data["period"],
                start_date=datetime.fromisoformat(
                    b_data["start_date"]
                )
            )
            self.budgets[budget.category] = budget
    
    def _save(self):
        """Save to file."""
        data = {
            "next_transaction_id": self._next_transaction_id,
            "transactions": [],
            "budgets": []
        }
        
        # Save transactions
        for transaction in self.transactions.values():
            t_data = {
                "id": transaction.id,
                "type": transaction.type.value,
                "category": transaction.category.value,
                "amount": float(transaction.amount),
                "description": transaction.description,
                "date": transaction.date.isoformat(),
                "tags": transaction.tags
            }
            
            if transaction.recurring:
                t_data["recurring"] = True
                t_data["recurring_period"] = (
                    transaction.recurring_period
                )
                
            data["transactions"].append(t_data)
        
        # Save budgets
        for budget in self.budgets.values():
            b_data = {
                "category": budget.category.value,
                "amount": float(budget.amount),
                "period": budget.period,
                "start_date": budget.start_date.isoformat()
            }
            data["budgets"].append(b_data)
        
        with open(self.file_path, "w") as f:
            json.dump(data, f, indent=2) 