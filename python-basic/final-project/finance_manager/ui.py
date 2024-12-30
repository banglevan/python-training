"""
Finance manager UI
"""

import tkinter as tk
from tkinter import ttk, messagebox
from datetime import datetime, date
import calendar
from typing import Optional
import matplotlib.pyplot as plt
from matplotlib.backends.backend_tkagg import (
    FigureCanvasTkAgg
)

from .models import (
    Transaction,
    Budget,
    TransactionType,
    Category
)
from .storage import FinanceStorage

class FinanceUI:
    """Finance manager UI."""
    
    def __init__(self, storage: FinanceStorage):
        """Initialize UI."""
        self.storage = storage
        
        # Create main window
        self.root = tk.Tk()
        self.root.title("Finance Manager")
        self.root.geometry("1200x800")
        
        # Create notebook for tabs
        self.notebook = ttk.Notebook(self.root)
        self.notebook.pack(fill=tk.BOTH, expand=True)
        
        # Create tabs
        self._create_dashboard_tab()
        self._create_transactions_tab()
        self._create_budgets_tab()
        self._create_reports_tab()
        
        # Load data
        self._load_data()
    
    def run(self):
        """Run UI."""
        self.root.mainloop()
    
    def _create_dashboard_tab(self):
        """Create dashboard tab."""
        tab = ttk.Frame(self.notebook)
        self.notebook.add(tab, text="Dashboard")
        
        # Summary frame
        summary_frame = ttk.LabelFrame(
            tab,
            text="Monthly Summary"
        )
        summary_frame.pack(fill=tk.X, padx=5, pady=5)
        
        # Income
        self.income_var = tk.StringVar()
        ttk.Label(
            summary_frame,
            text="Total Income:"
        ).pack(side=tk.LEFT)
        ttk.Label(
            summary_frame,
            textvariable=self.income_var
        ).pack(side=tk.LEFT, padx=10)
        
        # Expenses
        self.expenses_var = tk.StringVar()
        ttk.Label(
            summary_frame,
            text="Total Expenses:"
        ).pack(side=tk.LEFT)
        ttk.Label(
            summary_frame,
            textvariable=self.expenses_var
        ).pack(side=tk.LEFT, padx=10)
        
        # Savings
        self.savings_var = tk.StringVar()
        ttk.Label(
            summary_frame,
            text="Net Savings:"
        ).pack(side=tk.LEFT)
        ttk.Label(
            summary_frame,
            textvariable=self.savings_var
        ).pack(side=tk.LEFT, padx=10)
        
        # Charts frame
        charts_frame = ttk.Frame(tab)
        charts_frame.pack(fill=tk.BOTH, expand=True)
        
        # Income pie chart
        self.income_fig = plt.Figure(figsize=(6, 4))
        self.income_canvas = FigureCanvasTkAgg(
            self.income_fig,
            master=charts_frame
        )
        self.income_canvas.get_tk_widget().pack(
            side=tk.LEFT,
            fill=tk.BOTH,
            expand=True
        )
        
        # Expenses pie chart
        self.expenses_fig = plt.Figure(figsize=(6, 4))
        self.expenses_canvas = FigureCanvasTkAgg(
            self.expenses_fig,
            master=charts_frame
        )
        self.expenses_canvas.get_tk_widget().pack(
            side=tk.RIGHT,
            fill=tk.BOTH,
            expand=True
        )
    
    def _create_transactions_tab(self):
        """Create transactions tab."""
        tab = ttk.Frame(self.notebook)
        self.notebook.add(tab, text="Transactions")
        
        # Split into list and form
        list_frame = ttk.Frame(tab)
        list_frame.pack(side=tk.LEFT, fill=tk.BOTH, expand=True)
        
        form_frame = ttk.Frame(tab)
        form_frame.pack(side=tk.RIGHT, fill=tk.BOTH)
        
        # Transactions list
        columns = (
            "id",
            "date",
            "type",
            "category",
            "amount",
            "description"
        )
        self.transaction_tree = ttk.Treeview(
            list_frame,
            columns=columns,
            show="headings"
        )
        
        self.transaction_tree.heading("id", text="ID")
        self.transaction_tree.heading("date", text="Date")
        self.transaction_tree.heading("type", text="Type")
        self.transaction_tree.heading(
            "category",
            text="Category"
        )
        self.transaction_tree.heading("amount", text="Amount")
        self.transaction_tree.heading(
            "description",
            text="Description"
        )
        
        scrollbar = ttk.Scrollbar(
            list_frame,
            orient=tk.VERTICAL,
            command=self.transaction_tree.yview
        )
        
        self.transaction_tree.configure(
            yscrollcommand=scrollbar.set
        )
        
        self.transaction_tree.pack(
            side=tk.LEFT,
            fill=tk.BOTH,
            expand=True
        )
        scrollbar.pack(side=tk.RIGHT, fill=tk.Y)
        
        # Transaction form
        ttk.Label(form_frame, text="Type:").pack()
        self.type_var = tk.StringVar()
        ttk.Combobox(
            form_frame,
            textvariable=self.type_var,
            values=[t.value for t in TransactionType]
        ).pack(fill=tk.X)
        
        ttk.Label(form_frame, text="Category:").pack()
        self.category_var = tk.StringVar()
        ttk.Combobox(
            form_frame,
            textvariable=self.category_var,
            values=[c.value for c in Category]
        ).pack(fill=tk.X)
        
        ttk.Label(form_frame, text="Amount:").pack()
        self.amount_var = tk.StringVar()
        ttk.Entry(
            form_frame,
            textvariable=self.amount_var
        ).pack(fill=tk.X)
        
        ttk.Label(form_frame, text="Description:").pack()
        self.description_var = tk.StringVar()
        ttk.Entry(
            form_frame,
            textvariable=self.description_var
        ).pack(fill=tk.X)
        
        ttk.Label(form_frame, text="Date:").pack()
        self.date_var = tk.StringVar()
        ttk.Entry(
            form_frame,
            textvariable=self.date_var
        ).pack(fill=tk.X)
        
        ttk.Label(form_frame, text="Tags:").pack()
        self.tags_var = tk.StringVar()
        ttk.Entry(
            form_frame,
            textvariable=self.tags_var
        ).pack(fill=tk.X)
        
        # Recurring options
        self.recurring_var = tk.BooleanVar()
        ttk.Checkbutton(
            form_frame,
            text="Recurring",
            variable=self.recurring_var
        ).pack()
        
        ttk.Label(
            form_frame,
            text="Recurring Period:"
        ).pack()
        self.period_var = tk.StringVar()
        ttk.Combobox(
            form_frame,
            textvariable=self.period_var,
            values=["daily", "weekly", "monthly", "yearly"]
        ).pack(fill=tk.X)
        
        # Buttons
        button_frame = ttk.Frame(form_frame)
        button_frame.pack(fill=tk.X, pady=10)
        
        ttk.Button(
            button_frame,
            text="Add",
            command=self._add_transaction
        ).pack(side=tk.LEFT)
        
        ttk.Button(
            button_frame,
            text="Update",
            command=self._update_transaction
        ).pack(side=tk.LEFT)
        
        ttk.Button(
            button_frame,
            text="Delete",
            command=self._delete_transaction
        ).pack(side=tk.LEFT)
        
        # Bind selection
        self.transaction_tree.bind(
            "<Double-1>",
            self._on_transaction_selected
        )
        
        # Store selected transaction ID
        self.selected_transaction_id = None
    
    def _create_budgets_tab(self):
        """Create budgets tab."""
        tab = ttk.Frame(self.notebook)
        self.notebook.add(tab, text="Budgets")
        
        # Split into list and form
        list_frame = ttk.Frame(tab)
        list_frame.pack(side=tk.LEFT, fill=tk.BOTH, expand=True)
        
        form_frame = ttk.Frame(tab)
        form_frame.pack(side=tk.RIGHT, fill=tk.BOTH)
        
        # Budgets list
        columns = (
            "category",
            "amount",
            "period",
            "spent",
            "remaining"
        )
        self.budget_tree = ttk.Treeview(
            list_frame,
            columns=columns,
            show="headings"
        )
        
        self.budget_tree.heading(
            "category",
            text="Category"
        )
        self.budget_tree.heading("amount", text="Budget")
        self.budget_tree.heading("period", text="Period")
        self.budget_tree.heading("spent", text="Spent")
        self.budget_tree.heading(
            "remaining",
            text="Remaining"
        )
        
        scrollbar = ttk.Scrollbar(
            list_frame,
            orient=tk.VERTICAL,
            command=self.budget_tree.yview
        )
        
        self.budget_tree.configure(
            yscrollcommand=scrollbar.set
        )
        
        self.budget_tree.pack(
            side=tk.LEFT,
            fill=tk.BOTH,
            expand=True
        )
        scrollbar.pack(side=tk.RIGHT, fill=tk.Y)
        
        # Budget form
        ttk.Label(form_frame, text="Category:").pack()
        self.budget_category_var = tk.StringVar()
        ttk.Combobox(
            form_frame,
            textvariable=self.budget_category_var,
            values=[c.value for c in Category]
        ).pack(fill=tk.X)
        
        ttk.Label(form_frame, text="Amount:").pack()
        self.budget_amount_var = tk.StringVar()
        ttk.Entry(
            form_frame,
            textvariable=self.budget_amount_var
        ).pack(fill=tk.X)
        
        ttk.Label(form_frame, text="Period:").pack()
        self.budget_period_var = tk.StringVar()
        ttk.Combobox(
            form_frame,
            textvariable=self.budget_period_var,
            values=["monthly", "yearly"]
        ).pack(fill=tk.X)
        
        # Buttons
        button_frame = ttk.Frame(form_frame)
        button_frame.pack(fill=tk.X, pady=10)
        
        ttk.Button(
            button_frame,
            text="Set Budget",
            command=self._set_budget
        ).pack(side=tk.LEFT)
        
        ttk.Button(
            button_frame,
            text="Delete Budget",
            command=self._delete_budget
        ).pack(side=tk.LEFT)
        
        # Bind selection
        self.budget_tree.bind(
            "<Double-1>",
            self._on_budget_selected
        )
    
    def _create_reports_tab(self):
        """Create reports tab."""
        tab = ttk.Frame(self.notebook)
        self.notebook.add(tab, text="Reports")
        
        # Date range frame
        range_frame = ttk.LabelFrame(
            tab,
            text="Date Range"
        )
        range_frame.pack(fill=tk.X, padx=5, pady=5)
        
        ttk.Label(
            range_frame,
            text="From:"
        ).pack(side=tk.LEFT)
        
        self.from_date_var = tk.StringVar()
        ttk.Entry(
            range_frame,
            textvariable=self.from_date_var
        ).pack(side=tk.LEFT, padx=5)
        
        ttk.Label(
            range_frame,
            text="To:"
        ).pack(side=tk.LEFT)
        
        self.to_date_var = tk.StringVar()
        ttk.Entry(
            range_frame,
            textvariable=self.to_date_var
        ).pack(side=tk.LEFT, padx=5)
        
        ttk.Button(
            range_frame,
            text="Generate Report",
            command=self._generate_report
        ).pack(side=tk.LEFT, padx=5)
        
        # Report frame
        report_frame = ttk.Frame(tab)
        report_frame.pack(fill=tk.BOTH, expand=True)
        
        # Summary
        summary_frame = ttk.LabelFrame(
            report_frame,
            text="Summary"
        )
        summary_frame.pack(fill=tk.X, padx=5, pady=5)
        
        self.report_income_var = tk.StringVar()
        ttk.Label(
            summary_frame,
            text="Total Income:"
        ).pack(side=tk.LEFT)
        ttk.Label(
            summary_frame,
            textvariable=self.report_income_var
        ).pack(side=tk.LEFT, padx=10)
        
        self.report_expenses_var = tk.StringVar()
        ttk.Label(
            summary_frame,
            text="Total Expenses:"
        ).pack(side=tk.LEFT)
        ttk.Label(
            summary_frame,
            textvariable=self.report_expenses_var
        ).pack(side=tk.LEFT, padx=10)
        
        self.report_savings_var = tk.StringVar()
        ttk.Label(
            summary_frame,
            text="Net Savings:"
        ).pack(side=tk.LEFT)
        ttk.Label(
            summary_frame,
            textvariable=self.report_savings_var
        ).pack(side=tk.LEFT, padx=10)
        
        # Charts
        charts_frame = ttk.Frame(report_frame)
        charts_frame.pack(fill=tk.BOTH, expand=True)
        
        self.report_income_fig = plt.Figure(
            figsize=(6, 4)
        )
        self.report_income_canvas = FigureCanvasTkAgg(
            self.report_income_fig,
            master=charts_frame
        )
        self.report_income_canvas.get_tk_widget().pack(
            side=tk.LEFT,
            fill=tk.BOTH,
            expand=True
        )
        
        self.report_expenses_fig = plt.Figure(
            figsize=(6, 4)
        )
        self.report_expenses_canvas = FigureCanvasTkAgg(
            self.report_expenses_fig,
            master=charts_frame
        )
        self.report_expenses_canvas.get_tk_widget().pack(
            side=tk.RIGHT,
            fill=tk.BOTH,
            expand=True
        )
    
    def _load_data(self):
        """Load all data."""
        self._load_transactions()
        self._load_budgets()
        self._update_dashboard()
    
    def _load_transactions(self):
        """Load transactions into tree."""
        for item in self.transaction_tree.get_children():
            self.transaction_tree.delete(item)
            
        for transaction in self.storage.transactions.values():
            self.transaction_tree.insert(
                "",
                tk.END,
                values=(
                    transaction.id,
                    transaction.date.strftime("%Y-%m-%d"),
                    transaction.type.value,
                    transaction.category.value,
                    f"${transaction.amount:.2f}",
                    transaction.description
                )
            )
    
    def _load_budgets(self):
        """Load budgets into tree."""
        for item in self.budget_tree.get_children():
            self.budget_tree.delete(item)
            
        # Get current month's transactions
        today = date.today()
        start_date = date(today.year, today.month, 1)
        end_date = date(
            today.year,
            today.month,
            calendar.monthrange(
                today.year,
                today.month
            )[1]
        )
        
        transactions = self.storage.get_transactions(
            start_date=datetime.combine(
                start_date,
                datetime.min.time()
            ),
            end_date=datetime.combine(
                end_date,
                datetime.max.time()
            )
        )
        
        for category, budget in self.storage.budgets.items():
            spent = sum(
                t.amount for t in transactions
                if (t.category == category and
                    t.type == TransactionType.EXPENSE)
            )
            remaining = budget.amount - spent
            
            self.budget_tree.insert(
                "",
                tk.END,
                values=(
                    category.value,
                    f"${budget.amount:.2f}",
                    budget.period,
                    f"${spent:.2f}",
                    f"${remaining:.2f}"
                )
            )
    
    def _update_dashboard(self):
        """Update dashboard."""
        # Get current month's transactions
        today = date.today()
        start_date = date(today.year, today.month, 1)
        end_date = date(
            today.year,
            today.month,
            calendar.monthrange(
                today.year,
                today.month
            )[1]
        )
        
        report = self.storage.generate_report(
            start_date=datetime.combine(
                start_date,
                datetime.min.time()
            ),
            end_date=datetime.combine(
                end_date,
                datetime.max.time()
            )
        )
        
        # Update summary
        self.income_var.set(
            f"${report.total_income():.2f}"
        )
        self.expenses_var.set(
            f"${report.total_expense():.2f}"
        )
        self.savings_var.set(
            f"${report.net_savings():.2f}"
        )
        
        # Update charts
        self._update_charts(
            report.income_by_category(),
            report.expenses_by_category(),
            self.income_fig,
            self.expenses_fig,
            self.income_canvas,
            self.expenses_canvas
        )
    
    def _update_charts(
        self,
        income_data: dict,
        expense_data: dict,
        income_fig: plt.Figure,
        expense_fig: plt.Figure,
        income_canvas: FigureCanvasTkAgg,
        expense_canvas: FigureCanvasTkAgg
    ):
        """Update pie charts."""
        # Clear figures
        income_fig.clear()
        expense_fig.clear()
        
        # Income chart
        if income_data:
            ax = income_fig.add_subplot(111)
            labels = [
                c.value for c in income_data.keys()
            ]
            values = list(income_data.values())
            ax.pie(
                values,
                labels=labels,
                autopct='%1.1f%%'
            )
            ax.set_title("Income Distribution")
        
        # Expense chart
        if expense_data:
            ax = expense_fig.add_subplot(111)
            labels = [
                c.value for c in expense_data.keys()
            ]
            values = list(expense_data.values())
            ax.pie(
                values,
                labels=labels,
                autopct='%1.1f%%'
            )
            ax.set_title("Expense Distribution")
        
        # Redraw
        income_canvas.draw()
        expense_canvas.draw()
    
    def _add_transaction(self):
        """Add new transaction."""
        try:
            transaction_data = {
                "type": self.type_var.get(),
                "category": self.category_var.get(),
                "amount": self.amount_var.get(),
                "description": self.description_var.get(),
                "date": self.date_var.get(),
                "tags": [
                    t.strip()
                    for t in self.tags_var.get().split(",")
                    if t.strip()
                ]
            }
            
            if self.recurring_var.get():
                transaction_data["recurring"] = True
                transaction_data["recurring_period"] = (
                    self.period_var.get()
                )
            
            self.storage.add_transaction(transaction_data)
            self._clear_transaction_form()
            self._load_data()
            
        except Exception as e:
            messagebox.showerror(
                "Error",
                f"Failed to add transaction: {str(e)}"
            )
    
    def _update_transaction(self):
        """Update selected transaction."""
        if not self.selected_transaction_id:
            messagebox.showwarning(
                "Warning",
                "Please select a transaction to update"
            )
            return
            
        try:
            transaction_data = {
                "type": self.type_var.get(),
                "category": self.category_var.get(),
                "amount": self.amount_var.get(),
                "description": self.description_var.get(),
                "date": self.date_var.get(),
                "tags": [
                    t.strip()
                    for t in self.tags_var.get().split(",")
                    if t.strip()
                ]
            }
            
            if self.recurring_var.get():
                transaction_data["recurring"] = True
                transaction_data["recurring_period"] = (
                    self.period_var.get()
                )
            
            self.storage.update_transaction(
                self.selected_transaction_id,
                transaction_data
            )
            self._clear_transaction_form()
            self._load_data()
            
        except Exception as e:
            messagebox.showerror(
                "Error",
                f"Failed to update transaction: {str(e)}"
            )
    
    def _delete_transaction(self):
        """Delete selected transaction."""
        if not self.selected_transaction_id:
            messagebox.showwarning(
                "Warning",
                "Please select a transaction to delete"
            )
            return
            
        if messagebox.askyesno(
            "Confirm",
            "Delete this transaction?"
        ):
            self.storage.delete_transaction(
                self.selected_transaction_id
            )
            self._clear_transaction_form()
            self._load_data()
    
    def _set_budget(self):
        """Set budget for category."""
        try:
            budget_data = {
                "category": self.budget_category_var.get(),
                "amount": self.budget_amount_var.get(),
                "period": self.budget_period_var.get(),
                "start_date": datetime.now().isoformat()
            }
            
            self.storage.set_budget(budget_data)
            self._clear_budget_form()
            self._load_data()
            
        except Exception as e:
            messagebox.showerror(
                "Error",
                f"Failed to set budget: {str(e)}"
            )
    
    def _delete_budget(self):
        """Delete selected budget."""
        selection = self.budget_tree.selection()
        if not selection:
            messagebox.showwarning(
                "Warning",
                "Please select a budget to delete"
            )
            return
            
        category = Category(
            self.budget_tree.item(selection[0])["values"][0]
        )
        
        if messagebox.askyesno(
            "Confirm",
            f"Delete budget for {category.value}?"
        ):
            self.storage.delete_budget(category)
            self._clear_budget_form()
            self._load_data()
    
    def _generate_report(self):
        """Generate financial report."""
        try:
            start_date = datetime.fromisoformat(
                self.from_date_var.get()
            )
            end_date = datetime.fromisoformat(
                self.to_date_var.get()
            )
            
            report = self.storage.generate_report(
                start_date,
                end_date
            )
            
            # Update summary
            self.report_income_var.set(
                f"${report.total_income():.2f}"
            )
            self.report_expenses_var.set(
                f"${report.total_expense():.2f}"
            )
            self.report_savings_var.set(
                f"${report.net_savings():.2f}"
            )
            
            # Update charts
            self._update_charts(
                report.income_by_category(),
                report.expenses_by_category(),
                self.report_income_fig,
                self.report_expenses_fig,
                self.report_income_canvas,
                self.report_expenses_canvas
            )
            
        except Exception as e:
            messagebox.showerror(
                "Error",
                f"Failed to generate report: {str(e)}"
            )
    
    def _on_transaction_selected(self, event):
        """Handle transaction selection."""
        selection = self.transaction_tree.selection()
        if not selection:
            return
            
        # Get transaction ID
        transaction_id = int(
            self.transaction_tree.item(selection[0])["values"][0]
        )
        
        # Get transaction
        transaction = self.storage.get_transaction(
            transaction_id
        )
        if not transaction:
            return
            
        # Update form
        self.selected_transaction_id = transaction_id
        self.type_var.set(transaction.type.value)
        self.category_var.set(transaction.category.value)
        self.amount_var.set(str(transaction.amount))
        self.description_var.set(transaction.description)
        self.date_var.set(
            transaction.date.strftime("%Y-%m-%d")
        )
        self.tags_var.set(",".join(transaction.tags))
        self.recurring_var.set(transaction.recurring)
        if transaction.recurring:
            self.period_var.set(transaction.recurring_period)
    
    def _on_budget_selected(self, event):
        """Handle budget selection."""
        selection = self.budget_tree.selection()
        if not selection:
            return
            
        # Get category
        category = Category(
            self.budget_tree.item(selection[0])["values"][0]
        )
        
        # Get budget
        budget = self.storage.get_budget(category)
        if not budget:
            return
            
        # Update form
        self.budget_category_var.set(budget.category.value)
        self.budget_amount_var.set(str(budget.amount))
        self.budget_period_var.set(budget.period)
    
    def _clear_transaction_form(self):
        """Clear transaction form."""
        self.selected_transaction_id = None
        self.type_var.set("")
        self.category_var.set("")
        self.amount_var.set("")
        self.description_var.set("")
        self.date_var.set("")
        self.tags_var.set("")
        self.recurring_var.set(False)
        self.period_var.set("")
    
    def _clear_budget_form(self):
        """Clear budget form."""
        self.budget_category_var.set("")
        self.budget_amount_var.set("")
        self.budget_period_var.set("") 