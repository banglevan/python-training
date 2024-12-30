"""
Task manager UI
"""

import tkinter as tk
from tkinter import ttk, messagebox
from datetime import datetime

from .models import TaskStatus, TaskPriority
from .storage import TaskStorage

class TaskManagerUI:
    """Task manager UI."""
    
    def __init__(self, storage: TaskStorage):
        """Initialize UI."""
        self.storage = storage
        
        # Create main window
        self.root = tk.Tk()
        self.root.title("Task Manager")
        self.root.geometry("800x600")
        
        # Create widgets
        self._create_task_list()
        self._create_task_form()
        self._create_filters()
        
        # Load tasks
        self._load_tasks()
    
    def run(self):
        """Run UI."""
        self.root.mainloop()
    
    def _create_task_list(self):
        """Create task list."""
        # Create frame
        frame = ttk.Frame(self.root)
        frame.pack(side=tk.LEFT, fill=tk.BOTH, expand=True)
        
        # Create treeview
        columns = (
            "id",
            "title",
            "status",
            "priority",
            "due_date"
        )
        
        self.task_tree = ttk.Treeview(
            frame,
            columns=columns,
            show="headings"
        )
        
        # Add headings
        self.task_tree.heading("id", text="ID")
        self.task_tree.heading("title", text="Title")
        self.task_tree.heading("status", text="Status")
        self.task_tree.heading("priority", text="Priority")
        self.task_tree.heading("due_date", text="Due Date")
        
        # Add scrollbar
        scrollbar = ttk.Scrollbar(
            frame,
            orient=tk.VERTICAL,
            command=self.task_tree.yview
        )
        
        self.task_tree.configure(
            yscrollcommand=scrollbar.set
        )
        
        # Pack widgets
        self.task_tree.pack(
            side=tk.LEFT,
            fill=tk.BOTH,
            expand=True
        )
        scrollbar.pack(side=tk.RIGHT, fill=tk.Y)
        
        # Bind events
        self.task_tree.bind(
            "<Double-1>",
            self._on_task_selected
        )
    
    def _create_task_form(self):
        """Create task form."""
        # Create frame
        frame = ttk.Frame(self.root)
        frame.pack(side=tk.RIGHT, fill=tk.BOTH, padx=10)
        
        # Add fields
        ttk.Label(frame, text="Title:").pack()
        self.title_var = tk.StringVar()
        ttk.Entry(
            frame,
            textvariable=self.title_var
        ).pack(fill=tk.X)
        
        ttk.Label(frame, text="Description:").pack()
        self.desc_text = tk.Text(frame, height=5)
        self.desc_text.pack(fill=tk.X)
        
        ttk.Label(frame, text="Status:").pack()
        self.status_var = tk.StringVar()
        ttk.Combobox(
            frame,
            textvariable=self.status_var,
            values=[s.value for s in TaskStatus]
        ).pack(fill=tk.X)
        
        ttk.Label(frame, text="Priority:").pack()
        self.priority_var = tk.StringVar()
        ttk.Combobox(
            frame,
            textvariable=self.priority_var,
            values=[p.value for p in TaskPriority]
        ).pack(fill=tk.X)
        
        ttk.Label(frame, text="Due Date:").pack()
        self.due_date_var = tk.StringVar()
        ttk.Entry(
            frame,
            textvariable=self.due_date_var
        ).pack(fill=tk.X)
        
        ttk.Label(frame, text="Tags:").pack()
        self.tags_var = tk.StringVar()
        ttk.Entry(
            frame,
            textvariable=self.tags_var
        ).pack(fill=tk.X)
        
        # Add buttons
        button_frame = ttk.Frame(frame)
        button_frame.pack(fill=tk.X, pady=10)
        
        ttk.Button(
            button_frame,
            text="Add",
            command=self._add_task
        ).pack(side=tk.LEFT)
        
        ttk.Button(
            button_frame,
            text="Update",
            command=self._update_task
        ).pack(side=tk.LEFT)
        
        ttk.Button(
            button_frame,
            text="Delete",
            command=self._delete_task
        ).pack(side=tk.LEFT)
        
        # Store selected task ID
        self.selected_task_id = None
    
    def _create_filters(self):
        """Create filters."""
        frame = ttk.Frame(self.root)
        frame.pack(fill=tk.X, padx=10, pady=5)
        
        # Status filter
        ttk.Label(frame, text="Filter by:").pack(
            side=tk.LEFT
        )
        
        self.filter_var = tk.StringVar()
        filter_combo = ttk.Combobox(
            frame,
            textvariable=self.filter_var,
            values=["All", "Overdue"] + 
                   [s.value for s in TaskStatus]
        )
        filter_combo.pack(side=tk.LEFT, padx=5)
        filter_combo.set("All")
        
        # Bind event
        filter_combo.bind(
            "<<ComboboxSelected>>",
            lambda _: self._load_tasks()
        )
    
    def _load_tasks(self):
        """Load tasks into tree."""
        # Clear tree
        for item in self.task_tree.get_children():
            self.task_tree.delete(item)
        
        # Get tasks based on filter
        filter_value = self.filter_var.get()
        if filter_value == "All":
            tasks = self.storage.get_all_tasks()
        elif filter_value == "Overdue":
            tasks = self.storage.get_overdue_tasks()
        else:
            tasks = self.storage.get_tasks_by_status(
                TaskStatus(filter_value)
            )
        
        # Add tasks to tree
        for task in tasks:
            self.task_tree.insert(
                "",
                tk.END,
                values=(
                    task.id,
                    task.title,
                    task.status.value,
                    task.priority.value,
                    task.due_date.strftime("%Y-%m-%d")
                )
            )
    
    def _add_task(self):
        """Add new task."""
        try:
            task_data = {
                "title": self.title_var.get(),
                "description": self.desc_text.get(
                    "1.0",
                    tk.END
                ).strip(),
                "status": self.status_var.get(),
                "priority": self.priority_var.get(),
                "due_date": self.due_date_var.get(),
                "tags": [
                    t.strip()
                    for t in self.tags_var.get().split(",")
                    if t.strip()
                ]
            }
            
            self.storage.add_task(task_data)
            self._clear_form()
            self._load_tasks()
            
        except Exception as e:
            messagebox.showerror(
                "Error",
                f"Failed to add task: {str(e)}"
            )
    
    def _update_task(self):
        """Update selected task."""
        if not self.selected_task_id:
            messagebox.showwarning(
                "Warning",
                "Please select a task to update"
            )
            return
            
        try:
            task_data = {
                "title": self.title_var.get(),
                "description": self.desc_text.get(
                    "1.0",
                    tk.END
                ).strip(),
                "status": self.status_var.get(),
                "priority": self.priority_var.get(),
                "due_date": self.due_date_var.get(),
                "tags": [
                    t.strip()
                    for t in self.tags_var.get().split(",")
                    if t.strip()
                ]
            }
            
            self.storage.update_task(
                self.selected_task_id,
                task_data
            )
            self._clear_form()
            self._load_tasks()
            
        except Exception as e:
            messagebox.showerror(
                "Error",
                f"Failed to update task: {str(e)}"
            )
    
    def _delete_task(self):
        """Delete selected task."""
        if not self.selected_task_id:
            messagebox.showwarning(
                "Warning",
                "Please select a task to delete"
            )
            return
            
        if messagebox.askyesno(
            "Confirm",
            "Are you sure you want to delete this task?"
        ):
            self.storage.delete_task(self.selected_task_id)
            self._clear_form()
            self._load_tasks()
    
    def _on_task_selected(self, event):
        """Handle task selection."""
        selection = self.task_tree.selection()
        if not selection:
            return
            
        # Get task ID
        task_id = int(
            self.task_tree.item(selection[0])["values"][0]
        )
        
        # Get task
        task = self.storage.get_task(task_id)
        if not task:
            return
            
        # Update form
        self.selected_task_id = task_id
        self.title_var.set(task.title)
        self.desc_text.delete("1.0", tk.END)
        self.desc_text.insert("1.0", task.description)
        self.status_var.set(task.status.value)
        self.priority_var.set(task.priority.value)
        self.due_date_var.set(
            task.due_date.strftime("%Y-%m-%d")
        )
        self.tags_var.set(",".join(task.tags))
    
    def _clear_form(self):
        """Clear form fields."""
        self.selected_task_id = None
        self.title_var.set("")
        self.desc_text.delete("1.0", tk.END)
        self.status_var.set("")
        self.priority_var.set("")
        self.due_date_var.set("")
        self.tags_var.set("") 