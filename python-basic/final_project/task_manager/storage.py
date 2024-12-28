"""
Task storage module
"""

import json
from datetime import datetime
from pathlib import Path
from typing import Dict, List, Optional

from .models import Task, TaskStatus, TaskPriority

class TaskStorage:
    """Task storage."""
    
    def __init__(self, file_path: str):
        """
        Initialize storage.
        
        Args:
            file_path: Path to storage file
        """
        self.file_path = Path(file_path)
        self.tasks: Dict[int, Task] = {}
        self._next_id = 1
        self._load()
    
    def add_task(self, task_data: dict) -> Task:
        """Add new task."""
        task = Task(
            id=self._next_id,
            title=task_data["title"],
            description=task_data["description"],
            status=TaskStatus(task_data.get("status", "todo")),
            priority=TaskPriority(task_data.get("priority", "medium")),
            due_date=datetime.fromisoformat(task_data["due_date"]),
            created_at=datetime.now(),
            updated_at=datetime.now(),
            tags=task_data.get("tags", [])
        )
        
        self.tasks[task.id] = task
        self._next_id += 1
        self._save()
        
        return task
    
    def get_task(self, task_id: int) -> Optional[Task]:
        """Get task by ID."""
        return self.tasks.get(task_id)
    
    def update_task(
        self,
        task_id: int,
        task_data: dict
    ) -> Optional[Task]:
        """Update task."""
        task = self.get_task(task_id)
        if not task:
            return None
            
        task.update(
            title=task_data.get("title"),
            description=task_data.get("description"),
            status=TaskStatus(task_data["status"])
            if "status" in task_data else None,
            priority=TaskPriority(task_data["priority"])
            if "priority" in task_data else None,
            due_date=datetime.fromisoformat(task_data["due_date"])
            if "due_date" in task_data else None,
            tags=task_data.get("tags")
        )
        
        self._save()
        return task
    
    def delete_task(self, task_id: int) -> bool:
        """Delete task."""
        if task_id not in self.tasks:
            return False
            
        del self.tasks[task_id]
        self._save()
        return True
    
    def get_all_tasks(self) -> List[Task]:
        """Get all tasks."""
        return list(self.tasks.values())
    
    def get_tasks_by_status(
        self,
        status: TaskStatus
    ) -> List[Task]:
        """Get tasks by status."""
        return [
            task for task in self.tasks.values()
            if task.status == status
        ]
    
    def get_overdue_tasks(self) -> List[Task]:
        """Get overdue tasks."""
        return [
            task for task in self.tasks.values()
            if task.is_overdue()
        ]
    
    def _load(self):
        """Load tasks from file."""
        if not self.file_path.exists():
            return
            
        with open(self.file_path, "r") as f:
            data = json.load(f)
            
        self._next_id = data["next_id"]
        
        for task_data in data["tasks"]:
            task = Task(
                id=task_data["id"],
                title=task_data["title"],
                description=task_data["description"],
                status=TaskStatus(task_data["status"]),
                priority=TaskPriority(task_data["priority"]),
                due_date=datetime.fromisoformat(
                    task_data["due_date"]
                ),
                created_at=datetime.fromisoformat(
                    task_data["created_at"]
                ),
                updated_at=datetime.fromisoformat(
                    task_data["updated_at"]
                ),
                completed_at=datetime.fromisoformat(
                    task_data["completed_at"]
                ) if task_data.get("completed_at") else None,
                tags=task_data.get("tags", [])
            )
            self.tasks[task.id] = task
    
    def _save(self):
        """Save tasks to file."""
        data = {
            "next_id": self._next_id,
            "tasks": []
        }
        
        for task in self.tasks.values():
            task_data = {
                "id": task.id,
                "title": task.title,
                "description": task.description,
                "status": task.status.value,
                "priority": task.priority.value,
                "due_date": task.due_date.isoformat(),
                "created_at": task.created_at.isoformat(),
                "updated_at": task.updated_at.isoformat(),
                "tags": task.tags
            }
            
            if task.completed_at:
                task_data["completed_at"] = (
                    task.completed_at.isoformat()
                )
                
            data["tasks"].append(task_data)
        
        with open(self.file_path, "w") as f:
            json.dump(data, f, indent=2) 