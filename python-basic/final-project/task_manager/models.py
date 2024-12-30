"""
Task management models
"""

from dataclasses import dataclass
from datetime import datetime
from enum import Enum
from typing import List, Optional

class TaskStatus(Enum):
    """Task status."""
    TODO = "todo"
    IN_PROGRESS = "in_progress"
    DONE = "done"
    CANCELLED = "cancelled"

class TaskPriority(Enum):
    """Task priority."""
    LOW = "low"
    MEDIUM = "medium"
    HIGH = "high"

@dataclass
class Task:
    """Task model."""
    id: int
    title: str
    description: str
    status: TaskStatus
    priority: TaskPriority
    due_date: datetime
    created_at: datetime
    updated_at: datetime
    completed_at: Optional[datetime] = None
    tags: List[str] = None
    
    def __post_init__(self):
        """Post init."""
        if self.tags is None:
            self.tags = []
    
    def update(
        self,
        title: Optional[str] = None,
        description: Optional[str] = None,
        status: Optional[TaskStatus] = None,
        priority: Optional[TaskPriority] = None,
        due_date: Optional[datetime] = None,
        tags: Optional[List[str]] = None
    ):
        """Update task."""
        if title:
            self.title = title
        if description:
            self.description = description
        if status:
            self.status = status
            if status == TaskStatus.DONE:
                self.completed_at = datetime.now()
        if priority:
            self.priority = priority
        if due_date:
            self.due_date = due_date
        if tags:
            self.tags = tags
        self.updated_at = datetime.now()
    
    def is_overdue(self) -> bool:
        """Check if task is overdue."""
        if self.status == TaskStatus.DONE:
            return False
        return datetime.now() > self.due_date 