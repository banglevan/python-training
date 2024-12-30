"""
Unit tests cho Task Manager
Tiêu chí đánh giá:
1. Task Operations (40%): CRUD operations
2. File Storage (30%): Data persistence
3. Task Status (30%): Status tracking
"""

import pytest
from datetime import datetime, timedelta
from pathlib import Path

from task_manager.models import (
    Task,
    TaskStatus,
    TaskPriority
)
from task_manager.storage import TaskStorage

@pytest.fixture
def temp_file(tmp_path):
    """Fixture cho temporary file."""
    return tmp_path / "tasks.json"

@pytest.fixture
def storage(temp_file):
    """Fixture cho storage."""
    return TaskStorage(str(temp_file))

class TestTaskOperations:
    """Test task operations (40%)."""
    
    def test_add_task(self, storage):
        """Test adding task."""
        task_data = {
            "title": "Test Task",
            "description": "Test Description",
            "status": "todo",
            "priority": "medium",
            "due_date": datetime.now().isoformat(),
            "tags": ["test", "demo"]
        }
        
        task = storage.add_task(task_data)
        assert task.id == 1
        assert task.title == "Test Task"
        assert task.status == TaskStatus.TODO
        assert task.priority == TaskPriority.MEDIUM
        
        # Verify persistence
        loaded = storage.get_task(1)
        assert loaded is not None
        assert loaded.title == task.title
    
    def test_update_task(self, storage):
        """Test updating task."""
        # Add task
        task = storage.add_task({
            "title": "Original",
            "description": "Original",
            "due_date": datetime.now().isoformat()
        })
        
        # Update task
        updated = storage.update_task(
            task.id,
            {
                "title": "Updated",
                "status": "in_progress"
            }
        )
        
        assert updated is not None
        assert updated.title == "Updated"
        assert updated.status == TaskStatus.IN_PROGRESS
        
        # Verify persistence
        loaded = storage.get_task(task.id)
        assert loaded.title == "Updated"
    
    def test_delete_task(self, storage):
        """Test deleting task."""
        # Add task
        task = storage.add_task({
            "title": "To Delete",
            "description": "To Delete",
            "due_date": datetime.now().isoformat()
        })
        
        # Delete task
        assert storage.delete_task(task.id)
        assert storage.get_task(task.id) is None

class TestFileStorage:
    """Test file storage (30%)."""
    
    def test_persistence(self, temp_file):
        """Test data persistence."""
        # Create storage and add task
        storage1 = TaskStorage(str(temp_file))
        task = storage1.add_task({
            "title": "Persistent",
            "description": "Persistent",
            "due_date": datetime.now().isoformat()
        })
        
        # Create new storage instance
        storage2 = TaskStorage(str(temp_file))
        loaded = storage2.get_task(task.id)
        
        assert loaded is not None
        assert loaded.title == task.title
    
    def test_file_format(self, storage, temp_file):
        """Test file format."""
        # Add task
        storage.add_task({
            "title": "Test",
            "description": "Test",
            "due_date": datetime.now().isoformat()
        })
        
        # Verify file exists and is JSON
        assert temp_file.exists()
        with open(temp_file) as f:
            content = f.read()
            assert content.startswith("{")
            assert content.endswith("}")

class TestTaskStatus:
    """Test task status (30%)."""
    
    def test_status_changes(self, storage):
        """Test status transitions."""
        # Add task
        task = storage.add_task({
            "title": "Status Test",
            "description": "Status Test",
            "due_date": datetime.now().isoformat()
        })
        assert task.status == TaskStatus.TODO
        
        # Update status
        storage.update_task(
            task.id,
            {"status": "in_progress"}
        )
        task = storage.get_task(task.id)
        assert task.status == TaskStatus.IN_PROGRESS
        
        storage.update_task(
            task.id,
            {"status": "done"}
        )
        task = storage.get_task(task.id)
        assert task.status == TaskStatus.DONE
        assert task.completed_at is not None
    
    def test_overdue_detection(self, storage):
        """Test overdue detection."""
        # Add overdue task
        yesterday = datetime.now() - timedelta(days=1)
        task = storage.add_task({
            "title": "Overdue",
            "description": "Overdue",
            "due_date": yesterday.isoformat()
        })
        
        assert task.is_overdue()
        
        # Add future task
        tomorrow = datetime.now() + timedelta(days=1)
        task = storage.add_task({
            "title": "Future",
            "description": "Future",
            "due_date": tomorrow.isoformat()
        })
        
        assert not task.is_overdue() 