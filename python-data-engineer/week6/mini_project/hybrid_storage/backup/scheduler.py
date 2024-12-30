"""
Backup Scheduler
-------------

Manages backup scheduling with:
1. Cron-style scheduling
2. Concurrent execution
3. Error handling
4. Retry logic
"""

from typing import Dict, Any, Optional, List, Callable
import logging
from datetime import datetime, timedelta
import asyncio
from apscheduler.schedulers.asyncio import AsyncIOScheduler
from apscheduler.triggers.cron import CronTrigger
from apscheduler.events import EVENT_JOB_ERROR, EVENT_JOB_EXECUTED
import json
import sqlite3
from pathlib import Path

logger = logging.getLogger(__name__)

class BackupScheduler:
    """Backup job scheduler."""
    
    def __init__(
        self,
        config: Dict[str, Any]
    ):
        """Initialize backup scheduler."""
        self.scheduler = AsyncIOScheduler()
        self.db_path = Path('backup_schedule.db')
        self.max_retries = config.get('max_retries', 3)
        self.retry_delay = config.get('retry_delay', 300)
        
        # Initialize database
        self._init_database()
        
        # Add event listeners
        self.scheduler.add_listener(
            self._handle_job_error,
            EVENT_JOB_ERROR
        )
        self.scheduler.add_listener(
            self._handle_job_success,
            EVENT_JOB_EXECUTED
        )
        
        logger.info("Initialized BackupScheduler")
    
    def add_job(
        self,
        name: str,
        schedule: str,
        func: Callable,
        args: Optional[List] = None,
        kwargs: Optional[Dict] = None
    ):
        """Add backup job to scheduler."""
        try:
            # Parse cron schedule
            trigger = CronTrigger.from_crontab(schedule)
            
            # Add job
            self.scheduler.add_job(
                func,
                trigger=trigger,
                args=args or [],
                kwargs=kwargs or {},
                id=name,
                name=name,
                replace_existing=True
            )
            
            # Store job info
            self._store_job_info(
                name,
                schedule,
                args,
                kwargs
            )
            
            logger.info(f"Added backup job: {name}")
            
        except Exception as e:
            logger.error(f"Failed to add job {name}: {e}")
            raise
    
    def remove_job(
        self,
        name: str
    ):
        """Remove backup job."""
        try:
            # Remove from scheduler
            self.scheduler.remove_job(name)
            
            # Remove from database
            with sqlite3.connect(self.db_path) as conn:
                cursor = conn.cursor()
                cursor.execute(
                    "DELETE FROM jobs WHERE name = ?",
                    (name,)
                )
            
            logger.info(f"Removed backup job: {name}")
            
        except Exception as e:
            logger.error(
                f"Failed to remove job {name}: {e}"
            )
            raise
    
    def list_jobs(self) -> List[Dict[str, Any]]:
        """List scheduled backup jobs."""
        try:
            with sqlite3.connect(self.db_path) as conn:
                cursor = conn.cursor()
                cursor.execute(
                    """
                    SELECT name, schedule, args, kwargs,
                           last_run, next_run, status,
                           error_count
                    FROM jobs
                    """
                )
                
                return [
                    {
                        'name': row[0],
                        'schedule': row[1],
                        'args': json.loads(row[2]),
                        'kwargs': json.loads(row[3]),
                        'last_run': row[4],
                        'next_run': row[5],
                        'status': row[6],
                        'error_count': row[7]
                    }
                    for row in cursor.fetchall()
                ]
            
        except Exception as e:
            logger.error("Failed to list jobs: {e}")
            raise
    
    def start(self):
        """Start scheduler."""
        self.scheduler.start()
        logger.info("Backup scheduler started")
    
    def stop(self):
        """Stop scheduler."""
        self.scheduler.shutdown()
        logger.info("Backup scheduler stopped")
    
    def _init_database(self):
        """Initialize SQLite database."""
        try:
            with sqlite3.connect(self.db_path) as conn:
                cursor = conn.cursor()
                
                cursor.execute(
                    """
                    CREATE TABLE IF NOT EXISTS jobs (
                        name TEXT PRIMARY KEY,
                        schedule TEXT NOT NULL,
                        args TEXT NOT NULL,
                        kwargs TEXT NOT NULL,
                        last_run TEXT,
                        next_run TEXT,
                        status TEXT,
                        error_count INTEGER DEFAULT 0,
                        last_error TEXT
                    )
                    """
                )
            
        except Exception as e:
            logger.error(
                f"Database initialization failed: {e}"
            )
            raise
    
    def _store_job_info(
        self,
        name: str,
        schedule: str,
        args: Optional[List],
        kwargs: Optional[Dict]
    ):
        """Store job information in database."""
        try:
            with sqlite3.connect(self.db_path) as conn:
                cursor = conn.cursor()
                
                cursor.execute(
                    """
                    INSERT OR REPLACE INTO jobs
                    (name, schedule, args, kwargs, status)
                    VALUES (?, ?, ?, ?, ?)
                    """,
                    (
                        name,
                        schedule,
                        json.dumps(args or []),
                        json.dumps(kwargs or {}),
                        'scheduled'
                    )
                )
            
        except Exception as e:
            logger.error(
                f"Failed to store job info: {e}"
            )
            raise
    
    def _handle_job_error(
        self,
        event
    ):
        """Handle job execution error."""
        try:
            job_id = event.job_id
            exception = event.exception
            
            with sqlite3.connect(self.db_path) as conn:
                cursor = conn.cursor()
                
                # Update error count and status
                cursor.execute(
                    """
                    UPDATE jobs
                    SET error_count = error_count + 1,
                        last_error = ?,
                        status = ?,
                        last_run = ?
                    WHERE name = ?
                    """,
                    (
                        str(exception),
                        'failed',
                        datetime.now().isoformat(),
                        job_id
                    )
                )
                
                # Check retry count
                cursor.execute(
                    """
                    SELECT error_count
                    FROM jobs
                    WHERE name = ?
                    """,
                    (job_id,)
                )
                error_count = cursor.fetchone()[0]
                
                if error_count < self.max_retries:
                    # Schedule retry
                    next_run = datetime.now() + \
                              timedelta(seconds=self.retry_delay)
                    
                    cursor.execute(
                        """
                        UPDATE jobs
                        SET next_run = ?,
                            status = ?
                        WHERE name = ?
                        """,
                        (
                            next_run.isoformat(),
                            'retry_scheduled',
                            job_id
                        )
                    )
                else:
                    cursor.execute(
                        """
                        UPDATE jobs
                        SET status = ?
                        WHERE name = ?
                        """,
                        ('max_retries_reached', job_id)
                    )
            
            logger.error(
                f"Backup job {job_id} failed: {exception}"
            )
            
        except Exception as e:
            logger.error(
                f"Error handling job failure: {e}"
            )
    
    def _handle_job_success(
        self,
        event
    ):
        """Handle successful job execution."""
        try:
            job_id = event.job_id
            
            with sqlite3.connect(self.db_path) as conn:
                cursor = conn.cursor()
                
                # Update job status
                cursor.execute(
                    """
                    UPDATE jobs
                    SET error_count = 0,
                        last_error = NULL,
                        status = ?,
                        last_run = ?,
                        next_run = ?
                    WHERE name = ?
                    """,
                    (
                        'completed',
                        datetime.now().isoformat(),
                        event.scheduled_run_time.isoformat(),
                        job_id
                    )
                )
            
            logger.info(f"Backup job {job_id} completed")
            
        except Exception as e:
            logger.error(
                f"Error handling job success: {e}"
            ) 