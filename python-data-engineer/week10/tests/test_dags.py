"""
Test Airflow DAGs.
"""

import unittest
from unittest.mock import Mock, patch
from datetime import datetime
from airflow.models import DagBag
from airflow.utils.session import create_session
from airflow.utils.state import State

class TestUserProcessingDAG(unittest.TestCase):
    """Test user processing DAG."""
    
    @classmethod
    def setUpClass(cls):
        """Set up test DAG."""
        cls.dagbag = DagBag(
            dag_folder='../exercises',
            include_examples=False
        )
    
    def setUp(self):
        """Set up test fixtures."""
        self.dag_id = 'user_processing'
        self.dag = self.dagbag.get_dag(self.dag_id)
        # Clear any previous state
        self.dag.clear()
    
    def test_dag_loaded(self):
        """Test DAG loading."""
        self.assertIsNotNone(self.dag)
        self.assertEqual(len(self.dagbag.import_errors), 0)
    
    def test_task_count(self):
        """Test number of tasks in DAG."""
        self.assertEqual(len(self.dag.tasks), 6)
    
    def test_dependencies(self):
        """Test task dependencies."""
        task_ids = [
            'create_table',
            'is_api_available',
            'extract_user',
            'process_user',
            'store_user',
            'generate_report'
        ]
        
        for idx, task_id in enumerate(task_ids[:-1]):
            self.assertIn(
                self.dag.get_task(task_ids[idx + 1]),
                self.dag.get_task(task_id).downstream_list
            )
    
    def test_default_args(self):
        """Test DAG default arguments."""
        self.assertEqual(self.dag.default_args['owner'], 'airflow')
        self.assertEqual(self.dag.default_args['retries'], 3)
    
    @patch('airflow.providers.postgres.operators.postgres.PostgresOperator.execute')
    def test_create_table(self, mock_execute):
        """Test create table task."""
        task = self.dag.get_task('create_table')
        task.execute(context={})
        mock_execute.assert_called_once()
    
    @patch('airflow.providers.http.sensors.http.HttpSensor.poke')
    def test_api_sensor(self, mock_poke):
        """Test API sensor."""
        mock_poke.return_value = True
        task = self.dag.get_task('is_api_available')
        result = task.execute(context={})
        self.assertTrue(result)
    
    @patch('airflow.providers.http.operators.http.SimpleHttpOperator.execute')
    def test_extract_user(self, mock_execute):
        """Test user extraction."""
        mock_execute.return_value = {
            'id': 1,
            'username': 'test_user',
            'email': 'test@example.com'
        }
        task = self.dag.get_task('extract_user')
        result = task.execute(context={})
        self.assertEqual(result['username'], 'test_user')

class TestDAGIntegration(unittest.TestCase):
    """Test DAG integration."""
    
    def setUp(self):
        """Set up test environment."""
        self.dag_id = 'user_processing'
        self.dagbag = DagBag(
            dag_folder='../exercises',
            include_examples=False
        )
        self.dag = self.dagbag.get_dag(self.dag_id)
        self.dag.clear()
    
    def test_dag_structure(self):
        """Test overall DAG structure."""
        self.assertIsNotNone(self.dag)
        self.assertEqual(self.dag.schedule_interval, '@daily')
        self.assertFalse(self.dag.catchup)
    
    def test_dag_execution(self):
        """Test DAG execution."""
        execution_date = datetime(2024, 1, 1)
        
        with create_session() as session:
            # Run the DAG
            dagrun = self.dag.create_dagrun(
                state=State.RUNNING,
                execution_date=execution_date,
                run_id=f"test_{execution_date.isoformat()}",
                session=session
            )
            
            # Check task states
            tasks = dagrun.get_task_instances()
            self.assertEqual(len(tasks), 6) 