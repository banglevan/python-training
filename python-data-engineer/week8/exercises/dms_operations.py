"""
DMS Operations
----------

Practice with:
1. Migration tasks
2. Replication
3. Monitoring
"""

import logging
from typing import Dict, Any, Optional, List
import time
from datetime import datetime
import boto3
from botocore.exceptions import ClientError

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

class DMSOperations:
    """AWS DMS operations handler."""
    
    def __init__(
        self,
        region: str,
        aws_access_key: Optional[str] = None,
        aws_secret_key: Optional[str] = None
    ):
        """Initialize DMS client."""
        self.client = boto3.client(
            'dms',
            region_name=region,
            aws_access_key_id=aws_access_key,
            aws_secret_access_key=aws_secret_key
        )
    
    def create_replication_instance(
        self,
        instance_id: str,
        instance_class: str,
        allocated_storage: int = 50,
        **kwargs
    ) -> Optional[Dict[str, Any]]:
        """Create replication instance."""
        try:
            response = self.client.create_replication_instance(
                ReplicationInstanceIdentifier=instance_id,
                ReplicationInstanceClass=instance_class,
                AllocatedStorage=allocated_storage,
                **kwargs
            )
            
            logger.info(
                f"Created replication instance: {instance_id}"
            )
            return response['ReplicationInstance']
            
        except ClientError as e:
            logger.error(
                f"Failed to create replication instance: {e}"
            )
            return None
    
    def create_endpoints(
        self,
        source_config: Dict[str, Any],
        target_config: Dict[str, Any]
    ) -> tuple[Optional[str], Optional[str]]:
        """Create source and target endpoints."""
        try:
            # Create source endpoint
            source_response = self.client.create_endpoint(
                EndpointIdentifier=source_config['identifier'],
                EndpointType='source',
                EngineName=source_config['engine'],
                **source_config['settings']
            )
            
            # Create target endpoint
            target_response = self.client.create_endpoint(
                EndpointIdentifier=target_config['identifier'],
                EndpointType='target',
                EngineName=target_config['engine'],
                **target_config['settings']
            )
            
            return (
                source_response['Endpoint']['EndpointArn'],
                target_response['Endpoint']['EndpointArn']
            )
            
        except ClientError as e:
            logger.error(f"Failed to create endpoints: {e}")
            return None, None
    
    def create_replication_task(
        self,
        task_id: str,
        instance_arn: str,
        source_arn: str,
        target_arn: str,
        table_mappings: Dict[str, Any],
        migration_type: str = 'full-load',
        **kwargs
    ) -> Optional[str]:
        """Create replication task."""
        try:
            response = self.client.create_replication_task(
                ReplicationTaskIdentifier=task_id,
                SourceEndpointArn=source_arn,
                TargetEndpointArn=target_arn,
                ReplicationInstanceArn=instance_arn,
                MigrationType=migration_type,
                TableMappings=json.dumps(table_mappings),
                **kwargs
            )
            
            return response['ReplicationTask']['ReplicationTaskArn']
            
        except ClientError as e:
            logger.error(
                f"Failed to create replication task: {e}"
            )
            return None
    
    def start_replication_task(
        self,
        task_arn: str,
        start_position: Optional[str] = None
    ) -> bool:
        """Start replication task."""
        try:
            params = {'ReplicationTaskArn': task_arn}
            
            if start_position:
                params['StartReplicationTaskType'] = 'resume-processing'
                params['CdcStartPosition'] = start_position
            else:
                params['StartReplicationTaskType'] = 'start-replication'
            
            self.client.start_replication_task(**params)
            return True
            
        except ClientError as e:
            logger.error(
                f"Failed to start replication task: {e}"
            )
            return False
    
    def monitor_task(
        self,
        task_arn: str,
        interval: int = 30
    ):
        """Monitor replication task."""
        try:
            while True:
                # Get task status
                response = self.client.describe_replication_tasks(
                    Filters=[
                        {
                            'Name': 'replication-task-arn',
                            'Values': [task_arn]
                        }
                    ]
                )
                
                if not response['ReplicationTasks']:
                    logger.error("Task not found")
                    break
                
                task = response['ReplicationTasks'][0]
                status = task['Status']
                
                logger.info(
                    f"Task status: {status}, "
                    f"Progress: {task.get('ReplicationTaskStats', {})}"
                )
                
                if status in ['stopped', 'failed']:
                    break
                
                time.sleep(interval)
                
        except ClientError as e:
            logger.error(f"Monitoring failed: {e}")
    
    def get_task_logs(
        self,
        task_arn: str,
        duration: int = 60
    ) -> List[Dict[str, Any]]:
        """Get task logs."""
        try:
            response = self.client.describe_replication_task_assessment_runs(
                Filters=[
                    {
                        'Name': 'replication-task-arn',
                        'Values': [task_arn]
                    }
                ]
            )
            
            return response.get('ReplicationTaskAssessmentRuns', [])
            
        except ClientError as e:
            logger.error(f"Failed to get task logs: {e}")
            return []
    
    def cleanup_resources(
        self,
        task_arn: Optional[str] = None,
        instance_arn: Optional[str] = None,
        endpoint_arns: Optional[List[str]] = None
    ):
        """Clean up DMS resources."""
        try:
            # Delete task
            if task_arn:
                self.client.delete_replication_task(
                    ReplicationTaskArn=task_arn
                )
            
            # Delete endpoints
            if endpoint_arns:
                for arn in endpoint_arns:
                    self.client.delete_endpoint(
                        EndpointArn=arn
                    )
            
            # Delete instance
            if instance_arn:
                self.client.delete_replication_instance(
                    ReplicationInstanceArn=instance_arn
                )
            
        except ClientError as e:
            logger.error(f"Cleanup failed: {e}")

def main():
    """Run DMS operations example."""
    dms = DMSOperations('us-west-2')
    
    try:
        # Create replication instance
        instance = dms.create_replication_instance(
            instance_id='example-instance',
            instance_class='dms.t3.micro',
            allocated_storage=50,
            VpcSecurityGroupIds=['sg-xxxxx']
        )
        
        if not instance:
            return
        
        # Create endpoints
        source_config = {
            'identifier': 'source-postgres',
            'engine': 'postgres',
            'settings': {
                'ServerName': 'source-db',
                'Port': 5432,
                'DatabaseName': 'sourcedb',
                'Username': 'user',
                'Password': 'pass'
            }
        }
        
        target_config = {
            'identifier': 'target-postgres',
            'engine': 'postgres',
            'settings': {
                'ServerName': 'target-db',
                'Port': 5432,
                'DatabaseName': 'targetdb',
                'Username': 'user',
                'Password': 'pass'
            }
        }
        
        source_arn, target_arn = dms.create_endpoints(
            source_config,
            target_config
        )
        
        if not (source_arn and target_arn):
            return
        
        # Create and start task
        table_mappings = {
            'rules': [
                {
                    'rule-type': 'selection',
                    'rule-id': '1',
                    'rule-name': '1',
                    'object-locator': {
                        'schema-name': 'public',
                        'table-name': '%'
                    },
                    'rule-action': 'include'
                }
            ]
        }
        
        task_arn = dms.create_replication_task(
            task_id='example-task',
            instance_arn=instance['ReplicationInstanceArn'],
            source_arn=source_arn,
            target_arn=target_arn,
            table_mappings=table_mappings,
            migration_type='full-load-and-cdc'
        )
        
        if task_arn:
            # Start and monitor task
            if dms.start_replication_task(task_arn):
                dms.monitor_task(task_arn)
        
    finally:
        # Cleanup
        if 'task_arn' in locals():
            dms.cleanup_resources(
                task_arn=task_arn,
                instance_arn=instance['ReplicationInstanceArn'],
                endpoint_arns=[source_arn, target_arn]
            )

if __name__ == '__main__':
    main() 