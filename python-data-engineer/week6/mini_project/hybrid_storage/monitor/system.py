"""
Monitoring System
--------------

Coordinates monitoring activities:
1. Metrics collection
2. Alert management
3. Health checks
4. System status reporting
"""

from typing import Dict, Any, Optional, List, Union
import logging
from datetime import datetime, timedelta
import asyncio
from concurrent.futures import ThreadPoolExecutor
import json
from pathlib import Path

from .metrics import MetricsCollector
from .alerts import AlertManager

logger = logging.getLogger(__name__)

class MonitoringSystem:
    """Storage system monitor."""
    
    def __init__(
        self,
        config: Dict[str, Any],
        storage_backends: Dict[str, Any]
    ):
        """Initialize monitoring system."""
        self.storage = storage_backends
        self.metrics = MetricsCollector(
            config.get('metrics', {})
        )
        self.alerts = AlertManager(
            config.get('alerts', {})
        )
        
        # Set monitoring interval
        self.interval = config.get('interval', 60)
        
        # Initialize thread pool
        self.executor = ThreadPoolExecutor(
            max_workers=config.get('threads', 4)
        )
        
        # Control flag for monitoring loop
        self.running = False
        
        logger.info("Initialized MonitoringSystem")
    
    async def start_monitoring(self):
        """Start monitoring loop."""
        try:
            self.running = True
            
            while self.running:
                try:
                    # Collect metrics
                    metrics = await self.metrics.collect_metrics(
                        self.storage
                    )
                    
                    # Check for alerts
                    alerts = await self.alerts.check_alerts(
                        metrics
                    )
                    
                    # Run health checks
                    health = await self.check_health()
                    
                    # Store system status
                    await self._store_status(
                        metrics,
                        alerts,
                        health
                    )
                    
                except Exception as e:
                    logger.error(
                        f"Monitoring iteration failed: {e}"
                    )
                
                # Wait for next interval
                await asyncio.sleep(self.interval)
            
        except Exception as e:
            logger.error(f"Monitoring loop failed: {e}")
            raise
        
        finally:
            self.running = False
    
    def stop_monitoring(self):
        """Stop monitoring loop."""
        self.running = False
    
    async def check_health(self) -> Dict[str, Any]:
        """Run system health checks."""
        try:
            health = {
                'timestamp': datetime.now().isoformat(),
                'status': 'healthy',
                'checks': {}
            }
            
            # Check storage backends
            for name, storage in self.storage.items():
                try:
                    # Test basic operations
                    test_file = f"health_check_{name}.txt"
                    test_data = b"health check"
                    
                    # Try to store
                    store_result = await asyncio.to_thread(
                        storage.store,
                        test_file,
                        test_data
                    )
                    
                    # Try to retrieve
                    retrieve_result = await asyncio.to_thread(
                        storage.retrieve,
                        test_file
                    )
                    
                    # Try to delete
                    delete_result = await asyncio.to_thread(
                        storage.delete,
                        test_file
                    )
                    
                    health['checks'][name] = {
                        'status': 'healthy',
                        'latency': {
                            'store': store_result.get(
                                'latency',
                                0
                            ),
                            'retrieve': retrieve_result.get(
                                'latency',
                                0
                            ),
                            'delete': delete_result.get(
                                'latency',
                                0
                            )
                        }
                    }
                    
                except Exception as e:
                    health['checks'][name] = {
                        'status': 'unhealthy',
                        'error': str(e)
                    }
                    health['status'] = 'degraded'
            
            # Check system resources
            try:
                system_metrics = await self.metrics._get_system_metrics()
                
                # CPU check
                cpu_pct = system_metrics['cpu']['percent']
                if cpu_pct > 90:
                    health['checks']['cpu'] = {
                        'status': 'warning',
                        'message': f"High CPU usage: {cpu_pct}%"
                    }
                    health['status'] = 'degraded'
                else:
                    health['checks']['cpu'] = {
                        'status': 'healthy',
                        'usage': cpu_pct
                    }
                
                # Memory check
                mem = system_metrics['memory']
                if mem['percent'] > 90:
                    health['checks']['memory'] = {
                        'status': 'warning',
                        'message': (
                            f"High memory usage: "
                            f"{mem['percent']}%"
                        )
                    }
                    health['status'] = 'degraded'
                else:
                    health['checks']['memory'] = {
                        'status': 'healthy',
                        'usage': mem['percent']
                    }
                
                # Disk check
                for path, usage in system_metrics['disk'].items():
                    if usage['percent'] > 90:
                        health['checks'][f"disk_{path}"] = {
                            'status': 'warning',
                            'message': (
                                f"High disk usage on {path}: "
                                f"{usage['percent']}%"
                            )
                        }
                        health['status'] = 'degraded'
                    else:
                        health['checks'][f"disk_{path}"] = {
                            'status': 'healthy',
                            'usage': usage['percent']
                        }
                
            except Exception as e:
                health['checks']['system'] = {
                    'status': 'unhealthy',
                    'error': str(e)
                }
                health['status'] = 'degraded'
            
            return health
            
        except Exception as e:
            logger.error(f"Health check failed: {e}")
            raise
    
    def get_system_status(
        self,
        include_metrics: bool = True,
        include_alerts: bool = True,
        include_health: bool = True
    ) -> Dict[str, Any]:
        """Get current system status."""
        try:
            status = {
                'timestamp': datetime.now().isoformat()
            }
            
            if include_metrics:
                status['metrics'] = self.metrics.get_metrics(
                    start_time=datetime.now() - timedelta(
                        minutes=5
                    )
                )
            
            if include_alerts:
                status['alerts'] = self.alerts.get_alerts(
                    start_time=datetime.now() - timedelta(
                        hours=24
                    )
                )
            
            if include_health:
                status['health'] = asyncio.run(
                    self.check_health()
                )
            
            return status
            
        except Exception as e:
            logger.error(f"Status retrieval failed: {e}")
            raise
    
    async def _store_status(
        self,
        metrics: Dict[str, Any],
        alerts: List[Dict[str, Any]],
        health: Dict[str, Any]
    ):
        """Store system status."""
        try:
            status = {
                'timestamp': datetime.now().isoformat(),
                'metrics': metrics,
                'alerts': alerts,
                'health': health
            }
            
            # Store status (implementation depends on requirements)
            # Could be stored in:
            # - Database
            # - Time series database
            # - Log files
            # - Cloud monitoring service
            
            logger.debug(f"Stored system status: {status}")
            
        except Exception as e:
            logger.error(f"Status storage failed: {e}")
            raise
    
    def close(self):
        """Clean up resources."""
        self.stop_monitoring()
        self.metrics.close()
        self.alerts.close()
        self.executor.shutdown() 