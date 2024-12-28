"""Unit tests for Log Analyzer."""

import unittest
import os
from datetime import datetime
import json
from log_analyzer import LogAnalyzer, LogEntry

class TestLogAnalyzer(unittest.TestCase):
    """Test cases for Log Analyzer."""
    
    def setUp(self):
        """Set up test environment."""
        self.analyzer = LogAnalyzer()
        
        # Create test log file
        self.test_log = "test.log"
        with open(self.test_log, 'w') as f:
            f.write(
                "2024-01-15 10:30:45 INFO [UserService] "
                "User login successful - user_id=123 "
                "duration_ms=150\n"
            )
            f.write(
                "2024-01-15 10:31:00 ERROR [AuthService] "
                "Authentication failed - error_code=AUTH001 "
                "user_id=456\n"
            )
            f.write(
                "2024-01-15 10:32:15 WARN [DataService] "
                "Slow query detected - duration_ms=5000\n"
            )
    
    def tearDown(self):
        """Clean up test environment."""
        if os.path.exists(self.test_log):
            os.remove(self.test_log)
        
        # Clean up generated files
        for file in ['reports/analysis.json']:
            if os.path.exists(file):
                os.remove(file)
        
        # Clean up generated directories
        for dir in ['reports/plots']:
            if os.path.exists(dir):
                os.rmdir(dir)
    
    def test_parse_log_line(self):
        """Test log line parsing."""
        line = (
            "2024-01-15 10:30:45 INFO [UserService] "
            "User login successful - user_id=123 "
            "duration_ms=150"
        )
        entry = self.analyzer.parse_log_line(line)
        
        self.assertIsInstance(entry, LogEntry)
        self.assertEqual(entry.level, 'INFO')
        self.assertEqual(entry.service, 'UserService')
        self.assertEqual(entry.user_id, '123')
        self.assertEqual(entry.duration_ms, 150.0)
    
    def test_parse_log_file(self):
        """Test complete log file parsing."""
        self.analyzer.parse_log_file(self.test_log)
        
        self.assertEqual(len(self.analyzer.log_entries), 3)
        self.assertTrue(
            any(e.level == 'ERROR' 
                for e in self.analyzer.log_entries)
        )
    
    def test_calculate_metrics(self):
        """Test metrics calculation."""
        self.analyzer.parse_log_file(self.test_log)
        self.analyzer.calculate_metrics()
        
        self.assertEqual(
            self.analyzer.metrics['total_entries'],
            3
        )
        self.assertEqual(
            self.analyzer.metrics['error_count'],
            1
        )
        self.assertTrue(
            'service_stats' in self.analyzer.metrics
        )
        self.assertTrue(
            'hourly_stats' in self.analyzer.metrics
        )
    
    def test_generate_visualizations(self):
        """Test visualization generation."""
        self.analyzer.parse_log_file(self.test_log)
        self.analyzer.calculate_metrics()
        
        output_dir = "reports/plots"
        self.analyzer.generate_visualizations(output_dir)
        
        expected_files = [
            'errors_by_service.png',
            'duration_dist.png',
            'activity_heatmap.png'
        ]
        
        for file in expected_files:
            self.assertTrue(
                os.path.exists(
                    f"{output_dir}/{file}"
                )
            )
    
    def test_generate_report(self):
        """Test report generation."""
        self.analyzer.parse_log_file(self.test_log)
        self.analyzer.calculate_metrics()
        
        report_file = "reports/analysis.json"
        self.analyzer.generate_report(report_file)
        
        self.assertTrue(os.path.exists(report_file))
        
        with open(report_file) as f:
            report = json.load(f)
            self.assertTrue('metrics' in report)
            self.assertTrue('summary' in report)
            self.assertTrue('recommendations' in report)
    
    def test_error_handling(self):
        """Test error handling."""
        # Test with invalid log file
        with self.assertRaises(Exception):
            self.analyzer.parse_log_file("nonexistent.log")
        
        # Test with invalid log line
        entry = self.analyzer.parse_log_line(
            "Invalid log line"
        )
        self.assertIsNone(entry)

if __name__ == '__main__':
    unittest.main() 