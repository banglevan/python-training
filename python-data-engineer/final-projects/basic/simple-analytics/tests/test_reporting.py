"""
Test reporting components.
"""

import pytest
import pandas as pd
from pathlib import Path
from src.utils.config import Config
from src.reporting.templates.html import HTMLTemplate
from src.reporting.distributors.email import EmailDistributor
from src.reporting.distributors.slack import SlackDistributor

@pytest.fixture
def config():
    """Test configuration."""
    return Config("tests/fixtures/config")

@pytest.fixture
def sample_data():
    """Sample report data."""
    return pd.DataFrame({
        'date': pd.date_range('2024-01-01', '2024-01-10'),
        'value': range(10),
        'category': ['A', 'B'] * 5
    })

def test_html_template(config, sample_data, tmp_path):
    """Test HTML template."""
    template = HTMLTemplate(config)
    
    # Add sections
    template.add_section("Test Section", "Test content")
    template.add_chart(
        "Test Chart",
        sample_data,
        'line',
        x='date',
        y='value'
    )
    template.add_table("Test Table", sample_data)
    
    # Render
    output_path = tmp_path / "test_report.html"
    template.render(str(output_path))
    assert output_path.exists()

@pytest.mark.integration
def test_email_distributor(config, tmp_path):
    """Test email distribution."""
    distributor = EmailDistributor(config)
    
    # Create test file
    test_file = tmp_path / "test.txt"
    test_file.write_text("Test content")
    
    distributor.send_report(
        recipients=['test@example.com'],
        subject='Test Report',
        body='<h1>Test Report</h1>',
        attachments=[str(test_file)]
    )

@pytest.mark.integration
def test_slack_distributor(config):
    """Test Slack distribution."""
    distributor = SlackDistributor(config)
    
    blocks = distributor.create_report_blocks(
        "Test Report",
        [{
            'title': 'Test Section',
            'content': 'Test content'
        }]
    )
    
    distributor.send_report(
        channel='#test',
        message='Test Report',
        blocks=blocks
    ) 