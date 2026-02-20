"""
Unit tests for validators module.
"""

import pytest
import pandas as pd
from src.validators import DataValidator, PIIDetector


class TestDataValidator:
    """Tests for DataValidator class."""
    
    @pytest.fixture
    def sample_valid_data(self):
        """Create sample valid data."""
        return pd.DataFrame({
            'customer_id': ['1', '2', '3'],
            'first_name': ['John', 'Jane', 'Bob'],
            'last_name': ['Doe', 'Smith', 'Johnson'],
            'email': ['john@example.com', 'jane@example.com', 'bob@example.com'],
            'phone': ['555-123-4567', '555-987-6543', '555-234-5678'],
            'date_of_birth': ['1985-03-15', '1990-07-22', '1988-11-08'],
            'address': ['123 Main St', '456 Oak Ave', '789 Pine Rd'],
            'income': ['75000', '95000', '85000'],
            'account_status': ['active', 'active', 'suspended'],
            'created_date': ['2024-01-10', '2024-01-11', '2024-01-12']
        })
    
    def test_validator_initialization(self):
        """Test validator can be initialized."""
        validator = DataValidator()
        assert validator is not None
    
    def test_validate_with_details(self, sample_valid_data):
        """Test detailed validation."""
        validator = DataValidator()
        result = validator.validate_with_details(sample_valid_data, is_cleaned=False)
        
        assert 'total_rows' in result
        assert 'passed_rows' in result
        assert 'failed_rows' in result
        assert result['total_rows'] == 3


class TestPIIDetector:
    """Tests for PIIDetector class."""
    
    @pytest.fixture
    def sample_data(self):
        """Create sample data with PII."""
        return pd.DataFrame({
            'customer_id': ['1', '2'],
            'first_name': ['John', 'Jane'],
            'last_name': ['Doe', 'Smith'],
            'email': ['john@example.com', 'jane@example.com'],
            'phone': ['555-123-4567', '555-987-6543'],
            'date_of_birth': ['1985-03-15', '1990-07-22'],
            'address': ['123 Main St', '456 Oak Ave'],
            'income': ['75000', '95000'],
            'account_status': ['active', 'active'],
            'created_date': ['2024-01-10', '2024-01-11']
        })
    
    def test_pii_detector_initialization(self):
        """Test detector can be initialized."""
        detector = PIIDetector()
        assert detector is not None
        assert detector.pii_patterns is not None
    
    def test_detect_pii(self, sample_data):
        """Test PII detection."""
        detector = PIIDetector()
        pii_data = detector.detect_pii(sample_data)
        
        assert 'emails' in pii_data
        assert 'phones' in pii_data
        assert 'addresses' in pii_data
        assert 'dobs' in pii_data
        assert len(pii_data['emails']) == 2
        assert len(pii_data['phones']) == 2
    
    def test_calculate_exposure_risk(self, sample_data):
        """Test risk calculation."""
        detector = PIIDetector()
        pii_data = detector.detect_pii(sample_data)
        risk = detector.calculate_exposure_risk(sample_data, pii_data)
        
        assert 'email_coverage' in risk
        assert 'phone_coverage' in risk
        assert risk['email_coverage'] == 100.0
