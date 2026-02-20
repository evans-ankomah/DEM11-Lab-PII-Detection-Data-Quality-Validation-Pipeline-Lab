"""
Unit tests for cleaner module.
"""

import pytest
import pandas as pd
from src.cleaner import DataCleaner


class TestDataCleaner:
    """Tests for DataCleaner class."""
    
    @pytest.fixture
    def cleaner(self):
        """Create cleaner instance."""
        return DataCleaner()
    
    def test_cleaner_initialization(self, cleaner):
        """Test cleaner can be initialized."""
        assert cleaner is not None
        assert cleaner.cleaning_log is not None
    
    def test_normalize_phone_standard(self, cleaner):
        """Test normalizing standard phone format."""
        result = cleaner.normalize_phone('5551234567')
        assert result == '555-123-4567'
    
    def test_normalize_phone_already_formatted(self, cleaner):
        """Test phone already in correct format."""
        result = cleaner.normalize_phone('555-123-4567')
        assert result == '555-123-4567'
    
    def test_normalize_phone_with_parens(self, cleaner):
        """Test phone with parentheses."""
        result = cleaner.normalize_phone('(555) 234-5678')
        assert result == '555-234-5678'
    
    def test_normalize_phone_missing(self, cleaner):
        """Test missing phone."""
        result = cleaner.normalize_phone('')
        assert result == '[UNKNOWN]'
    
    def test_normalize_date_standard(self, cleaner):
        """Test normalizing standard date."""
        result = cleaner.normalize_date('1985-03-15')
        assert result == '1985-03-15'
    
    def test_normalize_date_slash_format(self, cleaner):
        """Test normalizing slash-formatted date."""
        result = cleaner.normalize_date('01/15/2024')
        assert result == '2024-01-15'
    
    def test_normalize_date_invalid(self, cleaner):
        """Test invalid date."""
        result = cleaner.normalize_date('invalid_date')
        assert result is None
    
    def test_normalize_name_lowercase(self, cleaner):
        """Test normalizing name to title case."""
        result = cleaner.normalize_name('john doe')
        assert result == 'John Doe'
    
    def test_normalize_name_uppercase(self, cleaner):
        """Test normalizing uppercase name."""
        result = cleaner.normalize_name('PATRICIA')
        assert result == 'Patricia'
    
    def test_normalize_name_missing(self, cleaner):
        """Test missing name."""
        result = cleaner.normalize_name('')
        assert result == '[UNKNOWN]'
    
    def test_normalize_email(self, cleaner):
        """Test email normalization."""
        result = cleaner.normalize_email('PATRICIA.DAVIS@GMAIL.COM')
        assert result == 'patricia.davis@gmail.com'
    
    def test_normalize_income(self, cleaner):
        """Test income normalization."""
        result = cleaner.normalize_income('75000')
        assert result == 75000
    
    def test_normalize_income_float(self, cleaner):
        """Test income as float."""
        result = cleaner.normalize_income('95000.50')
        assert result == 95000
    
    def test_normalize_income_missing(self, cleaner):
        """Test missing income."""
        result = cleaner.normalize_income('')
        assert result == 0
    
    def test_normalize_account_status_valid(self, cleaner):
        """Test valid account status."""
        result = cleaner.normalize_account_status('ACTIVE')
        assert result == 'active'
    
    def test_normalize_account_status_invalid(self, cleaner):
        """Test invalid account status."""
        result = cleaner.normalize_account_status('unknown_status')
        assert result == 'unknown'
