"""
Unit tests for masker module.
"""

import pytest
import pandas as pd
from src.masker import PIIMasker


class TestPIIMasker:
    """Tests for PIIMasker class."""
    
    @pytest.fixture
    def masker(self):
        """Create masker instance."""
        return PIIMasker()
    
    def test_masker_initialization(self, masker):
        """Test masker can be initialized."""
        assert masker is not None
    
    def test_mask_name_full(self, masker):
        """Test masking full name."""
        result = masker.mask_name('John Doe')
        assert result == 'J*** D***'
    
    def test_mask_name_single(self, masker):
        """Test masking single letter name."""
        result = masker.mask_name('X')
        assert result == '*'
    
    def test_mask_name_missing(self, masker):
        """Test masking missing name."""
        result = masker.mask_name('')
        assert result == '[UNKNOWN]'
    
    def test_mask_email(self, masker):
        """Test masking email."""
        result = masker.mask_email('john.doe@gmail.com')
        assert result == 'j***@gmail.com'
    
    def test_mask_email_uppercase(self, masker):
        """Test masking uppercase email."""
        result = masker.mask_email('PATRICIA.DAVIS@GMAIL.COM')
        assert result == 'p***@gmail.com'
    
    def test_mask_email_missing(self, masker):
        """Test masking missing email."""
        result = masker.mask_email('')
        assert result == '[UNKNOWN]'
    
    def test_mask_phone(self, masker):
        """Test masking phone."""
        result = masker.mask_phone('555-123-4567')
        assert result == '***-***-4567'
    
    def test_mask_phone_digits_only(self, masker):
        """Test masking phone with only digits."""
        result = masker.mask_phone('5551234567')
        assert result == '***-***-4567'
    
    def test_mask_phone_missing(self, masker):
        """Test masking missing phone."""
        result = masker.mask_phone('')
        assert result == '[UNKNOWN]'
    
    def test_mask_address(self, masker):
        """Test masking address."""
        result = masker.mask_address('123 Main St New York NY 10001')
        assert result == '[MASKED ADDRESS]'
    
    def test_mask_address_missing(self, masker):
        """Test masking missing address."""
        result = masker.mask_address('')
        assert result == '[MASKED ADDRESS]'
    
    def test_mask_dob(self, masker):
        """Test masking DOB."""
        result = masker.mask_dob('1985-03-15')
        assert result == '1985-**-**'
    
    def test_mask_dob_missing(self, masker):
        """Test masking missing DOB."""
        result = masker.mask_dob('')
        assert result == '[UNKNOWN]'
    
    def test_masking_is_deterministic(self, masker):
        """Test that masking produces same output for same input."""
        sample_df = pd.DataFrame({
            'first_name': ['John'],
            'last_name': ['Doe'],
            'email': ['john@example.com'],
            'phone': ['555-123-4567'],
            'address': ['123 Main St'],
            'date_of_birth': ['1985-03-15']
        })
        
        result1 = masker.mask_dataframe(sample_df.copy())
        result2 = masker.mask_dataframe(sample_df.copy())
        
        assert result1.equals(result2), "Masking should be deterministic"
