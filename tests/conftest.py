"""
Test configuration and fixtures.
"""

import pytest
import pandas as pd


@pytest.fixture
def sample_raw_data():
    """Fixture: sample raw data with quality issues."""
    return pd.DataFrame({
        'customer_id': ['1', '2', '3', '4', '5'],
        'first_name': ['John', 'jane', '', 'Mary', 'Robert'],
        'last_name': ['Doe', 'Smith', 'Johnson', 'Brown', ''],
        'email': ['john@example.com', 'jane@example.com', 'bob@example.com', 'mary@example.com', 'robert@example.com'],
        'phone': ['555-123-4567', '555.987.6543', '(555) 234-5678', '5554356789', '555-456-7890'],
        'date_of_birth': ['1985-03-15', '1990-07-22', '1988-11-08', '05/10/1975', 'invalid_date'],
        'address': ['123 Main St', '456 Oak Ave', '', '789 Pine Rd', '892 Elm St'],
        'income': ['75000', '95000', '', '120000', '55000'],
        'account_status': ['active', 'active', 'suspended', '', 'active'],
        'created_date': ['2024-01-10', '2024-01-11', '2024-01-12', '2024-01-13', '01/15/2024']
    })


@pytest.fixture
def sample_clean_data():
    """Fixture: sample cleaned data."""
    return pd.DataFrame({
        'customer_id': [1, 2, 3],
        'first_name': ['John', 'Jane', 'Bob'],
        'last_name': ['Doe', 'Smith', 'Johnson'],
        'email': ['john@example.com', 'jane@example.com', 'bob@example.com'],
        'phone': ['555-123-4567', '555-987-6543', '555-234-5678'],
        'date_of_birth': ['1985-03-15', '1990-07-22', '1988-11-08'],
        'address': ['123 Main St', '456 Oak Ave', '789 Pine Rd'],
        'income': [75000, 95000, 85000],
        'account_status': ['active', 'active', 'suspended'],
        'created_date': ['2024-01-10', '2024-01-11', '2024-01-12']
    })
