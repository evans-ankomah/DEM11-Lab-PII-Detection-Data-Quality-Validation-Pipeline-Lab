"""
Validators: Define and apply data validation schemas.
"""

import pandas as pd
import logging
import re
from typing import Dict, List, Tuple

logger = logging.getLogger(__name__)


class DataValidator:
    """Validates data against defined schemas."""
    
    def __init__(self):
        logger.info("Initialized DataValidator with manual schemas")
    
    def validate_raw(self, df: pd.DataFrame) -> Tuple[bool, Dict]:
        """Validate raw data against raw schema."""
        logger.info("Starting raw data validation...")
        failures = {
            'row_count': 0,
            'column_failures': {},
            'failed_rows': []
        }
        
        try:
            # Simple check that all columns exist
            expected_cols = ['customer_id', 'first_name', 'last_name', 'email', 'phone',
                           'date_of_birth', 'address', 'income', 'account_status', 'created_date']
            if list(df.columns) != expected_cols:
                raise ValueError(f"Column mismatch: expected {expected_cols}, got {list(df.columns)}")
            
            logger.info("Raw data validation PASSED")
            return True, failures
            
        except Exception as e:
            logger.warning(f"Raw data validation FAILED: {e}")
            failures['error'] = str(e)
            failures['row_count'] = len(df)
            return False, failures
    
    def validate_cleaned(self, df: pd.DataFrame) -> Tuple[bool, Dict]:
        """Validate cleaned data against cleaned schema."""
        logger.info("Starting cleaned data validation...")
        failures = {
            'row_count': 0,
            'column_failures': {},
            'failed_rows': []
        }
        
        try:
            # Simple check that all columns exist
            expected_cols = ['customer_id', 'first_name', 'last_name', 'email', 'phone',
                           'date_of_birth', 'address', 'income', 'account_status', 'created_date']
            if list(df.columns) != expected_cols:
                raise ValueError(f"Column mismatch: expected {expected_cols}, got {list(df.columns)}")
            
            logger.info("Cleaned data validation PASSED")
            return True, failures
            
        except Exception as e:
            logger.warning(f"Cleaned data validation FAILED: {e}")
            failures['error'] = str(e)
            failures['row_count'] = len(df)
            return False, failures
    
    def validate_with_details(self, df: pd.DataFrame, is_cleaned: bool = False) -> Dict:
        """Run detailed validation with row-level reporting."""
        logger.info(f"Running detailed validation (cleaned={is_cleaned})...")
        
        failed_rows = []
        
        # Manual row-by-row validation for detailed reporting
        for idx, row in df.iterrows():
            row_issues = []
            
            # Check customer_id
            try:
                cid = int(row['customer_id'])
                if cid <= 0:
                    row_issues.append("customer_id must be positive")
            except:
                row_issues.append("customer_id must be integer")
            
            # Check email format if not empty
            if pd.notna(row['email']) and str(row['email']).strip():
                if not re.match(r'^[a-zA-Z0-9._%+-]+@[a-zA-Z0-9.-]+\.[a-zA-Z]{2,}$', str(row['email'])):
                    row_issues.append(f"Invalid email format: {row['email']}")
            
            # Check phone format if not empty
            if pd.notna(row['phone']) and str(row['phone']).strip():
                phone_clean = re.sub(r'\D', '', str(row['phone']))
                if is_cleaned:
                    if not re.match(r'^\d{3}-\d{3}-\d{4}$', str(row['phone'])):
                        row_issues.append(f"Phone must be XXX-XXX-XXXX format: {row['phone']}")
                elif len(phone_clean) < 10:
                    row_issues.append(f"Phone number too short: {row['phone']}")
            
            # Check account_status
            if pd.notna(row['account_status']) and str(row['account_status']).strip():
                status = str(row['account_status']).strip().lower()
                if status not in ['active', 'inactive', 'suspended']:
                    row_issues.append(f"Invalid account_status: {row['account_status']}")
            
            if row_issues:
                failed_rows.append({
                    'row_number': idx + 2,  # +1 for header, +1 for 0-indexing
                    'issues': row_issues,
                    'data': row.to_dict()
                })
        
        return {
            'total_rows': len(df),
            'passed_rows': len(df) - len(failed_rows),
            'failed_rows': failed_rows,
            'failure_count': len(failed_rows),
            'pass_rate': (len(df) - len(failed_rows)) / len(df) * 100 if len(df) > 0 else 0
        }


class PIIDetector:
    """Detects PII patterns in data."""
    
    def __init__(self):
        self.pii_patterns = {
            'email': r'^[a-zA-Z0-9._%+-]+@[a-zA-Z0-9.-]+\.[a-zA-Z]{2,}$',
            'phone': r'[\d\-\.\(\)\s]{10,}',  # Flexible phone pattern
            'ssn': r'\d{3}-\d{2}-\d{4}',
            'credit_card': r'\d{4}[\s\-]?\d{4}[\s\-]?\d{4}[\s\-]?\d{4}'
        }
        logger.info("Initialized PIIDetector")
    
    def detect_pii(self, df: pd.DataFrame) -> Dict:
        """Detect PII in dataframe."""
        logger.info("Scanning for PII...")
        
        pii_found = {
            'emails': [],
            'phones': [],
            'addresses': [],
            'dobs': [],
            'names': [],
            'high_risk_rows': []
        }
        
        for idx, row in df.iterrows():
            row_num = idx + 2
            row_pii = []
            
            # Email detection
            if pd.notna(row['email']) and row['email']:
                pii_found['emails'].append((row_num, row['email']))
                row_pii.append('email')
            
            # Phone detection
            if pd.notna(row['phone']) and row['phone']:
                if re.search(r'\d{3}', str(row['phone'])):
                    pii_found['phones'].append((row_num, row['phone']))
                    row_pii.append('phone')
            
            # Address detection
            if pd.notna(row['address']) and row['address'] and str(row['address']).strip() and str(row['address']).strip() != '':
                pii_found['addresses'].append((row_num, row['address']))
                row_pii.append('address')
            
            # DOB detection
            if pd.notna(row['date_of_birth']) and row['date_of_birth']:
                if re.search(r'\d{4}-\d{2}-\d{2}|\d{2}/\d{2}/\d{4}', str(row['date_of_birth'])):
                    pii_found['dobs'].append((row_num, row['date_of_birth']))
                    row_pii.append('dob')
            
            # Names
            if (pd.notna(row['first_name']) and row['first_name'] and str(row['first_name']).strip()) or \
               (pd.notna(row['last_name']) and row['last_name'] and str(row['last_name']).strip()):
                pii_found['names'].append((row_num, f"{row['first_name']} {row['last_name']}"))
                row_pii.append('name')
            
            # High-risk rows (multiple PII types)
            if len(row_pii) >= 3:
                pii_found['high_risk_rows'].append({
                    'row': row_num,
                    'pii_types': row_pii,
                    'pii_count': len(row_pii)
                })
        
        logger.info(f"PII Detection Summary: "
                   f"{len(pii_found['emails'])} emails, "
                   f"{len(pii_found['phones'])} phones, "
                   f"{len(pii_found['addresses'])} addresses, "
                   f"{len(pii_found['dobs'])} DOBs, "
                   f"{len(pii_found['high_risk_rows'])} high-risk rows")
        
        return pii_found
    
    def calculate_exposure_risk(self, df: pd.DataFrame, pii_data: Dict) -> Dict:
        """Calculate data breach risk."""
        total_rows = len(df)
        
        risk = {
            'total_rows': total_rows,
            'email_coverage': len(pii_data['emails']) / total_rows * 100,
            'phone_coverage': len(pii_data['phones']) / total_rows * 100,
            'address_coverage': len(pii_data['addresses']) / total_rows * 100,
            'dob_coverage': len(pii_data['dobs']) / total_rows * 100,
            'name_coverage': len(pii_data['names']) / total_rows * 100,
            'risk_level': 'CRITICAL' if len(pii_data['high_risk_rows']) > total_rows * 0.5 else 'HIGH',
            'high_risk_count': len(pii_data['high_risk_rows'])
        }
        
        return risk
