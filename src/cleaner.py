"""
Data Cleaner: Normalizes formats and handles missing values.
"""

import pandas as pd
import logging
import re
from typing import Tuple, Dict, List
from datetime import datetime

logger = logging.getLogger(__name__)


class DataCleaner:
    """Cleans and normalizes data."""
    
    def __init__(self):
        self.cleaning_log = []
        logger.info("Initialized DataCleaner")
    
    def normalize_phone(self, phone: str) -> str:
        """Normalize phone to XXX-XXX-XXXX format."""
        if pd.isna(phone) or not phone or str(phone).strip() == '':
            return '[UNKNOWN]'
        
        # Remove all non-digit characters
        digits = re.sub(r'\D', '', str(phone))
        
        # Handle different lengths
        if len(digits) == 10:
            return f"{digits[0:3]}-{digits[3:6]}-{digits[6:10]}"
        elif len(digits) == 11 and digits[0] == '1':
            # Handle 1-XXX-XXX-XXXX
            return f"{digits[1:4]}-{digits[4:7]}-{digits[7:11]}"
        elif len(digits) > 10:
            # Take last 10 digits
            return f"{digits[-10:-7]}-{digits[-7:-4]}-{digits[-4:]}"
        else:
            # Too short, return original
            return str(phone)
    
    def normalize_date(self, date_str: str) -> str:
        """Normalize date to YYYY-MM-DD format."""
        if pd.isna(date_str) or not date_str or str(date_str).strip() == '' or str(date_str).lower() == 'invalid_date' or str(date_str).lower() == 'nan':
            return None
        
        date_str = str(date_str).strip()
        
        # Try multiple formats
        formats = [
            '%Y-%m-%d',      # 2024-01-15
            '%m/%d/%Y',      # 01/15/2024
            '%d/%m/%Y',      # 15/01/2024
            '%Y/%m/%d',      # 2024/01/15
            '%m-%d-%Y',      # 01-15-2024
        ]
        
        for fmt in formats:
            try:
                parsed = datetime.strptime(date_str, fmt)
                return parsed.strftime('%Y-%m-%d')
            except ValueError:
                continue
        
        # If no format matched, return None
        logger.warning(f"Could not parse date: {date_str}")
        return None
    
    def normalize_name(self, name: str) -> str:
        """Normalize name to title case."""
        if pd.isna(name) or not name or str(name).strip() == '':
            return '[UNKNOWN]'
        
        return str(name).strip().title()
    
    def normalize_email(self, email: str) -> str:
        """Normalize email to lowercase."""
        if pd.isna(email) or not email or str(email).strip() == '':
            return '[UNKNOWN]'
        
        return str(email).strip().lower()
    
    def normalize_income(self, income: str) -> int:
        """Normalize income to integer."""
        if pd.isna(income) or not income or str(income).strip() == '' or str(income).strip() == 'nan':
            return 0
        
        try:
            return int(float(re.sub(r'[^\d.]', '', str(income))))
        except:
            return 0
    
    def normalize_account_status(self, status: str) -> str:
        """Normalize account status."""
        if pd.isna(status) or not status or str(status).strip() == '' or str(status).strip() == 'nan':
            return 'unknown'
        
        status = str(status).strip().lower()
        valid_statuses = {'active', 'inactive', 'suspended'}
        
        if status in valid_statuses:
            return status
        return 'unknown'
    
    def clean_data(self, df: pd.DataFrame) -> Tuple[pd.DataFrame, Dict]:
        """Clean entire dataframe."""
        logger.info(f"Starting data cleaning on {len(df)} rows...")
        
        cleaned_df = df.copy()
        cleaning_stats = {
            'rows_processed': len(df),
            'rows_dropped': 0,
            'normalization_actions': {},
            'missing_value_actions': {}
        }
        
        # Store original for comparison
        original_values = {}
        
        # Normalize phone numbers
        phone_changes = 0
        for idx in cleaned_df.index:
            original = cleaned_df.loc[idx, 'phone']
            normalized = self.normalize_phone(cleaned_df.loc[idx, 'phone'])
            cleaned_df.loc[idx, 'phone'] = normalized
            if original != normalized and pd.notna(original):
                phone_changes += 1
        
        if phone_changes > 0:
            cleaning_stats['normalization_actions']['phone_format'] = phone_changes
            logger.info(f"Normalized {phone_changes} phone numbers")
        
        # Normalize dates
        date_changes = 0
        rows_with_invalid_dates = 0
        for idx in cleaned_df.index:
            dob_original = cleaned_df.loc[idx, 'date_of_birth']
            dob_normalized = self.normalize_date(cleaned_df.loc[idx, 'date_of_birth'])
            
            if dob_normalized is None:
                rows_with_invalid_dates += 1
            else:
                cleaned_df.loc[idx, 'date_of_birth'] = dob_normalized
                if dob_original != dob_normalized:
                    date_changes += 1
            
            created_original = cleaned_df.loc[idx, 'created_date']
            created_normalized = self.normalize_date(cleaned_df.loc[idx, 'created_date'])
            
            if created_normalized is None:
                pass  # Keep as is for now
            else:
                cleaned_df.loc[idx, 'created_date'] = created_normalized
                if created_original != created_normalized:
                    date_changes += 1
        
        if date_changes > 0:
            cleaning_stats['normalization_actions']['date_format'] = date_changes
            logger.info(f"Normalized {date_changes} dates")
        
        # Normalize names to title case
        name_changes = 0
        for idx in cleaned_df.index:
            fname_original = cleaned_df.loc[idx, 'first_name']
            fname_normalized = self.normalize_name(cleaned_df.loc[idx, 'first_name'])
            cleaned_df.loc[idx, 'first_name'] = fname_normalized
            if fname_original != fname_normalized and pd.notna(fname_original):
                name_changes += 1
            
            lname_original = cleaned_df.loc[idx, 'last_name']
            lname_normalized = self.normalize_name(cleaned_df.loc[idx, 'last_name'])
            cleaned_df.loc[idx, 'last_name'] = lname_normalized
            if lname_original != lname_normalized and pd.notna(lname_original):
                name_changes += 1
        
        if name_changes > 0:
            cleaning_stats['normalization_actions']['name_case'] = name_changes
            logger.info(f"Applied title case to {name_changes} names")
        
        # Normalize income
        income_changes = 0
        for idx in cleaned_df.index:
            income_original = cleaned_df.loc[idx, 'income']
            income_normalized = self.normalize_income(cleaned_df.loc[idx, 'income'])
            cleaned_df.loc[idx, 'income'] = income_normalized
            if str(income_original) != str(income_normalized):
                income_changes += 1
        
        if income_changes > 0:
            cleaning_stats['normalization_actions']['income_numeric'] = income_changes
            logger.info(f"Normalized {income_changes} income values")
        
        # Normalize email
        email_changes = 0
        for idx in cleaned_df.index:
            email_original = cleaned_df.loc[idx, 'email']
            email_normalized = self.normalize_email(cleaned_df.loc[idx, 'email'])
            cleaned_df.loc[idx, 'email'] = email_normalized
            if email_original != email_normalized and pd.notna(email_original):
                email_changes += 1
        
        if email_changes > 0:
            cleaning_stats['normalization_actions']['email_lowercase'] = email_changes
        
        # Normalize account_status
        status_changes = 0
        for idx in cleaned_df.index:
            status_original = cleaned_df.loc[idx, 'account_status']
            status_normalized = self.normalize_account_status(cleaned_df.loc[idx, 'account_status'])
            cleaned_df.loc[idx, 'account_status'] = status_normalized
            if str(status_original).lower() != status_normalized:
                status_changes += 1
        
        if status_changes > 0:
            cleaning_stats['normalization_actions']['account_status'] = status_changes
        
        # Handle rows with invalid dates (drop them)
        rows_before = len(cleaned_df)
        cleaned_df = cleaned_df[cleaned_df['date_of_birth'].isnull() == False]
        cleaned_df = cleaned_df[cleaned_df['date_of_birth'] != '']
        cleaned_df = cleaned_df[cleaned_df['date_of_birth'] != 'None']
        
        rows_dropped = rows_before - len(cleaned_df)
        if rows_dropped > 0:
            cleaning_stats['rows_dropped'] = rows_dropped
            logger.warning(f"Dropped {rows_dropped} rows with invalid dates")
        
        cleaned_df.reset_index(drop=True, inplace=True)
        
        cleaning_stats['rows_remaining'] = len(cleaned_df)
        logger.info(f"Cleaning complete: {len(cleaned_df)} rows remaining")
        
        return cleaned_df, cleaning_stats
    
    def generate_cleaning_log(self, stats: Dict) -> str:
        """Generate human-readable cleaning log."""
        log_lines = [
            "DATA CLEANING LOG",
            "=================",
            "",
            f"Processed: {stats['rows_processed']} rows",
            f"Rows dropped: {stats['rows_dropped']}",
            f"Output rows: {stats['rows_remaining']}",
            "",
            "ACTIONS TAKEN:",
            "-" * 40,
            ""
        ]
        
        if stats['normalization_actions']:
            log_lines.append("Normalization:")
            for action, count in stats['normalization_actions'].items():
                log_lines.append(f"  - {action}: {count} items affected")
        
        log_lines.extend([
            "",
            "VALIDATION AFTER CLEANING:",
            "-" * 40,
            "(See validation_results.txt for details)",
            "",
            "Output: customers_cleaned.csv",
            f"  - Rows: {stats['rows_remaining']}",
            "  - Columns: 10",
            ""
        ])
        
        return "\n".join(log_lines)
