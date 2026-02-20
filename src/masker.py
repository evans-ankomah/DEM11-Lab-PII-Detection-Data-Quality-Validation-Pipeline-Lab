"""
Data Masker: Masks PII while preserving data structure.
"""

import pandas as pd
import logging
import re
from typing import Dict

logger = logging.getLogger(__name__)


class PIIMasker:
    """Masks personally identifiable information in dataframe."""
    
    def __init__(self):
        logger.info("Initialized PIIMasker")
    
    def mask_name(self, name: str) -> str:
        """Mask name: John Doe → J*** D***"""
        if pd.isna(name) or not name or str(name).strip() == '' or str(name).lower() == '[unknown]':
            return '[UNKNOWN]'
        
        parts = str(name).strip().split()
        masked_parts = []
        
        for part in parts:
            if len(part) > 1:
                masked_parts.append(part[0] + '***')
            else:
                masked_parts.append('*')
        
        return ' '.join(masked_parts)
    
    def mask_email(self, email: str) -> str:
        """Mask email: john.doe@gmail.com → j***@gmail.com"""
        if pd.isna(email) or not email or str(email).strip() == '' or str(email).lower() == '[unknown]':
            return '[UNKNOWN]'
        
        email = str(email).strip().lower()
        
        if '@' in email:
            local, domain = email.split('@', 1)
            if len(local) > 1:
                masked_local = local[0] + '***'
            else:
                masked_local = '*'
            return f"{masked_local}@{domain}"
        
        return email
    
    def mask_phone(self, phone: str) -> str:
        """Mask phone: 555-123-4567 → ***-***-4567"""
        if pd.isna(phone) or not phone or str(phone).strip() == '' or str(phone).lower() == '[unknown]':
            return '[UNKNOWN]'
        
        phone = str(phone).strip()
        digits = re.sub(r'\D', '', phone)
        
        if len(digits) >= 4:
            last_four = digits[-4:]
            return f"***-***-{last_four}"
        
        return "***-***-****"
    
    def mask_address(self, address: str) -> str:
        """Mask address: 123 Main St → [MASKED ADDRESS]"""
        if pd.isna(address) or not address or str(address).strip() == '' or str(address).lower() == '[unknown]':
            return '[MASKED ADDRESS]'
        
        return '[MASKED ADDRESS]'
    
    def mask_dob(self, dob: str) -> str:
        """Mask DOB: 1985-03-15 → 1985-**-**"""
        if pd.isna(dob) or not dob or str(dob).strip() == '' or str(dob).lower() == '[unknown]':
            return '[UNKNOWN]'
        
        dob = str(dob).strip()
        
        # Extract year if in YYYY-MM-DD format
        if re.match(r'^\d{4}-', dob):
            year = dob[:4]
            return f"{year}-**-**"
        
        # For other formats, just mask
        return '****-**-**'
    
    def mask_dataframe(self, df: pd.DataFrame) -> pd.DataFrame:
        """Apply masking to all PII columns in dataframe."""
        logger.info(f"Masking PII in {len(df)} rows...")
        
        masked_df = df.copy()
        
        # Mask names
        masked_df['first_name'] = masked_df['first_name'].apply(self.mask_name)
        masked_df['last_name'] = masked_df['last_name'].apply(self.mask_name)
        
        # Mask email
        masked_df['email'] = masked_df['email'].apply(self.mask_email)
        
        # Mask phone
        masked_df['phone'] = masked_df['phone'].apply(self.mask_phone)
        
        # Mask address
        masked_df['address'] = masked_df['address'].apply(self.mask_address)
        
        # Mask DOB
        masked_df['date_of_birth'] = masked_df['date_of_birth'].apply(self.mask_dob)
        
        logger.info("PII masking complete")
        
        return masked_df
    
    def generate_masked_sample(self, original_df: pd.DataFrame, masked_df: pd.DataFrame, num_rows: int = 2) -> str:
        """Generate before/after sample report."""
        lines = [
            "BEFORE MASKING (first {} rows):".format(num_rows),
            "-" * 80
        ]
        
        # Show original header and rows
        lines.append(",".join(original_df.columns))
        
        for idx in range(min(num_rows, len(original_df))):
            row = original_df.iloc[idx]
            row_str = ",".join([str(val) for val in row.values])
            lines.append(row_str)
        
        lines.extend([
            "",
            "AFTER MASKING (first {} rows):".format(num_rows),
            "-" * 80
        ])
        
        # Show masked header and rows
        lines.append(",".join(masked_df.columns))
        
        for idx in range(min(num_rows, len(masked_df))):
            row = masked_df.iloc[idx]
            row_str = ",".join([str(val) for val in row.values])
            lines.append(row_str)
        
        lines.extend([
            "",
            "ANALYSIS:",
            "-" * 80,
            f"- Data structure preserved: {len(original_df)} rows × {len(original_df.columns)} columns",
            "- PII masked:",
            "  • Names: First letter + *** (e.g., 'J*** D***')",
            "  • Emails: First char + *** @ domain (e.g., 'j***@gmail.com')",
            "  • Phones: ***-***-XXXX where XXXX is last 4 digits",
            "  • Addresses: [MASKED ADDRESS]",
            "  • DOBs: YYYY-**-**",
            "- Business data intact: income, account_status, customer_id",
            "- Use case: Safe for analytics team (compliant with data protection regulations)",
            ""
        ])
        
        return "\n".join(lines)
