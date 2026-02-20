"""
Data Profiler: Analyzes data completeness, types, and quality issues.
"""

import pandas as pd
import logging
from typing import Dict, List, Tuple
from pathlib import Path
import re
from datetime import datetime

logger = logging.getLogger(__name__)


class DataProfiler:
    """Profiles raw data to identify quality issues."""
    
    def __init__(self, csv_path: str):
        """Initialize profiler with CSV file."""
        self.csv_path = csv_path
        self.df = None
        self.quality_issues = []
        self.format_issues = {
            'phone': set(),
            'date': set(),
            'case': set()
        }
        logger.info(f"Initialized DataProfiler with {csv_path}")
    
    def load_data(self) -> pd.DataFrame:
        """Load CSV file."""
        try:
            self.df = pd.read_csv(self.csv_path, dtype=str)
            logger.info(f"Loaded {len(self.df)} rows from {self.csv_path}")
            return self.df
        except Exception as e:
            logger.error(f"Failed to load CSV: {e}")
            raise
    
    def analyze_completeness(self) -> Dict[str, float]:
        """Calculate completeness percentage per column."""
        completeness = {}
        total_rows = len(self.df)
        
        for col in self.df.columns:
            non_empty = self.df[col].notna().sum()
            # Count non-empty strings (exclude NaN)
            non_empty = sum(1 for val in self.df[col] if pd.notna(val) and str(val).strip())
            percentage = (non_empty / total_rows * 100) if total_rows > 0 else 0
            completeness[col] = percentage
            logger.debug(f"{col}: {percentage:.1f}% complete ({non_empty}/{total_rows})")
        
        return completeness
    
    def analyze_data_types(self) -> Dict[str, str]:
        """Detect data types in each column."""
        types_found = {}
        
        for col in self.df.columns:
            # All loaded as strings initially
            sample_values = self.df[col].dropna().head(5).tolist()
            
            if col == 'customer_id':
                types_found[col] = 'INT'
            elif col in ['first_name', 'last_name', 'address', 'account_status']:
                types_found[col] = 'STRING'
            elif col in ['date_of_birth', 'created_date']:
                types_found[col] = 'STRING (should be DATE)'
            elif col == 'email':
                types_found[col] = 'STRING'
            elif col == 'phone':
                types_found[col] = 'STRING'
            elif col == 'income':
                types_found[col] = 'STRING (should be NUMERIC)'
            else:
                types_found[col] = 'STRING'
            
            logger.debug(f"{col}: detected as {types_found[col]}")
        
        return types_found
    
    def detect_format_issues(self) -> Dict[str, List[Tuple[int, str]]]:
        """Identify format inconsistencies."""
        issues = {
            'phone': [],
            'date': [],
            'case': [],
            'names': []
        }
        
        # Phone format issues
        for idx, row in self.df.iterrows():
            phone = str(row['phone']).strip()
            if pd.notna(row['phone']) and phone:
                # Detect different formats
                if not re.match(r'^\d{3}-\d{3}-\d{4}$', phone):
                    issues['phone'].append((idx + 2, phone))  # +2 for header + 1-index
        
        # Date format issues
        for idx, row in self.df.iterrows():
            dob = str(row['date_of_birth']).strip()
            created = str(row['created_date']).strip()
            
            if pd.notna(row['date_of_birth']) and dob != 'nan' and dob:
                if not re.match(r'^\d{4}-\d{2}-\d{2}$', dob):
                    issues['date'].append((idx + 2, f"date_of_birth: {dob}"))
            
            if pd.notna(row['created_date']) and created != 'nan' and created:
                if not re.match(r'^\d{4}-\d{2}-\d{2}$', created):
                    issues['date'].append((idx + 2, f"created_date: {created}"))
        
        # Case issues (names that aren't title case)
        for idx, row in self.df.iterrows():
            fname = str(row['first_name']).strip()
            lname = str(row['last_name']).strip()
            
            if pd.notna(row['first_name']) and fname and fname != fname.title():
                issues['case'].append((idx + 2, f"first_name: {fname}"))
            
            if pd.notna(row['last_name']) and lname and lname != lname.title():
                issues['case'].append((idx + 2, f"last_name: {lname}"))
        
        logger.info(f"Detected {len(issues['phone'])} phone format issues, "
                   f"{len(issues['date'])} date format issues, "
                   f"{len(issues['case'])} case issues")
        
        return issues
    
    def check_uniqueness(self) -> Dict[str, Tuple[int, int]]:
        """Check uniqueness constraints."""
        uniqueness = {}
        
        for col in self.df.columns:
            total = len(self.df[col].dropna())
            unique = len(self.df[col].dropna().unique())
            duplicates = total - unique
            uniqueness[col] = (unique, duplicates)
            
            if duplicates > 0:
                logger.warning(f"{col}: {duplicates} duplicates found")
        
        return uniqueness
    
    def check_categorical_validity(self) -> Dict[str, Tuple[List[str], List[Tuple[int, str]]]]:
        """Check if categorical columns have valid values."""
        validity = {}
        valid_statuses = {'active', 'inactive', 'suspended'}
        
        # account_status validation
        invalid_statuses = []
        for idx, row in self.df.iterrows():
            status = str(row['account_status']).strip().lower()
            if pd.notna(row['account_status']) and status and status != 'nan':
                if status not in valid_statuses:
                    invalid_statuses.append((idx + 2, status))
        
        found_statuses = set(self.df['account_status'].dropna().unique())
        validity['account_status'] = (list(found_statuses), invalid_statuses)
        
        logger.info(f"account_status: found {found_statuses}, invalid: {len(invalid_statuses)}")
        
        return validity
    
    def check_value_ranges(self) -> Dict[str, Dict]:
        """Check if values are within expected ranges."""
        ranges = {}
        
        # Customer ID checks
        try:
            customer_ids = pd.to_numeric(self.df['customer_id'], errors='coerce')
            ranges['customer_id'] = {
                'min': customer_ids.min(),
                'max': customer_ids.max(),
                'invalid': (customer_ids < 0).sum()
            }
        except:
            ranges['customer_id'] = {'error': 'Could not parse customer_id'}
        
        # Income checks
        try:
            incomes = pd.to_numeric(self.df['income'], errors='coerce')
            ranges['income'] = {
                'min': incomes.min(),
                'max': incomes.max(),
                'negative': (incomes < 0).sum(),
                'over_10m': (incomes > 10_000_000).sum()
            }
        except:
            ranges['income'] = {'error': 'Could not parse income'}
        
        logger.info(f"Value ranges analyzed: {ranges}")
        
        return ranges
    
    def generate_report(self, output_path: str = None) -> str:
        """Generate comprehensive quality report."""
        if self.df is None:
            self.load_data()
        
        completeness = self.analyze_completeness()
        data_types = self.analyze_data_types()
        format_issues = self.detect_format_issues()
        uniqueness = self.check_uniqueness()
        categorical = self.check_categorical_validity()
        ranges = self.check_value_ranges()
        
        report_lines = [
            "DATA QUALITY PROFILE REPORT",
            "===========================",
            "",
            f"Generated: {datetime.now().isoformat()}",
            f"Total Rows: {len(self.df)}",
            f"Total Columns: {len(self.df.columns)}",
            "",
            "COMPLETENESS:",
            "-" * 40
        ]
        
        for col, pct in completeness.items():
            missing = len(self.df) - int(self.df[col].notna().sum())
            status = "[OK]" if pct >= 80 else "[WARN]" if pct >= 50 else "[FAIL]"
            report_lines.append(f"  {status} {col}: {pct:.1f}% ({len(self.df) - missing}/{len(self.df)})")
        
        report_lines.extend([
            "",
            "DATA TYPES:",
            "-" * 40
        ])
        
        for col, dtype in data_types.items():
            status = "[OK]" if "should be" not in dtype else "[FAIL]"
            report_lines.append(f"  {status} {col}: {dtype}")
        
        report_lines.extend([
            "",
            "QUALITY ISSUES:",
            "-" * 40
        ])
        
        issue_count = 0
        
        if format_issues['phone']:
            report_lines.append(f"  Phone Format Issues ({len(format_issues['phone'])}):")
            for row_num, value in format_issues['phone'][:5]:
                report_lines.append(f"    - Row {row_num}: '{value}'")
            issue_count += len(format_issues['phone'])
        
        if format_issues['date']:
            report_lines.append(f"  Date Format Issues ({len(format_issues['date'])}):")
            for row_num, value in format_issues['date'][:5]:
                report_lines.append(f"    - Row {row_num}: '{value}'")
            issue_count += len(format_issues['date'])
        
        if format_issues['case']:
            report_lines.append(f"  Name Case Issues ({len(format_issues['case'])}):")
            for row_num, value in format_issues['case'][:3]:
                report_lines.append(f"    - Row {row_num}: '{value}'")
            issue_count += len(format_issues['case'])
        
        if categorical['account_status'][1]:
            report_lines.append(f"  Invalid account_status ({len(categorical['account_status'][1])}):")
            for row_num, value in categorical['account_status'][1][:3]:
                report_lines.append(f"    - Row {row_num}: '{value}'")
            issue_count += len(categorical['account_status'][1])
        
        if ranges['income'].get('negative', 0) > 0:
            report_lines.append(f"  Negative Income: {ranges['income']['negative']} rows")
            issue_count += ranges['income']['negative']
        
        report_lines.extend([
            "",
            "SEVERITY SUMMARY:",
            "-" * 40,
            f"  Critical (blocks processing): {ranges['customer_id'].get('invalid', 0)}",
            f"  High (data incorrect): {len(format_issues['date']) + ranges['income'].get('negative', 0)}",
            f"  Medium (needs cleaning): {len(format_issues['phone']) + len(format_issues['case'])}",
            f"  Total Issues: {issue_count}",
            ""
        ])
        
        report_text = "\n".join(report_lines)
        logger.info(f"Generated quality report with {issue_count} issues")
        
        if output_path:
            Path(output_path).parent.mkdir(parents=True, exist_ok=True)
            with open(output_path, 'w', encoding='utf-8') as f:
                f.write(report_text)
            logger.info(f"Report saved to {output_path}")
        
        return report_text
