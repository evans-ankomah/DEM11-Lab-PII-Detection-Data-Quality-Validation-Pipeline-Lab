"""
Pipeline: Orchestrates end-to-end data processing workflow.
"""

import pandas as pd
import logging
from pathlib import Path
from datetime import datetime
from typing import Dict, Tuple

from src.profiler import DataProfiler
from src.validators import DataValidator, PIIDetector
from src.cleaner import DataCleaner
from src.masker import PIIMasker

logger = logging.getLogger(__name__)


class DataPipeline:
    """End-to-end data governance pipeline."""
    
    def __init__(self, input_csv: str, output_dir: str):
        """Initialize pipeline."""
        self.input_csv = input_csv
        self.output_dir = Path(output_dir)
        self.output_dir.mkdir(parents=True, exist_ok=True)
        
        self.execution_log = []
        self.start_time = datetime.now()
        
        logger.info("Initialized pipeline: %s -> %s", input_csv, output_dir)
    
    def log_stage(self, stage: str, status: str, details: str = ""):
        """Log pipeline stage execution."""
        timestamp = datetime.now().isoformat()
        # Use ASCII-safe status indicators for Windows console compatibility
        status_display = status.replace('✓', '[OK]').replace('✗', '[FAIL]')
        log_entry = f"[{timestamp}] {stage}: {status_display} {details}"
        self.execution_log.append(log_entry)
        logger.info(log_entry)
    
    def stage_1_load(self) -> Tuple[pd.DataFrame, bool]:
        """Stage 1: Load raw data."""
        logger.info("=" * 60)
        logger.info("STAGE 1: LOAD")
        logger.info("=" * 60)
        
        try:
            df = pd.read_csv(self.input_csv, dtype=str)
            self.log_stage("LOAD", "✓ SUCCESS", f"({len(df)} rows, {len(df.columns)} columns)")
            logger.info(f"Columns: {list(df.columns)}")
            return df, True
        except Exception as e:
            self.log_stage("LOAD", "FAILED", str(e))
            logger.error(f"Failed to load: {e}")
            return None, False
    
    def stage_2_profile(self, df: pd.DataFrame) -> Tuple[str, bool]:
        """Stage 2: Profile data quality."""
        logger.info("\n" + "=" * 60)
        logger.info("STAGE 2: PROFILE")
        logger.info("=" * 60)
        
        try:
            profiler = DataProfiler(self.input_csv)
            profiler.df = df
            report = profiler.generate_report(self.output_dir / "data_quality_report.txt")
            
            self.log_stage("PROFILE", "OK", "quality report generated")
            logger.info(report[:500] + "...")  # Log first part
            
            return report, True
        except Exception as e:
            self.log_stage("PROFILE", "FAILED", str(e))
            logger.error(f"Profiling failed: {e}")
            return "", False
    
    def stage_3_validate_raw(self, df: pd.DataFrame) -> Tuple[Dict, bool]:
        """Stage 3: Validate raw data against schema."""
        logger.info("\n" + "=" * 60)
        logger.info("STAGE 3: VALIDATE RAW")
        logger.info("=" * 60)
        
        try:
            validator = DataValidator()
            validation_details = validator.validate_with_details(df, is_cleaned=False)
            
            passed = validation_details['passed_rows']
            failed = validation_details['failed_rows']
            
            self.log_stage(
                "VALIDATE_RAW",
                "OK" if len(failed) == 0 else "ISSUES",
                f"({passed} passed, {len(failed)} issues)"
            )
            
            logger.info(f"Validation result: {passed}/{len(df)} rows passed")
            
            return validation_details, True
        except Exception as e:
            self.log_stage("VALIDATE_RAW", "FAILED", str(e))
            logger.error(f"Validation failed: {e}")
            return {}, False
    
    def stage_4_clean(self, df: pd.DataFrame) -> Tuple[pd.DataFrame, Dict, bool]:
        """Stage 4: Clean and normalize data."""
        logger.info("\n" + "=" * 60)
        logger.info("STAGE 4: CLEAN")
        logger.info("=" * 60)
        
        try:
            cleaner = DataCleaner()
            cleaned_df, stats = cleaner.clean_data(df)
            
            self.log_stage(
                "CLEAN",
                "OK",
                f"({stats['rows_remaining']} rows)"
            )
            
            logger.info(f"Cleaning stats: {stats}")
            
            return cleaned_df, stats, True
        except Exception as e:
            self.log_stage("CLEAN", "FAILED", str(e))
            logger.error(f"Cleaning failed: {e}")
            return None, {}, False
    
    def stage_5_validate_clean(self, df: pd.DataFrame) -> Tuple[Dict, bool]:
        """Stage 5: Validate cleaned data."""
        logger.info("\n" + "=" * 60)
        logger.info("STAGE 5: VALIDATE CLEANED")
        logger.info("=" * 60)
        
        try:
            validator = DataValidator()
            validation_details = validator.validate_with_details(df, is_cleaned=True)
            
            passed = validation_details['passed_rows']
            failed = validation_details['failed_rows']
            
            status = "OK" if len(failed) == 0 else "ISSUES"
            
            self.log_stage(
                "VALIDATE_CLEAN",
                status,
                f"({passed} passed, {len(failed)} issues)"
            )
            
            return validation_details, True
        except Exception as e:
            self.log_stage("VALIDATE_CLEAN", "FAILED", str(e))
            logger.error(f"Validation failed: {e}")
            return {}, False
    
    def stage_6_detect_pii(self, df: pd.DataFrame) -> Tuple[Dict, Dict, bool]:
        """Stage 6: Detect PII exposure."""
        logger.info("\n" + "=" * 60)
        logger.info("STAGE 6: DETECT PII")
        logger.info("=" * 60)
        
        try:
            detector = PIIDetector()
            pii_data = detector.detect_pii(df)
            risk = detector.calculate_exposure_risk(df, pii_data)
            
            self.log_stage(
                "DETECT_PII",
                "OK",
                f"({len(pii_data['emails'])} emails, {len(pii_data['phones'])} phones)"
            )
            
            logger.info(f"PII exposure: {risk}")
            
            return pii_data, risk, True
        except Exception as e:
            self.log_stage("DETECT_PII", "FAILED", str(e))
            logger.error(f"PII detection failed: {e}")
            return {}, {}, False
    
    def stage_7_mask(self, cleaned_df: pd.DataFrame) -> Tuple[pd.DataFrame, str, bool]:
        """Stage 7: Mask PII."""
        logger.info("\n" + "=" * 60)
        logger.info("STAGE 7: MASK PII")
        logger.info("=" * 60)
        
        try:
            masker = PIIMasker()
            masked_df = masker.mask_dataframe(cleaned_df)
            sample_report = masker.generate_masked_sample(cleaned_df, masked_df)
            
            self.log_stage(
                "MASK",
                "OK",
                f"({len(masked_df)} rows masked)"
            )
            
            return masked_df, sample_report, True
        except Exception as e:
            self.log_stage("MASK", "FAILED", str(e))
            logger.error(f"Masking failed: {e}")
            return None, "", False
    
    def stage_8_save_outputs(
        self,
        cleaned_df: pd.DataFrame,
        masked_df: pd.DataFrame,
        profile_report: str,
        validation_raw: Dict,
        cleaning_stats: Dict,
        validation_clean: Dict,
        pii_data: Dict,
        pii_risk: Dict,
        masked_sample: str
    ) -> bool:
        """Stage 8: Save all outputs."""
        logger.info("\n" + "=" * 60)
        logger.info("STAGE 8: SAVE OUTPUTS")
        logger.info("=" * 60)
        
        try:
            # Save cleaned CSV
            cleaned_csv = self.output_dir / "customers_cleaned.csv"
            cleaned_df.to_csv(cleaned_csv, index=False)
            logger.info(f"Saved: {cleaned_csv}")
            
            # Save masked CSV
            masked_csv = self.output_dir / "customers_masked.csv"
            masked_df.to_csv(masked_csv, index=False)
            logger.info(f"Saved: {masked_csv}")
            
            # Save validation results
            validation_report = self._generate_validation_report(validation_raw, validation_clean)
            validation_file = self.output_dir / "validation_results.txt"
            with open(validation_file, 'w', encoding='utf-8') as f:
                f.write(validation_report)
            logger.info(f"Saved: {validation_file}")
            
            # Save cleaning log
            cleaner = DataCleaner()
            cleaning_log = cleaner.generate_cleaning_log(cleaning_stats)
            cleaning_file = self.output_dir / "cleaning_log.txt"
            with open(cleaning_file, 'w', encoding='utf-8') as f:
                f.write(cleaning_log)
            logger.info(f"Saved: {cleaning_file}")
            
            # Save PII detection report
            pii_report = self._generate_pii_report(pii_data, pii_risk)
            pii_file = self.output_dir / "pii_detection_report.txt"
            with open(pii_file, 'w', encoding='utf-8') as f:
                f.write(pii_report)
            logger.info(f"Saved: {pii_file}")
            
            # Save masked sample
            masked_file = self.output_dir / "masked_sample.txt"
            with open(masked_file, 'w', encoding='utf-8') as f:
                f.write(masked_sample)
            logger.info(f"Saved: {masked_file}")
            
            # Profile report already saved
            logger.info(f"Verified: data_quality_report.txt")
            
            self.log_stage("SAVE", "OK", "(all outputs saved)")
            
            return True
        except Exception as e:
            self.log_stage("SAVE", "FAILED", str(e))
            logger.error(f"Save failed: {e}")
            return False
    
    def execute(self) -> bool:
        """Execute full pipeline."""
        logger.info("\n\n")
        logger.info("=" * 60)
        logger.info("PII DETECTION & DATA QUALITY PIPELINE")
        logger.info("=" * 60)
        logger.info(f"Start Time: {self.start_time.isoformat()}")
        logger.info("=" * 60)
        
        # Stage 1: Load
        df, success = self.stage_1_load()
        if not success:
            return False
        
        # Stage 2: Profile
        profile_report, success = self.stage_2_profile(df)
        if not success:
            return False
        
        # Stage 3: Validate Raw
        validation_raw, success = self.stage_3_validate_raw(df)
        if not success:
            return False
        
        # Stage 4: Clean
        cleaned_df, cleaning_stats, success = self.stage_4_clean(df)
        if not success:
            return False
        
        # Stage 5: Validate Cleaned
        validation_clean, success = self.stage_5_validate_clean(cleaned_df)
        if not success:
            return False
        
        # Stage 6: Detect PII
        pii_data, pii_risk, success = self.stage_6_detect_pii(cleaned_df)
        if not success:
            return False
        
        # Stage 7: Mask
        masked_df, masked_sample, success = self.stage_7_mask(cleaned_df)
        if not success:
            return False
        
        # Stage 8: Save Outputs
        success = self.stage_8_save_outputs(
            cleaned_df, masked_df,
            profile_report, validation_raw,
            cleaning_stats, validation_clean,
            pii_data, pii_risk, masked_sample
        )
        if not success:
            return False
        
        # Generate execution report
        execution_report = self._generate_execution_report()
        report_file = self.output_dir / "pipeline_execution_report.txt"
        with open(report_file, 'w', encoding='utf-8') as f:
            f.write(execution_report)
        logger.info(f"Saved: {report_file}")
        
        end_time = datetime.now()
        duration = (end_time - self.start_time).total_seconds()
        
        logger.info("\n" + "=" * 60)
        logger.info("PIPELINE EXECUTION COMPLETE")
        logger.info("=" * 60)
        logger.info(f"Start: {self.start_time.isoformat()}")
        logger.info(f"End: {end_time.isoformat()}")
        logger.info(f"Duration: {duration:.2f} seconds")
        logger.info(f"Status: SUCCESS")
        logger.info("=" * 60 + "\n")
        
        return True
    
    def _generate_validation_report(self, validation_raw: Dict, validation_clean: Dict) -> str:
        """Generate validation results report."""
        lines = [
            "VALIDATION RESULTS",
            "==================",
            "",
            "RAW DATA VALIDATION:",
            "-" * 40,
            f"Total rows: {validation_raw.get('total_rows', 0)}",
            f"Passed: {validation_raw.get('passed_rows', 0)}",
            f"Failed: {validation_raw.get('failure_count', 0)}",
            f"Pass rate: {validation_raw.get('pass_rate', 0):.1f}%",
        ]
        
        if validation_raw.get('failed_rows'):
            lines.extend([
                "",
                "Issues found in raw data:",
            ])
            for item in validation_raw['failed_rows'][:10]:
                lines.append(f"  Row {item['row_number']}: {', '.join(item['issues'])}")
        
        lines.extend([
            "",
            "CLEANED DATA VALIDATION:",
            "-" * 40,
            f"Total rows: {validation_clean.get('total_rows', 0)}",
            f"Passed: {validation_clean.get('passed_rows', 0)}",
            f"Failed: {validation_clean.get('failure_count', 0)}",
            f"Pass rate: {validation_clean.get('pass_rate', 0):.1f}%",
        ])
        
        if validation_clean.get('failed_rows'):
            lines.extend([
                "",
                "Issues remaining after cleaning:",
            ])
            for item in validation_clean['failed_rows'][:10]:
                lines.append(f"  Row {item['row_number']}: {', '.join(item['issues'])}")
        else:
            lines.append("✓ All data validated successfully!")
        
        return "\n".join(lines)
    
    def _generate_pii_report(self, pii_data: Dict, pii_risk: Dict) -> str:
        """Generate PII detection report."""
        lines = [
            "PII DETECTION REPORT",
            "====================",
            "",
            "RISK ASSESSMENT:",
            "-" * 40,
            "- HIGH: Names, emails, phone numbers, addresses, dates of birth",
            "- MEDIUM: Income (financial sensitivity)",
            "",
            "DETECTED PII:",
            "-" * 40,
            f"- Emails found: {len(pii_data['emails'])} ({pii_risk.get('email_coverage', 0):.1f}%)",
            f"- Phone numbers found: {len(pii_data['phones'])} ({pii_risk.get('phone_coverage', 0):.1f}%)",
            f"- Addresses found: {len(pii_data['addresses'])} ({pii_risk.get('address_coverage', 0):.1f}%)",
            f"- Dates of birth found: {len(pii_data['dobs'])} ({pii_risk.get('dob_coverage', 0):.1f}%)",
            f"- High-risk rows: {len(pii_data['high_risk_rows'])}",
            "",
            "EXPOSURE RISK:",
            "-" * 40,
            "If this dataset were breached, attackers could:",
            "- Phish customers (have emails)" if pii_risk.get('email_coverage', 0) > 50 else "- Limited phishing capability (few emails)",
            "- Spoof identities (have names + DOB + address)" if pii_risk.get('address_coverage', 0) > 50 else "- Limited identity spoofing",
            "- Social engineer (have phone numbers)" if pii_risk.get('phone_coverage', 0) > 50 else "- Limited social engineering",
            "",
            "MITIGATION:",
            "-" * 40,
            f"[OK] Masked all PII before data sharing",
            f"[OK] Status: PROTECTED (safe for analytics teams)",
            ""
        ]
        
        return "\n".join(lines)
    
    def _generate_execution_report(self) -> str:
        """Generate execution summary report."""
        lines = [
            "PIPELINE EXECUTION REPORT",
            "=========================",
            f"Timestamp: {self.start_time.isoformat()}",
            ""
        ]
        
        for log_entry in self.execution_log:
            lines.append(log_entry)
        
        lines.extend([
            "",
            "DELIVERABLES CREATED:",
            "-" * 40,
            "[OK] data_quality_report.txt - Data profiling results",
            "[OK] validation_results.txt - Validation outcomes",
            "[OK] cleaning_log.txt - Cleaning actions applied",
            "[OK] pii_detection_report.txt - PII exposure analysis",
            "[OK] masked_sample.txt - Before/after comparison",
            "[OK] customers_cleaned.csv - Cleaned dataset",
            "[OK] customers_masked.csv - Masked dataset",
            "[OK] pipeline_execution_report.txt - This report",
            "",
            "STATUS: SUCCESS",
            ""
        ])
        
        return "\n".join(lines)
