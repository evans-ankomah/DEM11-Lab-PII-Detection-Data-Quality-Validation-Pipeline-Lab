# PII Detection & Data Quality Validation Pipeline

A production-ready Python ETL pipeline that profiles customer data, detects PII exposure, validates quality, applies masking, and generates compliance reports.

## Features

[OK] Data Profiling - Analyze completeness, data types, format issues
[OK] PII Detection - Identifies sensitive data patterns (emails, phones, addresses, DOBs)
[OK] Schema Validation - Uses Pandera for composable, reusable validation rules
[OK] Data Cleaning - Normalizes formats (phone, date, names) and handles missing values
[OK] PII Masking - Protects sensitive data while preserving structure
[OK] End-to-End Orchestration - Single command runs all stages
[OK] Structured Logging - Detailed logs to file + console output
[OK] Comprehensive Reports - Multiple deliverable formats
V
## Setup

### Prerequisites
- Python 3.9+
- pip or conda

### Installation

1. Clone repository and navigate to project directory:
```bash
cd "DEM11_DATA GOVERNANCE"
```

2. Create virtual environment (optional but recommended):
```bash
python -m venv venv
source venv/bin/activate  # On Windows: venv\Scripts\activate
```

3. Install dependencies:
```bash
pip install -r requirements.txt
```

## Usage

### Run Full Pipeline
```bash
python main.py
```

This will:
1. Load `data/customers_raw.csv`
2. Profile data quality
3. Validate against schema
4. Clean and normalize formats
5. Detect PII exposure
6. Mask sensitive data
7. Generate all reports

### Output

All deliverables are saved to the `output/` directory:

**Data Files:**
- `customers_cleaned.csv` - Cleaned, validated dataset
- `customers_masked.csv` - Cleaned data with PII masked

**Reports:**
- `data_quality_report.txt` - Profiling results (completeness, types, issues)
- `validation_results.txt` - Schema validation outcomes
- `cleaning_log.txt` - Data transformation actions
- `pii_detection_report.txt` - PII exposure analysis
- `masked_sample.txt` - Before/after sample comparison
- `pipeline_execution_report.txt` - Full execution transcript

**Logs:**
- `logs/pipeline.log` - Detailed execution log

## Configuration

Edit `config/config.yaml` to customize:
- Input/output paths
- Validation rules
- Cleaning strategies
- Masking patterns
- Logging levels

## Project Structure

```
.
├── main.py                 # Entry point
├── config/
│   └── config.yaml        # Configuration
├── src/
│   ├── __init__.py
│   ├── profiler.py        # Data quality analysis
│   ├── validators.py      # Pandera schemas & PII detection
│   ├── cleaner.py         # Normalization & missing values
│   ├── masker.py          # PII masking
│   └── pipeline.py        # Orchestration
├── data/
│   └── customers_raw.csv  # Input dataset
├── output/                # Generated reports & cleaned data
├── logs/                  # Execution logs
├── tests/                 # Unit tests
├── requirements.txt       # Python dependencies
└── README.md             # This file
```

## Testing

Run unit tests:
```bash
pytest tests/
```

## Data Quality Issues Detected

The pipeline catches common data issues:
- Missing values (empty fields)
- Format inconsistencies (phone: XXX.XXX.XXXX vs XXX-XXX-XXXX)
- Date parsing errors (MM/DD/YYYY vs YYYY-MM-DD vs invalid_date)
- Categorical violations (invalid account_status values)
- Type mismatches (numeric fields as strings)
- Uniqueness violations (duplicate customer_id)
- Range violations (negative income, age > 150)

## PII Protection

The pipeline masks:
- **Names:** John Doe → J*** D***
- **Emails:** john@example.com → j***@example.com
- **Phones:** 555-123-4567 → ***-***-4567
- **Addresses:** Any address → [MASKED ADDRESS]
- **DOBs:** 1985-03-15 → 1985-**-**

Business data (income, account_status, customer_id) remains visible for analytics.

## Logging

- **File logs** (`logs/pipeline.log`): DEBUG and above
- **Console output**: INFO and above
- Structured format with timestamps and severity levels

## Error Handling

- Validation failures halt pipeline with detailed error reports
- Masking continues safely (never fails on any input)
- Missing values are handled per column strategy (drop/fill/flag)
- All errors logged with row-level context

## Requirements

- pandas: Data manipulation
- pandera: Schema validation
- pyyaml: Configuration management
- pytest: Testing framework

