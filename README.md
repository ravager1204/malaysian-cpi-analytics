# Malaysian CPI Analytics - End-to-End Data Pipeline

![Project Status](https://img.shields.io/badge/status-complete-success)
![Python](https://img.shields.io/badge/python-3.10+-blue)
![PostgreSQL](https://img.shields.io/badge/postgresql-15-blue)
![AWS S3](https://img.shields.io/badge/AWS-S3-orange)

An end-to-end data engineering project analyzing Malaysia's Consumer Price Index (CPI) data from the Department of Statistics Malaysia (DOSM). This project demonstrates a complete modern data stack: extraction, transformation, storage, and visualization.

---

## 📊 Project Overview

This project builds a production-ready data pipeline that:
- Extracts real CPI data from OpenDOSM API (43,008 records, 2010-2025)
- Transforms data through staging and mart layers
- Calculates inflation metrics (month-over-month, year-over-year)
- Stores data in PostgreSQL with proper data warehouse design
- Backs up to AWS S3 with date partitioning
- Visualizes insights in Power BI dashboards

---

## 🎯 Key Insights Discovered

- **Johor has the highest inflation rate:** 2.26% YoY (Dec 2025)
- **Putrajaya is 14.24% more expensive than Sabah** (cheapest state)
- **Personal Care category sees highest inflation:** +6.03%
- **Clothing prices are falling:** -0.39% (deflation)
- **Price gap between most and least expensive states:** 17.7 CPI points

---

## 🏗️ Architecture
```
OpenDOSM API
     ↓
┌─────────────────┐
│  Extraction     │  Python (requests, pandas)
│  & Validation   │  
└────────┬────────┘
         ↓
┌─────────────────┐
│   AWS S3        │  Cloud Backup (date partitioned)
│   Raw Storage   │  
└─────────────────┘
         ↓
┌─────────────────┐
│  PostgreSQL     │  3-Layer Data Warehouse
│  Database       │  - Raw: Source data
│                 │  - Staging: Cleaned data
│                 │  - Mart: Analytics-ready
└────────┬────────┘
         ↓
┌─────────────────┐
│ Transformations │  SQL + Python
│ (Staging/Mart)  │  Window functions, CTEs
└────────┬────────┘
         ↓
┌─────────────────┐
│   Power BI      │  Interactive Dashboards
│   Dashboards    │  
└─────────────────┘
```

---

## 📂 Project Structure
```
malaysian-cpi-analytics/
├── data_ingestion/              # ETL modules
│   ├── dosm_client.py          # API client
│   ├── cpi_extractor.py        # Data extraction with validation
│   ├── db_loader.py            # PostgreSQL loader
│   ├── staging_transformer.py  # Staging transformations
│   ├── mart_transformer.py     # Mart transformations (inflation calcs)
│   └── s3_uploader.py          # AWS S3 uploader
│
├── scripts/                     # Execution scripts
│   ├── extract_cpi_data.py     # Main extraction script
│   ├── run_full_pipeline.py    # Complete pipeline runner
│   ├── explore_data.py         # Data exploration
│   └── view_mart_data.py       # View analytics results
│
├── sql/                         # Database DDL
│   ├── schema/
│   │   └── 01_create_schemas.sql
│   └── ddl/
│       ├── staging_tables.sql
│       └── mart_tables.sql
│
├── powerbi/                     # Power BI dashboards
│   └── CPI_Analytics_Dashboard.pbix
│
├── data/                        # Local data storage
│   ├── raw/                    # Raw extracted data
│   ├── outputs/                # Analysis outputs & charts
│   └── processed/              # Processed data
│
├── logs/                        # Pipeline execution logs
├── airflow/dags/               # Airflow DAG (optional)
└── test_scripts/               # Test files
```

---

## 🛠️ Technology Stack

### Data Engineering
- **Language:** Python 3.10+
- **Data Processing:** pandas, numpy, pyarrow
- **Database:** PostgreSQL 15
- **Cloud Storage:** AWS S3
- **Orchestration:** Apache Airflow (optional)
- **Containerization:** Docker

### Python Libraries
```
pandas==2.1.4
requests==2.31.0
psycopg2-binary==2.9.9
sqlalchemy==2.0.25
boto3==1.34.34
python-dotenv==1.0.0
pyarrow==14.0.2
```

### Visualization
- **Power BI Desktop:** Interactive dashboards

---

## 🗄️ Database Design

### Three-Layer Data Warehouse

Implements a medallion-style architecture pattern with progressive data refinement:
- **Raw Layer**: Unmodified source data from OpenDOSM API
- **Staging Layer**: Cleaned, validated, and standardized data
- **Mart Layer**: Business-logic applied, analytics-ready dimensional models

This follows industry-standard data lakehouse principles (Bronze/Silver/Gold) 
adapted for a relational database environment.

**1. Raw Layer**
- `raw.cpi_data` - Unmodified CPI data from source
- `raw.categories` - Category lookup table
- `raw.load_metadata` - Tracking table for ETL runs

**2. Staging Layer**
- `staging.cpi_monthly` - Cleaned CPI data with category names
- `staging.categories` - Standardized category lookup
- `staging.states` - State reference with regional grouping

**3. Mart Layer (Analytics-Ready)**
- `mart.inflation_by_state` - State-level inflation metrics
  - Columns: state, date, index_value, mom_change, yoy_change, inflation_rate
- `mart.inflation_by_category` - Category-level inflation trends
  - Columns: date, division, category_name, avg_index, mom_change, yoy_change
- `mart.state_comparison` - Latest month state rankings
  - Columns: state, overall_cpi, food_cpi, housing_cpi, transport_cpi, rank, pct_vs_cheapest

---

## 🚀 Getting Started

### Prerequisites
- Python 3.10+
- Docker Desktop
- PostgreSQL (via Docker)
- AWS Account (free tier)
- Power BI Desktop

### Installation

1. **Clone the repository**
```bash
git clone https://github.com/ravager1204/malaysian-cpi-analytics.git
cd malaysian-cpi-analytics
```

2. **Create virtual environment**
```bash
python -m venv venv
source venv/bin/activate  # On Windows: venv\Scripts\activate
```

3. **Install dependencies**
```bash
pip install -r requirements.txt
```

4. **Set up environment variables**
```bash
cp .env.example .env
# Edit .env with your credentials
```

5. **Start PostgreSQL**
```bash
docker-compose up -d
```

6. **Initialize database schemas**
```bash
python scripts/run_staging_ddl.py
python scripts/run_mart_ddl.py
```

---

## 📊 Usage

### Run Complete Pipeline

Extract, transform, and upload data in one command:
```bash
python scripts/run_full_pipeline.py
```

This will:
1. ✅ Extract 43,008 CPI records from OpenDOSM
2. ✅ Load to PostgreSQL raw schema
3. ✅ Transform to staging layer
4. ✅ Calculate inflation metrics in mart layer
5. ✅ Upload backup to AWS S3

### Run Individual Steps
```bash
# Extract only
python scripts/extract_cpi_data.py

# Transform staging
python data_ingestion/staging_transformer.py

# Transform mart
python data_ingestion/mart_transformer.py

# Upload to S3
python data_ingestion/s3_uploader.py

# Explore data
python scripts/explore_data.py

# View analytics
python scripts/view_mart_data.py
```

---

## 📈 Sample Queries

### Get Latest Inflation by State
```sql
SELECT 
    state,
    ROUND(yoy_change::numeric, 2) as inflation_rate
FROM mart.inflation_by_state
WHERE date = (SELECT MAX(date) FROM mart.inflation_by_state)
ORDER BY yoy_change DESC;
```

### Top Inflating Categories
```sql
SELECT 
    category_name,
    ROUND(yoy_change::numeric, 2) as yoy_inflation
FROM mart.inflation_by_category
WHERE date = (SELECT MAX(date) FROM mart.inflation_by_category)
ORDER BY yoy_change DESC
LIMIT 5;
```

### State Price Comparison
```sql
SELECT 
    state,
    ROUND(overall_cpi::numeric, 2) as cpi,
    ROUND(pct_vs_cheapest::numeric, 2) as pct_above_cheapest,
    region
FROM mart.state_comparison
ORDER BY overall_cpi DESC;
```

---

## 📊 Power BI Dashboards

The Power BI dashboard includes:

1. **State Comparison Chart** - Bar chart ranking states by CPI
2. **Price Gap Card** - Shows % difference between most/least expensive
3. **Inflation Trends** - Line chart showing inflation over time for key states

To view:
1. Open `powerbi/CPI_Analytics_Dashboard.pbix`
2. Refresh data to get latest from PostgreSQL

---

## 🧪 Data Quality & Validation

The pipeline includes comprehensive data quality checks:

- ✅ **Schema validation** - Ensures required columns exist
- ✅ **Null checks** - Validates critical fields have no nulls
- ✅ **Row count reconciliation** - Verifies data across layers
- ✅ **Date range validation** - Confirms data coverage
- ✅ **Business logic tests** - Validates inflation calculations
- ✅ **Load metadata tracking** - Logs all ETL runs

---

## 📝 Data Source

**Source:** Department of Statistics Malaysia (DOSM) - OpenDOSM  
**API:** https://open.dosm.gov.my/data-catalogue  
**Dataset:** Consumer Price Index (CPI) 2-digit state level  
**License:** Creative Commons Attribution 4.0  
**Update Frequency:** Monthly (mid-month for previous month)

---

## 🎓 Skills Demonstrated

- ✅ **Data Engineering:** ETL pipeline design and implementation
- ✅ **Python Programming:** OOP, modules, error handling, logging
- ✅ **SQL:** Complex queries, CTEs, window functions, dimensional modeling
- ✅ **Database Design:** 3-layer data warehouse (raw/staging/mart)
- ✅ **Cloud Services:** AWS S3, boto3
- ✅ **Data Validation:** Quality checks and testing
- ✅ **Version Control:** Git workflow
- ✅ **Containerization:** Docker for PostgreSQL
- ✅ **Data Visualization:** Power BI dashboard design
- ✅ **Documentation:** Technical writing

---

## 🔮 Future Enhancements

- [ ] Add dbt for transformation layer
- [ ] Implement Apache Airflow for daily scheduling
- [ ] Add more detailed CPI categories (3-digit, 4-digit)
- [ ] Integrate household income data for affordability analysis
- [ ] Build predictive models for inflation forecasting
- [ ] Create REST API for data access
- [ ] Deploy to cloud (AWS EC2/RDS)
- [ ] Add CI/CD pipeline

---

## 📧 Contact

**GitHub:** [@ravager1204](https://github.com/ravager1204)  
**Project Link:** [https://github.com/ravager1204/malaysian-cpi-analytics](https://github.com/ravager1204/malaysian-cpi-analytics)

---

## 📄 License

This project is open source and available under the MIT License.

The data used in this project is from OpenDOSM and is licensed under Creative Commons Attribution 4.0 International License.

---

## 🙏 Acknowledgments

- Department of Statistics Malaysia (DOSM) for providing open data
- OpenDOSM API for easy data access
- The data engineering community for best practices and inspiration

---

**⭐ If you found this project useful, please consider giving it a star!**

