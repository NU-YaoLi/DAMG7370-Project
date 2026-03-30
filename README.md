# DAMG7370-Project: Washington Real Estate Investment Analysis

## 📖 Topic: House Investment in Washington State
In this project, we analyze property investment in Washington State from two primary financial lenses:

1. **Capital Appreciation:** The change in property value over time. If the value increases, the owner realizes a profit upon sale. This is the "potential return" of the asset.
2. **Rental Income:** By leasing the property, the owner receives a stable monthly cash flow. 

Combined, property value growth and rental income represent the core ways real estate investment generates wealth. Based on this thesis, we analyze historical housing prices and rental data in **WA** to identify trends and patterns. By leveraging a comprehensive national data warehouse, this analysis provides data-driven insights into whether buying a property in a specific Washington location and time is a sound investment decision compared to broader market benchmarks.

---

## 🎯 Project Goals & Outcomes
The objective is to build and deploy a fully automated, state-wide data platform using a **Modern Data Stack**:
* **Automated Extraction:** Ingest national ZHVI (Home Value) and ZORI (Rent Index) datasets from Zillow Research.
* **Distributed ETL:** Use PySpark to handle the high-volume task of cleaning and "unpivoting" time-series data for the entire US.
* **Relational Storage:** Load the complete processed dataset into an RDS instance for high-performance SQL analysis.
* **Interactive BI:** Deploy a Streamlit dashboard to visualize 5-year ROI, gross yields, and net cash flow correlations specifically for **Washington State**.

---

## 🛠️ Tech Stack & Infrastructure
This project utilizes **Infrastructure as Code (IaC)** to ensure the entire pipeline is reproducible and production-ready.

| Component | Tool | Purpose |
| :--- | :--- | :--- |
| **Infrastructure** | **AWS CloudFormation** | Automates the deployment of the S3-Glue-RDS ecosystem using native AWS templates. |
| **Storage** | **Amazon S3** | Data Lake storage with **Partitioning** (by State/Year) for query optimization. |
| **Orchestration** | **AWS Step Functions** | State machine logic to manage job dependencies and error retries. |
| **Compute / ETL** | **AWS Glue (PySpark)** | Distributed processing engine for large-scale data transformation. |
| **Database** | **Amazon RDS (Postgres)** | High-availability relational storage for the final analytics layer. |
| **Visualization** | **Streamlit** | Python-based interactive dashboard for real-time investment metrics. |
| **Identity Management** | **AWS IAM** | Centralized workforce authentication and Single Sign-On (SSO) for secure account access. |

---

## 🚀 Pipeline Architecture

![Pipeline Architecture](docs/architecture-diagram.png)

1. **Data Ingestion:** Raw Zillow Research CSVs (containing national historical data) are uploaded to the `landing/` prefix in **Amazon S3**.
2. **State Machine Orchestration:** **AWS Step Functions** manages the end-to-end workflow, verifying the **RDS** instance availability and initiating the **AWS Glue** Spark environment.
3. **Distributed Processing:** **AWS Glue** runs a PySpark job that:
    * **Data Cleaning:** Handles null values and performs schema enforcement (type mapping) for all regions and ZIP codes nationally.
    * **Relational Transformation:** "Unpivots" the time-series date columns into a standardized "long" format (Row-per-Month), making the multi-gigabyte dataset ready for relational queries.
4. **Secure Loading:** The entire cleaned and transformed dataset is loaded into an **Amazon RDS (PostgreSQL)** instance via a JDBC connection within a private VPC, ensuring data security and high-performance indexing.
5. **Targeted Analytics:** A **Streamlit application** connects to the Amazon RDS warehouse via SQLAlchemy. It performs on-the-fly feature engineering to calculate:

Gross Rental Yield: Monthly rent annualized against total property value.

Net Cash Flow: Difference between rental income and estimated mortgage payments.

Price-to-Rent Ratio: Standardized affordability metric for WA metros.

---

## 📂 Project Structure
```text
├── cloudformation/           # Native AWS Infrastructure as Code
│   ├── main-stack.yaml       # S3, RDS, Glue, and Step Function resource definitions
│   ├── networking-stack.yaml # VPC, Subnets, Security Groups, and Glue Connections
│   ├── parameters.json       # Configurable AWS environments & DB credentials
├── src/
│   └── glue_etl_script.py    # PySpark transformation script (National)
├── scripts/
│   └── setup_db.sql          # RDS Schema and Indexing definitions
└── README.md
```

---
## 📂 Run Dashboard locally
- Install packages: pip install streamlit pandas sqlalchemy psycopg2-binary plotly
- Add credentials in FOLDER/secrets.toml:
[postgres]
host = "your-rds-endpoint.aws.com"
port = 5432
database = "your_db"
username = "your_user"
password = "your_password"
- Execute dashboard: streamlit run dashboard.py

## 📂 visual representation
![unnamed](https://github.com/user-attachments/assets/9bdc4fd5-01b0-4320-a2fe-c3ce09cb0dc9)

