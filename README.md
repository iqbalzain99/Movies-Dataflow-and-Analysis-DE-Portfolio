# End-to-End Movie Data Pipeline

![Pipeline Architecture](lib/assets/movies_image.png)

An automated data pipeline for processing movie metadata and ratings data, featuring data lake storage, data warehousing, and visualization.

## 📌 Overview

This project implements a complete data pipeline that:

1. Extracts movie data from Kaggle
2. Processes and cleans the data
3. Stores raw/processed data in AWS S3 (Data Lake)
4. Loads structured data into Snowflake (Data Warehouse)
5. Orchestrates workflows with Apache Airflow
6. Visualizes insights through Metabase
7. Runs in Docker containers deployed on AWS EC2

## ✨ Key Features

- **Automated Data Ingestion**: Fetch data from Kaggle using Python API
- **Data Validation**: Comprehensive data cleaning and transformation
- **Cloud Storage**: AWS S3 integration for Parquet file storage
- **Data Warehousing**: Snowflake integration for structured storage
- **Workflow Orchestration**: Airflow DAGs for pipeline scheduling
- **Containerization**: Docker-compose for service management
- **BI Dashboard**: Metabase for interactive visualizations

## 📂 Project Structure

### Root Directory

```
.
├── dags/ # Airflow DAG definitions
│ ├── movie_data_dag.py # Main data pipeline DAG
│ └── my_custom_dag.py # Experimental/custom DAGs
├── docker-compose.yml # Docker orchestration config
├── Dockerfile # Airflow image configuration
├── requirements.txt # Python dependencies
├── lib/
│ ├── utils/ # Utility modules
| | ├── config/ # Snowflake, Kaggle, AWS credentials loader from .env
│ │ ├── snowflake_handler.py # Snowflake connector
│ │ ├── s3_parquet_handler.py # S3 Parquet operations
│ │ └── movie_dataset_etl.py # ETL operations
│ └── assets/ # Data samples & images
├── config/ # Airflow configuration
├── data_pipeline_processing.ipynb # Data exploration notebook
└── ETL_Testing.ipynb # Pipeline testing notebook
```

## 🚀 Getting Started

### Prerequisites

- AWS Account (EC2, S3, IAM)
- Snowflake Account
- Docker & Docker-compose
- Python 3.8+
- Kaggle API Token

### Installation

1. **Clone Repository**

   ```bash
   git clone https://github.com/iqbalzain99/Movies-Dataflow-and-Analysis-DE-Portfolio.git
   cd Movies-Dataflow-and-Analysis-DE-Portfolio
   ```

2. **Configure Environment**

   ```bash
   cp .env.example .env
   # Update with your credentials:
   # - Snowflake
   # - AWS
   # - Kaggle
   # - Airflow
   ```

3. **Build & Start Services**

   ```bash
   docker-compose build
   docker-compose up -d
   ```

4. **Access Services**
   - Airflow: `http://<EC2-IP>:8080` (airflow/airflow)
   - Metabase: `http://<EC2-IP>:3000`

## ⚙️ Pipeline Workflow

1. **Data Extraction**

   - Download dataset from Kaggle using Python API
   - Validate dataset completeness

2. **Data Transformation**

   - Clean invalid/missing values
   - Normalize nested JSON structures
   - Convert datatypes (fix datetime formats)
   - Split combined columns

3. **Data Storage**

   - Write processed data to S3 as Parquet
   - Load into Snowflake tables

   ```python
   # Parquet write with Snowflake-compatible timestamps
   df.to_parquet(..., engine='pyarrow', use_deprecated_int96_timestamps=True)
   ```

4. **Data Mart Creation**

   ```sql
   CREATE OR REPLACE TABLE MART.MOVIE_FIN_MART AS
   SELECT BUDGET, REVENUE, RELEASE_DATE...
   WHERE BUDGET > 1000 AND REVENUE != 0;
   ```

5. **Visualization**
   - Metabase connects to Snowflake

## 🔧 Technologies Used

| Technology     | Purpose                |
| -------------- | ---------------------- |
| Apache Airflow | Workflow orchestration |
| AWS S3         | Data Lake storage      |
| Snowflake      | Data Warehousing       |
| Metabase       | Data Visualization     |
| Docker         | Containerization       |
| Python         | Data processing & ETL  |
| Pandas         | Data manipulation      |

## 📧 Contact

Project Maintainer - [Muhammad Iqbal Zain](iqbalzain99@gmail.com)

## 💻 Details Slide

Detail of the Data Arcitecture and this project can be accessed [here](https://drive.google.com/file/d/13ZCL2aYgbuFRpRCY42o4pFwlUKWJcXBh/view?usp=drive_link).
