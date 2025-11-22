telecom-cdr-production-pipeline/
├── dags/
│   └── telecom_daily_pipeline.py          # ← your final DAG
├── spark_jobs/
│   └── direct_to_bigquery.py              # ← your PySpark script
├── cloud_functions/
│   └── send_telecom_alert/
│       └── main.py
├── sql/
│   └── daily_usage_summary.sql
├── README.md
└── requirements.txt
