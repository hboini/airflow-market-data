# Airflow Mini-Project: Market Volume Data Pipeline (`marketvol`)

An automated data pipeline built with **Apache Airflow**, **Python**, and **Yahoo Finance (`yfinance`)** to extract, process, and analyze intraday market data for Apple (`AAPL`) and Tesla (`TSLA`).

---

## Prerequisites & Installation

Make sure you have the following installed in your Python virtual environment:
* Python 3.x
* Apache Airflow
* `yfinance` (`pip install yfinance`)
* `pandas` (`pip install pandas`)

---

## 🚀 Project Overview & Architecture

This project uses Apache Airflow TaskFlow API decorators (`@task`, `@task.bash`) to orchestrate a robust data workflow. The DAG is scheduled to run on weekdays at 6:00 PM (`0 18 * * 1-5`) and executes the following sequence:

1. **`t0` (Initialize Directory):** Dynamically creates an execution-date-specific temporary folder (`/tmp/data/{{ ds }}`) using a Bash operator.
2. **`t1` & `t2` (Download Data):** Runs in parallel using `yfinance` to fetch 1-minute interval intraday price data for **AAPL** and **TSLA**.
3. **`t3` & `t4` (Save & Move Files):** Safely copies and relocates the downloaded CSVs to their designated query targets using Python's `pathlib` and `shutil`.
4. **`t5` (Query Stock Data):** Runs a custom analytical query using `pandas` once both stock datasets have successfully landed.

---

## 📁 Project Directory Structure

```text
airflow-project/
├── dags/
│   └── marketvol.py           # Main Airflow DAG implementation
├── include/                   # Helper scripts or shared utilities
├── logs/                      # Airflow task and scheduler logs
└── README.md                  # Project documentation