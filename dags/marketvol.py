"""Airflow Mini-Project DAG Scheduling"""

from datetime import datetime, timedelta
from airflow import DAG
from airflow.decorators import task
from pathlib import Path
import shutil
import yfinance as yf
import pandas as pd

with DAG(
    'marketvol',
    description='A simple DAG',
    default_args={
        'owner': 'airflow',
        'retries': 2,
        'retry_delay': timedelta(minutes=5),
    },
    start_date=datetime(2026, 8, 26),  
    schedule='0 18 * * 1-5',  # Runs at 6 PM on weekdays (Mon-Fri)
    catchup=False,
) as dag:

    @task.bash(task_id="t0_create_temp_dir")
    def create_temp_dir() -> str:
        return f"mkdir -p /tmp/data/{{{{ ds }}}}"

    def download_market_data(symbol: str, ds: str | None = None) -> str:
        start_date = datetime.strptime(ds, "%Y-%m-%d")
        end_date = start_date + timedelta(days=1)
        df = yf.download(symbol, start=start_date, end=end_date, interval="1m")
        file_path = Path(f"/tmp/data/{ds}/{symbol.lower()}.csv")
        df.to_csv(file_path, header=False)
        return str(file_path)

    @task(task_id="t1_download_aapl")
    def fetch_aapl(ds: str | None = None) -> str:
        return download_market_data(symbol="AAPL", ds=ds)

    @task(task_id="t2_download_tsla")
    def download_tsla(ds: str | None = None) -> str:
        return download_market_data(symbol="TSLA", ds=ds)

    def save_csv_to_destination(symbol: str, data_file: str, ds: str | None = None) -> str:
        destination_path = Path(f"/tmp/data/{ds}/{symbol.lower()}_final.csv")
        destination_path.parent.mkdir(parents=True, exist_ok=True)
        shutil.copy(data_file, destination_path)
        print(f"Saved {symbol} data from {data_file} to {destination_path}")
        return str(destination_path)

    @task(task_id="t3_save_aapl")
    def save_aapl(data_file: str, ds: str | None = None) -> str:
        return save_csv_to_destination(symbol="AAPL", data_file=data_file, ds=ds)

    @task(task_id="t4_save_tsla")
    def save_tsla(data_file: str, ds: str | None = None) -> str:
        return save_csv_to_destination(symbol="TSLA", data_file=data_file, ds=ds)

    @task(task_id="t5_query_stock_data")
    def query_stock_data(aapl_file: str, tsla_file: str, ds: str | None = None) -> str:
        aapl_df = pd.read_csv(aapl_file, header=None)
        tsla_df = pd.read_csv(tsla_file, header=None)
        print(f"--- Market Volume Query Results for {ds} ---")
        print(f"AAPL Records Loaded: {len(aapl_df)}")
        print(f"TSLA Records Loaded: {len(tsla_df)}")
        return "Stock data query executed successfully."

    # Instantiate tasks
    t0 = create_temp_dir()
    t1 = fetch_aapl(ds="{{ ds }}")
    t2 = download_tsla(ds="{{ ds }}")
    t3 = save_aapl(t1, ds="{{ ds }}")
    t4 = save_tsla(t2, ds="{{ ds }}")
    t5 = query_stock_data(t3, t4, ds="{{ ds }}")

    # Set task dependencies
    # - t1 and t2 must run only after t0
    # - t3 must run after t1
    # - t4 must run after t2
    # - t5 must run after both t3 and t4 are complete
    t0 >> [t1,t2]
    t1 >> t3
    t2 >> t4
    [t3, t4] >> t5