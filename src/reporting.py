import pandas as pd
import os
from src.database import Database

class ReportGenerator:
    def __init__(self):
        self.db = Database()
        self.output_path = "data/output/"
        os.makedirs(self.output_path, exist_ok=True)

    def generate_all(self):
        print("Generating reports...")
        self._output_1_batch_log()
        self._output_2_sales_metrics()
        self._output_3_top_stores()

    def _output_1_batch_log(self):
        query = """
        SELECT 
            CURRENT_DATE as snapshot_date,
            batch_date,
            SUM(total_rows) as total_processed,
            SUM(valid_rows) as valid_rows,
            SUM(invalid_rows) as ignored_rows
        FROM analytics.sys_batch_log
        GROUP BY batch_date
        ORDER BY batch_date DESC
        LIMIT 40;
        """
        self._save_csv(query, "output_1_batches.csv")

    def _output_2_sales_metrics(self):
        query = """
        SELECT 
            CURRENT_DATE as snapshot_date,
            DATE(transaction_time) as transaction_date,
            COUNT(DISTINCT store_token) as active_stores,
            SUM(amount) as total_sales,
            AVG(amount) as avg_sales,
            SUM(SUM(amount)) OVER (
                PARTITION BY TO_CHAR(DATE(transaction_time), 'YYYY-MM') 
                ORDER BY DATE(transaction_time)
            ) as mtd_sales_accumulated
        FROM analytics.fact_sales
        GROUP BY DATE(transaction_time)
        ORDER BY transaction_date DESC
        LIMIT 40;
        """
        self._save_csv(query, "output_2_daily_sales.csv")

    def _output_3_top_stores(self):
        query = """
        WITH DailyStats AS (
            SELECT 
                DATE(transaction_time) as t_date,
                s.store_token,
                s.store_name,
                SUM(fs.amount) as total
            FROM analytics.fact_sales fs
            JOIN analytics.dim_stores s ON fs.store_token = s.store_token
            GROUP BY 1, 2, 3
        ),
        Ranked AS (
            SELECT *, DENSE_RANK() OVER (PARTITION BY t_date ORDER BY total DESC) as rnk
            FROM DailyStats
        )
        SELECT CURRENT_DATE as snapshot_date, t_date, rnk, total, store_token, store_name
        FROM Ranked
        WHERE rnk <= 5
        ORDER BY t_date DESC, rnk ASC;
        """
        self._save_csv(query, "output_3_top_stores.csv")

    def _save_csv(self, query, filename):
        try:
            conn = self.db.get_connection()
            df = pd.read_sql(query, conn)
            df.to_csv(f"{self.output_path}{filename}", index=False)
            print(f"Generated {filename}")
            conn.close()
        except Exception as e:
            print(f"Error generating {filename}: {e}")
