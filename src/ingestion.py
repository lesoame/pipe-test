import os
import shutil
import pandas as pd
from datetime import datetime
from src.database import Database

class IngestionEngine:
    def __init__(self):
        self.db = Database()
        self.inbox_path = "data/inbox/"
        self.history_path = "data/history/"
        
        # Garante que as pastas existem
        os.makedirs(self.history_path, exist_ok=True)

    def process_inbox(self):
        files = [f for f in os.listdir(self.inbox_path) if f.endswith('.csv')]
        
        if not files:
            print("No files found in inbox.")
            return

        for file in files:
            print(f"Processing {file}...")
            try:
                if "stores" in file:
                    self._process_stores(file)
                elif "sales" in file:
                    self._process_sales(file)
                
                # Move to history
                shutil.move(
                    os.path.join(self.inbox_path, file),
                    os.path.join(self.history_path, file)
                )
                print(f"Moved {file} to history.")
            except Exception as e:
                print(f"Error processing {file}: {e}")

    def _process_stores(self, filename):
        df = pd.read_csv(os.path.join(self.inbox_path, filename))
        
        upsert_sql = """
        INSERT INTO analytics.dim_stores (store_group, store_token, store_name)
        VALUES (%s, %s, %s)
        ON CONFLICT (store_token) 
        DO UPDATE SET store_name = EXCLUDED.store_name, updated_at = NOW();
        """
        
        conn = self.db.get_connection()
        cur = conn.cursor()
        
        for _, row in df.iterrows():
            try:
                cur.execute(upsert_sql, (row['store_group'], row['store_token'], row['store_name']))
            except Exception as e:
                print(f"Skipping row in stores: {e}")
        
        conn.commit()
        conn.close()

    def _process_sales(self, filename):
        # Extract date from filename sales_20251128.csv
        try:
            batch_date_str = filename.split('_')[1].replace('.csv', '')
            batch_date = datetime.strptime(batch_date_str, "%Y%m%d").date()
        except:
            batch_date = datetime.now().date()

        df = pd.read_csv(os.path.join(self.inbox_path, filename))
        total_rows = len(df)
        valid_rows = 0
        
        conn = self.db.get_connection()
        cur = conn.cursor()

        insert_sql = """
        INSERT INTO analytics.fact_sales 
        (store_token, transaction_id, receipt_token, transaction_time, amount, user_role, batch_date)
        VALUES (%s, %s, %s, %s, %s, %s, %s)
        ON CONFLICT (store_token, transaction_id)
        DO UPDATE SET amount = EXCLUDED.amount, transaction_time = EXCLUDED.transaction_time;
        """

        for _, row in df.iterrows():
            try:
                # Clean Amount ($63.98 -> 63.98)
                raw_amt = str(row['amount']).replace('$', '').strip()
                clean_amount = float(raw_amt)
                
                cur.execute(insert_sql, (
                    row['store_token'], row['transaction_id'], 
                    row['receipt_token'], row['transaction_time'], 
                    clean_amount, row['user_role'], batch_date
                ))
                valid_rows += 1
            except Exception as e:
                continue

        # Log Batch
        cur.execute("""
            INSERT INTO analytics.sys_batch_log 
            (file_name, batch_date, file_type, total_rows, valid_rows, invalid_rows)
            VALUES (%s, %s, 'sales', %s, %s, %s)
            ON CONFLICT (file_name) DO NOTHING;
        """, (filename, batch_date, total_rows, valid_rows, total_rows - valid_rows))

        conn.commit()
        conn.close()