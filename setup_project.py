import os

def create_file(path, content):
    # Garante que o diretório pai existe
    os.makedirs(os.path.dirname(path), exist_ok=True)
    with open(path, "w", encoding="utf-8") as f:
        f.write(content.strip())
    print(f" Criado: {path}")

# --- CONTEÚDOS DOS ARQUIVOS ---

DOCKER_COMPOSE = """
version: '3.8'

services:
  postgres:
    image: postgres:15
    container_name: de_assessment_db
    environment:
      POSTGRES_USER: admin
      POSTGRES_PASSWORD: password
      POSTGRES_DB: assessment_db
    volumes:
      - ./sql/init.sql:/docker-entrypoint-initdb.d/init.sql
      - pg_data:/var/lib/postgresql/data
    ports:
      - "5432:5432"
    healthcheck:
      test: ["CMD-SHELL", "pg_isready -U admin -d assessment_db"]
      interval: 10s
      timeout: 5s
      retries: 5

volumes:
  pg_data:
"""

SQL_INIT = """
CREATE SCHEMA IF NOT EXISTS analytics;

-- 1. Tabela de Controle
CREATE TABLE IF NOT EXISTS analytics.sys_batch_log (
    file_name VARCHAR(255) PRIMARY KEY,
    batch_date DATE,
    file_type VARCHAR(50),
    processed_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    total_rows INT DEFAULT 0,
    valid_rows INT DEFAULT 0,
    invalid_rows INT DEFAULT 0
);

-- 2. Dimensão Lojas (SCD Type 1)
CREATE TABLE IF NOT EXISTS analytics.dim_stores (
    store_token UUID PRIMARY KEY,
    store_group VARCHAR(50),
    store_name VARCHAR(255),
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- 3. Fato Vendas
CREATE TABLE IF NOT EXISTS analytics.fact_sales (
    transaction_id UUID,
    store_token UUID,
    receipt_token VARCHAR(50),
    transaction_time TIMESTAMP,
    amount NUMERIC(12,2),
    user_role VARCHAR(50),
    batch_date DATE,
    loaded_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    PRIMARY KEY (store_token, transaction_id)
);

-- Indexes
CREATE INDEX IF NOT EXISTS idx_sales_time ON analytics.fact_sales(transaction_time);
CREATE INDEX IF NOT EXISTS idx_sales_batch ON analytics.fact_sales(batch_date);
"""

CONFIG_YAML = """
database:
  host: "localhost"
  port: "5432"
  dbname: "assessment_db"
  user: "admin"
  password: "password"
"""

REQUIREMENTS = """
pandas
psycopg2-binary
pyyaml
"""

SRC_DATABASE = """
import psycopg2
import yaml
import os

class Database:
    def __init__(self, config_path="config/config.yaml"):
        # Ajuste de path para rodar da raiz
        if not os.path.exists(config_path):
            raise FileNotFoundError(f"Config file not found at {config_path}")
            
        with open(config_path, "r") as f:
            self.cfg = yaml.safe_load(f)['database']
        
    def get_connection(self):
        conn = psycopg2.connect(
            host=self.cfg['host'],
            port=self.cfg['port'],
            database=self.cfg['dbname'],
            user=self.cfg['user'],
            password=self.cfg['password']
        )
        return conn
"""

SRC_INGESTION = """
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
        
        upsert_sql = \"\"\"
        INSERT INTO analytics.dim_stores (store_group, store_token, store_name)
        VALUES (%s, %s, %s)
        ON CONFLICT (store_token) 
        DO UPDATE SET store_name = EXCLUDED.store_name, updated_at = NOW();
        \"\"\"
        
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

        insert_sql = \"\"\"
        INSERT INTO analytics.fact_sales 
        (store_token, transaction_id, receipt_token, transaction_time, amount, user_role, batch_date)
        VALUES (%s, %s, %s, %s, %s, %s, %s)
        ON CONFLICT (store_token, transaction_id)
        DO UPDATE SET amount = EXCLUDED.amount, transaction_time = EXCLUDED.transaction_time;
        \"\"\"

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
        cur.execute(\"\"\"
            INSERT INTO analytics.sys_batch_log 
            (file_name, batch_date, file_type, total_rows, valid_rows, invalid_rows)
            VALUES (%s, %s, 'sales', %s, %s, %s)
            ON CONFLICT (file_name) DO NOTHING;
        \"\"\", (filename, batch_date, total_rows, valid_rows, total_rows - valid_rows))

        conn.commit()
        conn.close()
"""

SRC_REPORTING = """
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
        query = \"\"\"
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
        \"\"\"
        self._save_csv(query, "output_1_batches.csv")

    def _output_2_sales_metrics(self):
        query = \"\"\"
        SELECT 
            CURRENT_DATE as snapshot_date,
            DATE(transaction_time) as transaction_date,
            COUNT(DISTINCT store_token) as active_stores,
            SUM(amount) as total_sales,
            AVG(amount) as avg_sales,
            SUM(SUM(amount)) OVER (
                PARTITION BY TO_CHAR(transaction_time, 'YYYY-MM') 
                ORDER BY DATE(transaction_time)
            ) as mtd_sales_accumulated
        FROM analytics.fact_sales
        GROUP BY 1, 2
        ORDER BY 2 DESC
        LIMIT 40;
        \"\"\"
        self._save_csv(query, "output_2_daily_sales.csv")

    def _output_3_top_stores(self):
        query = \"\"\"
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
        \"\"\"
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
"""

MAIN_PY = """
from src.ingestion import IngestionEngine
from src.reporting import ReportGenerator
import time

def main():
    print("--- Starting Data Pipeline ---")
    
    # 1. Ingest Data (Extract & Load)
    try:
        ingestor = IngestionEngine()
        ingestor.process_inbox()
    except Exception as e:
        print(f"Ingestion failed: {e}")
        return
    
    # 2. Transform & Report
    try:
        reporter = ReportGenerator()
        reporter.generate_all()
    except Exception as e:
        print(f"Reporting failed: {e}")
    
    print("--- Pipeline Finished Successfully ---")

if __name__ == "__main__":
    # Pequeno delay para garantir que o DB subiu se rodar tudo junto
    main()
"""

README_MD = """
# Data Engineering Assessment

## Estrutura
- `data/inbox`: Coloque arquivos CSV aqui.
- `data/output`: Relatórios gerados aparecem aqui.
- `src/`: Código fonte.

## Como rodar
1. Suba o banco: `docker-compose up -d`
2. Instale deps: `pip install -r requirements.txt`
3. Execute: `python main.py`
"""

# --- DADOS DE TESTE (CSV) ---

TEST_STORES = """store_group,store_token,store_name
00001A4A,00001a4a-786e-49de-b9cf-2a3f06a1fad9,Loja Centro
00001E36,00001e36-06bf-4195-9cf0-3cb07eda8b62,Loja Shopping
00007AB7,00007ab7-417b-4ccb-b156-0ddbf2a8aa21,Loja Aeroporto
"""

TEST_SALES = """store_token,transaction_id,receipt_token,transaction_time,amount,user_role
00001a4a-786e-49de-b9cf-2a3f06a1fad9,27a7720a-c0a9-4391-8458-316059e10fca,REC001,2025-11-28 10:00:00,$100.50,Cashier
00001a4a-786e-49de-b9cf-2a3f06a1fad9,27a7720a-c0a9-4391-8458-316059e10fcb,REC002,2025-11-28 10:05:00,$50.00,Cashier
00001e36-06bf-4195-9cf0-3cb07eda8b62,33a7720a-c0a9-4391-8458-316059e10fcc,REC003,2025-11-28 11:00:00,$200.00,Manager
"""

# --- GERANDO A ESTRUTURA ---

BASE_DIR = "data-assessment"

structure = {
    f"{BASE_DIR}/docker-compose.yml": DOCKER_COMPOSE,
    f"{BASE_DIR}/sql/init.sql": SQL_INIT,
    f"{BASE_DIR}/config/config.yaml": CONFIG_YAML,
    f"{BASE_DIR}/requirements.txt": REQUIREMENTS,
    f"{BASE_DIR}/src/__init__.py": "",
    f"{BASE_DIR}/src/database.py": SRC_DATABASE,
    f"{BASE_DIR}/src/ingestion.py": SRC_INGESTION,
    f"{BASE_DIR}/src/reporting.py": SRC_REPORTING,
    f"{BASE_DIR}/main.py": MAIN_PY,
    f"{BASE_DIR}/README.md": README_MD,
    # Dados Mockados para teste imediato
    f"{BASE_DIR}/data/inbox/stores_20251128.csv": TEST_STORES,
    f"{BASE_DIR}/data/inbox/sales_20251128.csv": TEST_SALES
}

print(f" Gerando projeto em '{BASE_DIR}'...")

for path, content in structure.items():
    create_file(path, content)

print("\n Projeto criado com sucesso!")
print(f"1. cd {BASE_DIR}")
print("2. docker-compose up -d  (Aguarde o Postgres subir)")
print("3. pip install -r requirements.txt")
print("4. python main.py")
