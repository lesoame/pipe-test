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