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