import logging
from pathlib import Path

import pandas as pd

from validator import validate_row
from cleaner import clean_row
from loader import load_to_sqlite
from utils import ensure_directory, save_json, setup_logging


# Percorsi principali del progetto
INPUT_FILE = "input/raw_customers.csv"
OUTPUT_DIR = "output"
DATA_DIR = "data"

CLEANED_FILE = f"{OUTPUT_DIR}/cleaned_customers.csv"
REJECTED_FILE = f"{OUTPUT_DIR}/rejected_customers.csv"
REPORT_FILE = f"{OUTPUT_DIR}/pipeline_report.json"
LOG_FILE = f"{OUTPUT_DIR}/pipeline.log"
SQLITE_FILE = f"{DATA_DIR}/customers.db"


def main() -> None:
    # Creiamo le cartelle di output e data se non esistono
    ensure_directory(OUTPUT_DIR)
    ensure_directory(DATA_DIR)

    # Inizializziamo il logging
    setup_logging(LOG_FILE)

    logging.info("Reading input CSV file")

    # Leggiamo il file CSV grezzo
    df = pd.read_csv(INPUT_FILE)

    # Convertiamo il dataframe in una lista di dizionari
    # Questo rende più semplice lavorare riga per riga
    rows = df.to_dict(orient="records")

    # Liste che useremo per separare righe valide e scartate
    cleaned_rows = []
    rejected_rows = []

    # Set per controllare i duplicati sul customer_id
    seen_customer_ids = set()

    # Contatori utili per il report finale
    duplicates_removed = 0

    # Elaboriamo ogni riga singolarmente
    for row in rows:
        # Prima validiamo la riga
        rejection_reason = validate_row(row)

        # Se la riga non è valida, la salviamo tra gli scarti
        if rejection_reason is not None:
            rejected_row = dict(row)
            rejected_row["rejection_reason"] = rejection_reason
            rejected_rows.append(rejected_row)
            continue

        # Se la riga è valida, la puliamo e normalizziamo
        cleaned_row = clean_row(row)

        # Controllo duplicati sul customer_id
        customer_id = cleaned_row["customer_id"]

        if customer_id in seen_customer_ids:
            rejected_row = dict(row)
            rejected_row["rejection_reason"] = "duplicate_customer_id"
            rejected_rows.append(rejected_row)
            duplicates_removed += 1
            continue

        # Se il customer_id non è duplicato, lo aggiungiamo all'insieme
        seen_customer_ids.add(customer_id)

        # Aggiungiamo la riga pulita all'output valido
        cleaned_rows.append(cleaned_row)

    logging.info("Validation and cleaning completed")

    # Convertiamo le liste finali in DataFrame pandas
    cleaned_df = pd.DataFrame(cleaned_rows)
    rejected_df = pd.DataFrame(rejected_rows)

    # Salviamo i risultati in CSV
    cleaned_df.to_csv(CLEANED_FILE, index=False)
    rejected_df.to_csv(REJECTED_FILE, index=False)

    logging.info("CSV outputs generated")

    # Carichiamo i dati puliti in SQLite
    sqlite_loaded_rows = load_to_sqlite(cleaned_df, SQLITE_FILE)

    logging.info("SQLite loading completed")

    # Creiamo il report finale della pipeline
    report = {
        "rows_read": len(rows),
        "rows_cleaned": len(cleaned_rows),
        "rows_rejected": len(rejected_rows),
        "duplicates_removed": duplicates_removed,
        "sqlite_loaded_rows": sqlite_loaded_rows,
        "cleaned_file": CLEANED_FILE,
        "rejected_file": REJECTED_FILE,
        "sqlite_database": SQLITE_FILE
    }

    # Salviamo il report in JSON
    save_json(report, REPORT_FILE)

    logging.info("JSON report generated")
    logging.info("Pipeline completed successfully")

    # Stampiamo un piccolo riepilogo a terminale
    print("Pipeline completed successfully")
    print(f"Rows read: {report['rows_read']}")
    print(f"Rows cleaned: {report['rows_cleaned']}")
    print(f"Rows rejected: {report['rows_rejected']}")
    print(f"Duplicates removed: {report['duplicates_removed']}")
    print(f"SQLite loaded rows: {report['sqlite_loaded_rows']}")


if __name__ == "__main__":
    main()