import json
import logging
from pathlib import Path


def ensure_directory(path: str) -> None:
    # Crea una cartella se non esiste già
    Path(path).mkdir(parents=True, exist_ok=True)


def save_json(data: dict, output_path: str) -> None:
    # Salva un dizionario Python in formato JSON
    with open(output_path, "w", encoding="utf-8") as file:
        json.dump(data, file, indent=4)


def setup_logging(log_path: str) -> None:
    # Configura il file di log della pipeline
    logging.basicConfig(
        filename=log_path,
        level=logging.INFO,
        format="%(asctime)s - %(levelname)s - %(message)s",
        force=True
    )

    # Scriviamo subito una riga iniziale per segnalare l'avvio
    logging.info("Pipeline started")