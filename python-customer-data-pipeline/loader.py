import sqlite3
import pandas as pd


def load_to_sqlite(df: pd.DataFrame, db_path: str, table_name: str = "customers") -> int:
    # Crea una connessione SQLite al file indicato
    conn = sqlite3.connect(db_path)

    try:
        # Salviamo il dataframe nella tabella SQL
        # if_exists="replace" sostituisce la tabella se esiste già
        df.to_sql(table_name, conn, if_exists="replace", index=False)

        # Restituiamo il numero di righe caricate
        return len(df)

    finally:
        # Chiudiamo sempre la connessione
        conn.close()