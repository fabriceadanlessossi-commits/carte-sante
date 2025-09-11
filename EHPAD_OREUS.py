import os
import sys
import time
import requests
import pandas as pd

API_URL = "https://api-adresse.data.gouv.fr/search/csv/"

# === Chemins de fichiers ===
INPUT_CSV  = r"C:\Users\marketing\Desktop\Projets codes\carte\EHPAD_OREUS.csv"  
OUTPUT_CSV = r"C:\Users\marketing\Desktop\Projets codes\carte\EHPAD_OREUS_geocoded.csv"  

# Colonnes nécessaires pour l’API
NEEDED_COLS = ["adresse", "commune", "code_postal"]

# Options réseau
TIMEOUT = 60
MAX_RETRIES = 3
BACKOFF_SEC = 5

def normalize_headers(df: pd.DataFrame) -> pd.DataFrame:
    df.columns = [c.strip().lower().replace(" ", "_") for c in df.columns]
    return df

def ensure_columns(input_csv: str, sep: str = ";") -> None:
   # df = pd.read_csv(input_csv, sep=sep, dtype=str, encoding="utf-8", engine="python")
   try:
    df = pd.read_csv(input_csv, sep=";", dtype=str, encoding="utf-8", engine="python")
   except Exception:
    try:
        df = pd.read_csv(input_csv, sep=",", dtype=str, encoding="utf-8", engine="python")
    except Exception:
        df = pd.read_csv(input_csv, sep="\t", dtype=str, encoding="utf-8", engine="python")

    df = normalize_headers(df)
    missing = [c for c in NEEDED_COLS if c not in df.columns]
    if missing:
        print("\n⚠️ Colonnes manquantes :", missing)
        print("👉 Colonnes présentes :", list(df.columns))
        print("Astuce : renomme dans Excel tes colonnes en :", NEEDED_COLS)
        sys.exit(1)
    print(f"✅ CSV prêt. Lignes: {len(df)} | Colonnes: {list(df.columns)}")

def post_csv(input_csv: str, output_csv: str) -> None:
    files = {
        "data": (os.path.basename(input_csv), open(input_csv, "rb"), "text/csv"),
    }
    data = [("columns", col) for col in NEEDED_COLS]

    attempt = 0
    while attempt < MAX_RETRIES:
        try:
            print(f"⏳ Envoi à l'API (tentative {attempt+1}/{MAX_RETRIES})…")
            with requests.post(API_URL, files=files, data=data, stream=True, timeout=TIMEOUT) as r:
                if r.status_code != 200:
                    raise RuntimeError(f"HTTP {r.status_code} - {r.text[:300]}")
                with open(output_csv, "wb") as f:
                    for chunk in r.iter_content(chunk_size=8192):
                        if chunk:
                            f.write(chunk)
            print(f"✅ Géocodage terminé. Fichier écrit : {output_csv}")
            return
        except Exception as e:
            attempt += 1
            print(f"⚠️ Erreur: {e}")
            if attempt < MAX_RETRIES:
                print(f"… nouvelle tentative dans {BACKOFF_SEC}s")
                time.sleep(BACKOFF_SEC)
            else:
                print("❌ Échec après plusieurs tentatives.")
                sys.exit(1)
        finally:
            try:
                files["data"][1].close()
            except Exception:
                pass
            if attempt < MAX_RETRIES:
                files["data"] = (os.path.basename(input_csv), open(input_csv, "rb"), "text/csv")

def quick_report(geo_csv: str, sep: str = ";") -> None:
    try:
        df = pd.read_csv(geo_csv, sep=sep, dtype=str, encoding="utf-8", engine="python")
    except Exception:
        df = pd.read_csv(geo_csv, sep=",", dtype=str, encoding="utf-8", engine="python")

    print("\n--- RAPPORT ---")
    print(f"Lignes totales : {len(df)}")

    for col in df.columns:
        if col.lower() in ["result_lat", "result_lon", "result_score"]:
            print(f"Colonne trouvée : {col}")

if __name__ == "__main__":
    if not os.path.exists(INPUT_CSV):
        print(f"❌ Fichier introuvable : {INPUT_CSV}")
        sys.exit(1)

    ensure_columns(INPUT_CSV, sep=";")
    post_csv(INPUT_CSV, OUTPUT_CSV)
    quick_report(OUTPUT_CSV)
