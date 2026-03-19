import io
import zipfile

import pandas as pd
import requests
from google.cloud import bigquery

# ── Config ────────────────────────────────────────────────────────────────
PROJECT_ID = "project-7634b355-01fb-4460-96c"
DATASET_ID = "pa1_election"
TABLE_ID   = "precinct_results"

# MIT Election Lab — PA precinct results
# 2025 congressional data does not exist (off-year; no federal House elections)
# 2022: zip contains pa22_cleaned.csv; district stored as float string "1.0"
# 2024: zip contains cleaned/2024-pa-precinct-general.csv; district stored as "001"
SOURCES = [
    {
        "year": 2022,
        "url": "https://raw.githubusercontent.com/MEDSL/2022-elections-official/main/individual_states/2022-pa-local-precinct-general.zip",
        "csv_path": "pa22_cleaned.csv",
        "district_match": "1.0",
    },
    {
        "year": 2024,
        "url": "https://raw.githubusercontent.com/MEDSL/2024-elections-official/main/individual_states/pa24.zip",
        "csv_path": "cleaned/2024-pa-precinct-general.csv",
        "district_match": "001",
    },
]


# ── Extract ───────────────────────────────────────────────────────────────
def extract(source: dict) -> pd.DataFrame:
    year = source["year"]
    print(f"Downloading {year} data from MIT Election Lab...")
    r = requests.get(source["url"], timeout=60)
    r.raise_for_status()
    z = zipfile.ZipFile(io.BytesIO(r.content))
    df = pd.read_csv(z.open(source["csv_path"]), low_memory=False)
    print(f"  {len(df):,} raw rows")
    return df


# ── Transform ─────────────────────────────────────────────────────────────
def transform(df: pd.DataFrame, source: dict) -> pd.DataFrame:
    year = source["year"]
    print(f"Transforming {year} data...")

    df.columns = df.columns.str.lower().str.strip()

    # Filter to US House only
    df = df[df["office"].str.upper() == "US HOUSE"].copy()

    # Filter to congressional district 1
    df = df[df["district"].astype(str).str.strip() == source["district_match"]].copy()

    # Normalize party to DEM / REP / OTHER using party_simplified column
    party_col = "party_simplified" if "party_simplified" in df.columns else "party_detailed"
    df["party"] = (
        df[party_col]
        .str.upper()
        .str.strip()
        .map({"DEMOCRAT": "DEM", "REPUBLICAN": "REP"})
        .fillna("OTHER")
    )

    # Standardize county column name
    county_col = "county_name" if "county_name" in df.columns else "county"
    df = df.rename(columns={county_col: "county"})

    # Clean votes
    df["votes"] = pd.to_numeric(df["votes"], errors="coerce").fillna(0).astype(int)

    # Add election year
    df["election_year"] = year

    # Normalize district to plain integer string ("1")
    df["district"] = "1"

    # Keep only the columns we need
    keep = ["election_year", "county", "precinct", "office", "district",
            "candidate", "party", "votes"]
    df = df[[c for c in keep if c in df.columns]].copy()

    # Normalize text fields
    for col in ["county", "precinct", "candidate", "office", "district"]:
        if col in df.columns:
            df[col] = df[col].astype(str).str.strip().str.title()

    # Ensure correct dtypes so pyarrow serializes cleanly
    df["election_year"] = df["election_year"].astype(int)
    df["votes"] = df["votes"].astype(int)

    print(f"  {len(df):,} rows after filtering to PA-1")
    return df


# ── Load ──────────────────────────────────────────────────────────────────
def load(df: pd.DataFrame) -> None:
    print(f"\nLoading {len(df):,} rows into BigQuery...")
    client = bigquery.Client(project=PROJECT_ID)

    dataset_ref = bigquery.Dataset(f"{PROJECT_ID}.{DATASET_ID}")
    dataset_ref.location = "US"
    client.create_dataset(dataset_ref, exists_ok=True)

    table_ref = f"{PROJECT_ID}.{DATASET_ID}.{TABLE_ID}"
    job_config = bigquery.LoadJobConfig(
        write_disposition="WRITE_TRUNCATE",
        autodetect=True,
    )
    job = client.load_table_from_dataframe(df, table_ref, job_config=job_config)
    job.result()

    # Confirm row count from BigQuery
    table = client.get_table(table_ref)
    print(f"  BigQuery confirms {table.num_rows:,} rows in {table_ref}")


# ── Run ───────────────────────────────────────────────────────────────────
if __name__ == "__main__":
    frames = []
    for source in SOURCES:
        raw = extract(source)
        clean = transform(raw, source)
        frames.append(clean)

    combined = pd.concat(frames, ignore_index=True)
    print(f"\nTotal rows to load: {len(combined):,}")
    load(combined)
    print("\nDone.")
