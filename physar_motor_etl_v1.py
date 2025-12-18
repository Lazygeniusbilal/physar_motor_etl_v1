import os
import io
import numpy as np
import pandas as pd
from datetime import datetime
from faker import Faker
from supabase import create_client
from dotenv import load_dotenv

# =============================
# 1. Load environment variables
# =============================
load_dotenv()
SUPABASE_URL = os.getenv("SUPABASE_URL")
SUPABASE_KEY = os.getenv("SUPABASE_KEY")
RAW_SCHEMA= os.getenv("RAW_SCHEMA_NAME")

# Connect to Supabase
supabase = create_client(SUPABASE_URL, SUPABASE_KEY)

# Faker for generating fake data
fake = Faker("en_GB")
np.random.seed(42)

# =============================
# 2. Data Generation Engine
# =============================
VEH_MAKES = ["Ford", "Vauxhall", "BMW", "Audi", "Mercedes", "Toyota", "Honda"]
VEH_BODY_TYPES = ["Hatchback", "Saloon", "Estate", "SUV", "Coupe"]
FUEL_TYPES = ["Petrol", "Diesel", "Hybrid", "Electric"]
GEARBOX_TYPES = ["Manual", "Automatic"]
COVER_TYPES = ["TPFT", "TPO", "Comprehensive"]
USE_TYPES = ["SDP", "SDP+Commute", "Business"]
PAYMENT_METHODS = ["Annual CC", "Annual DD", "Monthly DD"]

def make_base_frame(n_rows: int) -> pd.DataFrame:
    driver_age = np.random.randint(17, 90, size=n_rows)
    gender = np.random.choice(["M", "F"], size=n_rows)
    licence_years = np.clip(driver_age - np.random.randint(17, 25, size=n_rows), 0, None)
    ncd_years = np.random.randint(0, 15, size=n_rows)
    total_claims = np.random.poisson(0.3, size=n_rows)
    vehicle_year = datetime.now().year - np.random.randint(0, 20, size=n_rows)
    
    return pd.DataFrame({
        "policy_id": [f"POL{100000 + i}" for i in range(n_rows)],
        "driver_age": driver_age,
        "gender": gender,
        "licence_years": licence_years,
        "ncd_years": ncd_years,
        "total_claims": total_claims,
        "vehicle_make": np.random.choice(VEH_MAKES, size=n_rows),
        "vehicle_model": ["Model_" + str(np.random.randint(1, 50)) for _ in range(n_rows)],
        "vehicle_year": vehicle_year,
        "vehicle_body_type": np.random.choice(VEH_BODY_TYPES, size=n_rows),
        "fuel_type": np.random.choice(FUEL_TYPES, size=n_rows),
        "gearbox_type": np.random.choice(GEARBOX_TYPES, size=n_rows),
        "annual_mileage": np.random.randint(2000, 25000, size=n_rows),
        "postcode": [fake.postcode() for _ in range(n_rows)],
        "cover_type": np.random.choice(COVER_TYPES, size=n_rows),
        "use_type": np.random.choice(USE_TYPES, size=n_rows),
        "payment_method": np.random.choice(PAYMENT_METHODS, size=n_rows),
    })

# =============================
# 3. Function to push DataFrame to raw schema
# =============================
def push_df_to_raw_schema(df: pd.DataFrame, table_base_name: str):
    date_suffix = datetime.now().strftime("%Y%m%d")
    table_name = f"{table_base_name}_{date_suffix}"

    # Generate CREATE TABLE SQL dynamically
    columns = ", ".join([f"{c} text" for c in df.columns])
    create_sql = f"CREATE TABLE IF NOT EXISTS {RAW_SCHEMA}.{table_name} ({columns});"

    # Execute CREATE TABLE
    supabase.postgrest.rpc("execute_sql", {"sql": create_sql}).execute()

    # Insert rows
    records = df.astype(str).to_dict(orient="records")  # convert all to str for safety
    for row in records:
        supabase.table(f"raw.{table_name}").insert(row).execute()

    print(f"✅ Uploaded DataFrame to raw.{table_name}")

# =============================
# 4. Execution: Generate & Upload 3 datasets
# =============================
print("--- Starting Data Generation & Raw Schema Upload ---")

# Dataset A
dfA = make_base_frame(1000).rename(columns={"driver_age": "drv_age", "vehicle_make": "veh_make"})
dfA["source_file"] = "A"
push_df_to_raw_schema(dfA, "table_1")

# Dataset B
dfB = make_base_frame(5000).rename(columns={"ncd_years": "ncd_yrs", "total_claims": "claims_cnt"})
dfB["source_file"] = "B"
push_df_to_raw_schema(dfB, "table_2")

# Dataset C
dfC = make_base_frame(7000).rename(columns={"cover_type": "cov_type", "postcode": "post_code"})
dfC["source_file"] = "C"
push_df_to_raw_schema(dfC, "table_3")

print("--- All datasets uploaded to raw schema ---")
