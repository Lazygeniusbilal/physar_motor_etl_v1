import io
import numpy as np
import pandas as pd
from datetime import datetime, timedelta
from faker import Faker
from supabase import create_client

# =========================================================
# 1. SUPABASE CONFIGURATION
# =========================================================
SUPABASE_URL = "https://qlpxsymlkhqmhxpqctkj.supabase.co/"
SUPABASE_KEY = "sb_secret_18VaeYpat3EreLOtoMc83w_vpvwKjUr" 
BUCKET_NAME = "motor_raw"

supabase = create_client(SUPABASE_URL, SUPABASE_KEY)
fake = Faker("en_GB")
np.random.seed(42)

# =========================================================
# 2. DATA GENERATION ENGINE
# =========================================================
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

# =========================================================
# 3. STORAGE UPLOAD LOGIC
# =========================================================
def upload_df_as_csv_to_storage(df: pd.DataFrame, base_filename: str):
    # 1. Generate Date Suffix (e.g., _20231027)
    date_suffix = datetime.now().strftime("%Y%m%d")
    object_path = f"{base_filename}_{date_suffix}.csv"

    buf = io.StringIO()
    df.to_csv(buf, index=False)
    csv_bytes = buf.getvalue().encode("utf-8")

    try:
        res = supabase.storage.from_(BUCKET_NAME).upload(
            path=object_path,
            file=csv_bytes,
            file_options={"cache-control": "3600", "upsert": "true"},
        )
        print(f"✅ Successfully uploaded: {object_path}")
    except Exception as e:
        print(f"❌ Failed to upload {object_path}: {e}")

# =========================================================
# 4. EXECUTION (The ETL Phase)
# =========================================================
print(f"--- Starting Data Generation for Motor_ETL_Test ---")

# Source A
dfA = make_base_frame(1000).rename(columns={"driver_age": "drv_age", "vehicle_make": "veh_make"})
dfA["source_file"] = "A"
upload_df_as_csv_to_storage(dfA, "motor_A") # Note: Extension added in function

# Source B
dfB = make_base_frame(5000).rename(columns={"ncd_years": "ncd_yrs", "total_claims": "claims_cnt"})
dfB["source_file"] = "B"
upload_df_as_csv_to_storage(dfB, "motor_B")

# Source C
dfC = make_base_frame(7000).rename(columns={"cover_type": "cov_type", "postcode": "post_code"})
dfC["source_file"] = "C"
upload_df_as_csv_to_storage(dfC, "motor_C")

print(f"--- Extraction Complete. Files are in '{BUCKET_NAME}' bucket ---")
