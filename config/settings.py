import os
import sys

# ─ OS AWARE BASE PATH ─
if sys.platform == "win32":
    BASE_DIR = r"D:\pharma_pipeline"
else:
    BASE_DIR = "/mnt/d/pharma_pipeline"  # WSL path to Windows D drive

DATA_DIR = os.path.join(BASE_DIR, "data")

RAW_DIR = os.path.join(DATA_DIR, "raw")
BRONZE_DIR = os.path.join(DATA_DIR, "bronze")
SILVER_DIR = os.path.join(DATA_DIR, "silver")
GOLD_DIR = os.path.join(DATA_DIR, "gold")

OUTPUT_DIR = os.path.join(BASE_DIR, "outputs")


PRODUCTS = {
    "ANTIVEGF": ["EYLEA", "EYLEA HD", "LUCENTIS", "BEOVU"],
    "MS": ["AVONEX", "REBIF", "TECFIDERA", "OCREVUS"]
}

SALES_TEAMS = ["MS_ZIP", "MS_ACCOUNT"]

SUBJECT_AREAS = ["SYMPHONY", "VALUECENTRIC"]

START_DATE = "2023-01-01"
END_DATE = "2024-12-31"

# ZIP codes (synthetic)
ZIP_CODES = [f"ZIP{str(i).zfill(3)}" for i in range(1, 51)]

# Account IDs (synthetic)
ACCOUNT_IDS = [f"ACC{str(i).zfill(4)}" for i in range(1, 201)]