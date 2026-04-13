import pandas as pd
import sqlite3
from pathlib import Path

BASE_DIR = Path(__file__).resolve().parent
excel_path = BASE_DIR / "inventory.xlsx"
db_path = BASE_DIR / "inventory.db"

print("엑셀 경로:", excel_path)
print("DB 경로:", db_path)

df = pd.read_excel(excel_path)
df.columns = df.columns.str.strip().str.lower()

required_cols = [
    "hazardous", "part", "item_code", "item_name", "lot_no",
    "expiry_date", "spec", "unit", "reagent_type",
    "equipment", "vendor", "safety_stock"
]

missing = [c for c in required_cols if c not in df.columns]
if missing:
    raise ValueError(f"엑셀에 필요한 컬럼이 없음: {missing}")

# 날짜 정리
df["expiry_date"] = pd.to_datetime(df["expiry_date"], errors="coerce").dt.strftime("%Y-%m-%d")

# 문자열 정리
str_cols = [
    "hazardous", "part", "item_code", "item_name", "lot_no",
    "expiry_date", "spec", "unit", "reagent_type", "equipment", "vendor"
]
for col in str_cols:
    df[col] = df[col].fillna("").astype(str).str.strip()

# 숫자 정리
df["safety_stock"] = pd.to_numeric(df["safety_stock"], errors="coerce").fillna(0).astype(int)

# current_stock이 엑셀에 있으면 반영, 없으면 0
if "current_stock" in df.columns:
    df["current_stock"] = pd.to_numeric(df["current_stock"], errors="coerce").fillna(0).astype(int)
else:
    df["current_stock"] = 0

df["required_qty"] = (df["safety_stock"] - df["current_stock"]).clip(lower=0)

conn = sqlite3.connect(db_path)
cursor = conn.cursor()

cursor.execute("DROP TABLE IF EXISTS inventory")

cursor.execute("""
CREATE TABLE inventory (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    hazardous TEXT,
    part TEXT,
    item_code TEXT NOT NULL,
    item_name TEXT,
    lot_no TEXT NOT NULL,
    expiry_date TEXT NOT NULL,
    spec TEXT,
    unit TEXT,
    reagent_type TEXT,
    equipment TEXT,
    vendor TEXT,
    safety_stock INTEGER DEFAULT 0,
    current_stock INTEGER DEFAULT 0,
    required_qty INTEGER DEFAULT 0,
    UNIQUE(item_code, lot_no, expiry_date)
)
""")

insert_sql = """
INSERT OR IGNORE INTO inventory (
    hazardous, part, item_code, item_name, lot_no,
    expiry_date, spec, unit, reagent_type, equipment,
    vendor, safety_stock, current_stock, required_qty
) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
"""

insert_cols = [
    "hazardous", "part", "item_code", "item_name", "lot_no",
    "expiry_date", "spec", "unit", "reagent_type", "equipment",
    "vendor", "safety_stock", "current_stock", "required_qty"
]

rows = df[insert_cols].values.tolist()
cursor.executemany(insert_sql, rows)

conn.commit()

cursor.execute("PRAGMA table_info(inventory)")
for row in cursor.fetchall():
    print(row)

cursor.execute("SELECT COUNT(*) FROM inventory")
print("저장된 행 수:", cursor.fetchone()[0])

conn.close()
print("완료")