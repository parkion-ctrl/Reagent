from datetime import date

from app.core.db import get_connection, get_current_schema
from app.services.reagent_history_service import sync_expired_reagents
from app.utils.constants import get_part_map, REAGENT_TYPE_MAP


Y_VALUES = {"1", "Y", "y", "YES", "Yes", "yes", "예", "사용"}
N_VALUES = {"0", "N", "n", "NO", "No", "no", "아니오", "무"}


def get_inventory_items(
    part: str = "",
    q: str = "",
    reagent_type: str = "",
    equipment: str = "",
    vendor: str = "",
    hazardous: str = "",
    expiry_filter: str = "",
    sort: str = "",
    order: str = "",
):
    sync_expired_reagents()

    conn = get_connection()
    cursor = conn.cursor()

    query = "SELECT * FROM inventory WHERE disposed_at IS NULL"
    params = []

    if part:
        query += " AND part = ?"
        params.append(part)
    if q:
        query += " AND (item_name LIKE ? OR item_code LIKE ?)"
        params.extend([f"%{q}%", f"%{q}%"])
    if reagent_type:
        query += " AND reagent_type = ?"
        params.append(reagent_type)
    if equipment:
        if equipment == "__BLANK__":
            query += " AND (equipment IS NULL OR TRIM(equipment) = '')"
        else:
            query += " AND equipment = ?"
            params.append(equipment)
    if vendor:
        if vendor == "__BLANK__":
            query += " AND (vendor IS NULL OR TRIM(vendor) = '')"
        else:
            query += " AND vendor = ?"
            params.append(vendor)
    if hazardous == "Y":
        query += " AND hazardous IN ('1', 'Y', 'y', 'Yes', 'yes', '예', '사용')"
    elif hazardous == "N":
        query += " AND hazardous IN ('0', 'N', 'n', 'No', 'no', '아니오', '무')"
    if expiry_filter == "1w":
        query += " AND expiry_date <= (CURRENT_DATE + INTERVAL '7 days')::date::text"
    elif expiry_filter == "2w":
        query += " AND expiry_date <= (CURRENT_DATE + INTERVAL '14 days')::date::text"
    elif expiry_filter == "4w":
        query += " AND expiry_date <= (CURRENT_DATE + INTERVAL '28 days')::date::text"

    allowed_sort = [
        "item_code", "item_name", "expiry_date",
        "current_stock", "required_qty", "safety_stock",
        "hazardous", "reagent_type", "equipment", "vendor",
    ]
    if sort in allowed_sort:
        order_sql = "DESC" if order == "desc" else "ASC"
        query += f" ORDER BY {sort} {order_sql}"
    else:
        query += " ORDER BY item_code ASC, lot_no ASC, expiry_date ASC"

    cursor.execute(query, params)
    rows = cursor.fetchall()
    conn.close()

    items = []
    for row in rows:
        row = dict(row)
        required_qty = max(row["safety_stock"] - row["current_stock"], 0)
        status = "정상"
        if row["current_stock"] <= row["safety_stock"]:
            status = "부족"

        part_code = str(row.get("part", "")).strip()
        part_name = get_part_map(get_current_schema()).get(part_code, "")
        raw_date = str(row.get("expiry_date", "")).strip()
        expiry_text = ""
        expiry_class = ""

        if raw_date and raw_date != "9999-12-31":
            expiry_text = raw_date[:10]
            expiry_date = date.fromisoformat(raw_date[:10])
            days_left = (expiry_date - date.today()).days
            if days_left <= 7:
                expiry_class = "expiry-red"
            elif days_left <= 14:
                expiry_class = "expiry-yellow"
            elif days_left <= 28:
                expiry_class = "expiry-green"

        reagent_type_code = str(row.get("reagent_type", "")).strip()
        reagent_type_label = REAGENT_TYPE_MAP.get(reagent_type_code, reagent_type_code)

        hazardous_raw = str(row.get("hazardous", "")).strip()
        if hazardous_raw in Y_VALUES:
            hazardous_label = "Y"
        elif hazardous_raw in N_VALUES:
            hazardous_label = "N"
        else:
            hazardous_label = hazardous_raw

        items.append(
            {
                "id": row["id"],
                "hazardous": hazardous_label,
                "part": part_code,
                "part_label": f"{part_code} ({part_name})" if part_name else part_code,
                "item_code": row["item_code"],
                "item_name": row["item_name"],
                "lot_no": row["lot_no"],
                "expiry_date": expiry_text,
                "spec": row["spec"],
                "unit": row["unit"],
                "reagent_type": reagent_type_label,
                "equipment": row.get("equipment", ""),
                "vendor": row.get("vendor", ""),
                "current_stock": row["current_stock"],
                "safety_stock": row["safety_stock"],
                "required_qty": required_qty,
                "expiry_class": expiry_class,
                "status": status,
            }
        )

    return items


def get_inventory_filter_options():
    sync_expired_reagents()

    conn = get_connection()
    cursor = conn.cursor()

    cursor.execute(
        """
        SELECT DISTINCT reagent_type
        FROM inventory
        WHERE disposed_at IS NULL
          AND reagent_type IS NOT NULL
          AND reagent_type != ''
        ORDER BY reagent_type
        """
    )
    reagent_types = [row[0] for row in cursor.fetchall()]

    cursor.execute("SELECT DISTINCT equipment FROM inventory WHERE disposed_at IS NULL ORDER BY equipment")
    equipment_rows = [row[0] for row in cursor.fetchall()]

    cursor.execute("SELECT DISTINCT vendor FROM inventory WHERE disposed_at IS NULL ORDER BY vendor")
    vendor_rows = [row[0] for row in cursor.fetchall()]

    conn.close()

    equipments = []
    if any(v is None or str(v).strip() == "" for v in equipment_rows):
        equipments.append("__BLANK__")
    equipments.extend(str(v).strip() for v in equipment_rows if v is not None and str(v).strip() != "")

    vendors = []
    if any(v is None or str(v).strip() == "" for v in vendor_rows):
        vendors.append("__BLANK__")
    vendors.extend(str(v).strip() for v in vendor_rows if v is not None and str(v).strip() != "")

    return {
        "reagent_types": [
            {
                "value": str(v).strip(),
                "label": REAGENT_TYPE_MAP.get(str(v).strip(), str(v).strip()),
            }
            for v in reagent_types
        ],
        "equipments": equipments,
        "vendors": vendors,
        "hazardous_options": [{"value": "Y", "label": "Y"}, {"value": "N", "label": "N"}],
    }
