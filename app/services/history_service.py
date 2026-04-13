from datetime import datetime, timedelta

from app.core.db import get_connection
from app.utils.constants import get_part_map


def backfill_remaining_stock():
    conn = get_connection()
    cursor = conn.cursor()

    cursor.execute(
        """
        SELECT id, inventory_id, tx_type, qty
        FROM transaction_history
        ORDER BY tx_date ASC, id ASC
        """
    )
    rows = cursor.fetchall()

    running_stock = {}
    for row in rows:
        history_id = row["id"]
        inventory_id = row["inventory_id"]
        qty = int(row["qty"] or 0)
        current = running_stock.get(inventory_id, 0)
        next_stock = current + qty if row["tx_type"] == "IN" else current - qty
        running_stock[inventory_id] = next_stock
        cursor.execute(
            "UPDATE transaction_history SET remaining_stock = ? WHERE id = ?",
            (next_stock, history_id),
        )

    conn.commit()
    conn.close()


def get_history_items(
    tx_type: str = "",
    part: str = "",
    q: str = "",
    date_from: str = "",
    date_to: str = "",
):
    backfill_remaining_stock()

    conn = get_connection()
    cursor = conn.cursor()

    query = """
        SELECT
            id, inventory_id, tx_type, qty, tx_date, remaining_stock,
            item_code, item_name, lot_no, part, unit, created_at
        FROM transaction_history
        WHERE 1 = 1
    """
    params = []

    if tx_type:
        query += " AND tx_type = ?"
        params.append(tx_type)

    if part:
        query += " AND part = ?"
        params.append(part)

    if q:
        query += " AND (item_code LIKE ? OR item_name LIKE ? OR lot_no LIKE ?)"
        keyword = f"%{q}%"
        params.extend([keyword, keyword, keyword])

    if date_from:
        query += " AND tx_date >= ?"
        params.append(date_from)

    if date_to:
        query += " AND tx_date <= ?"
        params.append(date_to)

    query += " ORDER BY tx_date DESC, id DESC"

    cursor.execute(query, params)
    rows = cursor.fetchall()
    conn.close()

    items = []
    for row in rows:
        row = dict(row)
        part_code = str(row.get("part", "")).strip()
        part_name = get_part_map().get(part_code, "")
        row["part_label"] = f"{part_code} ({part_name})" if part_name else part_code
        row["tx_type_label"] = "입고" if row["tx_type"] == "IN" else "출고"
        row["tx_badge_class"] = "text-bg-primary" if row["tx_type"] == "IN" else "text-bg-secondary"
        row["inbound_qty"] = row["qty"] if row["tx_type"] == "IN" else ""
        row["outbound_qty"] = row["qty"] if row["tx_type"] == "OUT" else ""

        created_at = str(row.get("created_at", "")).strip()
        if created_at:
            try:
                utc_dt = datetime.strptime(created_at, "%Y-%m-%d %H:%M:%S")
                row["created_at_display"] = (utc_dt + timedelta(hours=9)).strftime("%Y-%m-%d %H:%M:%S")
            except ValueError:
                row["created_at_display"] = created_at
        else:
            row["created_at_display"] = ""

        items.append(row)

    return items
