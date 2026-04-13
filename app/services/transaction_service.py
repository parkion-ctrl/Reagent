from datetime import date, datetime

from app.core.db import get_connection, get_current_schema
from app.services.reagent_history_service import sync_expired_reagents
from app.utils.constants import get_part_map


TRANSACTION_UPLOAD_REQUIRED_COLUMNS = ["item_code", "lot_no", "qty", "tx_date"]


def get_transaction_table_items(tx_type: str, q: str = "", part: str = "", sort: str = "", order: str = ""):
    sync_expired_reagents()

    if not q.strip() and not part.strip():
        return []

    conn = get_connection()
    cursor = conn.cursor()

    query = """
        SELECT
            id, part, item_code, item_name, lot_no, unit,
            current_stock, safety_stock, expiry_date
        FROM inventory
        WHERE disposed_at IS NULL
    """
    params = []

    if tx_type == "OUT":
        query += " AND current_stock >= 1"
    if part:
        query += " AND part = ?"
        params.append(part)
    if q:
        query += " AND (item_code LIKE ? OR item_name LIKE ?)"
        keyword = f"%{q}%"
        params.extend([keyword, keyword])

    allowed_sort = ["item_name"]
    if sort in allowed_sort:
        order_sql = "DESC" if order == "desc" else "ASC"
        query += f" ORDER BY {sort} {order_sql}, item_code ASC, lot_no ASC, expiry_date ASC"
    else:
        query += " ORDER BY item_code ASC, lot_no ASC, expiry_date ASC"

    cursor.execute(query, params)
    rows = cursor.fetchall()
    conn.close()

    items = []
    for row in rows:
        row = dict(row)
        part_code = str(row.get("part", "")).strip()
        part_name = get_part_map(get_current_schema()).get(part_code, "")
        row["part_label"] = f"{part_code} ({part_name})" if part_name else part_code
        row["display_name"] = f"{row['item_code']} | {row['item_name']} | Lot {row.get('lot_no', '') or '-'}"
        raw_expiry = str(row.get("expiry_date", "") or "").strip()
        row["expiry_date"] = "" if raw_expiry == "9999-12-31" else raw_expiry[:10]
        items.append(row)

    return items


def apply_stock_transaction(tx_type: str, inventory_id: int, qty: int, tx_date: str):
    rows = [{"inventory_id": inventory_id, "qty": qty, "tx_date": tx_date}]
    return apply_bulk_stock_transactions(tx_type=tx_type, rows=rows)


def apply_bulk_stock_transactions(tx_type: str, rows: list[dict]):
    if tx_type not in {"IN", "OUT"}:
        return False, "지원하지 않는 거래 유형입니다."
    if not rows:
        return False, "처리할 항목이 없습니다."

    sync_expired_reagents()
    conn = get_connection()
    cursor = conn.cursor()

    try:
        for row in rows:
            inventory_id = int(row.get("inventory_id"))
            qty = int(row.get("qty"))
            tx_date = str(row.get("tx_date", "")).strip()

            if qty <= 0:
                raise ValueError("수량은 1 이상이어야 합니다.")
            if not tx_date:
                raise ValueError("거래 일자는 필수입니다.")

            cursor.execute(
                "SELECT * FROM inventory WHERE id = ? AND disposed_at IS NULL",
                (inventory_id,),
            )
            inventory_row = cursor.fetchone()
            if not inventory_row:
                raise ValueError("선택한 품목을 찾을 수 없습니다.")

            item = dict(inventory_row)
            current_stock = int(item.get("current_stock", 0) or 0)
            safety_stock = int(item.get("safety_stock", 0) or 0)

            if tx_type == "OUT" and qty > current_stock:
                raise ValueError(
                    f"{item.get('item_code', '')} / {item.get('item_name', '')}의 출고 수량이 현재고보다 많습니다."
                )

            next_stock = current_stock + qty if tx_type == "IN" else current_stock - qty
            required_qty = max(safety_stock - next_stock, 0)

            cursor.execute(
                """
                UPDATE inventory
                SET current_stock = ?, required_qty = ?
                WHERE id = ?
                """,
                (next_stock, required_qty, inventory_id),
            )

            cursor.execute(
                """
                INSERT INTO transaction_history (
                    inventory_id, tx_type, qty, tx_date, note, remaining_stock,
                    item_code, item_name, lot_no, part, unit
                )
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                """,
                (
                    inventory_id,
                    tx_type,
                    qty,
                    tx_date,
                    "",
                    next_stock,
                    item.get("item_code", ""),
                    item.get("item_name", ""),
                    item.get("lot_no", ""),
                    item.get("part", ""),
                    item.get("unit", ""),
                ),
            )

        conn.commit()
        return True, f"{len(rows)}건을 처리했습니다."
    except Exception as exc:
        conn.rollback()
        return False, str(exc)
    finally:
        conn.close()


def preview_bulk_transaction_items(tx_type: str, df):
    for column in TRANSACTION_UPLOAD_REQUIRED_COLUMNS:
        if column not in df.columns:
            raise ValueError(f"필수 컬럼이 없습니다: {column}")

    sync_expired_reagents()
    conn = get_connection()
    cursor = conn.cursor()

    preview_rows = []
    upload_rows = []
    invalid_messages = []
    total_count = len(df)

    try:
        for idx, row in df.iterrows():
            excel_row_num = idx + 2
            try:
                item_code = normalize_text(row.get("item_code"))
                lot_no = normalize_text(row.get("lot_no"))
                qty = normalize_qty(row.get("qty"))
                tx_date = normalize_tx_date(row.get("tx_date"))

                if not item_code:
                    raise ValueError("item_code 값이 비어 있습니다.")
                if not lot_no:
                    raise ValueError("lot_no 값이 비어 있습니다.")

                inventory_row = find_inventory_row(cursor, tx_type=tx_type, item_code=item_code, lot_no=lot_no)
                if not inventory_row:
                    raise ValueError("일치하는 활성 시약을 찾을 수 없습니다.")

                inventory_item = dict(inventory_row)
                current_stock = int(inventory_item.get("current_stock", 0) or 0)
                if tx_type == "OUT" and qty > current_stock:
                    raise ValueError("출고 수량이 현재고보다 많습니다.")

                remaining_stock = current_stock + qty if tx_type == "IN" else current_stock - qty
                part_code = str(inventory_item.get("part", "")).strip()
                part_name = get_part_map(get_current_schema()).get(part_code, "")

                preview_row = {
                    "inventory_id": inventory_item["id"],
                    "part_label": f"{part_code} ({part_name})" if part_name else part_code,
                    "part_pill_class": "part-pill",
                    "item_code": inventory_item.get("item_code", ""),
                    "item_name": inventory_item.get("item_name", ""),
                    "lot_no": inventory_item.get("lot_no", ""),
                    "unit": inventory_item.get("unit", ""),
                    "expiry_date": format_date_text(inventory_item.get("expiry_date")),
                    "current_stock": current_stock,
                    "qty": qty,
                    "tx_date": tx_date,
                    "remaining_stock": remaining_stock,
                    "status": "등록 예정",
                }
                preview_rows.append(preview_row)
                upload_rows.append(
                    {
                        "inventory_id": inventory_item["id"],
                        "qty": qty,
                        "tx_date": tx_date,
                    }
                )
            except Exception as exc:
                fallback_item = find_inventory_row(
                    cursor,
                    tx_type="IN",
                    item_code=normalize_text(row.get("item_code")),
                    lot_no=normalize_text(row.get("lot_no")),
                )
                fallback_data = dict(fallback_item) if fallback_item else {}
                fallback_part = str(fallback_data.get("part", "")).strip()
                fallback_part_name = get_part_map(get_current_schema()).get(fallback_part, "")
                fallback_current_stock = int(fallback_data.get("current_stock", 0) or 0) if fallback_data else ""
                fallback_qty_raw = normalize_text(row.get("qty"))
                fallback_qty = int(float(fallback_qty_raw)) if fallback_qty_raw not in {"", None} else ""
                preview_rows.append(
                    {
                        "inventory_id": "",
                        "part_label": f"{fallback_part} ({fallback_part_name})" if fallback_part_name else fallback_part,
                        "part_pill_class": "part-pill part-pill-neutral",
                        "item_code": normalize_text(row.get("item_code")),
                        "item_name": fallback_data.get("item_name", ""),
                        "lot_no": normalize_text(row.get("lot_no")),
                        "unit": fallback_data.get("unit", ""),
                        "expiry_date": "",
                        "current_stock": fallback_current_stock,
                        "qty": fallback_qty_raw,
                        "tx_date": normalize_text(row.get("tx_date")),
                        "remaining_stock": compute_fallback_remaining_stock(
                            tx_type=tx_type,
                            current_stock=fallback_current_stock,
                            qty=fallback_qty,
                        ),
                        "status": "오류",
                    }
                )
                invalid_messages.append(f"{excel_row_num}행: {str(exc)}")
    finally:
        conn.close()

    return {
        "preview_rows": preview_rows,
        "upload_rows": upload_rows,
        "total_count": total_count,
        "valid_count": len(upload_rows),
        "invalid_count": len(invalid_messages),
        "invalid_messages": invalid_messages,
    }


def preview_manual_transaction_items(tx_type: str, rows: list[dict]):
    sync_expired_reagents()
    conn = get_connection()
    cursor = conn.cursor()

    preview_rows = []
    upload_rows = []
    invalid_messages = []

    try:
        for idx, row in enumerate(rows, start=1):
            try:
                inventory_id = int(row.get("inventory_id"))
                qty = normalize_qty(row.get("qty"))
                tx_date = normalize_tx_date(row.get("tx_date"))

                cursor.execute(
                    """
                    SELECT
                        id, part, item_code, item_name, lot_no, unit,
                        current_stock, safety_stock, expiry_date
                    FROM inventory
                    WHERE id = ? AND disposed_at IS NULL
                    """,
                    (inventory_id,),
                )
                inventory_row = cursor.fetchone()
                if not inventory_row:
                    raise ValueError("선택한 시약을 찾을 수 없습니다.")

                inventory_item = dict(inventory_row)
                current_stock = int(inventory_item.get("current_stock", 0) or 0)
                if tx_type == "OUT" and qty > current_stock:
                    raise ValueError("출고 수량이 현재고보다 많습니다.")

                remaining_stock = current_stock + qty if tx_type == "IN" else current_stock - qty
                part_code = str(inventory_item.get("part", "")).strip()
                part_name = get_part_map(get_current_schema()).get(part_code, "")

                preview_rows.append(
                    {
                        "inventory_id": inventory_item["id"],
                        "part_label": f"{part_code} ({part_name})" if part_name else part_code,
                        "part_pill_class": "part-pill",
                        "item_code": inventory_item.get("item_code", ""),
                        "item_name": inventory_item.get("item_name", ""),
                        "lot_no": inventory_item.get("lot_no", ""),
                        "unit": inventory_item.get("unit", ""),
                        "expiry_date": format_date_text(inventory_item.get("expiry_date")),
                        "current_stock": current_stock,
                        "qty": qty,
                        "tx_date": tx_date,
                        "remaining_stock": remaining_stock,
                        "status": "반영 예정",
                    }
                )
                upload_rows.append(
                    {
                        "inventory_id": inventory_item["id"],
                        "qty": qty,
                        "tx_date": tx_date,
                    }
                )
            except Exception as exc:
                fallback_data = {}
                inventory_id_raw = normalize_text(row.get("inventory_id"))
                if inventory_id_raw:
                    try:
                        cursor.execute(
                            """
                            SELECT
                                id, part, item_code, item_name, lot_no, unit,
                                current_stock, expiry_date
                            FROM inventory
                            WHERE id = ? AND disposed_at IS NULL
                            """,
                            (int(inventory_id_raw),),
                        )
                        fallback_item = cursor.fetchone()
                        fallback_data = dict(fallback_item) if fallback_item else {}
                    except ValueError:
                        fallback_data = {}

                fallback_part = str(fallback_data.get("part", "")).strip()
                fallback_part_name = get_part_map(get_current_schema()).get(fallback_part, "")
                fallback_current_stock = int(fallback_data.get("current_stock", 0) or 0) if fallback_data else ""
                fallback_qty_raw = normalize_text(row.get("qty"))
                fallback_qty = int(float(fallback_qty_raw)) if fallback_qty_raw not in {"", None} else ""
                preview_rows.append(
                    {
                        "inventory_id": inventory_id_raw,
                        "part_label": f"{fallback_part} ({fallback_part_name})" if fallback_part_name else fallback_part,
                        "part_pill_class": "part-pill part-pill-neutral",
                        "item_code": fallback_data.get("item_code", ""),
                        "item_name": fallback_data.get("item_name", ""),
                        "lot_no": fallback_data.get("lot_no", ""),
                        "unit": fallback_data.get("unit", ""),
                        "expiry_date": "",
                        "current_stock": fallback_current_stock,
                        "qty": fallback_qty_raw,
                        "tx_date": normalize_text(row.get("tx_date")),
                        "remaining_stock": compute_fallback_remaining_stock(
                            tx_type=tx_type,
                            current_stock=fallback_current_stock,
                            qty=fallback_qty,
                        ),
                        "status": "오류",
                    }
                )
                invalid_messages.append(f"{idx}번째 항목: {str(exc)}")
    finally:
        conn.close()

    return {
        "preview_rows": preview_rows,
        "upload_rows": upload_rows,
        "total_count": len(rows),
        "valid_count": len(upload_rows),
        "invalid_count": len(invalid_messages),
        "invalid_messages": invalid_messages,
    }


def confirm_bulk_transaction_items(tx_type: str, rows: list[dict]):
    normalized_rows = []
    for row in rows:
        normalized_rows.append(
            {
                "inventory_id": int(row["inventory_id"]),
                "qty": int(row["qty"]),
                "tx_date": normalize_tx_date(row["tx_date"]),
            }
        )
    return apply_bulk_stock_transactions(tx_type=tx_type, rows=normalized_rows)


def find_inventory_row(cursor, tx_type: str, item_code: str, lot_no: str):
    query = """
        SELECT *
        FROM inventory
        WHERE disposed_at IS NULL
          AND item_code = ?
          AND lot_no = ?
    """
    params = [item_code, lot_no]
    if tx_type == "OUT":
        query += " AND current_stock >= 1"
    query += " ORDER BY expiry_date ASC, id ASC LIMIT 1"
    cursor.execute(query, params)
    return cursor.fetchone()


def normalize_text(value):
    if value is None:
        return ""
    return str(value).strip()


def normalize_qty(value):
    raw = normalize_text(value)
    if not raw:
        raise ValueError("qty 값이 비어 있습니다.")
    qty = int(float(raw))
    if qty <= 0:
        raise ValueError("수량은 1 이상이어야 합니다.")
    return qty


def normalize_tx_date(value):
    raw = normalize_text(value)
    if not raw:
        raise ValueError("tx_date 값이 비어 있습니다.")
    for fmt, length in (("%Y%m%d", 8), ("%Y-%m-%d", 10)):
        try:
            return datetime.strptime(raw[:length], fmt).strftime("%Y-%m-%d")
        except ValueError:
            continue
    raise ValueError("거래일 형식은 YYYYMMDD 여야 합니다.")


def format_date_text(value):
    raw = str(value or "").strip()
    if not raw or raw == "9999-12-31":
        return ""
    return raw[:10]


def get_today_text():
    return date.today().isoformat()


def compute_fallback_remaining_stock(tx_type: str, current_stock, qty):
    if current_stock == "" or qty == "":
        return ""
    if tx_type == "IN":
        return current_stock + qty
    return current_stock - qty
