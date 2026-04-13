from app.services.transaction_service import (
    confirm_bulk_transaction_items,
    get_today_text,
    get_transaction_table_items,
    preview_manual_transaction_items,
    preview_bulk_transaction_items,
)


def get_inbound_page_data(q: str = "", part: str = "", sort: str = "", order: str = ""):
    return {
        "items": get_transaction_table_items(tx_type="IN", q=q, part=part, sort=sort, order=order),
        "today": get_today_text(),
    }


def preview_bulk_inbound_items(df):
    return preview_bulk_transaction_items(tx_type="IN", df=df)


def create_bulk_inbound_transactions(rows: list[dict]):
    return confirm_bulk_transaction_items(tx_type="IN", rows=rows)


def preview_manual_inbound_items(rows: list[dict]):
    return preview_manual_transaction_items(tx_type="IN", rows=rows)
