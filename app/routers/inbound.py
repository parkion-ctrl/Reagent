import io
import json
from urllib.parse import quote_plus

import pandas as pd
from fastapi import APIRouter, File, Form, Request, UploadFile
from fastapi.responses import HTMLResponse, RedirectResponse, StreamingResponse

from app.core.config import templates
from app.services.inbound_service import (
    create_bulk_inbound_transactions,
    get_inbound_page_data,
    preview_manual_inbound_items,
    preview_bulk_inbound_items,
)
from app.utils.constants import PART_MAP

router = APIRouter()


def get_inbound_base_context(request: Request, q: str = "", part: str = "", sort: str = "", order: str = ""):
    context = {
        "request": request,
        "active_menu": "inbound",
        "tx_mode": "inbound",
        "tx_title": "시약 입고 등록",
        "tx_button_label": "입고 등록",
        "tx_qty_label": "입고량",
        "tx_date_label": "입고일",
        "message": "",
        "error": "",
        "q": q,
        "part": part,
        "sort": sort,
        "order": order,
        "part_map": PART_MAP,
    }
    context.update(get_inbound_page_data(q=q, part=part, sort=sort, order=order))
    return context


@router.get("/inbound")
def inbound_page(
    request: Request,
    q: str = "",
    part: str = "",
    sort: str = "",
    order: str = "",
    message: str = "",
    error: str = "",
):
    context = get_inbound_base_context(request, q=q, part=part, sort=sort, order=order)
    context["message"] = message
    context["error"] = error
    return templates.TemplateResponse("transaction_entry.html", context)


@router.post("/inbound/bulk-create")
def inbound_bulk_create(
    rows_json: str = Form(...),
):
    try:
        rows = json.loads(rows_json)
    except json.JSONDecodeError:
        return RedirectResponse("/inbound?error=선택한+항목+정보를+읽을+수+없습니다", status_code=303)

    ok, msg = create_bulk_inbound_transactions(rows)
    if ok:
        return RedirectResponse(f"/inbound?message={quote_plus(msg)}", status_code=303)
    return RedirectResponse(f"/inbound?error={quote_plus(msg)}", status_code=303)


@router.post("/inbound/bulk-preview", response_class=HTMLResponse)
async def inbound_bulk_preview(
    request: Request,
    rows_json: str = Form(...),
    q: str = Form(""),
    part: str = Form(""),
):
    try:
        rows = json.loads(rows_json)
        preview_result = preview_manual_inbound_items(rows)
        context = get_inbound_base_context(request, q=q, part=part)
        context.update(
            {
                "preview_mode": True,
                "preview_rows": preview_result["preview_rows"],
                "preview_json": json.dumps(preview_result["upload_rows"], ensure_ascii=False),
                "total_count": preview_result["total_count"],
                "valid_count": preview_result["valid_count"],
                "invalid_count": preview_result["invalid_count"],
                "invalid_messages": preview_result["invalid_messages"],
            }
        )
        return templates.TemplateResponse("transaction_entry.html", context)
    except Exception as exc:
        context = get_inbound_base_context(request, q=q, part=part)
        context["error"] = str(exc)
        return templates.TemplateResponse("transaction_entry.html", context)


@router.get("/inbound/upload-template")
def download_inbound_upload_template():
    df = pd.DataFrame(
        [
            {
                "item_code": "CRP001",
                "lot_no": "LOT202603",
                "qty": 5,
                "tx_date": "20260409",
            }
        ]
    )

    output = io.BytesIO()
    with pd.ExcelWriter(output, engine="openpyxl") as writer:
        df.to_excel(writer, index=False, sheet_name="inbound_upload_template")
    output.seek(0)

    return StreamingResponse(
        output,
        media_type="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
        headers={"Content-Disposition": "attachment; filename=inbound_upload_template.xlsx"},
    )


@router.post("/inbound/upload-preview", response_class=HTMLResponse)
async def inbound_upload_preview(
    request: Request,
    file: UploadFile = File(...),
    q: str = Form(""),
    part: str = Form(""),
):
    try:
        filename = file.filename.lower()
        if filename.endswith(".xlsx"):
            df = pd.read_excel(file.file, dtype=str)
        else:
            df = pd.read_csv(file.file, dtype=str)
        df = df.fillna("")

        preview_result = preview_bulk_inbound_items(df)
        context = get_inbound_base_context(request, q=q, part=part)
        context.update(
            {
                "preview_mode": True,
                "preview_rows": preview_result["preview_rows"],
                "preview_json": json.dumps(preview_result["upload_rows"], ensure_ascii=False),
                "total_count": preview_result["total_count"],
                "valid_count": preview_result["valid_count"],
                "invalid_count": preview_result["invalid_count"],
                "invalid_messages": preview_result["invalid_messages"],
            }
        )
        return templates.TemplateResponse("transaction_entry.html", context)
    except Exception as exc:
        context = get_inbound_base_context(request, q=q, part=part)
        context["upload_error"] = str(exc)
        return templates.TemplateResponse("transaction_entry.html", context)


@router.post("/inbound/upload-confirm", response_class=HTMLResponse)
async def inbound_upload_confirm(
    request: Request,
    upload_data: str = Form(...),
    q: str = Form(""),
    part: str = Form(""),
):
    try:
        rows = json.loads(upload_data)
        ok, msg = create_bulk_inbound_transactions(rows)
        context = get_inbound_base_context(request, q=q, part=part)
        if ok:
            context["message"] = msg
        else:
            context["error"] = msg
        return templates.TemplateResponse("transaction_entry.html", context)
    except Exception as exc:
        context = get_inbound_base_context(request, q=q, part=part)
        context["upload_error"] = str(exc)
        return templates.TemplateResponse("transaction_entry.html", context)
