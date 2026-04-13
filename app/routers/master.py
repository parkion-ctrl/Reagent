import io
import json
from urllib.parse import quote_plus

import pandas as pd
from fastapi import APIRouter, File, Form, Request, UploadFile
from fastapi.responses import HTMLResponse, RedirectResponse, StreamingResponse

from app.core.config import templates
from app.services.inventory_service import get_inventory_filter_options
from app.services.master_service import (
    confirm_bulk_master_items,
    create_master_item,
    delete_master_item,
    get_master_item_by_id,
    get_master_items,
    preview_bulk_master_items_v3,
    update_master_item,
)
from app.services.reagent_history_service import dispose_reagent
from app.utils.constants import PART_MAP

router = APIRouter()


def get_master_base_context(request: Request):
    return {
        "request": request,
        "active_menu": "master",
        "items": get_master_items(),
        "part_map": PART_MAP,
        "part": "",
        "q": "",
        "sort": "",
        "order": "",
        "show_form": "",
        "message": "",
        "error": "",
        "reagent_type": "",
        "edit_item": None,
        "equipment": "",
        "vendor": "",
        "hazardous": "",
        "filter_options": get_inventory_filter_options(),
    }


@router.get("/master")
def master_page(
    request: Request,
    part: str = "",
    q: str = "",
    sort: str = "",
    order: str = "",
    show_form: str = "",
    edit_id: str = "",
    message: str = "",
    error: str = "",
    reagent_type: str = "",
    equipment: str = "",
    vendor: str = "",
    hazardous: str = "",
):
    items = get_master_items(
        part=part,
        q=q,
        reagent_type=reagent_type,
        equipment=equipment,
        vendor=vendor,
        hazardous=hazardous,
        sort=sort,
        order=order,
    )
    edit_item = get_master_item_by_id(int(edit_id)) if edit_id else None

    return templates.TemplateResponse(
        "master.html",
        {
            "request": request,
            "active_menu": "master",
            "items": items,
            "part_map": PART_MAP,
            "part": part,
            "q": q,
            "sort": sort,
            "order": order,
            "show_form": show_form,
            "message": message,
            "error": error,
            "reagent_type": reagent_type,
            "edit_item": edit_item,
            "equipment": equipment,
            "vendor": vendor,
            "hazardous": hazardous,
            "filter_options": get_inventory_filter_options(),
        },
    )


@router.post("/master/create")
def master_create(
    hazardous: str = Form(...),
    part: str = Form(...),
    item_code: str = Form(...),
    item_name: str = Form(...),
    lot_no: str = Form(...),
    expiry_date: str = Form(...),
    spec: str = Form(""),
    unit: str = Form(""),
    reagent_type: str = Form(...),
    equipment: str = Form(""),
    vendor: str = Form(""),
    safety_stock: int = Form(...),
):
    ok, msg = create_master_item(
        hazardous=hazardous,
        part=part,
        item_code=item_code,
        item_name=item_name,
        lot_no=lot_no,
        expiry_date=expiry_date,
        spec=spec,
        unit=unit,
        reagent_type=reagent_type,
        equipment=equipment,
        vendor=vendor,
        safety_stock=safety_stock,
    )
    if ok:
        return RedirectResponse(url="/master?message=품목이+등록되었습니다.", status_code=303)
    return RedirectResponse(url=f"/master?show_form=1&error={quote_plus(msg)}", status_code=303)


@router.post("/master/{item_id}/edit")
def master_edit_submit(
    item_id: int,
    hazardous: str = Form(...),
    part: str = Form(...),
    item_code: str = Form(...),
    item_name: str = Form(...),
    lot_no: str = Form(...),
    expiry_date: str = Form(...),
    spec: str = Form(""),
    unit: str = Form(""),
    reagent_type: str = Form(...),
    equipment: str = Form(""),
    vendor: str = Form(""),
    safety_stock: int = Form(...),
):
    update_master_item(
        item_id=item_id,
        hazardous=hazardous,
        part=part,
        item_code=item_code,
        item_name=item_name,
        lot_no=lot_no,
        expiry_date=expiry_date,
        spec=spec,
        unit=unit,
        reagent_type=reagent_type,
        equipment=equipment,
        vendor=vendor,
        safety_stock=safety_stock,
    )
    return RedirectResponse(url="/master?message=품목이+수정되었습니다.", status_code=303)


@router.post("/master/{item_id}/delete")
def master_delete(item_id: int):
    delete_master_item(item_id)
    return RedirectResponse(url="/master?message=품목이+삭제되었습니다.", status_code=303)


@router.post("/master/{item_id}/dispose")
def master_dispose(item_id: int):
    dispose_reagent(item_id=item_id, reason="수동 폐기", disposal_type="MANUAL")
    return RedirectResponse(url="/master?message=시약이+폐기되었습니다.", status_code=303)


@router.post("/master/bulk-delete")
def master_bulk_delete(item_ids: list[int] = Form(...)):
    for item_id in item_ids:
        delete_master_item(item_id)
    return RedirectResponse(url=f"/master?message={len(item_ids)}개+항목이+삭제되었습니다.", status_code=303)


@router.post("/master/bulk-dispose")
def master_bulk_dispose(item_ids: list[int] = Form(...)):
    for item_id in item_ids:
        dispose_reagent(item_id=item_id, reason="수동 폐기", disposal_type="MANUAL")
    return RedirectResponse(url=f"/master?message={len(item_ids)}개+항목이+폐기되었습니다.", status_code=303)


@router.get("/master/upload-template")
def download_master_upload_template():
    df = pd.DataFrame(
        [
            {
                "hazardous": "Y",
                "part": "TA",
                "item_code": "CRP001",
                "item_name": "CRP 시약",
                "lot_no": "*",
                "expiry_date": "20261231",
                "spec": "500mL",
                "unit": "EA",
                "reagent_type": "Reagent",
                "equipment": "c702",
                "vendor": "Roche",
                "safety_stock": 10,
            }
        ]
    )

    output = io.BytesIO()
    with pd.ExcelWriter(output, engine="openpyxl") as writer:
        df.to_excel(writer, index=False, sheet_name="master_upload_template")
    output.seek(0)

    return StreamingResponse(
        output,
        media_type="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
        headers={"Content-Disposition": "attachment; filename=master_upload_template.xlsx"},
    )


@router.post("/master/upload-preview", response_class=HTMLResponse)
async def upload_master_preview(request: Request, file: UploadFile = File(...)):
    try:
        filename = file.filename.lower()
        if filename.endswith(".xlsx"):
            df = pd.read_excel(file.file, dtype=str)
        else:
            df = pd.read_csv(file.file, dtype=str)

        df = df.fillna("")
        preview_result = preview_bulk_master_items_v3(df)
        context = get_master_base_context(request)
        context.update(
            {
                "preview_mode": True,
                "preview_rows": preview_result["preview_rows"],
                "preview_json": json.dumps(preview_result["upload_rows"], ensure_ascii=False),
                "total_count": preview_result["total_count"],
                "valid_count": preview_result["valid_count"],
                "duplicate_count": preview_result["duplicate_count"],
                "duplicate_names": preview_result["duplicate_names"],
                "invalid_count": preview_result["invalid_count"],
                "invalid_messages": preview_result["invalid_messages"],
            }
        )
        return templates.TemplateResponse("master.html", context)
    except Exception as exc:
        context = get_master_base_context(request)
        context["upload_error"] = str(exc)
        return templates.TemplateResponse("master.html", context)


@router.post("/master/upload-confirm", response_class=HTMLResponse)
async def upload_master_confirm(request: Request, upload_data: str = Form(...)):
    try:
        rows = json.loads(upload_data)
        result = confirm_bulk_master_items(rows)
        context = get_master_base_context(request)
        context.update(
            {
                "upload_done": True,
                "total_uploaded": result["total"],
                "success_count": result["success"],
                "fail_count": result["fail"],
                "fail_messages": result["fail_messages"],
            }
        )
        return templates.TemplateResponse("master.html", context)
    except Exception as exc:
        context = get_master_base_context(request)
        context["upload_error"] = str(exc)
        return templates.TemplateResponse("master.html", context)
