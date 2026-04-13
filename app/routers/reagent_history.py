from datetime import datetime
from urllib.parse import quote_plus

from fastapi import APIRouter, Form, Request
from fastapi.responses import RedirectResponse

from app.core.config import templates
from app.services.reagent_history_service import (
    get_reagent_history_filter_options,
    get_reagent_history_items,
    get_old_new_lot_items,
    save_old_new_lot_selection,
    update_opened_at,
    update_parallel_at,
)
from app.utils.constants import PART_MAP

router = APIRouter()


@router.get("/reagent-history")
def reagent_history_page(
    request: Request,
    part: str = "",
    q: str = "",
    sort: str = "",
    order: str = "",
    message: str = "",
    error: str = "",
    reagent_type: str = "",
    equipment: str = "",
    vendor: str = "",
    hazardous: str = "",
    disposed: str = "",
    show_form: str = "",
    selected_item_id: str = "",
    selected_item_label: str = "",
    selected_ids: str = "",
    old_new_part: str = "",
    old_new_mode: str = "",
    manage_part: str = "",
    manage_mode: str = "",
):
    effective_part = manage_part or part
    effective_q = "" if manage_part else q
    effective_reagent_type = "" if manage_part else reagent_type
    effective_equipment = "" if manage_part else equipment
    effective_vendor = "" if manage_part else vendor
    effective_hazardous = "" if manage_part else hazardous
    effective_disposed = "" if manage_part else disposed
    effective_lot_status = "NEW" if manage_mode == "new" else ""

    return templates.TemplateResponse(
        "reagent_history.html",
        {
            "request": request,
            "active_menu": "reagent_history",
            "items": get_reagent_history_items(
                part=effective_part,
                q=effective_q,
                reagent_type=effective_reagent_type,
                equipment=effective_equipment,
                vendor=effective_vendor,
                hazardous=effective_hazardous,
                disposed=effective_disposed,
                lot_status=effective_lot_status,
                sort=sort,
                order=order,
            ),
            "part_map": PART_MAP,
            "part": part,
            "q": q,
            "sort": sort,
            "order": order,
            "message": message,
            "error": error,
            "reagent_type": reagent_type,
            "equipment": equipment,
            "vendor": vendor,
            "hazardous": hazardous,
            "disposed": disposed,
            "show_form": show_form,
            "selected_item_id": selected_item_id,
            "selected_item_label": selected_item_label,
            "selected_ids": selected_ids,
            "old_new_part": old_new_part,
            "old_new_mode": old_new_mode,
            "manage_part": manage_part,
            "manage_mode": manage_mode,
            "old_new_items": get_old_new_lot_items(part=old_new_part, only_new=(old_new_mode == "new")),
            "default_date": datetime.now().strftime("%Y-%m-%d"),
            "filter_options": get_reagent_history_filter_options(),
        },
    )


@router.post("/reagent-history/opened-at")
def reagent_history_opened_at(
    item_id: int = Form(...),
    opened_at: str = Form(...),
    manage_part: str = Form(""),
    manage_mode: str = Form(""),
    q: str = Form(""),
    sort: str = Form(""),
    order: str = Form(""),
):
    ok, msg = update_opened_at(item_id=item_id, opened_at=opened_at)
    query = f"?manage_part={quote_plus(manage_part)}&manage_mode={quote_plus(manage_mode)}&q={quote_plus(q)}&sort={quote_plus(sort)}&order={quote_plus(order)}"
    if ok:
        return RedirectResponse(f"/reagent-history{query}&message=개봉+일시가+등록되었습니다", status_code=303)
    return RedirectResponse(f"/reagent-history{query}&show_form=opened&error={quote_plus(msg)}", status_code=303)


@router.post("/reagent-history/parallel-at")
def reagent_history_parallel_at(
    item_id: int = Form(...),
    parallel_at: str = Form(...),
    manage_part: str = Form(""),
    manage_mode: str = Form(""),
    q: str = Form(""),
    sort: str = Form(""),
    order: str = Form(""),
):
    ok, msg = update_parallel_at(item_id=item_id, parallel_at=parallel_at)
    query = f"?manage_part={quote_plus(manage_part)}&manage_mode={quote_plus(manage_mode)}&q={quote_plus(q)}&sort={quote_plus(sort)}&order={quote_plus(order)}"
    if ok:
        return RedirectResponse(f"/reagent-history{query}&message=Parallel+일시가+등록되었습니다", status_code=303)
    return RedirectResponse(f"/reagent-history{query}&show_form=parallel&error={quote_plus(msg)}", status_code=303)


@router.post("/reagent-history/old-new-lot")
def reagent_history_old_new_lot_save(
    part: str = Form(...),
    visible_item_ids: list[int] = Form(...),
    new_lot_item_ids: list[int] = Form(default=[]),
):
    ok, msg = save_old_new_lot_selection(
        part=part,
        visible_item_ids=visible_item_ids,
        new_lot_item_ids=new_lot_item_ids,
    )
    target = f"/reagent-history?old_new_part={quote_plus(part)}"
    if ok:
        return RedirectResponse(f"{target}&message={quote_plus(msg)}", status_code=303)
    return RedirectResponse(f"{target}&error={quote_plus(msg)}", status_code=303)
