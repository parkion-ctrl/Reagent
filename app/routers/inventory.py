from fastapi import APIRouter, Request
from fastapi.responses import HTMLResponse

from app.core.config import templates
from app.services.inventory_service import (
    get_inventory_items,
    get_inventory_filter_options,
)
from app.utils.constants import PART_MAP

router = APIRouter()


@router.get("/", response_class=HTMLResponse)
@router.get("/inventory", response_class=HTMLResponse)
def inventory_page(
    request: Request,
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
    items = get_inventory_items(
        part=part,
        q=q,
        reagent_type=reagent_type,
        equipment=equipment,
        vendor=vendor,
        hazardous=hazardous,
        expiry_filter=expiry_filter,
        sort=sort,
        order=order,
    )

    return templates.TemplateResponse(
        "inventory.html",
        {
            "request": request,
            "active_menu": "inventory",
            "items": items,
            "part_map": PART_MAP,
            "part": part,
            "q": q,
            "reagent_type": reagent_type,
            "equipment": equipment,
            "vendor": vendor,
            "hazardous": hazardous,
            "expiry_filter": expiry_filter,
            "sort": sort,
            "order": order,
            "filter_options": get_inventory_filter_options(),
        },
    )