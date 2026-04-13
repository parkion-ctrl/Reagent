from datetime import date, timedelta

from fastapi import APIRouter, Request

from app.core.config import templates
from app.services.history_service import get_history_items
from app.utils.constants import PART_MAP

router = APIRouter()


@router.get("/history")
def history_page(
    request: Request,
    tx_type: str = "",
    part: str = "",
    q: str = "",
    date_from: str = "",
    date_to: str = "",
    period: str = "7d",
):
    today = date.today()

    if not date_from and not date_to:
        if period == "1m":
            date_from = (today - timedelta(days=30)).isoformat()
        elif period == "6m":
            date_from = (today - timedelta(days=183)).isoformat()
        else:
            period = "7d"
            date_from = (today - timedelta(days=7)).isoformat()
        date_to = today.isoformat()

    return templates.TemplateResponse(
        "history.html",
        {
            "request": request,
            "active_menu": "history",
            "items": get_history_items(
                tx_type=tx_type,
                part=part,
                q=q,
                date_from=date_from,
                date_to=date_to,
            ),
            "tx_type": tx_type,
            "part": part,
            "q": q,
            "date_from": date_from,
            "date_to": date_to,
            "period": period,
            "part_map": PART_MAP,
        },
    )
