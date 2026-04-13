from ninja import NinjaAPI

from .models import Inventory


api = NinjaAPI(title="Reagent API")


@api.get("/health")
def health(request):
    return {"status": "ok"}


@api.get("/inventory")
def inventory_list(request, part: str = ""):
    queryset = Inventory.objects.filter(disposed_at__isnull=True)
    if part:
        queryset = queryset.filter(part=part)
    rows = queryset.order_by("item_code", "lot_no")[:200]
    return [
        {
            "id": row.id,
            "part": row.part,
            "item_code": row.item_code,
            "item_name": row.item_name,
            "lot_no": row.lot_no,
            "current_stock": row.current_stock,
        }
        for row in rows
    ]
