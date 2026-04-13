from fastapi import FastAPI, Request
from fastapi.templating import Jinja2Templates
from fastapi.staticfiles import StaticFiles
import pandas as pd
from datetime import datetime, date, timedelta

app = FastAPI()

templates = Jinja2Templates(directory="templates")
app.mount("/static", StaticFiles(directory="static"), name="static")

PART_MAP = {
    "HE": "진단혈액",
    "TA": "임상화학",
    "BB": "혈액은행",
    "ML": "임상미생물",
    "IM": "진단면역",
    "CO": "특수분자",
    "PB": "접수채혈",
    "ZZ": "기타",
}


@app.get("/")
def inventory_page(
    request: Request,
    part: str = "",
    q: str = "",
    expiry_filter: str = "",
    sort: str = "",
    order :str = ""
):
    df = pd.read_excel("inventory.xlsx")
    df.columns = df.columns.str.strip().str.lower()

    # 유효기간 날짜형 변환
    df["expiry_date"] = pd.to_datetime(df["expiry_date"], errors="coerce")

    # 파트 필터
    if part:
        df = df[df["part"] == part]

    # 물품명 검색
    if q:
        df = df[df["item_name"].astype(str).str.contains(q, case=False, na=False)]

    # 유효기간 필터
    today = pd.Timestamp(date.today())
    if expiry_filter == "1w":
        limit = today + pd.Timedelta(days=7)
        df = df[(df["expiry_date"].notna()) & (df["expiry_date"] <= limit)]
    elif expiry_filter == "2w":
        limit = today + pd.Timedelta(days=14)
        df = df[(df["expiry_date"].notna()) & (df["expiry_date"] <= limit)]
    elif expiry_filter == "4w":
        limit = today + pd.Timedelta(days=28)
        df = df[(df["expiry_date"].notna()) & (df["expiry_date"] <= limit)]

    # 정렬
    if sort:
        ascending = True if order != "desc" else False
    
        if sort in ["item_code", "item_name", "expiry_date"]:
            df = df.sort_values(by=sort, ascending=ascending)
            
    items = []
    for _, row in df.iterrows():
        status = "정상"
        if row["current_stock"] <= row["safety_stock"]:
            status = "부족"

        part_code = str(row["part"]).strip()
        part_name = PART_MAP.get(part_code, "")

        expiry_date = row["expiry_date"]
        expiry_text = ""
        expiry_class = ""

        if pd.notna(expiry_date):
            expiry_text = expiry_date.strftime("%Y-%m-%d")
            days_left = (expiry_date.date() - date.today()).days

            if days_left <= 7:
                expiry_class = "expiry-red"
            elif days_left <= 14:
                expiry_class = "expiry-yellow"
            elif days_left <= 28:
                expiry_class = "expiry-green"

        items.append({
            "part": part_code,
            "part_label": f"{part_code} ({part_name})" if part_name else part_code,
            "item_code": row["item_code"],
            "item_name": row["item_name"],
            "lot_no": row["lot_no"],
            "spec": row["spec"],
            "unit": row["unit"],
            "expiry_date": expiry_text,
            "expiry_class": expiry_class,
            "current_stock": row["current_stock"],
            "optimal_stock": row["optimal_stock"],
            "safety_stock": row["safety_stock"],
            "required_qty": row["required_qty"],
            "status": status
        })

    return templates.TemplateResponse(
        "inventory.html",
        {
            "request": request,
            "items": items,
            "part": part,
            "q": q,
            "expiry_filter": expiry_filter,
            "sort": sort,
            "order": order,
            "part_map": PART_MAP,
            "active_menu": "inventory"
        }
    )


@app.get("/inbound")
def inbound_page(request: Request):
    return templates.TemplateResponse(
        "inbound.html",
        {"request": request, "active_menu": "inbound"}
    )


@app.get("/outbound")
def outbound_page(request: Request):
    return templates.TemplateResponse(
        "outbound.html",
        {"request": request, "active_menu": "outbound"}
    )


@app.get("/history")
def history_page(request: Request):
    return templates.TemplateResponse(
        "history.html",
        {"request": request, "active_menu": "history"}
    )


@app.get("/master")
def master_page(request: Request):
    return templates.TemplateResponse(
        "master.html",
        {"request": request, "active_menu": "master"}
    )