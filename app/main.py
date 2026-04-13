from fastapi import FastAPI
from fastapi.staticfiles import StaticFiles
from fastapi.responses import RedirectResponse
from app.core.db import init_db
from app.routers import inventory, inbound, outbound, history, master, reagent_history


app = FastAPI()

app.mount("/static", StaticFiles(directory="static"), name="static")

app.include_router(inventory.router)
app.include_router(inbound.router)
app.include_router(outbound.router)
app.include_router(history.router)
app.include_router(reagent_history.router)
app.include_router(master.router)

@app.get("/", include_in_schema=False)
def root():
    return RedirectResponse(url="/inventory")

@app.on_event("startup")
def startup():
    init_db()
