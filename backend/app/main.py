from __future__ import annotations

from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware

from app.routers import documents, map_events, search, stats

app = FastAPI(title="News Events Search API")

app.add_middleware(
    CORSMiddleware,
    allow_origins=["http://localhost:5173", "http://127.0.0.1:5173"],
    allow_methods=["GET"],
    allow_headers=["*"],
)

app.include_router(search.router)
app.include_router(documents.router)
app.include_router(stats.router)
app.include_router(map_events.router)


@app.get("/api/health")
def health():
    return {"status": "ok"}
