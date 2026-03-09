import logging
from contextlib import asynccontextmanager
from pathlib import Path

from fastapi import FastAPI
from fastapi.responses import FileResponse
from fastapi.staticfiles import StaticFiles

from server.routes import dashboard, reports, audit, approvals

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
)
logger = logging.getLogger(__name__)

FRONTEND_DIR = Path(__file__).parent / "frontend" / "dist"


@asynccontextmanager
async def lifespan(application: FastAPI):
    """Startup / shutdown lifecycle hook."""
    logger.info("Starting Solvency II QRT Reporting App")
    # Ensure the approvals table exists
    try:
        await approvals.ensure_approvals_table()
        logger.info("Approvals table ready")
    except Exception:
        logger.exception("Failed to ensure approvals table on startup — will retry on first request")
    yield
    logger.info("Shutting down")


app = FastAPI(
    title="Solvency II QRT Reporting & Approval",
    version="1.0.0",
    lifespan=lifespan,
)

# ── API routers ──────────────────────────────────────────────────────────────
app.include_router(dashboard.router)
app.include_router(reports.router)
app.include_router(audit.router)
app.include_router(approvals.router)


# ── Health check ─────────────────────────────────────────────────────────────
@app.get("/api/health")
async def health():
    return {"status": "ok"}


# ── SPA frontend serving ────────────────────────────────────────────────────
if FRONTEND_DIR.is_dir():
    app.mount("/assets", StaticFiles(directory=FRONTEND_DIR / "assets"), name="assets")

    @app.get("/{full_path:path}")
    async def serve_spa(full_path: str):
        """Serve the React SPA. Any non-API path returns index.html."""
        file_path = FRONTEND_DIR / full_path
        if file_path.is_file():
            return FileResponse(file_path)
        return FileResponse(FRONTEND_DIR / "index.html")
