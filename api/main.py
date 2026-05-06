from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware
from shared.utils.logger import get_logger
from api.routers import health, discovery, devices, jobs

logger = get_logger("api")

app = FastAPI(
    title="NetFleet API",
    description=(
        "REST API for NetFleet — distributed network device "
        "fleet management platform"
    ),
    version="1.0.0",
    docs_url="/docs",
    redoc_url="/redoc",
)

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_methods=["*"],
    allow_headers=["*"],
)

app.include_router(
    health.router,
    prefix="/api/v1",
    tags=["health"],
)
app.include_router(
    discovery.router,
    prefix="/api/v1",
    tags=["discovery"],
)
app.include_router(
    devices.router,
    prefix="/api/v1",
    tags=["devices"],
)
app.include_router(
    jobs.router,
    prefix="/api/v1",
    tags=["jobs"],
)


@app.on_event("startup")
async def on_startup():
    logger.info("NetFleet API started")


@app.on_event("shutdown")
async def on_shutdown():
    logger.info("NetFleet API stopped")
