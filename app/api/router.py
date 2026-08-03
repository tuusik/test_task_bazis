from fastapi import APIRouter, Depends

from app.api.deps import require_api_key
from app.api.routes.payments import router as payments_router

api_router = APIRouter(dependencies=[Depends(require_api_key)])
api_router.include_router(payments_router)
