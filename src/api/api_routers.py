from fastapi import APIRouter

api_router = APIRouter()

from src.api.v1.greeting import router as func_greetings

api_router.include_router(func_greetings, prefix="/greetings", tags=["Greetings"])