# src/api/v1/greeting.py
from typing import Annotated

from fastapi import APIRouter, Body, HTTPException

from src.schemas import HellowRequest
from src.services.greeting_service import greet_users

from src.core.logger import logger

router = APIRouter()

@router.post("/")
async def func_greetings(
    body: Annotated[
        HellowRequest,
        Body(example={"names": ["Nikita", "Dima", "Anastasia", "Nikita", "Ilia"]}),
    ]
):
    try:
        names = body.names
        if names:
            res = greet_users(names)
            return res
        else:
            logger.error("Names list is empty")
            raise HTTPException(
                status_code=400,
                detail="Bad Request",
                headers={"X-Error": "Names list is empty"},
            )
    except Exception as application_error:
        logger.error(application_error.__repr__())
        raise HTTPException(
            status_code=400,
            detail="Unknown Error",
            headers={"X-Error": f"{application_error.__repr__()}"},
        )
