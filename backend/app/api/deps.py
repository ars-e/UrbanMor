from fastapi import Depends
from sqlalchemy.ext.asyncio import AsyncSession

from app.db.session import get_db_session
from app.services.analyse import AnalyseService


async def get_analyse_service(session: AsyncSession = Depends(get_db_session)) -> AnalyseService:
    return AnalyseService(session)
