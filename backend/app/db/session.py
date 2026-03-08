from collections.abc import AsyncIterator
from functools import lru_cache

from sqlalchemy.ext.asyncio import AsyncEngine, AsyncSession, async_sessionmaker, create_async_engine
from sqlalchemy.pool import NullPool

from app.core.config import get_settings


@lru_cache(maxsize=1)
def get_async_engine() -> AsyncEngine:
    settings = get_settings()
    if settings.app_env.lower() == "test":
        return create_async_engine(
            settings.database_url,
            echo=settings.sql_echo,
            poolclass=NullPool,
        )

    return create_async_engine(
        settings.database_url,
        echo=settings.sql_echo,
        pool_pre_ping=True,
        pool_size=settings.db_pool_size,
        max_overflow=settings.db_max_overflow,
    )


@lru_cache(maxsize=1)
def get_session_factory() -> async_sessionmaker[AsyncSession]:
    return async_sessionmaker(get_async_engine(), expire_on_commit=False, class_=AsyncSession)


async def get_db_session() -> AsyncIterator[AsyncSession]:
    session_factory = get_session_factory()
    async with session_factory() as session:
        yield session


async def dispose_async_engine() -> None:
    engine = get_async_engine()
    await engine.dispose()
    get_session_factory.cache_clear()
    get_async_engine.cache_clear()
