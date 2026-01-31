from contextlib import asynccontextmanager
from typing import AsyncGenerator

import asyncpg
from asyncpg import Pool

from app.config import get_settings

pool: Pool | None = None


async def init_db():
    global pool
    settings = get_settings()
    pool = await asyncpg.create_pool(settings.database_url)


async def close_db():
    global pool
    if pool:
        await pool.close()


async def get_db() -> AsyncGenerator[asyncpg.Connection, None]:
    async with pool.acquire() as conn:
        yield conn


@asynccontextmanager
async def lifespan(app):
    await init_db()
    yield
    await close_db()
