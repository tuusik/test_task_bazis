import os
from collections.abc import AsyncIterator
from uuid import uuid4

import pytest
from sqlalchemy import text
from sqlalchemy.ext.asyncio import (
    AsyncSession,
    async_sessionmaker,
    create_async_engine,
)

from app.models.base import Base

RUN_INTEGRATION_TESTS = os.getenv("RUN_INTEGRATION_TESTS") == "1"


@pytest.fixture
async def integration_session_factory() -> AsyncIterator[
    async_sessionmaker[AsyncSession]
]:
    if not RUN_INTEGRATION_TESTS:
        pytest.skip("Set RUN_INTEGRATION_TESTS=1 to run integration tests")

    database_url = os.getenv(
        "TEST_DATABASE_URL",
        "postgresql+asyncpg://payments:payments@localhost:5432/payments",
    )
    schema_name = f"integration_{uuid4().hex}"
    admin_engine = create_async_engine(database_url)

    async with admin_engine.begin() as connection:
        await connection.execute(text(f'CREATE SCHEMA "{schema_name}"'))

    test_engine = create_async_engine(
        database_url,
        connect_args={
            "server_settings": {
                "search_path": schema_name,
            },
        },
    )

    try:
        async with test_engine.begin() as connection:
            await connection.run_sync(Base.metadata.create_all)

        yield async_sessionmaker(
            test_engine,
            expire_on_commit=False,
            autoflush=False,
        )
    finally:
        await test_engine.dispose()
        async with admin_engine.begin() as connection:
            await connection.execute(text(f'DROP SCHEMA "{schema_name}" CASCADE'))
        await admin_engine.dispose()
