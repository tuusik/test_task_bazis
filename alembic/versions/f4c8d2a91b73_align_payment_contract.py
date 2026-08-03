"""align payment metadata and amount contract

Revision ID: f4c8d2a91b73
Revises: e7b4c2d91f63
Create Date: 2026-08-03 00:00:00.000000

"""

from collections.abc import Sequence

import sqlalchemy as sa
from sqlalchemy.dialects import postgresql

from alembic import op

revision: str = "f4c8d2a91b73"
down_revision: str | Sequence[str] | None = "e7b4c2d91f63"
branch_labels: str | Sequence[str] | None = None
depends_on: str | Sequence[str] | None = None


def upgrade() -> None:
    op.alter_column(
        "payments",
        "amount",
        existing_type=sa.Numeric(),
        type_=sa.Numeric(18, 2),
        existing_nullable=False,
        postgresql_using="amount::numeric(18, 2)",
    )
    op.alter_column(
        "payments",
        "meta",
        new_column_name="metadata",
        existing_type=postgresql.JSONB(),
        existing_nullable=False,
    )


def downgrade() -> None:
    op.alter_column(
        "payments",
        "metadata",
        new_column_name="meta",
        existing_type=postgresql.JSONB(),
        existing_nullable=False,
    )
    op.alter_column(
        "payments",
        "amount",
        existing_type=sa.Numeric(18, 2),
        type_=sa.Numeric(),
        existing_nullable=False,
        postgresql_using="amount::numeric",
    )
