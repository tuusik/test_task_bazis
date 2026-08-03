"""add webhook delivery state

Revision ID: d2a8e4f7b9c1
Revises: a66526a7698e
Create Date: 2026-08-01 23:10:00.000000

"""

from collections.abc import Sequence

import sqlalchemy as sa

from alembic import op

revision: str = "d2a8e4f7b9c1"
down_revision: str | Sequence[str] | None = "a66526a7698e"
branch_labels: str | Sequence[str] | None = None
depends_on: str | Sequence[str] | None = None


def upgrade() -> None:
    op.add_column(
        "payments",
        sa.Column(
            "webhook_sent_at",
            sa.DateTime(timezone=True),
            nullable=True,
        ),
    )


def downgrade() -> None:
    op.drop_column("payments", "webhook_sent_at")
