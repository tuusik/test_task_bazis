"""add delivery claims and outbox retries

Revision ID: e7b4c2d91f63
Revises: d2a8e4f7b9c1
Create Date: 2026-08-02 00:30:00.000000

"""

from collections.abc import Sequence

import sqlalchemy as sa

from alembic import op

revision: str = "e7b4c2d91f63"
down_revision: str | Sequence[str] | None = "d2a8e4f7b9c1"
branch_labels: str | Sequence[str] | None = None
depends_on: str | Sequence[str] | None = None


def upgrade() -> None:
    op.drop_constraint(
        "payments_idempotency_key_key",
        "payments",
        type_="unique",
    )
    op.create_unique_constraint(
        "uq_payments_idempotency_key",
        "payments",
        ["idempotency_key"],
    )
    op.add_column(
        "payments",
        sa.Column(
            "webhook_claimed_at",
            sa.DateTime(timezone=True),
            nullable=True,
        ),
    )

    op.drop_index(
        "ix_outbox_unpublished_created_at",
        table_name="outbox",
        postgresql_where=sa.text("published_at IS NULL"),
    )
    op.add_column(
        "outbox",
        sa.Column(
            "claimed_at",
            sa.DateTime(timezone=True),
            nullable=True,
        ),
    )
    op.add_column(
        "outbox",
        sa.Column(
            "next_attempt_at",
            sa.DateTime(timezone=True),
            server_default=sa.text("now()"),
            nullable=False,
        ),
    )
    op.add_column(
        "outbox",
        sa.Column(
            "publish_attempts",
            sa.Integer(),
            server_default="0",
            nullable=False,
        ),
    )
    op.add_column(
        "outbox",
        sa.Column("last_error", sa.Text(), nullable=True),
    )
    op.create_index(
        "ix_outbox_pending_publish",
        "outbox",
        ["next_attempt_at", "created_at", "outbox_id"],
        unique=False,
        postgresql_where=sa.text("published_at IS NULL"),
    )


def downgrade() -> None:
    op.drop_index(
        "ix_outbox_pending_publish",
        table_name="outbox",
        postgresql_where=sa.text("published_at IS NULL"),
    )
    op.drop_column("outbox", "last_error")
    op.drop_column("outbox", "publish_attempts")
    op.drop_column("outbox", "next_attempt_at")
    op.drop_column("outbox", "claimed_at")
    op.create_index(
        "ix_outbox_unpublished_created_at",
        "outbox",
        ["created_at"],
        unique=False,
        postgresql_where=sa.text("published_at IS NULL"),
    )

    op.drop_column("payments", "webhook_claimed_at")
    op.drop_constraint(
        "uq_payments_idempotency_key",
        "payments",
        type_="unique",
    )
    op.create_unique_constraint(
        "payments_idempotency_key_key",
        "payments",
        ["idempotency_key"],
    )
