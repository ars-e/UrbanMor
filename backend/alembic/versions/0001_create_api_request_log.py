"""create api request log

Revision ID: 0001_create_api_request_log
Revises:
Create Date: 2026-03-08

"""

from typing import Sequence, Union

from alembic import op
import sqlalchemy as sa


revision: str = "0001_create_api_request_log"
down_revision: Union[str, Sequence[str], None] = None
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def upgrade() -> None:
    op.execute("CREATE SCHEMA IF NOT EXISTS meta")
    op.create_table(
        "api_request_log",
        sa.Column("id", sa.BigInteger(), primary_key=True, autoincrement=True),
        sa.Column("path", sa.String(length=512), nullable=False),
        sa.Column("method", sa.String(length=16), nullable=False),
        sa.Column("status_code", sa.BigInteger(), nullable=False),
        sa.Column("request_id", sa.String(length=128), nullable=True),
        sa.Column("error_message", sa.Text(), nullable=True),
        sa.Column("created_at", sa.DateTime(timezone=True), nullable=False),
        schema="meta",
    )


def downgrade() -> None:
    op.drop_table("api_request_log", schema="meta")
