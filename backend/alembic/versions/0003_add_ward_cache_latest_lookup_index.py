"""add ward cache latest lookup index

Revision ID: 0003_ward_cache_idx
Revises: 0002_create_analysis_jobs
Create Date: 2026-03-08

"""

from typing import Sequence, Union

from alembic import op


revision: str = "0003_ward_cache_idx"
down_revision: Union[str, Sequence[str], None] = "0002_create_analysis_jobs"
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def upgrade() -> None:
    op.execute(
        """
        CREATE INDEX IF NOT EXISTS idx_ward_cache_city_ward_latest
        ON metrics.ward_cache (city, ward_id, vintage_year DESC, computed_at DESC)
        """
    )


def downgrade() -> None:
    op.execute("DROP INDEX IF EXISTS metrics.idx_ward_cache_city_ward_latest")
