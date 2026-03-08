"""create analysis jobs

Revision ID: 0002_create_analysis_jobs
Revises: 0001_create_api_request_log
Create Date: 2026-03-08

"""

from typing import Sequence, Union

from alembic import op


revision: str = "0002_create_analysis_jobs"
down_revision: Union[str, Sequence[str], None] = "0001_create_api_request_log"
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def upgrade() -> None:
    op.execute("CREATE SCHEMA IF NOT EXISTS meta")
    op.execute(
        """
        CREATE TABLE IF NOT EXISTS meta.analysis_jobs (
          job_id text PRIMARY KEY,
          mode text NOT NULL,
          city text NOT NULL,
          payload_json json NOT NULL,
          status text NOT NULL,
          progress_pct integer NOT NULL DEFAULT 0,
          progress_message text,
          result_json json,
          error_text text,
          created_at timestamptz NOT NULL DEFAULT now(),
          started_at timestamptz,
          completed_at timestamptz,
          updated_at timestamptz NOT NULL DEFAULT now()
        )
        """
    )
    op.execute(
        """
        CREATE INDEX IF NOT EXISTS analysis_jobs_status_created_idx
        ON meta.analysis_jobs (status, created_at DESC)
        """
    )
    op.execute(
        """
        CREATE INDEX IF NOT EXISTS analysis_jobs_city_created_idx
        ON meta.analysis_jobs (city, created_at DESC)
        """
    )


def downgrade() -> None:
    op.drop_index("analysis_jobs_city_created_idx", table_name="analysis_jobs", schema="meta")
    op.drop_index("analysis_jobs_status_created_idx", table_name="analysis_jobs", schema="meta")
    op.drop_table("analysis_jobs", schema="meta")
