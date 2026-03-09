#!/usr/bin/env python3
"""Calculate CNR for all cities and wards after applying updated SQL."""
import asyncio
import asyncpg
from pathlib import Path


async def main():
    # Connect to database
    conn = await asyncpg.connect(database='urbanmor')
    print("✓ Connected to urbanmor database\n")

    try:
        # Apply updated SQL
        print("Applying updated roads_metrics.sql...")
        sql_file = Path(__file__).parent / "sql" / "roads_metrics.sql"
        sql = sql_file.read_text()
        await conn.execute(sql)
        print("✓ SQL functions updated\n")

        # Get all cities
        cities = await conn.fetch("""
            SELECT regexp_replace(table_name, '_wards_normalized$', '') as city
            FROM information_schema.tables
            WHERE table_schema = 'boundaries'
              AND table_name LIKE '%_wards_normalized'
            ORDER BY city
        """)

        city_names = [r['city'] for r in cities]
        print(f"Found {len(city_names)} cities: {', '.join(city_names)}\n")
        print("=" * 100)

        # Calculate CNR for each city
        for city in city_names:
            print(f"\n{city.upper()}")
            print("-" * 100)

            results = await conn.fetch(f"""
                SELECT
                    ward_id,
                    ward_name,
                    metrics.compute_road_cnr('{city}', geom) as cnr
                FROM boundaries.{city}_wards_normalized
                WHERE geom IS NOT NULL
                ORDER BY ward_id
            """)

            print(f"{'Ward ID':<15} {'Ward Name':<50} {'CNR (%)':<12}")
            print("-" * 100)

            cnr_values = []
            for row in results:
                cnr = row['cnr']
                cnr_str = f"{cnr:.2f}" if cnr is not None else "NULL"
                print(f"{row['ward_id']:<15} {(row['ward_name'] or 'N/A'):<50} {cnr_str:<12}")
                if cnr is not None:
                    cnr_values.append(cnr)

            if cnr_values:
                print("-" * 100)
                print(f"Stats: Avg={sum(cnr_values)/len(cnr_values):.2f}%, "
                      f"Min={min(cnr_values):.2f}%, Max={max(cnr_values):.2f}%, "
                      f"NULL={len(results)-len(cnr_values)}")

    finally:
        await conn.close()


if __name__ == "__main__":
    asyncio.run(main())
