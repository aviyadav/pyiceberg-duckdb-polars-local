import duckdb
import polars as pl
from pyiceberg.catalog.sql import SqlCatalog
import uuid
from datetime import date
import numpy as np
import pyarrow

warehouse_path = "./icehouse"
catalog = SqlCatalog(
    "default",
    **{
        "uri": f"sqlite:///{warehouse_path}/icyhot.db",
        "warehouse": f"file://{warehouse_path}",
    },
)

catalog.create_namespace("dummy_data")

rows = 5000

# duckdb

sql = f"""
    select t.row_id, uuid() as txn_key, current_date as rpt_dt, round(random() * 100, 2) as some_val
    from generate_series(1, {rows}) t(row_id)
"""

duck_df = duckdb.execute(sql).arrow()

# polars
polars_df = pl.DataFrame({
    'row_id': pl.arange(0, rows, eager=True),
    'rpt_dt': pl.Series([date.today()] * rows),
    'some_val': pl.Series(np.floor(np.random.rand(rows) * 100).astype(int)),
    'txn_key': pl.Series([str(uuid.uuid4()) for _ in range(rows)])
}).to_arrow()


table_ducks = catalog.create_table("dummy_data.duckdb_data", schema = duck_df.schema)
table_polars = catalog.create_table("dummy_data.polars_data", schema = polars_df.schema)

table_ducks.append(duck_df)
table_polars.append(polars_df)

print(f'duckdb table has {len(table_ducks.scan().to_arrow())} rows')
print(f'polars table has {len(table_polars.scan().to_arrow())} rows')


# read back
cn = table_ducks.scan().to_duckdb(table_name="duck_back")
row_count_duck = cn.sql("select count(row_id) as tot_rows, sum(some_val) as agg_val from duck_back")
print(f"Row count in duck db {row_count_duck}")

# cn = duckdb.connect()
# cn.execute("install iceberg; load iceberg; SET unsafe_enable_version_guessing = true;")

# sql = """
# select *
# from iceberg_scan('./icehouse/dummy_data.db/duckdb_data')
# """

# data = cn.sql(sql)

# print(f"DuckDB reading from iceberg {data}")


# read back in polars
df = pl.scan_iceberg(table_polars).collect()

pol_data = df.select(
    pl.col('row_id').count().alias("tot_rows")
    , pl.col('some_val').sum().alias('agg_val')
)

print(pol_data)





