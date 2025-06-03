# DuckTyped

DuckTyped is a Python library providing a fluent and intuitive API for working with DuckDB, inspired by Polars syntax. This library allows you to build SQL queries programmatically with an expressive and chainable style.

## Core Concepts

DuckTyped is built around several key concepts:

- **Query**: Represents a SQL query under construction
- **Expr**: Expressions that can be used in queries (columns, operations, etc.)
- **DuckType**: Data types used in DuckDB
- **TABLE**: Represents a DuckDB table, linked to a file (Parquet, CSV, etc.)

## Basic Syntax

### Using SELECT and FROM

```python
import ducktyped as dk
from pathlib import Path

# Create a query with SELECT...FROM
query = dk.SELECT(
    dk.col.date,
    dk.col.ticker,
    dk.col.close
).FROM(Path("prices.parquet"))

# Execute the query
result = query.execute()
```

### Using TABLE for more concise syntax

```python
import ducktyped as dk
from pathlib import Path

# Create a table reference
prices = dk.TABLE(name="prices", path=Path("prices.parquet"))

# Create a query
query = prices.SELECT(
    dk.col.date,
    dk.col.ticker,
    dk.col.close
)

# Filter with a condition
query = prices.SELECT(
    dk.col.date,
    dk.col.ticker,
    dk.col.close
).WHERE(
    dk.col.ticker.eq("SPY")
)

# Execute the query
result = query.execute()
```

### Expressions and Operations

```python
# Arithmetic operations
query = prices.SELECT(
    dk.col.date,
    dk.col.close,
    dk.col.close.add(1).alias("close_plus_one"),
    dk.col.close.sub(dk.col.open).div(dk.col.open).mul(100).alias("pct_change")
)

# Mathematical functions
query = prices.SELECT(
    dk.col.close,
    dk.col.close.sqrt().alias("sqrt_close"),
    dk.col.returns.abs().alias("abs_returns"),
    dk.col.volatility.clip(0, 1).alias("clipped_vol")
)

# Conditional expressions
query = prices.SELECT(
    dk.col.ticker,
    dk.col.close
).WHERE(
    dk.col.close.gt(100)
)
```

### Type Casting

```python
query = prices.SELECT(
    dk.col.date.cast(dk.Date()),
    dk.col.ticker.cast(dk.Varchar()),
    dk.col.close.cast(dk.Float()).alias("close_float"),
    dk.col.category.cast(dk.Enum(categories=["SPY", "AAPL", "GOOG"]))
)
```

### Rolling Windows

```python
query = prices.SELECT(
    dk.col.date,
    dk.col.close,
    dk.col.close.rolling_mean(20).over(dk.col.date).alias("ma20"),
    dk.col.close.rolling_stdev(20).over(dk.col.date).alias("std20")
)
```

### Aggregation Functions

```python
query = prices.SELECT(
    dk.col.ticker,
    dk.col.close.mean().alias("avg_close"),
    dk.col.close.max().alias("max_close"),
    dk.col.close.min().alias("min_close"),
    dk.col.volume.sum().alias("total_volume")
).GROUP_BY(
    dk.col.ticker
)
```

### Sorting Results

```python
query = prices.SELECT(
    dk.col.date,
    dk.col.ticker,
    dk.col.close
).ORDER_BY(
    dk.col.date,
    dk.col.ticker
)

# Descending order
query = prices.SELECT(
    dk.col.ticker,
    dk.col.close.mean().alias("avg_close")
).GROUP_BY(
    dk.col.ticker
).ORDER_BY(
    dk.col("avg_close"), ascending=False
)
```

## Comparison with Polars

### Similarities

- **Fluent API**: DuckTyped adopts the method chaining approach like Polars
- **Expressions**: Uses a similar expression system for operations and transformations
- **Lazy evaluation**: Queries are built first and executed only when necessary

```python
# Polars
import polars as pl
df = pl.scan_parquet("prices.parquet").select(
    pl.col("date"),
    pl.col("ticker"),
    pl.col("close")
).filter(
    pl.col("ticker") == "SPY"
).collect()

# DuckTyped
import ducktyped as dk
from pathlib import Path
prices = dk.TABLE("prices", Path("prices.parquet"))
df = prices.SELECT(
    dk.col.date,
    dk.col.ticker,
    dk.col.close
).WHERE(
    dk.col.ticker.eq("SPY")
).execute()
```

### Differences

- **Underlying Engine**: DuckTyped uses DuckDB while Polars has its own engine
- **Comparison Operations**: Polars uses Python operators (`==`, `>`, etc.) while DuckTyped uses methods (`.eq()`, `.gt()`, etc.)
- **Query Execution**: Polars uses `.collect()` while DuckTyped uses `.execute()`
- **Method Names**: Different naming conventions:
  - `filter()` in Polars vs `WHERE()` in DuckTyped
  - `group_by()` in Polars vs `GROUP_BY()` in DuckTyped

## Complete Examples

### Example 1: Stock Price Analysis

```python
import ducktyped as dk
from pathlib import Path

# Define a table
prices = dk.TABLE("prices", Path("prices.parquet"))

# Build a complex query
query = prices.SELECT(
    dk.col.date,
    dk.col.ticker,
    dk.col.close,
    dk.col.close.rolling_mean(20).over(dk.col.date).alias("ma20"),
    dk.col.close.rolling_mean(50).over(dk.col.date).alias("ma50"),
    dk.col.close.div(dk.col.close.rolling_mean(200).over(dk.col.date)).sub(1).mul(100).alias("pct_above_ma200")
).WHERE(
    dk.col.ticker.eq("SPY")
)

# Display generated SQL query
print(query.explain())

# Execute query and get Polars DataFrame
result = query.execute()
print(result.head())
```

### Example 2: Transformations and Formatting

```python
import ducktyped as dk
from pathlib import Path

# Create a table reference
sales = dk.TABLE("sales", Path("sales.parquet"))

# Query with multiple transformations
query = sales.SELECT(
    dk.col.date,
    dk.col.category.cast(dk.Enum(categories=["Electronics", "Clothing", "Food"])).alias("category"),
    dk.col.quantity,
    dk.col.price,
    dk.col.quantity.mul(dk.col.price).alias("total_sales"),
    dk.col.quantity.mul(dk.col.price).rolling_sum(7).over(dk.col.date).alias("rolling_7day_sales")
).WHERE(
    dk.col.price.gt(10)
)

# Execute and display results
result = query.execute()
print(result)
```

### Example 3: Using All Columns

```python
import ducktyped as dk
from pathlib import Path

# Create a table reference
customers = dk.TABLE("customers", Path("customers.parquet"))

# Select all columns
query = customers.SELECT(
    dk.all()
).WHERE(
    dk.col.age.gte(18)
)

# Select all columns plus derived columns
query = customers.SELECT(
    dk.all(),
    dk.col.age.div(100).alias("age_century")
).WHERE(
    dk.col.status.eq("active")
)

# Execute query
result = query.execute()
print(result)
```

### Example 4: IN Operation and Other Conditionals

```python
import ducktyped as dk
from pathlib import Path

# Create a table reference
stocks = dk.TABLE("stocks", Path("stocks.parquet"))

# Query using IN operator and multiple conditions
query = stocks.SELECT(
    dk.col.date,
    dk.col.ticker,
    dk.col.close
).WHERE(
    dk.col.ticker.is_in("AAPL", "MSFT", "GOOG"),
    dk.col.close.gt(100)
)

# Execute query
result = query.execute()
print(result)
```