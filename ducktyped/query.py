from pathlib import Path
from typing import Self

import duckdb
import polars as pl

from ducktyped.expressions import Expr
from ducktyped.parsing import (
    SQLRaw,
    get_executable_query,
    get_explained_query,
    get_sql_raw,
)


class Query:
    __slots__ = ("_table", "_selected", "_where_clause", "_group_by", "_order_by")

    def __init__(self, table: Path, selected: list[Expr]) -> None:
        self._table: Path = table
        self._selected: list[Expr] = selected
        self._where_clause: list[Expr] = []
        self._group_by: list[Expr] = []
        self._order_by: list[tuple[Expr, bool]] = []

    def __repr__(self) -> str:
        return f"{self.__class__.__name__}:\n({self.explain()})"

    def _repr_html_(self) -> str:
        return f"{self.__class__.__name__}:\n({self._explain_html()})"

    def SELECT(self, *cols: Expr) -> Self:
        self._selected.extend(cols)
        return self

    def WHERE(self, *cols: Expr) -> Self:
        self._where_clause.extend(cols)
        return self

    def GROUP_BY(self, *cols: Expr) -> Self:
        for c in cols:
            self._group_by.append(c)
        return self

    def ORDER_BY(self, *cols: Expr, ascending: bool = True) -> Self:
        for c in cols:
            self._order_by.append((c, ascending))
        return self

    def to_sql(self) -> str:
        sql_raw: SQLRaw = get_sql_raw(
            selected=self._selected,
            where_clause=self._where_clause,
            group_by=self._group_by,
            order_by=self._order_by,
        )
        return get_executable_query(table=str(self._table), sql_raw=sql_raw)

    def execute(self) -> pl.DataFrame:
        conn: duckdb.DuckDBPyConnection = duckdb.connect(database=self._table)  # type: ignore[call-arg]
        try:
            return conn.execute(query=self.to_sql()).pl()
        finally:
            conn.close()

    def explain(self) -> str:
        sql_raw: SQLRaw = get_sql_raw(
            selected=self._selected,
            where_clause=self._where_clause,
            group_by=self._group_by,
            order_by=self._order_by,
        )
        return get_explained_query(sql_raw=sql_raw, table=str(self._table))

    def _explain_html(self) -> str:
        lines: list[str] = self.explain().splitlines()
        html_lines: list[str] = [f"<pre>{line}</pre>" for line in lines]
        return "".join(html_lines)
