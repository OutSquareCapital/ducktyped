from dataclasses import dataclass
from pathlib import Path
from typing import Self

import duckdb
import polars as pl

from ducktyped.enums import KeyWord
from ducktyped.expressions import AllExpr, Col, Expr
from ducktyped.types import DuckType


def col(name: str) -> Col:
    return Col(name=name)


def all() -> AllExpr:
    return AllExpr()


@dataclass(slots=True)
class Table:
    filepath: str | Path
    schema: dict[str, DuckType]

    def select(self, *cols: Col | Expr) -> "Query":
        return Query(table=self).select(*cols)


class Query:
    __slots__ = ("_table", "_selected", "_where_clause")

    def __init__(self, table: Table) -> None:
        self._table: Table = table
        self._selected: list[Expr] = []
        self._where_clause: list[Expr] = []

    def __repr__(self) -> str:
        return f"{self.__class__.__name__}:\n({self.explain()})"

    def _repr_html_(self) -> str:
        return f"{self.__class__.__name__}:\n({self._explain_html()})"

    def select(self, *cols: Expr) -> Self:
        self._selected.extend(cols)
        return self

    def where(self, condition: Expr) -> Self:
        self._where_clause.append(condition)
        return self

    def to_sql(self) -> str:
        select_sql: str = ", ".join(col.to_sql() for col in self._selected)
        where_sql: str = ""
        if self._where_clause:
            where_sql: str = f" {KeyWord.WHERE} " + f" {KeyWord.AND} ".join(
                cond.to_sql() for cond in self._where_clause
            )
        return f"{KeyWord.SELECT} {select_sql} {KeyWord.FROM} '{self._table.filepath}'{where_sql}"

    def execute(self) -> pl.DataFrame:
        conn: duckdb.DuckDBPyConnection = duckdb.connect(database=self._table.filepath)  # type: ignore[call-arg]
        try:
            result: pl.DataFrame = conn.execute(query=self.to_sql()).pl()
            return result
        finally:
            conn.close()

    def explain(self) -> str:
        select_parts: list[str] = [col.to_sql() for col in self._selected]
        select_sql: str = ",\n    ".join(select_parts)

        query: str = f"{KeyWord.SELECT}\n    {select_sql}\n{KeyWord.FROM} '{self._table.filepath}'"

        if self._where_clause:
            where_conditions: list[str] = [cond.to_sql() for cond in self._where_clause]
            where_sql: str = f"\n{KeyWord.WHERE}\n    " + f"\n    {KeyWord.AND} ".join(
                where_conditions
            )
            query += where_sql

        return query

    def _explain_html(self) -> str:
        lines: list[str] = self.explain().splitlines()
        html_lines: list[str] = [f"<pre>{line}</pre>" for line in lines]
        return "".join(html_lines)
