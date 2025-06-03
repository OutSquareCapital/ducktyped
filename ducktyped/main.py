from dataclasses import dataclass
from typing import Self
from pathlib import Path
from ducktyped.types import DuckType, KeyWord
from ducktyped.expressions import Expr, Col, AllExpr

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
    def __init__(self, table: Table) -> None:
        self.table: Table = table
        self.selected: list[Expr] = []
        self.where_clause: list[Expr] = []

    def select(self, *cols: Expr) -> Self:
        self.selected.extend(cols)
        return self

    def where(self, condition: Expr) -> Self:
        self.where_clause.append(condition)
        return self

    def to_sql(self) -> str:
        select_sql: str = ", ".join(col.to_sql() for col in self.selected)
        where_sql: str = ""
        if self.where_clause:
            where_sql: str = f" {KeyWord.WHERE} " + f" {KeyWord.AND} ".join(
                cond.to_sql() for cond in self.where_clause
            )
        return f"{KeyWord.SELECT} {select_sql} {KeyWord.FROM} '{self.table.filepath}'{where_sql}"

