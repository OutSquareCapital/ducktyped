from dataclasses import dataclass, field
from pathlib import Path
from typing import Self

import duckdb
import polars as pl

from ducktyped.cols import Col
from ducktyped.enums import JoinTypes
from ducktyped.expressions import Expr
from ducktyped.parsing import (
    SQLRaw,
    TableProtocol,
    get_executable_query,
    get_explained_query,
    get_sql_raw,
)


def _selection(*cols: Col | Expr) -> list[Expr]:
    _selected: list[Expr] = []
    for c in cols:
        _selected.append(c)
    return _selected


@dataclass(slots=True)
class TABLE:
    path: Path
    name: str = field(init=False)

    def __post_init__(self) -> None:
        self.name: str = self.path.stem

    def SELECT(self, *cols: Col) -> "Query":
        qualified_cols: list[Expr] = []
        for col in cols:
            if col.table is None:
                qualified_cols.append(Col(name=col.name, table=self.name))
            else:
                qualified_cols.append(col)
        return Query(table=self, selected=_selection(*cols))


class SELECT:
    __slots__ = ("_selected",)

    def __init__(self, *cols: Col) -> None:
        self._selected: list[Expr] = _selection(*cols)

    def FROM(self, table: TABLE) -> "Query":
        return Query(table=table, selected=self._selected)


class Query:
    __slots__ = (
        "_table",
        "_selected",
        "_where_clause",
        "_group_by",
        "_order_by",
        "_joins",
        "_table_aliases",
    )

    def __init__(self, table: TableProtocol, selected: list[Expr]) -> None:
        self._table: TableProtocol = table
        self._selected: list[Expr] = selected
        self._where_clause: list[Expr] = []
        self._group_by: list[Expr] = []
        self._order_by: list[tuple[Expr, bool]] = []
        self._joins: list[tuple[TableProtocol, Expr, JoinTypes]] = []
        self._table_aliases: dict[str, str] = {table.name: table.name}

    def __repr__(self) -> str:
        return f"{self.__class__.__name__}:\n({self.explain()})"

    def _repr_html_(self) -> str:
        return f"{self.__class__.__name__}:\n({self._explain_html()})"

    def _qualify_expression(self, expr: Expr) -> Expr:
        if isinstance(expr, Col) and expr.table is None:
            expr = Col(name=expr.name, table=self._table.name)
        return expr

    def SELECT(self, *cols: Expr) -> Self:
        self._selected.extend(self._qualify_expression(c) for c in cols)
        return self

    def WHERE(self, *cols: Expr) -> Self:
        self._where_clause.extend(self._qualify_expression(c) for c in cols)
        return self

    def GROUP_BY(self, *cols: Expr) -> Self:
        for c in cols:
            self._group_by.append(self._qualify_expression(c))
        return self

    def ORDER_BY(self, *cols: Expr, ascending: bool = True) -> Self:
        for c in cols:
            self._order_by.append((self._qualify_expression(c), ascending))
        return self

    def JOIN(
        self,
        table: TABLE,
        on: Expr,
        join_type: JoinTypes,
    ) -> Self:
        self._table_aliases[table.name] = table.name

        qualified_on: Expr = on
        if isinstance(on, Col) and on.table is None:
            qualified_on = Col(name=on.name, table=table.name)

        self._joins.append((table, qualified_on, join_type))
        return self

    def _to_sql(self) -> SQLRaw:
        return get_sql_raw(
            selected=self._selected,
            where_clause=self._where_clause,
            group_by=self._group_by,
            order_by=self._order_by,
            joins=self._joins,
        )

    def execute(self) -> pl.DataFrame:
        conn: duckdb.DuckDBPyConnection = duckdb.connect(database=self._table.path) # type: ignore[call-arg]
        try:
            query: str = get_executable_query(
                table=str(object=self._table.path), sql_raw=self._to_sql()
            )
            return conn.execute(query=query).pl()
        finally:
            conn.close()

    def explain(self) -> str:
        return get_explained_query(sql_raw=self._to_sql(), table=str(self._table.path))

    def _explain_html(self) -> str:
        lines: list[str] = self.explain().splitlines()
        html_lines: list[str] = [f"<pre>{line}</pre>" for line in lines]
        return "".join(html_lines)
