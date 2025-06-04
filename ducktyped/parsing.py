from pathlib import Path
from typing import Protocol

from ducktyped.enums import Context, JoinTypes, KeyWord
from ducktyped.expressions import Expr


class TableProtocol(Protocol):
    name: str
    path: Path


class SQLParser:
    def __init__(
        self,
        selected: list[Expr],
        where_clause: list[Expr],
        group_by: list[Expr],
        order_by: list[tuple[Expr, bool]],
        joins: list[tuple[TableProtocol, Expr, JoinTypes]],
    ) -> None:
        self.select: list[str] = [col.to_sql() for col in selected]
        self.where: str = ""
        if where_clause:
            where_conditions: list[str] = [cond.to_sql() for cond in where_clause]
            self.where: str = f" {KeyWord.AND} ".join(where_conditions)
        self.group: str = ""
        if group_by:
            self.group: str = ", ".join(col.to_sql() for col in group_by)

        order_parts: list[str] = []
        if order_by:
            for expr, is_asc in order_by:
                direction: KeyWord | KeyWord = KeyWord.ASC if is_asc else KeyWord.DESC
                order_parts.append(f"{expr.to_sql()} {direction}")
        self.order: str = ", ".join(order_parts)
        join_parts: list[str] = []
        if joins:
            for table, on_condition, join_type in joins:
                table_ref: str = f"'{str(table.path)}'"
                table_ref += f" AS {table.name}"
                join_parts.append(
                    f"{join_type} JOIN {table_ref} ON {on_condition.to_sql()}"
                )
        self.joins: list[str] = join_parts

    def get_explained_query(self, table: str) -> str:
        select_sql: str = ",\n    ".join(self.select)
        query: str = f"{Context.SELECT}\n    {select_sql}\n{Context.FROM} '{table}'"
        if self.joins:
            formatted_joins: str = "\n".join(self.joins)
            query += f"\n{formatted_joins}"

        if self.where:
            formatted_where: str = f"\n    {KeyWord.AND} ".join(
                self.where.split(f" {KeyWord.AND} ")
            )
            query += f"\n{Context.WHERE}\n    {formatted_where}"

        if self.group:
            formatted_group: str = ",\n    ".join(self.group.split(", "))
            query += f"\n{Context.GROUP_BY}\n    {formatted_group}"

        if self.order:
            formatted_order: str = ",\n    ".join(self.order.split(", "))
            query += f"\n{Context.ORDER_BY}\n    {formatted_order}"

        return query

    def get_executable_query(
        self,
        table: str,
    ) -> str:
        select_sql: str = ", ".join(self.select)
        joins_sql: str = " ".join(self.joins) if self.joins else ""
        where_sql: str = f" {Context.WHERE} {self.where}" if self.where else ""
        group_sql: str = f" {Context.GROUP_BY} {self.group}" if self.group else ""
        order_sql: str = f" {Context.ORDER_BY} {self.order}" if self.order else ""

        return f"{Context.SELECT} {select_sql} {Context.FROM} '{table}' {joins_sql}{where_sql}{group_sql}{order_sql}"
