from dataclasses import dataclass

from ducktyped.enums import Context, KeyWord
from ducktyped.expressions import Expr


@dataclass(slots=True)
class SQLRaw:
    select: list[str]
    where: str
    group: str
    order: str


def get_sql_raw(
    selected: list[Expr],
    where_clause: list[Expr],
    group_by: list[Expr],
    order_by: list[tuple[Expr, bool]],
) -> SQLRaw:
    select_parts: list[str] = [col.to_sql() for col in selected]

    where_parts = ""
    if where_clause:
        where_conditions: list[str] = [cond.to_sql() for cond in where_clause]
        where_parts: str = f" {KeyWord.AND} ".join(where_conditions)

    group_parts = ""
    if group_by:
        group_parts: str = ", ".join(col.to_sql() for col in group_by)

    order_parts: list[str] = []
    if order_by:
        for expr, is_asc in order_by:
            direction: KeyWord | KeyWord = KeyWord.ASC if is_asc else KeyWord.DESC
            order_parts.append(f"{expr.to_sql()} {direction}")
    order_sql: str = ", ".join(order_parts)

    return SQLRaw(
        select=select_parts,
        where=where_parts,
        group=group_parts,
        order=order_sql,
    )


def get_explained_query(sql_raw: SQLRaw, table: str) -> str:
    select_sql: str = ",\n    ".join(sql_raw.select)
    query: str = f"{Context.SELECT}\n    {select_sql}\n{Context.FROM} '{table}'"

    if sql_raw.where:
        formatted_where: str = f"\n    {KeyWord.AND} ".join(
            sql_raw.where.split(f" {KeyWord.AND} ")
        )
        query += f"\n{Context.WHERE}\n    {formatted_where}"

    if sql_raw.group:
        formatted_group: str = ",\n    ".join(sql_raw.group.split(", "))
        query += f"\n{Context.GROUP_BY}\n    {formatted_group}"

    if sql_raw.order:
        formatted_order: str = ",\n    ".join(sql_raw.order.split(", "))
        query += f"\n{Context.ORDER_BY}\n    {formatted_order}"

    return query


def get_executable_query(
    sql_raw: SQLRaw,
    table: str,
) -> str:
    select_sql: str = ", ".join(sql_raw.select)
    where_sql: str = f" {Context.WHERE} {sql_raw.where}" if sql_raw.where else ""
    group_sql: str = f" {Context.GROUP_BY} {sql_raw.group}" if sql_raw.group else ""
    order_sql: str = f" {Context.ORDER_BY} {sql_raw.order}" if sql_raw.order else ""

    return f"{Context.SELECT} {select_sql} {Context.FROM} '{table}'{where_sql}{group_sql}{order_sql}"
