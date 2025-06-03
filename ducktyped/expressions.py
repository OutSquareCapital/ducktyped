from dataclasses import dataclass
from typing import Any, Self
from ducktyped.enums import KeyWord, Operators, Functions


class Expr:
    def to_sql(self) -> str:
        raise NotImplementedError()


@dataclass(slots=True)
class DuckType:
    nullable: bool = True

    def __str__(self) -> str:
        raise NotImplementedError

    def to_sql(self) -> str:
        return str(self)

    def cast_from(self, expr: Any) -> "CastExpr":
        if not isinstance(expr, Expr):
            expr = LiteralExpr(value=expr)
        return CastExpr(expr=expr, target_type=self)


@dataclass(slots=True)
class CastExpr(Expr):
    expr: Expr
    target_type: DuckType

    def to_sql(self) -> str:
        return f"{KeyWord.CAST}({self.expr.to_sql()} {KeyWord.AS} {self.target_type.to_sql()})"


@dataclass(slots=True)
class LiteralExpr(Expr):
    value: Any

    def to_sql(self) -> str:
        return str(self.value)


@dataclass(slots=True)
class BinaryOpExpr(Expr):
    left: Expr
    op: Operators
    right: Expr

    def to_sql(self) -> str:
        return f"({self.left.to_sql()} {self.op} {self.right.to_sql()})"


@dataclass(slots=True)
class Col(Expr):
    name: str

    def to_sql(self) -> str:
        return self.name

    def add(self, other: Self | float | int) -> BinaryOpExpr:
        return BinaryOpExpr(left=self, op=Operators.ADD, right=self._wrap(value=other))

    def gt(self, other: Self | float | int) -> BinaryOpExpr:
        return BinaryOpExpr(left=self, op=Operators.GT, right=self._wrap(value=other))

    def _wrap(self, value: Self | float | int) -> Expr:
        if isinstance(value, Col):
            return value
        return LiteralExpr(value=value)

    def rolling_mean(self, window: int) -> "RollingExprBuilder":
        return RollingExprBuilder(col=self, func=Functions.AVG, window=window)

    def rolling_median(self, window: int) -> "RollingExprBuilder":
        return RollingExprBuilder(col=self, func=Functions.MEDIAN, window=window)

    def rolling_sum(self, window: int) -> "RollingExprBuilder":
        return RollingExprBuilder(col=self, func=Functions.SUM, window=window)

    def rolling_max(self, window: int) -> "RollingExprBuilder":
        return RollingExprBuilder(col=self, func=Functions.MAX, window=window)

    def rolling_min(self, window: int) -> "RollingExprBuilder":
        return RollingExprBuilder(col=self, func=Functions.MIN, window=window)

    def rolling_kurtosis(self, window: int) -> "RollingExprBuilder":
        return RollingExprBuilder(col=self, func=Functions.KURTOSIS, window=window)

    def rolling_skew(self, window: int) -> "RollingExprBuilder":
        return RollingExprBuilder(col=self, func=Functions.SKEWNESS, window=window)

    def rolling_stddev(self, window: int) -> "RollingExprBuilder":
        return RollingExprBuilder(col=self, func=Functions.STDDEV_SAMP, window=window)

    def cast(self, type: "DuckType") -> CastExpr:
        return CastExpr(expr=self, target_type=type)


@dataclass(slots=True)
class AllExpr(Expr):
    def to_sql(self) -> str:
        return "*"


@dataclass(slots=True)
class WindowExpr(Expr):
    func: str
    col: Col
    window: int
    order_by: Col

    def to_sql(self) -> str:
        return (
            f"{self.func}({self.col.name}) {KeyWord.OVER} ({KeyWord.ORDER_BY} {self.order_by.name} "
            f"ROWS BETWEEN {self.window} PRECEDING AND CURRENT ROW)"
        )


@dataclass(slots=True)
class RollingExprBuilder:
    col: Col
    func: str
    window: int

    def over(self, order_by: Col) -> WindowExpr:
        return WindowExpr(
            func=self.func, col=self.col, window=self.window, order_by=order_by
        )
