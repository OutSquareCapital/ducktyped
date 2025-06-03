from dataclasses import dataclass
from typing import Any, Self
from ducktyped.types import KeyWord, Operators


class Expr:
    def to_sql(self) -> str:
        raise NotImplementedError()


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
        return RollingExprBuilder(col=self, func="avg", window=window)
    
    def rolling_median(self, window: int) -> "RollingExprBuilder":
        return RollingExprBuilder(col=self, func="median", window=window)

    def rolling_sum(self, window: int) -> "RollingExprBuilder":
        return RollingExprBuilder(col=self, func="sum", window=window)
    
    def rolling_max(self, window: int) -> "RollingExprBuilder":
        return RollingExprBuilder(col=self, func="max", window=window)
    
    def rolling_min(self, window: int) -> "RollingExprBuilder":
        return RollingExprBuilder(col=self, func="min", window=window)
    
    def rolling_kurtosis(self, window: int) -> "RollingExprBuilder":
        return RollingExprBuilder(col=self, func="kurtosis", window=window)

    def rolling_skew(self, window: int) -> "RollingExprBuilder":
        return RollingExprBuilder(col=self, func="skewness", window=window)
    
    def rolling_stddev(self, window: int) -> "RollingExprBuilder":
        return RollingExprBuilder(col=self, func="stddev_samp", window=window)


@dataclass
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
