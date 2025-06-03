from dataclasses import dataclass
from typing import Any

from ducktyped.enums import Functions, KeyWord, Operators
from ducktyped.types import DuckType


def _wrap_value(value: Any) -> "Expr":
    if isinstance(value, Expr):
        return value
    return LiteralExpr(_value=value)


class Expr:
    def to_sql(self) -> str:
        raise NotImplementedError()

    def cast(self, type: DuckType) -> "CastExpr":
        return CastExpr(_expr=self, _target_type=type)

    def add(self, other: "Expr | float | int | str") -> "BinaryOpExpr":
        return BinaryOpExpr(
            _left=self, _op=Operators.ADD, _right=_wrap_value(value=other)
        )

    def sub(self, other: "Expr | float | int | str") -> "BinaryOpExpr":
        return BinaryOpExpr(
            _left=self, _op=Operators.SUBTRACT, _right=_wrap_value(value=other)
        )

    def mul(self, other: "Expr | float | int | str") -> "BinaryOpExpr":
        return BinaryOpExpr(
            _left=self, _op=Operators.MULTIPLY, _right=_wrap_value(value=other)
        )

    def div(self, other: "Expr | float | int | str") -> "BinaryOpExpr":
        return BinaryOpExpr(
            _left=self, _op=Operators.DIVIDE, _right=_wrap_value(value=other)
        )

    def gt(self, other: "Expr | float | int | str") -> "BinaryOpExpr":
        return BinaryOpExpr(
            _left=self, _op=Operators.GT, _right=_wrap_value(value=other)
        )

    def lt(self, other: "Expr | float | int | str") -> "BinaryOpExpr":
        return BinaryOpExpr(
            _left=self, _op=Operators.LT, _right=_wrap_value(value=other)
        )

    def gte(self, other: "Expr | float | int | str") -> "BinaryOpExpr":
        return BinaryOpExpr(
            _left=self, _op=Operators.GTE, _right=_wrap_value(value=other)
        )

    def lte(self, other: "Expr | float | int | str") -> "BinaryOpExpr":
        return BinaryOpExpr(
            _left=self, _op=Operators.LTE, _right=_wrap_value(value=other)
        )

    def eq(self, other: "Expr | float | int | str") -> "BinaryOpExpr":
        return BinaryOpExpr(
            _left=self, _op=Operators.EQ, _right=_wrap_value(value=other)
        )

    def neq(self, other: "Expr | float | int | str") -> "BinaryOpExpr":
        return BinaryOpExpr(
            _left=self, _op=Operators.NEQ, _right=_wrap_value(value=other)
        )

    def sqrt(self) -> "UnaryFuncExpr":
        return UnaryFuncExpr(_func=Functions.SQRT, _expr=self)

    def abs(self) -> "UnaryFuncExpr":
        return UnaryFuncExpr(_func=Functions.ABS, _expr=self)

    def sign(self) -> "UnaryFuncExpr":
        return UnaryFuncExpr(_func=Functions.SIGN, _expr=self)

    def clip(self, min_val: int | float, max_val: int | float) -> "ClipExpr":
        return ClipExpr(_expr=self, _min_val=min_val, _max_val=max_val)


@dataclass(slots=True)
class CastExpr(Expr):
    _expr: Expr
    _target_type: DuckType

    def to_sql(self) -> str:
        return f"{KeyWord.CAST}({self._expr.to_sql()} {KeyWord.AS} {self._target_type.to_sql()})"


@dataclass(slots=True)
class LiteralExpr(Expr):
    _value: Any

    def to_sql(self) -> str:
        if isinstance(self._value, str):
            return f"'{self._value}'"
        return str(self._value)


@dataclass(slots=True)
class UnaryFuncExpr(Expr):
    _func: str
    _expr: Expr

    def to_sql(self) -> str:
        return f"{self._func}({self._expr.to_sql()})"


@dataclass(slots=True)
class BinaryOpExpr(Expr):
    _left: Expr
    _op: Operators
    _right: Expr

    def to_sql(self) -> str:
        return f"({self._left.to_sql()} {self._op} {self._right.to_sql()})"


@dataclass(slots=True)
class ClipExpr(Expr):
    _expr: Expr
    _min_val: float | int
    _max_val: float | int

    def to_sql(self) -> str:
        min_expr: str = _wrap_value(value=self._min_val).to_sql()
        max_expr: str = _wrap_value(value=self._max_val).to_sql()
        return f"{KeyWord.LEAST}({KeyWord.GREATEST}({self._expr.to_sql()}, {min_expr}), {max_expr})"


@dataclass(slots=True)
class Col(Expr):
    name: str

    def to_sql(self) -> str:
        return self.name

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
            f"ROWS {KeyWord.BETWEEN} {self.window} {KeyWord.PRECEDING} {KeyWord.AND} {KeyWord.CURRENT} ROW)"
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
