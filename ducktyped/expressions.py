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

    def alias(self, name: str) -> "AliasExpr":
        return AliasExpr(_expr=self, _alias=name)

    def cast(self, dtype: DuckType) -> "CastExpr":
        return CastExpr(_expr=self, _dtype=dtype)

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

    def sum(self) -> "AggExpr":
        return AggExpr(_func=Functions.SUM, _expr=self)

    def mean(self) -> "AggExpr":
        return AggExpr(_func=Functions.AVG, _expr=self)

    def count(self) -> "AggExpr":
        return AggExpr(_func=Functions.COUNT, _expr=self)

    def max(self) -> "AggExpr":
        return AggExpr(_func=Functions.MAX, _expr=self)

    def min(self) -> "AggExpr":
        return AggExpr(_func=Functions.MIN, _expr=self)

    def first(self) -> "AggExpr":
        return AggExpr(_func=Functions.FIRST, _expr=self)

    def last(self) -> "AggExpr":
        return AggExpr(_func=Functions.LAST, _expr=self)


@dataclass(slots=True)
class AliasExpr(Expr):
    _expr: Expr
    _alias: str

    def to_sql(self) -> str:
        return f"{self._expr.to_sql()} AS {self._alias}"


@dataclass(slots=True)
class CastExpr(Expr):
    _expr: Expr
    _dtype: DuckType

    def to_sql(self) -> str:
        return f"{KeyWord.CAST}({self._expr.to_sql()} {KeyWord.AS} {self._dtype.to_sql()})"


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
        return RollingExprBuilder(_col=self, _func=Functions.AVG, _window=window)

    def rolling_median(self, window: int) -> "RollingExprBuilder":
        return RollingExprBuilder(_col=self, _func=Functions.MEDIAN, _window=window)

    def rolling_sum(self, window: int) -> "RollingExprBuilder":
        return RollingExprBuilder(_col=self, _func=Functions.SUM, _window=window)

    def rolling_max(self, window: int) -> "RollingExprBuilder":
        return RollingExprBuilder(_col=self, _func=Functions.MAX, _window=window)

    def rolling_min(self, window: int) -> "RollingExprBuilder":
        return RollingExprBuilder(_col=self, _func=Functions.MIN, _window=window)

    def rolling_kurtosis(self, window: int) -> "RollingExprBuilder":
        return RollingExprBuilder(_col=self, _func=Functions.KURTOSIS, _window=window)

    def rolling_skew(self, window: int) -> "RollingExprBuilder":
        return RollingExprBuilder(_col=self, _func=Functions.SKEWNESS, _window=window)

    def rolling_stdev(self, window: int) -> "RollingExprBuilder":
        return RollingExprBuilder(
            _col=self, _func=Functions.STDDEV_SAMP, _window=window
        )


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
    _col: Col
    _func: str
    _window: int

    def over(self, order_by: Col) -> WindowExpr:
        return WindowExpr(
            func=self._func, col=self._col, window=self._window, order_by=order_by
        )


@dataclass(slots=True)
class AggExpr(Expr):
    _func: str
    _expr: Expr

    def to_sql(self) -> str:
        return f"{self._func}({self._expr.to_sql()})"
