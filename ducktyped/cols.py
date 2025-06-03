from dataclasses import dataclass
from ducktyped.enums import Functions, KeyWord
from ducktyped.expressions import Expr, AllExpr

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


class ColSelector:
    def __call__(self, name: str) -> Col:
        return Col(name=name)

    def __getattr__(self, name: str) -> Col:
        return self(name)


def all() -> AllExpr:
    return AllExpr()

col = ColSelector()
