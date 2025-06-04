from dataclasses import dataclass
from ducktyped.enums import Functions, KeyWord, Context
from ducktyped.expressions import Expr


@dataclass(slots=True)
class Col(Expr):
    _name: str
    table: str | None = None

    def to_sql(self) -> str:
        return self.name

    def rolling_mean(self, window_size: int) -> "RollingExprBuilder":
        return RollingExprBuilder(
            table=self.table, _col=self, _func=Functions.AVG, _window_size=window_size
        )

    def rolling_median(self, window_size: int) -> "RollingExprBuilder":
        return RollingExprBuilder(
            table=self.table,
            _col=self,
            _func=Functions.MEDIAN,
            _window_size=window_size,
        )

    def rolling_sum(self, window_size: int) -> "RollingExprBuilder":
        return RollingExprBuilder(
            table=self.table, _col=self, _func=Functions.SUM, _window_size=window_size
        )

    def rolling_max(self, window_size: int) -> "RollingExprBuilder":
        return RollingExprBuilder(
            table=self.table, _col=self, _func=Functions.MAX, _window_size=window_size
        )

    def rolling_min(self, window_size: int) -> "RollingExprBuilder":
        return RollingExprBuilder(
            table=self.table, _col=self, _func=Functions.MIN, _window_size=window_size
        )

    def rolling_kurtosis(self, window_size: int) -> "RollingExprBuilder":
        return RollingExprBuilder(
            table=self.table,
            _col=self,
            _func=Functions.KURTOSIS,
            _window_size=window_size,
        )

    def rolling_skew(self, window_size: int) -> "RollingExprBuilder":
        return RollingExprBuilder(
            table=self.table,
            _col=self,
            _func=Functions.SKEWNESS,
            _window_size=window_size,
        )

    def rolling_stdev(self, window_size: int) -> "RollingExprBuilder":
        return RollingExprBuilder(
            table=self.table,
            _col=self,
            _func=Functions.STDDEV_SAMP,
            _window_size=window_size,
        )


@dataclass(slots=True)
class WindowExpr(Expr):
    table: str | None
    func: str
    col: Col
    window_size: int
    order_by: list[Col]

    def to_sql(self) -> str:
        order_by_clause: str = ", ".join(col.name for col in self.order_by)
        return (
            f"{self.func}({self.col.name}) {Context.OVER} ({Context.ORDER_BY} {order_by_clause} "
            f"ROWS {KeyWord.BETWEEN} {self.window_size} {KeyWord.PRECEDING} {KeyWord.AND} {KeyWord.CURRENT} ROW)"
        )


@dataclass(slots=True)
class RollingExprBuilder:
    table: str | None
    _col: Col
    _func: str
    _window_size: int

    def over(self, *order_by: Col) -> WindowExpr:
        cols: list[Col] = []
        for c in order_by:
            cols.append(c)
        return WindowExpr(
            table=self.table,
            func=self._func,
            col=self._col,
            window_size=self._window_size,
            order_by=cols,
        )