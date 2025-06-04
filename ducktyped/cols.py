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
    partition_by: list[Col]
    order_by: Col | None = None

    def to_sql(self) -> str:
        partition_clause: str = ""
        if self.partition_by:
            partition_clause = f"{Context.PARTITION_BY} " + ", ".join(c.name for c in self.partition_by)
        order_by_clause: str = ""
        if self.order_by:
            order_by_clause = f"{Context.ORDER_BY} {self.order_by.name}"
        clauses: list[str] = []

        if partition_clause:
            clauses.append(partition_clause)
        if order_by_clause:
            clauses.append(order_by_clause)
        window_clause: str = " ".join(clauses)
        return (
            f"{self.func}({self.col.name}) {Context.OVER} ("
            f"{window_clause} ROWS {KeyWord.BETWEEN} {self.window_size} {KeyWord.PRECEDING} {KeyWord.AND} {KeyWord.CURRENT} ROW)"
        )

@dataclass(slots=True)
class RollingExprBuilder:
    table: str | None
    _col: Col
    _func: str
    _window_size: int

    def over(self, *partition_by: Col, order_by: Col|None = None) -> WindowExpr:
        partition: list[Col] = []
        for col in partition_by:
            partition.append(col)
        return WindowExpr(
            table=self.table,
            func=self._func,
            col=self._col,
            window_size=self._window_size,
            order_by=order_by,
            partition_by=partition,
        )