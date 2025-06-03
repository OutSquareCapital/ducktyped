from dataclasses import dataclass
from ducktyped.enums import Types
from ducktyped.expressions import DuckType


@dataclass(slots=True)
class Varchar(DuckType):
    length: int | None = None

    def __str__(self) -> str:
        if self.length is not None:
            return f"{Types.VARCHAR}({self.length})"
        return Types.VARCHAR


@dataclass(slots=True)
class Integer(DuckType):
    def __str__(self) -> str:
        return Types.INTEGER


@dataclass(slots=True)
class Float(DuckType):
    def __str__(self) -> str:
        return Types.FLOAT


@dataclass(slots=True)
class Boolean(DuckType):
    def __str__(self) -> str:
        return Types.BOOLEAN


@dataclass(slots=True)
class Date(DuckType):
    def __str__(self) -> str:
        return Types.DATE