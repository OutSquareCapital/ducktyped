from dataclasses import dataclass

from ducktyped.enums import Types


@dataclass(slots=True)
class DuckType:
    def __str__(self) -> str:
        raise NotImplementedError

    def to_sql(self) -> str:
        return str(self)


@dataclass(slots=True)
class String(DuckType):
    length: int | None = None

    def __str__(self) -> str:
        if self.length is not None:
            return f"{Types.VARCHAR}({self.length})"
        return Types.VARCHAR


@dataclass(slots=True)
class Int8(DuckType):
    def __str__(self) -> str:
        return Types.TINYINT


@dataclass(slots=True)
class Int16(DuckType):
    def __str__(self) -> str:
        return Types.SMALLINT


@dataclass(slots=True)
class Int32(DuckType):
    def __str__(self) -> str:
        return Types.INTEGER


@dataclass(slots=True)
class Int64(DuckType):
    def __str__(self) -> str:
        return Types.BIGINT


@dataclass(slots=True)
class UInt8(DuckType):
    def __str__(self) -> str:
        return Types.UTINYINT


@dataclass(slots=True)
class UInt16(DuckType):
    def __str__(self) -> str:
        return Types.USMALLINT


@dataclass(slots=True)
class UInt32(DuckType):
    def __str__(self) -> str:
        return Types.UINTEGER


@dataclass(slots=True)
class UInt64(DuckType):
    def __str__(self) -> str:
        return Types.UBIGINT


@dataclass(slots=True)
class Float32(DuckType):
    def __str__(self) -> str:
        return Types.FLOAT


@dataclass(slots=True)
class Float64(DuckType):
    def __str__(self) -> str:
        return Types.DOUBLE


@dataclass(slots=True)
class Boolean(DuckType):
    def __str__(self) -> str:
        return Types.BOOLEAN


@dataclass(slots=True)
class Date(DuckType):
    def __str__(self) -> str:
        return Types.DATE


@dataclass(slots=True)
class Datetime(DuckType):
    def __str__(self) -> str:
        return Types.TIME


@dataclass(slots=True)
class Enum(DuckType):
    categories: list[str]

    def __str__(self) -> str:
        quoted_values: list[str] = [f"'{val}'" for val in self.categories]
        values_str: str = ", ".join(quoted_values)
        return f"{Types.ENUM}({values_str})"
