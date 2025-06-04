from enum import StrEnum, auto
from typing import Literal

class Context(StrEnum):
    FROM = "FROM"
    OVER = "OVER"
    PARTITION_BY = "PARTITION BY"
    ORDER_BY = "ORDER BY"
    SELECT = "SELECT"
    WHERE = "WHERE"
    GROUP_BY = "GROUP BY"

JoinTypes = Literal["INNER", "LEFT", "RIGHT", "FULL"]

class KeyWord(StrEnum):
    AND = "AND"
    CAST = "CAST"
    AS = "AS"
    OR = "OR"
    LEAST = "LEAST"
    GREATEST = "GREATEST"
    PRECEDING = "PRECEDING"
    CURRENT = "CURRENT"
    BETWEEN = "BETWEEN"
    IN = "IN"
    ASC = "ASC"
    DESC = "DESC"


class Operators(StrEnum):
    ADD = "+"
    SUBTRACT = "-"
    MULTIPLY = "*"
    DIVIDE = "/"
    GT = ">"
    LT = "<"
    GTE = ">="
    LTE = "<="
    EQ = "="
    NEQ = "!="

class Types(StrEnum):
    VARCHAR = "VARCHAR"
    FLOAT = "FLOAT"
    DOUBLE = "DOUBLE"
    TIMESTAMP = "TIMESTAMP"
    BOOLEAN = "BOOLEAN"
    DECIMAL = "DECIMAL"
    DATE = "DATE"
    TIME = "TIME"
    ENUM = "ENUM"
    SMALLINT = "SMALLINT"
    TINYINT = "TINYINT"
    INTEGER = "INTEGER"
    BIGINT = "BIGINT"
    USMALLINT = "USMALLINT"
    UTINYINT = "UTINYINT"
    UINTEGER = "UINTEGER"
    UBIGINT = "UBIGINT"


class Functions(StrEnum):
    AVG = auto()
    MEDIAN = auto()
    SUM = auto()
    COUNT = auto()
    MAX = auto()
    MIN = auto()
    KURTOSIS = auto()
    SKEWNESS = auto()
    STDDEV_SAMP = auto()
    ABS = auto()
    SIGN = auto()
    SQRT = auto()
    FIRST = auto()
    LAST = auto()
