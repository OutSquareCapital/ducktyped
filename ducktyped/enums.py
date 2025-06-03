from enum import StrEnum, auto


class KeyWord(StrEnum):
    SELECT = "SELECT"
    FROM = "FROM"
    WHERE = "WHERE"
    OVER = "OVER"
    ORDER_BY = "ORDER BY"
    AND = "AND"
    CAST = "CAST"
    AS = "AS"
    OR = "OR"
    LEAST = "LEAST"
    GREATEST = "GREATEST"
    PRECEDING = "PRECEDING"
    CURRENT = "CURRENT"
    BETWEEN = "BETWEEN"


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
    INTEGER = "INTEGER"
    FLOAT = "FLOAT"
    TIMESTAMP = "TIMESTAMP"
    BOOLEAN = "BOOLEAN"
    DECIMAL = "DECIMAL"
    DATE = "DATE"


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