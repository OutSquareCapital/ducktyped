from enum import StrEnum, auto


class Context(StrEnum):
    FROM = "FROM"
    OVER = "OVER"
    ORDER_BY = "ORDER BY"
    SELECT = "SELECT"
    WHERE = "WHERE"
    GROUP_BY = "GROUP BY"


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
    ENUM = "ENUM"


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
