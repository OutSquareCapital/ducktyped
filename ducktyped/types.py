from enum import StrEnum


class KeyWord(StrEnum):
    SELECT = "SELECT"
    FROM = "FROM"
    WHERE = "WHERE"
    OVER = "OVER"
    ORDER_BY = "ORDER BY"
    AND = "AND"


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


class DuckType:
    def __str__(self) -> str: ...


class Varchar(DuckType): ...


class Integer(DuckType): ...


class Float(DuckType): ...


class Timestamp(DuckType): ...
