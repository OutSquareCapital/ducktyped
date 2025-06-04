use modern python syntax -> no import typing List, use list instead, no Union, use | instead, no Optional, use ...|None. this include EVERY outdated syntax. for example no T = TypeVar, use [T: type] instead.
For predefined values, use Literal if it's arguments for a function/method, or Enums, StringEnums for other use cases.
Use auto with stringEnums if lowercase is applicable.
if dataclasses are used, specifiy slots=True.
if complex init method are needed, prefer to use plain classes, and if possible, specifiy __slots__.
if constant collections of attribute, but no method, and no predefined values, use NamedTuple, unless methods are needed, in which case use dataclass with frozen=True (and slots=True of course).
if dicts can't be avoided, use TypedDict (unless keys are unknown, in which case use dict[str, wathever types are needed])
by default, consider all methods, attributes and functions as private, unless specified otherwise.
avoid default arguments, unless specified otherwise.
avoid using None as a default value for arguments, unless specified otherwise.
no docstrings, no comments, unless specified otherwise.
type hints should be used for all function parameters, return types, every variable, etc...
add only specified functions, no extra "for convenience" functions