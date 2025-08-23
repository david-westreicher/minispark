from dataclasses import dataclass
import operator
from typing import Any, Iterable, Callable, Self

from src.io import ColumnType


class Col:
    def __init__(self, name: str):
        self.name = name

    def __lt__(self, other: Any):
        return BinaryOperatorColumn(self, other, operator.lt)

    def __le__(self, other: Any):
        return BinaryOperatorColumn(self, other, operator.le)

    def __gt__(self, other: Any):
        return BinaryOperatorColumn(self, other, operator.gt)

    def __ge__(self, other: Any):
        return BinaryOperatorColumn(self, other, operator.ge)

    def __mul__(self, other: Any):
        return BinaryOperatorColumn(self, other, operator.mul)

    def __add__(self, other: Any):
        return BinaryOperatorColumn(self, other, operator.add)

    def __sub__(self, other: Any):
        return BinaryOperatorColumn(self, other, operator.sub)

    def __floordiv__(self, other: Any):
        return BinaryOperatorColumn(self, other, operator.floordiv)

    def __truediv__(self, other: Any):
        return BinaryOperatorColumn(self, other, operator.truediv)

    def __eq__(self, other: Any) -> Self:  # type:ignore
        return BinaryOperatorColumn(self, other, operator.eq)  # type:ignore

    def execute(self, row: dict[str, Any]) -> Any:
        return row[self.name]

    def alias(self, name: str):
        return AliasColumn(self, name)

    @property
    def all_nested_columns(self) -> Iterable["Col"]:
        yield self

    @property
    def type(self) -> ColumnType:
        return ColumnType.UNKNOWN

    def __str__(self) -> str:
        return self.name


@dataclass
class AliasColumn(Col):
    original_col: Col
    name: str

    @property
    def all_nested_columns(self) -> Iterable[Col]:
        yield self
        yield from self.original_col.all_nested_columns

    def execute(self, row: dict[str, Any]) -> Any:
        return self.original_col.execute(row)

    def __str__(self) -> str:
        return f"({self.original_col}) AS {self.name}"


OP_SYMBOLS = {
    operator.add: "+",
    operator.sub: "-",
    operator.mul: "*",
    operator.truediv: "/",
    operator.floordiv: "//",
    operator.mod: "%",
    operator.pow: "**",
    operator.eq: "==",
    operator.ne: "!=",
    operator.lt: "<",
    operator.le: "<=",
    operator.gt: ">",
    operator.ge: ">=",
}


@dataclass
class BinaryOperatorColumn(Col):
    left_side: Col
    right_side: Col
    operator: Callable[[Any, Any], Any]

    def __post_init__(self):
        if not isinstance(self.left_side, Col):
            self.left_side = Lit(self.left_side)
        if not isinstance(self.right_side, Col):
            self.right_side = Lit(self.right_side)
        self.name = (
            f"{self.left_side.name}_{self.operator.__name__}_{self.right_side.name}"
        )

    def execute(self, row: dict[str, Any]) -> Any:
        return self.operator(self.left_side.execute(row), self.right_side.execute(row))

    @property
    def all_nested_columns(self) -> Iterable[Col]:
        yield self
        yield from self.left_side.all_nested_columns
        yield from self.right_side.all_nested_columns

    def __str__(self) -> str:
        return f"{self.left_side} {OP_SYMBOLS[self.operator]} {self.right_side}"


@dataclass
class Lit(Col):
    value: Any

    def __post_init__(self):
        self.name = f"lit_{self.value}"

    def execute(self, row: dict[str, Any]) -> Any:
        return self.value

    @property
    def all_nested_columns(self) -> Iterable[Col]:
        yield from []

    def __str__(self) -> str:
        return str(self.value)
