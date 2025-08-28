import operator
from collections.abc import Iterable
from dataclasses import dataclass
from typing import Any, Callable, Self

from .constants import ColumnType, Schema


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

    def __eq__(self, other: object) -> Self:  # type:ignore
        return BinaryOperatorColumn(self, other, operator.eq)  # type:ignore

    def execute(self, row: dict[str, Any]) -> Any:
        return row[self.name]

    def execute_row(self, row: tuple[Any, ...]) -> Any:
        raise NotImplementedError

    def alias(self, name: str):
        return AliasColumn(self, name)

    @property
    def all_nested_columns(self) -> Iterable["Col"]:
        yield self

    def infer_type(self, schema: Schema) -> ColumnType:
        col_type = next(
            (type for col_name, type in schema if col_name == self.name), None,
        )
        if col_type is None:
            raise ValueError(f"Column {self.name} not found in schema {schema}")
        return col_type

    def schema_executor(self, schema: Schema) -> "Col":
        col_pos = next(
            (i for i, (col_name, _) in enumerate(schema) if col_name == self.name), None,
        )
        if col_pos is None:
            raise ValueError(f"Column {self.name} not found in schema {schema}")
        return SchemaCol(self.name, col_pos)

    def __str__(self) -> str:
        return self.name


@dataclass
class SchemaCol(Col):
    def __init__(self, name: str, col_pos: int):
        super().__init__(name)
        self.col_pos = col_pos

    def execute(self, row: dict[str, Any]) -> Any:
        raise NotImplementedError

    def execute_row(self, row: tuple[Any, ...]) -> Any:
        return row[self.col_pos]


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

    def execute_row(self, row: tuple[Any, ...]) -> Any:
        return self.original_col.execute_row(row)

    def __str__(self) -> str:
        return f"({self.original_col}) AS {self.name}"

    def infer_type(self, schema: Schema) -> ColumnType:
        return self.original_col.infer_type(schema)

    def schema_executor(self, schema: Schema) -> "Col":
        return self.original_col.schema_executor(schema)


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

    def execute_row(self, row: tuple[Any, ...]) -> Any:
        return self.operator(
            self.left_side.execute_row(row), self.right_side.execute_row(row),
        )

    @property
    def all_nested_columns(self) -> Iterable[Col]:
        yield self
        yield from self.left_side.all_nested_columns
        yield from self.right_side.all_nested_columns

    def __str__(self) -> str:
        return f"{self.left_side} {OP_SYMBOLS[self.operator]} {self.right_side}"

    def infer_type(self, schema: Schema) -> ColumnType:
        left_type = self.left_side.infer_type(schema)
        right_type = self.right_side.infer_type(schema)
        if left_type != right_type:
            raise TypeError(
                f"Type mismatch in binary operation: {left_type} {self.operator} {right_type}",
            )
        return left_type

    def schema_executor(self, schema: Schema) -> "Col":
        return BinaryOperatorColumn(
            self.left_side.schema_executor(schema),
            self.right_side.schema_executor(schema),
            self.operator,
        )


@dataclass
class Lit(Col):
    value: Any

    def __post_init__(self):
        self.name = f"lit_{self.value}"

    def execute(self, row: dict[str, Any]) -> Any:
        return self.value

    def execute_row(self, row: tuple[Any, ...]) -> Any:
        return self.value

    @property
    def all_nested_columns(self) -> Iterable[Col]:
        yield from []

    def __str__(self) -> str:
        return str(self.value)

    def infer_type(self, schema: Schema) -> ColumnType:
        return ColumnType.of(self.value)

    def schema_executor(self, schema: Schema) -> "Col":
        return self
