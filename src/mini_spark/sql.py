from __future__ import annotations

import operator
from dataclasses import dataclass
from typing import TYPE_CHECKING, Any, Literal, cast

from .constants import ColumnType, ColumnTypePython, Schema

if TYPE_CHECKING:
    from collections.abc import Callable, Iterable


class Col:
    def __init__(self, name: str) -> None:
        self.name = name

    def __lt__(self, other: Col | ColumnTypePython) -> Col:
        return BinaryOperatorColumn(self, cast("Col", other), operator.lt)

    def __le__(self, other: Col | ColumnTypePython) -> Col:
        return BinaryOperatorColumn(self, cast("Col", other), operator.le)

    def __gt__(self, other: Col | ColumnTypePython) -> Col:
        return BinaryOperatorColumn(self, cast("Col", other), operator.gt)

    def __ge__(self, other: Col | ColumnTypePython) -> Col:
        return BinaryOperatorColumn(self, cast("Col", other), operator.ge)

    def __mul__(self, other: Col | ColumnTypePython) -> Col:
        return BinaryOperatorColumn(self, cast("Col", other), operator.mul)

    def __add__(self, other: Col | ColumnTypePython) -> Col:
        return BinaryOperatorColumn(self, cast("Col", other), operator.add)

    def __and__(self, other: Col | ColumnTypePython) -> Col:
        return BinaryOperatorColumn(self, cast("Col", other), operator.and_)

    def __or__(self, other: Col | ColumnTypePython) -> Col:
        return BinaryOperatorColumn(self, cast("Col", other), operator.or_)

    def __invert__(self) -> Col:
        raise NotImplementedError

    def __sub__(self, other: Col | ColumnTypePython) -> Col:
        return BinaryOperatorColumn(self, cast("Col", other), operator.sub)

    def __floordiv__(self, other: Col | ColumnTypePython) -> Col:
        return BinaryOperatorColumn(self, cast("Col", other), operator.floordiv)

    def __truediv__(self, other: Col | ColumnTypePython) -> Col:
        return BinaryOperatorColumn(self, cast("Col", other), operator.truediv)

    def __mod__(self, other: Col | ColumnTypePython) -> Col:
        return BinaryOperatorColumn(self, cast("Col", other), operator.mod)

    def __eq__(self, other: Col | ColumnTypePython) -> Col:  # type:ignore[override]
        return BinaryOperatorColumn(self, cast("Col", other), operator.eq)

    def __ne__(self, other: Col | ColumnTypePython) -> Col:  # type:ignore[override]
        return BinaryOperatorColumn(self, cast("Col", other), operator.ne)

    def __hash__(self) -> int:
        return hash((self.__class__, self.name))

    # TODO(david): can be removed after refactoring join
    def execute(self, row: dict[str, ColumnTypePython]) -> ColumnTypePython:
        return row[self.name]

    def execute_row(self, row: tuple[ColumnTypePython, ...]) -> ColumnTypePython:
        raise NotImplementedError

    def alias(self, name: str) -> Col:
        return AliasColumn(self, name)

    @property
    def all_nested_columns(self) -> Iterable[Col]:
        yield self

    def infer_type(self, schema: Schema) -> ColumnType:
        col_type = next(
            (col_type for col_name, col_type in schema if col_name == self.name),
            None,
        )
        if col_type is None:
            raise ValueError(f'Column "{self.name}" not found in schema {schema}')
        return col_type

    def schema_executor(self, schema: Schema) -> Col:
        col_pos = next(
            (i for i, (col_name, _) in enumerate(schema) if col_name == self.name),
            None,
        )
        if col_pos is None:
            raise ValueError(f"Column {self.name} not found in schema {schema}")
        return SchemaCol(self.name, col_pos)

    def zig_code_representation(self, schema: Schema) -> str:  # noqa: ARG002
        return self.name

    def __str__(self) -> str:
        return self.name

    def __repr__(self) -> str:
        return self.__str__()


@dataclass
class SchemaCol(Col):
    def __init__(self, name: str, col_pos: int) -> None:
        super().__init__(name)
        self.col_pos = col_pos

    def __hash__(self) -> int:
        return super().__hash__()

    def __eq__(self, other: Col | ColumnTypePython) -> Col:  # type:ignore[override]
        return super().__eq__(other)

    def execute(self, row: dict[str, Any]) -> Any:  # noqa: ANN401
        raise NotImplementedError

    def execute_row(self, row: tuple[Any, ...]) -> Any:  # noqa: ANN401
        return row[self.col_pos]


@dataclass
class AliasColumn(Col):
    original_col: Col
    name: str

    def __eq__(self, other: Col | ColumnTypePython) -> Col:  # type:ignore[override]
        return super().__eq__(other)

    def __hash__(self) -> int:
        return hash((self.__class__, hash(self.original_col), self.name))

    @property
    def all_nested_columns(self) -> Iterable[Col]:
        yield self
        yield from self.original_col.all_nested_columns

    def execute(self, row: dict[str, Any]) -> Any:  # noqa: ANN401
        return self.original_col.execute(row)

    def execute_row(self, row: tuple[Any, ...]) -> Any:  # noqa: ANN401
        return self.original_col.execute_row(row)

    def __str__(self) -> str:
        return f"({self.original_col}) AS {self.name}"

    def __repr__(self) -> str:
        return self.__str__()

    def infer_type(self, schema: Schema) -> ColumnType:
        return self.original_col.infer_type(schema)

    def schema_executor(self, schema: Schema) -> Col:
        return self.original_col.schema_executor(schema)

    def zig_code_representation(self, schema: Schema) -> str:
        return self.original_col.zig_code_representation(schema)


BINOP_SYMBOLS: dict[Callable[[Col, Col], Col], str] = {
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
    operator.and_: "and",
    operator.or_: "or",
}
UNOP_SYMBOLS: dict[Callable[[Col], Col], str] = {
    operator.invert: "not",
}


@dataclass
class BinaryOperatorColumn(Col):
    left_side: Col
    right_side: Col
    operator: Callable[[Any, Any], Any]

    def __eq__(self, other: Col | ColumnTypePython) -> Col:  # type:ignore[override]
        return super().__eq__(other)

    def __hash__(self) -> int:
        return hash((self.__class__, hash(self.left_side), hash(self.right_side), self.operator))

    def __post_init__(self) -> None:
        if not isinstance(self.left_side, Col):
            self.left_side = Lit(self.left_side)
        if not isinstance(self.right_side, Col):
            self.right_side = Lit(self.right_side)
        self.name = f"{self.left_side.name}_{self.operator.__name__}_{self.right_side.name}"

    def execute(self, row: dict[str, Any]) -> Any:  # noqa: ANN401
        return self.operator(self.left_side.execute(row), self.right_side.execute(row))

    def execute_row(self, row: tuple[Any, ...]) -> Any:  # noqa: ANN401
        return self.operator(
            self.left_side.execute_row(row),
            self.right_side.execute_row(row),
        )

    @property
    def all_nested_columns(self) -> Iterable[Col]:
        yield self
        yield from self.left_side.all_nested_columns
        yield from self.right_side.all_nested_columns

    def __str__(self) -> str:
        return f"({self.left_side}) {BINOP_SYMBOLS[self.operator]} ({self.right_side})"

    def infer_type(self, schema: Schema) -> ColumnType:
        left_type = self.left_side.infer_type(schema)
        right_type = self.right_side.infer_type(schema)
        if left_type != right_type:
            raise TypeError(
                f"Type mismatch in binary operation: {left_type} {self.operator} {right_type}",
            )
        return left_type

    def schema_executor(self, schema: Schema) -> Col:
        return BinaryOperatorColumn(
            self.left_side.schema_executor(schema),
            self.right_side.schema_executor(schema),
            self.operator,
        )

    def zig_code_representation(self, schema: Schema) -> str:
        if self.operator == operator.eq:
            left_type = self.left_side.infer_type(schema)
            right_type = self.left_side.infer_type(schema)
            assert left_type == right_type
            if left_type == ColumnType.STRING:
                return (
                    f"std.mem.eql(u8, "
                    f"({self.left_side.zig_code_representation(schema)}),"
                    f" ({self.right_side.zig_code_representation(schema)}))"
                )
        if self.operator == operator.mod:
            return (
                "@rem("
                f"({self.left_side.zig_code_representation(schema)}),"
                f" ({self.right_side.zig_code_representation(schema)}))"
            )
        return (
            f"({self.left_side.zig_code_representation(schema)}) {BINOP_SYMBOLS[self.operator]}"
            f" ({self.right_side.zig_code_representation(schema)})"
        )


@dataclass
class Lit(Col):
    value: ColumnTypePython

    def __eq__(self, other: Col | ColumnTypePython) -> Col:  # type:ignore[override]
        return super().__eq__(other)

    def __hash__(self) -> int:
        return hash((self.__class__, self.value))

    def __post_init__(self) -> None:
        self.name = f"lit_{self.value}"

    def execute(self, row: dict[str, Any]) -> Any:  # noqa: ANN401, ARG002
        return self.value

    def execute_row(self, row: tuple[Any, ...]) -> Any:  # noqa: ANN401, ARG002
        return self.value

    @property
    def all_nested_columns(self) -> Iterable[Col]:
        yield from []

    def __str__(self) -> str:
        return str(self.value)

    def infer_type(self, schema: Schema) -> ColumnType:  # noqa: ARG002
        return ColumnType.of(self.value)

    def schema_executor(self, schema: Schema) -> Col:  # noqa: ARG002
        return self

    def zig_code_representation(self, schema: Schema) -> str:  # noqa: ARG002
        if type(self.value) is int:
            return str(self.value)
        if type(self.value) is str:
            return f'"{self.value}"'
        raise NotImplementedError(f"Zig code generation for literal {self.value} not implemented")


AggregationType = Literal["sum", "min", "max"]


@dataclass
class AggCol(Col):
    original_col: Col
    name: str
    type: AggregationType

    def __init__(self, agg_type: AggregationType, original_col: Col) -> None:
        self.original_col = original_col
        self.type = agg_type
        self.name = f"{agg_type}_{original_col.name}"
        super().__init__(self.name)

    def infer_type(self, schema: Schema) -> ColumnType:  # noqa: ARG002
        return ColumnType.INTEGER

    def schema_executor(self, schema: Schema) -> Col:
        return self.original_col.schema_executor(schema) if self.original_col else self

    def execute_row(self, row: tuple[ColumnTypePython, ...]) -> ColumnTypePython:
        return self.original_col.execute_row(row) if self.original_col else 0

    def alias(self, name: str) -> AggCol:
        self.name = name
        return self

    @property
    def all_nested_columns(self) -> Iterable[Col]:
        yield self
        if self.original_col:
            yield from self.original_col.all_nested_columns


class Functions:
    @staticmethod
    def min(col: Col) -> AggCol:
        return AggCol("min", col)

    @staticmethod
    def max(col: Col) -> AggCol:
        return AggCol("max", col)

    @staticmethod
    def sum(col: Col) -> AggCol:
        return AggCol("sum", col)

    @staticmethod
    def count() -> AggCol:
        return AggCol("sum", Lit(1)).alias("count")
