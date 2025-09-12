import operator
from collections.abc import Callable, Iterable
from typing import Any

from parsimonious.exceptions import VisitationError
from parsimonious.grammar import Grammar
from parsimonious.nodes import Node, NodeVisitor

from mini_spark.constants import ColumnTypePython
from mini_spark.sql import AggCol, Col, Lit
from mini_spark.sql import Functions as F  # noqa: N817

from .dataframe import DataFrame

sql_grammar = Grammar(
    r"""
    query            = ws? "SELECT" ws select_list ws "FROM" ws table_name (ws where_clause)? (ws group_by_clause)?
                       ws? ";" ws?

    select_list      = select_item (ws? "," ws? select_item)*
    select_item      = star / aggregate_function_call / expr_aliased
    expr_aliased     = expr alias?
    aggregate_function_call = aggregate_function "(" expr? ")" alias?
    aggregate_function = "COUNT" / "SUM" / "AVG" / "MIN" / "MAX"
    alias            = ws "AS" ws identifier

    where_clause     = "WHERE" ws condition
    group_by_clause  = "GROUP" ws "BY" ws column_name (ws? "," ws? column_name)*

    # --- Operator precedence ---
    condition        = or_expr
    or_expr          = and_expr (ws "OR" ws and_expr)*
    and_expr         = not_expr (ws "AND" ws not_expr)*
    not_expr         = ("NOT" ws)? predicate
    predicate        = comparison / parenthised_condition
    parenthised_condition = "(" ws? condition ws? ")"
    comparison       = expr ws comparator ws expr

    comparator       = "=" / "!=" / "<=" / ">=" / "<" / ">"

    # --- Arithmetic expressions ---
    expr             = add_expr
    add_expr         = mul_expr (ws? add_op ws? mul_expr)*
    mul_expr         = atom (ws? mul_op ws? atom)*
    add_op           = "+" / "-"
    mul_op           = "*" / "/"
    atom             = number / column_name / parenthised_expr
    parenthised_expr = "(" ws? expr ws? ")"

    # --- Terminals ---
    star             = "*"
    table_name       = "'" ~"[a-zA-Z0-9_\\-\\. ]+" "'"
    column_name      = ~"[A-Za-z_][A-Za-z0-9_]*"
    identifier       = ~"[A-Za-z_][A-Za-z0-9_]*"
    number           = ~"[0-9]+(\\.[0-9]+)?"
    string_literal   = "'" ~"[^']*" "'"
    value            = number / string_literal

    ws               = ~"\\s+"
    """
)

#                 ws   sel  ws   sell ws   from ws   table      maybe      ws,  wher          grou ws   ;    ws
QueryType = tuple[Any, Any, Any, Any, Any, Any, Any, DataFrame, list[tuple[Any, Col]] | Node, Any, Any, Any, Any]
#                 wher ws   cond
WhereType = tuple[Any, Any, Col]
#                  left repeat         ws   OR   ws   right
OrExprType = tuple[Col, Iterable[tuple[Any, Any, Any, Col]]]
#                   left repeat         ws   op   ws   right
AddExprType = tuple[Col, Iterable[tuple[Any, Any, Any, Col]]]
#                   left repeat         ws   AND  ws   right
AndExprType = tuple[Col, Iterable[tuple[Any, Any, Any, Col]]]
#                   maybe      not  ws            pred
NotExprType = tuple[list[tuple[Any, Any]] | Node, Col]
#                     col  ws   comp ws   value                     (    ws   cond ws  )
PredicateType = tuple[Col, Any, str, Any, ColumnTypePython] | tuple[Any, Any, Col, Any, Any]
#                  sel  repeat         ws   ,    ws   sel
SelectList = tuple[Col, Iterable[tuple[Any, Any, Any, Col]]]
#                      star / agg / col
SelectItemType = tuple[Col]
#                             col  alias
ExprAliasedType = tuple[Col, list[str] | Node]
#                 ws   AS   ws   identifier
AliasType = tuple[Any, Any, Any, str]
ComparatorType = Callable[[Col, Col | ColumnTypePython], Col]
ComparisonType = tuple[Col, Any, ComparatorType, Any, Col | ColumnTypePython]
ParenthisedConditionType = tuple[Any, Any, Col, Any, Any]
AtomType = tuple[Col]


class SemanticError(Exception):
    def __init__(self, message: str) -> None:
        super().__init__(message)


class GroupByError(SemanticError):
    def __init__(self, message: str) -> None:
        super().__init__(message)


class SQLVisitor(NodeVisitor):  # type:ignore[misc]
    def visit_table_name(self, node: Node, visited_children: list[Any]) -> DataFrame:  # noqa: ARG002
        _, table_name, _ = visited_children
        return DataFrame().table(table_name.text)

    def visit_query(self, node: Node, visited_children: QueryType) -> DataFrame:  # noqa: ARG002
        _, _, _, select_list, _, _, _, load_table, where_clause, group_by_clause, _, _, _ = visited_children
        if type(group_by_clause) is list:
            _, group_by_cols = group_by_clause[0]
            group_by_col_names = {col.name for col in group_by_cols}
            agg_cols = [col for col in select_list if type(col) is AggCol]
            invalid_agg_cols = [
                col for col in select_list if type(col) is not AggCol and col.name not in group_by_col_names
            ]
            if invalid_agg_cols:
                raise GroupByError(
                    "All selected columns must be aggregate functions or part of the key when using GROUP BY:\n"
                    f"{invalid_agg_cols}"
                )
            return load_table.group_by(*group_by_cols).agg(*agg_cols).select(*[Col(col.name) for col in select_list])
        df = load_table.select(*select_list)
        if type(where_clause) is list:
            assert len(where_clause) == 1
            _, where = where_clause[0]
            df = df.filter(where)
        return df

    def visit_select_list(self, node: Node, visited_children: SelectList) -> list[Col]:  # noqa: ARG002
        selections = [visited_children[0]]
        for right_expansion in visited_children[1]:
            _, _, _, select_item = right_expansion
            selections.append(select_item)
        return selections

    def visit_where_clause(self, node: Node, visited_children: WhereType) -> Col:  # noqa: ARG002
        _, _, condition = visited_children
        return condition

    def visit_select_item(self, node: Node, visited_children: SelectItemType) -> Col:  # noqa: ARG002
        assert len(visited_children) == 1
        return visited_children[0]

    def visit_group_by_clause(self, node: Node, visited_children: list[Any]) -> list[Col]:  # noqa: ARG002
        _, _, _, _, column, right = visited_children
        columns = [column]
        for right_expansion in right:
            _, _, _, right_col = right_expansion
            columns.append(right_col)
        return columns

    def visit_aggregate_function_call(self, node: Node, visited_children: list[Any]) -> AggCol:  # noqa: ARG002
        aggregate_function, _, arguments, _, alias = visited_children
        real_alias = None
        if type(alias) is list:
            assert len(alias) == 1
            real_alias = alias[0]
        agg_col = None
        if aggregate_function == "COUNT":
            assert type(arguments) is Node
            agg_col = F.count()
        if aggregate_function == "SUM":
            assert type(arguments) is list
            assert len(arguments) == 1
            agg_col = F.sum(arguments[0])
        if aggregate_function == "MIN":
            assert type(arguments) is list
            assert len(arguments) == 1
            agg_col = F.min(arguments[0])
        if aggregate_function == "MAX":
            assert type(arguments) is list
            assert len(arguments) == 1
            agg_col = F.max(arguments[0])
        assert agg_col is not None
        if real_alias:
            return agg_col.alias(real_alias)
        return agg_col

    def visit_aggregate_function(self, node: Node, visited_children: list[Any]) -> str:  # noqa: ARG002
        return str(node.text)

    def visit_expr_aliased(self, node: Node, visited_children: ExprAliasedType) -> Col:  # noqa: ARG002
        col, alias = visited_children
        if type(alias) is list:
            assert len(alias) == 1
            return col.alias(alias[0])
        return col

    def visit_add_expr(self, node: Node, visited_children: AddExprType) -> Col:  # noqa: ARG002
        left, right = visited_children
        for right_expansion in right:
            _, add_op, _, right_col = right_expansion
            left = add_op(left, right_col)
        return left

    def visit_mul_expr(self, node: Node, visited_children: AddExprType) -> Col:  # noqa: ARG002
        left, right = visited_children
        for right_expansion in right:
            _, mul_op, _, right_col = right_expansion
            left = mul_op(left, right_col)
        return left

    def visit_or_expr(self, node: Node, visited_children: OrExprType) -> Col:  # noqa: ARG002
        left, right = visited_children
        for right_expansion in right:
            _, _, _, right_col = right_expansion
            left = left | right_col
        return left

    def visit_and_expr(self, node: Node, visited_children: AndExprType) -> Col:  # noqa: ARG002
        left, right = visited_children
        for right_expansion in right:
            _, _, _, right_col = right_expansion
            left = left & right_col
        return left

    def visit_not_expr(self, node: Node, visited_children: NotExprType) -> Col:  # noqa: ARG002
        negation, pred = visited_children
        if type(negation) is Node:
            return pred
        assert type(negation) is list
        assert len(negation) == 1
        return ~pred

    def visit_predicate(self, node: Node, visited_children: PredicateType) -> Col:  # noqa: ARG002
        return visited_children[0]

    def visit_comparison(self, node: Node, visited_children: ComparisonType) -> Col:  # noqa: ARG002
        left, _, comparator, _, right = visited_children
        return comparator(left, right)

    def visit_column_name(self, node: Node, visited_children: list[Any]) -> Col:  # noqa: ARG002
        return Col(node.text)

    def visit_comparator(self, node: Node, visited_children: list[Any]) -> ComparatorType:  # noqa: ARG002
        if node.text == "=":
            return operator.eq
        if node.text == ">":
            return operator.gt
        if node.text == ">=":
            return operator.ge
        if node.text == "<":
            return operator.lt
        if node.text == "<=":
            return operator.le
        if node.text == "!=":
            return operator.ne
        raise ValueError(f"Unsupported comparator: {node.text}")

    def visit_add_op(self, node: Node, visited_children: list[Any]) -> ComparatorType:  # noqa: ARG002
        if node.text == "+":
            return operator.add
        if node.text == "-":
            return operator.sub
        raise ValueError(f"Unsupported comparator: {node.text}")

    def visit_mul_op(self, node: Node, visited_children: list[Any]) -> ComparatorType:  # noqa: ARG002
        if node.text == "*":
            return operator.mul
        if node.text == "/":
            return operator.truediv
        raise ValueError(f"Unsupported comparator: {node.text}")

    def visit_parenthised_condition(self, node: Node, visited_children: ParenthisedConditionType) -> Col:  # noqa: ARG002
        _, _, col, _, _ = visited_children
        return col

    def visit_parenthised_expr(self, node: Node, visited_children: ParenthisedConditionType) -> Col:  # noqa: ARG002
        _, _, col, _, _ = visited_children
        return col

    def visit_alias(self, node: Node, visited_children: AliasType) -> str:  # noqa: ARG002
        _, _, _, identifier = visited_children
        return identifier

    def visit_value(self, node: Node, visited_children: list[ColumnTypePython]) -> ColumnTypePython:  # noqa: ARG002
        return visited_children[0]

    def visit_string_literal(self, node: Node, visited_children: AliasType) -> str:  # noqa: ARG002
        return str(node.text)

    def visit_number(self, node: Node, visited_children: AliasType) -> Lit:  # noqa: ARG002
        return Lit(int(node.text))

    def visit_identifier(self, node: Node, visited_children: list[Any]) -> str:  # noqa: ARG002
        return str(node.text)

    def visit_star(self, node: Node, visited_children: list[Any]) -> Col:  # noqa: ARG002
        return Col("*")

    def visit_atom(self, node: Node, visited_children: AtomType) -> Col:  # noqa: ARG002
        (col,) = visited_children
        return col

    def generic_visit(self, node: Node, visited_children: list[Any]) -> Node | list[Any]:
        return visited_children or node


def parse_sql(sql: str) -> DataFrame:
    tree = sql_grammar.parse(sql)
    try:
        df: DataFrame = SQLVisitor().visit(tree)
    except VisitationError as e:
        raise e.__context__ from e
    return df
