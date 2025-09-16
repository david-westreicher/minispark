from mini_spark.constants import ColumnType
from mini_spark.sql import Col, Lit


def test_column_equality_with_other():
    # arrange / act
    res = Col("asd") == Col("other")

    # assert
    assert isinstance(res, Col)


def test_column_equality_with_lit():
    # arrange / act
    res = Col("asd") == Lit(1)

    # assert
    assert isinstance(res, Col)


def test_column_equality_with_python_object():
    # arrange / act
    res = Col("asd") == 1

    # assert
    assert isinstance(res, Col)


def test_column_equality_with_python_object_complex():
    # arrange / act
    res = (Col("asd") % 2) == 1

    # assert
    assert isinstance(res, Col)


def test_lit_equality_with_python_object():
    # arrange / act
    res = Lit(2) == 1

    # assert
    assert isinstance(res, Col)


def test_generate_zig_code_string_concatenation_simple():
    # arrange
    schema = [("left", ColumnType.STRING), ("right", ColumnType.STRING)]
    col = Col("left") + Col("right")

    # act
    code = col.zig_code_representation(schema)

    # assert
    assert code == "try concatStrings(allocator, &[_][]const u8{left, right})"


def test_generate_zig_code_string_concatenation_with_literal():
    # arrange
    schema = [("left", ColumnType.STRING)]
    col = Col("left") + Lit(" ")

    # act
    code = col.zig_code_representation(schema)

    # assert
    assert code == 'try concatStrings(allocator, &[_][]const u8{left, " "})'


def test_generate_zig_code_string_concatenation_multiple():
    # arrange
    schema = [("left", ColumnType.STRING), ("right", ColumnType.STRING)]
    col = Col("left") + " " + Col("right")

    # act
    code = col.zig_code_representation(schema)

    # assert
    assert (
        code == 'try concatStrings(allocator, &[_][]const u8{try concatStrings(allocator, &[_][]const u8{left, " "}), '
        "right})"
    )
