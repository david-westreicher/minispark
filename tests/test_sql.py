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
