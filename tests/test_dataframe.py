from mini_spark.dataframe import DataFrame


def test_table_load():
    # act
    rows = DataFrame().table("tests/test_data/fruits.bin").collect()

    # assert
    assert rows == [
        {"fruit": "apple", "quantity": 3, "color": "red"},
        {"fruit": "banana", "quantity": 5, "color": "yellow"},
        {"fruit": "orange", "quantity": 2, "color": "orange"},
        {"fruit": "apple", "quantity": 4, "color": "green"},
        {"fruit": "banana", "quantity": 7, "color": "yellow"},
    ]
