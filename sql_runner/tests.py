
def assert_almost_equal(tolerance=0.1, rows=None):
    if len(rows) != 2:
        raise Exception("Almost Equal Assertion works only on queries that return 2 exact rows and 1 column")
    val0 = rows[0][0]
    val1 = rows[1][0]
    assert -tolerance <= val0 - val1 <= tolerance, \
        f"Values are too far apart. Tolerance {tolerance}, {val0}-{val1}={val0-val1}"

def assert_row_count(num_rows, rows=None):
    assert len(rows) == int(num_rows), f"Incorrect number of rows. Expected {int(num_rows)}, got {len(rows)}"
