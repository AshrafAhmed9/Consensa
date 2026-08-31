from linearizability import Operation, is_linearizable


def test_accepts_valid_register_history():
    assert is_linearizable([Operation(0, 1, "write", "x", None), Operation(2, 3, "read", None, "x")])


def test_rejects_stale_read_after_completed_write():
    assert not is_linearizable([Operation(0, 1, "write", "x", None), Operation(2, 3, "read", None, None)])
