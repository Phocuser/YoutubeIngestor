from app.indexer_client import run_indexer


def test_legacy_indexer_path_is_explicitly_disabled():
    success, output = run_indexer(
        {"id": "youtube:v"}, indexer_bin="unused", dict_path="unused",
        markers_path="unused", redis_addr="unused",
    )
    assert success is False
    assert "LEGACY_PATH_DISABLED" in output
