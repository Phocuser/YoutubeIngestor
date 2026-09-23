def run_indexer(
    article: dict,
    *,
    indexer_bin: str,
    dict_path: str,
    markers_path: str,
    redis_addr: str,
    timeout: float = 30.0,
) -> tuple[bool, str]:
    """Return a truthful disabled result; never start a legacy subprocess."""
    del article, indexer_bin, dict_path, markers_path, redis_addr, timeout
    return False, "LEGACY_PATH_DISABLED: use the durable Mycelium capture boundary"
