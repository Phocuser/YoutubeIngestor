import json
import logging
import subprocess

LOGGER = logging.getLogger(__name__)


def run_indexer(
    article: dict,
    *,
    indexer_bin: str,
    dict_path: str,
    markers_path: str,
    redis_addr: str,
    timeout: float = 30.0,
) -> tuple[bool, str]:
    """Run mycelium cmd/indexer CLI subprocess with article JSON piped to stdin.

    Returns (success, output) where success is True if returncode == 0.
    FileNotFoundError and subprocess.TimeoutExpired are handled gracefully as failures.
    """
    cmd = [
        indexer_bin,
        "-dict",
        dict_path,
        "-markers",
        markers_path,
        "-redis-addr",
        redis_addr,
    ]
    try:
        proc = subprocess.run(
            cmd,
            input=json.dumps(article),
            capture_output=True,
            text=True,
            timeout=timeout,
        )
        combined_output = ((proc.stdout or "") + "\n" + (proc.stderr or "")).strip()
        success = proc.returncode == 0
        if not success and not combined_output:
            combined_output = f"Indexer process exited with code {proc.returncode}"
        return success, combined_output
    except FileNotFoundError as exc:
        msg = f"Indexer binary not found at '{indexer_bin}': {exc}"
        LOGGER.warning(msg)
        return False, msg
    except subprocess.TimeoutExpired as exc:
        msg = f"Indexer execution timed out after {timeout}s: {exc}"
        LOGGER.warning(msg)
        return False, msg
    except Exception as exc:
        msg = f"Indexer execution failed unexpectedly: {exc}"
        LOGGER.warning(msg)
        return False, msg
