import os
import json
import logging
import threading
from datetime import datetime

from . import config

log = logging.getLogger("watchlod")

_lock = threading.Lock()
_paths = None
_output_path = None
_dir_ready = False


def activate(job):
    template = config.get((), "created-file-log")
    if not template:
        return

    global _paths, _output_path
    with _lock:
        if _paths is None:
            _paths = []
            _output_path = template.replace(
                "{timestamp}", datetime.now().strftime("%Y%m%d_%H%M%S"))
    job.register_hooks({"after": _record})


def _record(pathfmt):
    with _lock:
        if _paths is None:
            return
        _paths.append(pathfmt.realpath)
        paths_snapshot = sorted(set(_paths))
        output_path = _output_path
        _ensure_dir(output_path)
    _write_snapshot(paths_snapshot, output_path)


def _ensure_dir(output_path):
    global _dir_ready
    if _dir_ready:
        return
    directory = os.path.dirname(output_path)
    if directory:
        try:
            os.makedirs(directory, exist_ok=True)
        except Exception:
            log.exception(
                "Failed to create directory for created-file-log '%s'",
                output_path)
    _dir_ready = True


def _write_snapshot(paths, output_path):
    tmp_path = f"{output_path}.tmp-{os.getpid()}" # yes, pid.
    try:
        with open(tmp_path, "w") as fp:
            json.dump(paths, fp, indent=1)
            fp.flush()
            os.fsync(fp.fileno())
        os.replace(tmp_path, output_path)
    except Exception:
        log.exception(
            "Failed to write created-file-log to '%s'", output_path)
        try:
            os.unlink(tmp_path)
        except OSError:
            pass
