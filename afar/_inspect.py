"""Utilities to get the lines of the context body."""
import dis
from inspect import findsource

from ._utils import is_ipython


def get_lines(frame):
    try:
        lines, offset = findsource(frame)
    except OSError:
        # Try to fine the source if we are in %%time or %%timeit magic
        if frame.f_code.co_filename in {"<timed exec>", "<magic-timeit>"} and is_ipython():
            from IPython import get_ipython

            ip = get_ipython()
            if ip is None:
                raise
            cell = ip.history_manager._i00  # The current cell!
            lines = cell.splitlines(keepends=True)
            # strip the magic
            for i, line in enumerate(lines):
                if line.strip().startswith("%%time"):
                    lines = lines[i + 1 :]
                    break
            else:
                raise
            # strip blank lines
            for i, line in enumerate(lines):
                if line.strip():
                    if i:
                        lines = lines[i:]
                    lines[-1] += "\n"
                    break
            else:
                raise
        else:
            raise
    return lines


def get_body_start(lines, with_start):
    line = lines[with_start]
    stripped = line.lstrip()
    body = line[: len(line) - len(stripped)] + " pass\n"
    body *= 2
    with_lines = [stripped]
    try:
        code = compile(stripped, "<exec>", "exec")
    except Exception:
        pass
    else:
        raise RuntimeError(
            "Failed to analyze the context!  When using afar, "
            "please put the context body on a new line."
        )
    for i, line in enumerate(lines[with_start:]):
        if i > 0:
            with_lines.append(line)
        if ":" in line:
            source = "".join(with_lines) + body
            try:
                code = compile(source, "<exec>", "exec")
            except Exception:
                pass
            else:
                num_with = code.co_code.count(dis.opmap["SETUP_WITH"])
                body_start = with_start + i + 1
                return num_with, body_start
    raise RuntimeError("Failed to analyze the context!")


def get_body(lines):
    head = "def f():\n with x:\n  "
    tail = " pass\n pass\n"
    while lines:
        source = head + "  ".join(lines) + tail
        try:
            compile(source, "<exec>", "exec")
        except Exception:
            lines.pop()
        else:
            return lines
    raise RuntimeError("Failed to analyze the context body!")
