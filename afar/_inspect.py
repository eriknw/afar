"""Utilities to get the lines of the context body.

These read source text with ``tokenize`` and ``ast`` rather than inspecting bytecode,
which changes with every Python release.
"""

import ast
import re
import tokenize
from inspect import findsource

from ._utils import is_ipython

_WITH_START = re.compile(r"\s*with\b")


def get_lines(frame):
    try:
        lines, _ = findsource(frame)
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


def find_with_start(lines, lineno):
    """Index of the line that begins the ``with`` statement executing at ``lineno`` (1-based).

    While a context manager's ``__enter__`` runs, the caller's line number may point to any
    line of a multi-line ``with`` statement, so search backwards from there.
    """
    index = min(lineno, len(lines)) - 1
    while index >= 0:
        if _WITH_START.match(lines[index]):
            return index
        index -= 1
    raise RuntimeError("Failed to analyze the context!")


def analyze_with(lines, with_start):
    """Analyze the ``with`` statement that begins at ``lines[with_start]``.

    Returns ``(num_items, body_start, body_end)``: the number of context managers in the
    statement, and the slice of ``lines`` from the line after the header through the last
    line of the last statement of the body.
    """
    one_line_body = RuntimeError(
        "Failed to analyze the context!  When using afar, "
        "please put the context body on a new line."
    )
    depth = 0
    with_depth = body_start = body_end = None
    body_indented = False
    tokens = tokenize.generate_tokens(iter(lines[with_start:]).__next__)
    try:
        for tok in tokens:
            if tok.type == tokenize.INDENT:
                depth += 1
                if body_start is not None and depth == with_depth + 1:
                    body_indented = True
            elif tok.type == tokenize.DEDENT:
                depth -= 1
                if body_start is not None and depth <= with_depth:
                    break
            elif with_depth is None:
                # The first token after any INDENT must be `with`
                if tok.type != tokenize.NAME or tok.string != "with":
                    raise RuntimeError("Failed to analyze the context!")
                with_depth = depth
            elif body_start is None:
                if tok.type == tokenize.NEWLINE:
                    # End of the header.  Rows are 1-based, so this indexes the next line.
                    body_start = with_start + tok.start[0]
            elif tok.type == tokenize.NEWLINE:
                body_end = with_start + tok.start[0]
            elif tok.type == tokenize.ENDMARKER:
                break
            elif not body_indented and tok.type not in {tokenize.NL, tokenize.COMMENT}:
                # The next statement did not begin with an INDENT, so the header line
                # already contained the body, as in `with run, remotely: x = 1`.
                raise one_line_body
    except SyntaxError:
        # IndentationError from a dedent past the `with` statement's own indentation, which
        # ends the body.  The tokens start at the `with`, so outer indentation is unknown.
        pass
    except tokenize.TokenError as exc:
        raise RuntimeError("Failed to analyze the context!") from exc
    if body_start is not None and not body_indented:
        raise one_line_body
    if body_end is None:
        raise RuntimeError("Failed to analyze the context body!")

    # Let `ast` count the context managers; it handles parentheses and line continuations
    header = [lines[with_start].lstrip(), *lines[with_start + 1 : body_start]]
    try:
        tree = ast.parse("".join(header) + " pass\n")
    except SyntaxError as exc:
        raise RuntimeError("Failed to analyze the context!") from exc
    node = tree.body[0]
    if not isinstance(node, ast.With):
        raise RuntimeError("Failed to analyze the context!")  # noqa: TRY004
    return len(node.items), body_start, body_end
