"""Find the parts of a ``with`` statement from source lines."""

import pytest

from afar._inspect import analyze_with, find_with_start


def lines_of(source):
    return source.splitlines(keepends=True)


def body_of(source, with_line=0):
    lines = lines_of(source)
    num_items, start, end = analyze_with(lines, with_line)
    return num_items, lines[start:end]


def test_simple():
    source = "with a, b:\n    x = 1\n    y = 2\nz = 3\n"
    assert body_of(source) == (2, ["    x = 1\n", "    y = 2\n"])


def test_end_of_file():
    assert body_of("with a, b:\n    x = 1\n") == (2, ["    x = 1\n"])
    # No trailing newline
    assert body_of("with a, b:\n    x = 1") == (2, ["    x = 1"])


def test_one_item():
    assert body_of("with a:\n    x = 1\n") == (1, ["    x = 1\n"])


def test_three_items():
    assert body_of("with a, b as c, d():\n    x = 1\n") == (3, ["    x = 1\n"])


def test_parenthesized_items():
    source = "with (\n    a,\n    b as c,\n):\n    x = 1\n"
    assert body_of(source) == (2, ["    x = 1\n"])


def test_parenthesized_tuple_is_one_item():
    # A tuple of context managers is a single (broken) context manager
    assert body_of("with (a, b) as c:\n    x = 1\n") == (1, ["    x = 1\n"])


def test_backslash_header():
    source = "with \\\n    a, \\\n    b \\\n:\n\n    x = 1\n"
    assert body_of(source) == (2, ["\n", "    x = 1\n"])


def test_header_comment_with_colon():
    source = "with a, b(\n    c=':',  # :\n):  # comment:\n    # :\n    x = 1\n"
    assert body_of(source) == (2, ["    # :\n", "    x = 1\n"])


def test_trailing_comments_and_blank_lines_excluded():
    source = "with a, b:\n    x = 1\n    # trailing\n\n# outer\n\ny = 2\n"
    assert body_of(source) == (2, ["    x = 1\n"])


def test_nested_blocks():
    source = (
        "with a, b:\n"
        "    for i in range(3):\n"
        "        if i:\n"
        "            x = i\n"
        "    else:\n"
        "        y = 2\n"
        "z = 3\n"
    )
    assert body_of(source) == (2, lines_of(source)[1:6])


def test_multiline_string_in_body():
    # The string's lines are less indented than the body; tokenize knows they are a string
    source = 'with a, b:\n    s = """\nnot a dedent\n"""\n    t = 1\nu = 2\n'
    assert body_of(source) == (2, lines_of(source)[1:5])


def test_indented_with():
    source = "def f():\n    with a, b:\n        x = 1\n    y = 2\n"
    assert body_of(source, 1) == (2, ["        x = 1\n"])


def test_dedent_past_with_level():
    # The `with` is inside two blocks and the next statement is at the outermost level.
    # Tokens start at the `with`, so this dedent is an IndentationError for `tokenize`.
    source = (
        "class C:\n"
        "    def f(self):\n"
        "        with a, b:\n"
        "            x = 1\n"
        "\n"
        "        # comment\n"
        "x = 2\n"
    )
    assert body_of(source, 2) == (2, ["            x = 1\n"])


def test_dedent_to_middle_level():
    source = "def f():\n    if c:\n        with a, b:\n            x = 1\n    y = 2\n"
    assert body_of(source, 2) == (2, ["            x = 1\n"])


def test_tabs():
    source = "if c:\n\twith a, b:\n\t\tx = 1\n\ty = 2\n"
    assert body_of(source, 1) == (2, ["\t\tx = 1\n"])


def test_line_continuation_in_body():
    source = "with a, b:\n    x = (\n1\n    )\n    y = x + \\\n2\nz = 3\n"
    assert body_of(source) == (2, lines_of(source)[1:6])


@pytest.mark.parametrize(
    "source",
    [
        "with a, b: x = 1\n",
        "with a, b: x = 1\ny = 2\n",
        "with a, b: x = 1; y = 2\n\n# comment\n",
        "def f():\n    with a, b: x = 1\nz = 3\n",
        "class C:\n    def f():\n        with a, b: x = 1\n\n    y = 2\n",
        "with \\\n    a, b: x = 1\n",
    ],
)
def test_one_line_body(source):
    lines = lines_of(source)
    with pytest.raises(RuntimeError, match="please put the context body on a new line"):
        analyze_with(lines, find_with_start(lines, 1 + source.count("\n", 0, source.index("with"))))


def test_not_a_with():
    with pytest.raises(RuntimeError, match="Failed to analyze"):
        analyze_with(["x = 1\n"], 0)


def test_find_with_start():
    lines = lines_of("x = 1\nwith \\\n    a, \\\n    b:\n    y = 2\n")
    assert find_with_start(lines, 2) == 1
    assert find_with_start(lines, 4) == 1
    # Do not match names that begin with "with"
    lines = lines_of("with a, b(\n    without=1,\n    with_=2,\n):\n    pass\n")
    assert find_with_start(lines, 3) == 0
    with pytest.raises(RuntimeError, match="Failed to analyze"):
        find_with_start(["x = 1\n", "y = 2\n"], 2)


def test_find_with_start_skips_string_lines():
    # Since Python 3.12, the line number points at the item being entered, here line 6
    lines = lines_of('with (\n    f("""\nwith\n""") as g,\n    run,\n    later,\n):\n    x = 1\n')
    assert lines[2] == "with\n"
    assert find_with_start(lines, 6) == 0
    assert find_with_start(lines, 2) == 0
