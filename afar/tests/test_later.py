from pytest import raises

import afar


def test_later_doesnt_execute():
    run = afar.run()
    with run, later:
        1 / 0
    assert run.context_body == ["        1 / 0\n"]


def test_single_line():
    with raises(RuntimeError, match="please put the context body on a new line"):
        # fmt: off
        with afar.run, later: pass
        # fmt: on


def test_later_bodies():
    run = afar.run()
    with run, later:
        pass

    assert run.context_body == ["        pass\n", "\n"]

    with raises(Exception, match="missing"):
        with run:
            pass

    with run, later:
        b = a + 1
        c = a + b

    assert run.context_body == ["        b = a + 1\n", "        c = a + b\n", "\n"]

    with raises(Exception, match="missing"):
        with run:
            with later:
                pass

    # It would be nice if we could make these fail
    with run, later as z:
        pass

    with run, later as [z, *other]:
        pass

    with run, later, z:
        pass

    # fmt: off
    with \
        run, \
        later \
    :

        pass

    # fmt: on

    assert run.context_body == ["\n", "        pass\n", "\n", "    # fmt: on\n", "\n"]

    # fmt: off
    with \
        run as c, \
        later( \
        d= \
        ":" \
        ) \
    :

        f
        g
        h(
            z
            =
            2
        )

    # fmt: on

    assert run.context_body == [
        "\n",
        "        f\n",
        "        g\n",
        "        h(\n",
        "            z\n",
        "            =\n",
        "            2\n",
        "        )\n",
        "\n",
        "    # fmt: on\n",
        "\n",
    ]

    # fmt: off
    with \
        run, \
        later:
        # :
        (
            1
            +
            2
        )
    x = (
        3
        +
        4
    )
    # fmt: on
    assert run.context_body == [
        "        # :\n",
        "        (\n",
        "            1\n",
        "            +\n",
        "            2\n",
        "        )\n",
    ]
