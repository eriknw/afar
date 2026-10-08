def pytest_addoption(parser):
    # Here, not in afar/tests/conftest.py: pytest reads command-line options only from the conftest
    # files it loads before parsing them, which `testpaths` does not include
    parser.addoption(
        "--innerscope-method",
        action="store",
        default=None,
        choices=["bytecode", "trace"],
        help=(
            "Select innerscope's default method, here and on the workers of the test cluster "
            "(default: innerscope's default for this Python)"
        ),
    )
