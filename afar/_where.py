from ._exceptions import AfarException

_errors_to_locations = {}
try:
    remotely
except NameError as exc:
    _errors_to_locations[exc.args[0]] = "remotely"

try:
    locally
except NameError as exc:
    _errors_to_locations[exc.args[0]] = "locally"

try:
    later
except NameError as exc:
    _errors_to_locations[exc.args[0]] = "later"


class Where:
    def __init__(self, where, client=None, submit_kwargs=None):
        self.where = where
        self.client = client
        self.submit_kwargs = submit_kwargs

    def __enter__(self):
        raise AfarException(self)

    def __exit__(self, exc_type, exc_value, exc_traceback):  # pragma: no cover
        return False

    def __call__(self, client=None, **submit_kwargs):
        return Where(self.where, client, submit_kwargs)


remotely = Where("remotely")
locally = Where("locally")
later = Where("later")


def find_where(exc_type, exc_value):
    if issubclass(exc_type, AfarException):
        return exc_value.args[0]
    elif issubclass(exc_type, NameError) and exc_value.args[0] in _errors_to_locations:
        return globals()[_errors_to_locations[exc_value.args[0]]]
    else:
        return None
