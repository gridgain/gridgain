import pytest

from pygridgain import Client
from pygridgain.constants import DEFAULT_PORT
from pygridgain.exceptions import ReconnectError


def test_invalid_node():
    """
    Given no valid nodes, the client must yield `ReconnectError`.
    """
    client = Client()
    with pytest.raises(ReconnectError):
        client.connect('inaccessible.host', DEFAULT_PORT)
