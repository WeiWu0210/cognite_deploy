import pytest

from compressor_ts_forecast.handler import handle


@pytest.mark.unit
def test_handler(mocked_data, cognite_client_mock):
    result = handle(mocked_data, cognite_client_mock)
    assert result == mocked_data
