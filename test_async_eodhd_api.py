import pytest
import asyncio
from async_eodhd_api import EodhdAPISession
from aiohttp import ClientSession, ClientResponseError, RequestInfo
from yarl import URL
from unittest.mock import patch, MagicMock

@pytest.fixture
def api_key():
    return "test_api_key"

@pytest.mark.asyncio
async def test_get_exchange_symbols(api_key):
    async with EodhdAPISession(api_key) as session:
        with patch.object(session, '_make_request') as mock_request:
            mock_request.return_value = [{"Code": "AAPL"}, {"Code": "GOOGL"}]
            result = await session.get_exchange_symbols("NASDAQ")
            assert result == ["AAPL", "GOOGL"]
            mock_request.assert_called_once_with('/api/exchange-symbol-list/NASDAQ', {})

@pytest.mark.asyncio
async def test_get_historical_data(api_key):
    async with EodhdAPISession(api_key) as session:
        with patch.object(session, '_make_request') as mock_request:
            mock_data = [{"date": "2023-06-01", "close": 100}, {"date": "2023-06-02", "close": 101}]
            mock_request.return_value = mock_data
            symbol, data = await session.get_historical_data("AAPL")
            assert symbol == "AAPL"
            assert data == mock_data
            mock_request.assert_called_once_with('/api/eod/AAPL', {'period': 'd'})

@pytest.mark.asyncio
async def test_get_fundamental_data(api_key):
    async with EodhdAPISession(api_key) as session:
        with patch.object(session, '_make_request') as mock_request:
            mock_data = {"General": {"Code": "AAPL", "Name": "Apple Inc"}}
            mock_request.return_value = mock_data
            symbol, data = await session.get_fundamental_data("AAPL")
            assert symbol == "AAPL"
            assert data == mock_data
            mock_request.assert_called_once_with('/api/fundamentals/AAPL', {})

@pytest.mark.asyncio
async def test_get_news_data(api_key):
    async with EodhdAPISession(api_key) as session:
        with patch.object(session, '_make_request') as mock_request:
            mock_data = [{"title": "News 1"}, {"title": "News 2"}]
            mock_request.return_value = mock_data
            symbol, data = await session.get_news_data("AAPL")
            assert symbol == "AAPL"
            assert data == mock_data
            mock_request.assert_called_once_with('/api/news', {'s': 'AAPL'})

@pytest.mark.asyncio
async def test_make_request_retry(api_key):
    async with EodhdAPISession(api_key, max_retries=3, retry_delay=0.01) as session:
        with patch.object(session.session, 'get') as mock_get:
            mock_response = MagicMock()
            request_info = RequestInfo(URL("https://eodhd.com/test"), "GET", ())
            mock_response.raise_for_status.side_effect = [
                ClientResponseError(request_info, (), status=500),
                ClientResponseError(request_info, (), status=500),
                None
            ]
            mock_response.json.return_value = asyncio.Future()
            mock_response.json.return_value.set_result({"data": "test"})
            mock_get.return_value.__aenter__.return_value = mock_response

            result = await session._make_request('/test', {})
            assert result == {"data": "test"}
            assert mock_get.call_count == 3

@pytest.mark.asyncio
async def test_make_request_failure(api_key):
    async with EodhdAPISession(api_key, max_retries=3, retry_delay=0.01) as session:
        with patch.object(session.session, 'get') as mock_get:
            mock_response = MagicMock()
            request_info = RequestInfo(URL("https://eodhd.com/test"), "GET", ())
            mock_response.raise_for_status.side_effect = ClientResponseError(request_info, (), status=500)
            mock_get.return_value.__aenter__.return_value = mock_response

            with pytest.raises(RuntimeError):
                await session._make_request('/test', {})
            assert mock_get.call_count == 3

if __name__ == '__main__':
    pytest.main()