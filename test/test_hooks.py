from unittest.mock import Mock, patch

import httpx
import pytest

from plugins.hooks.animals_api_hook import AnimalsAPIHook
from utils.exceptions import ExternalAPIException


class TestAnimalsAPIHook:
    def setup_method(self):
        self.hook = AnimalsAPIHook()

    @patch("httpx.Client")
    def test_get_animals_page_success(self, mock_client_class):
        mock_response = Mock()
        mock_response.json.return_value = {
            "items": [
                {"id": 1, "name": "Lion", "born_at": None},
                {"id": 2, "name": "Tiger", "born_at": None},
            ],
            "page": 1,
            "total_pages": 10,
        }
        mock_response.raise_for_status.return_value = None

        mock_client = Mock()
        mock_client.get.return_value = mock_response
        mock_client_class.return_value.__enter__.return_value = mock_client
        result = self.hook.get_animals_page(1)

        assert len(result.items) == 2
        assert result.page == 1
        assert result.total_pages == 10
        mock_client.get.assert_called_once_with(
            "/animals/v1/animals", params={"page": 1}
        )

    @patch("httpx.Client")
    def test_get_animals_page_http_error(self, mock_client_class):
        mock_client = Mock()
        mock_client.get.side_effect = httpx.HTTPStatusError(
            "Server Error", request=Mock(), response=Mock(status_code=500)
        )
        mock_client_class.return_value.__enter__.return_value = mock_client

        with pytest.raises(ExternalAPIException):
            self.hook.get_animals_page(1)

    @patch(
        "plugins.hooks.animals_api_hook.AnimalsAPIHook.get_animals_page"
    )
    def test_get_all_animals(self, mock_get_page):
        page1_response = Mock()
        page1_response.items = [
            Mock(dict=lambda: {"id": 1}),
            Mock(dict=lambda: {"id": 2}),
        ]
        page1_response.has_next = True

        page2_response = Mock()
        page2_response.items = [Mock(dict=lambda: {"id": 3})]
        page2_response.has_next = False

        mock_get_page.side_effect = [page1_response, page2_response]

        result = self.hook.get_all_animals()

        assert len(result) == 3
        assert mock_get_page.call_count == 2
