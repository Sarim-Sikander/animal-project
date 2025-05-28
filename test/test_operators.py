from datetime import datetime
from unittest.mock import Mock, patch

import pytest

from plugins.operators.animal_etl_operators import (
    ExtractAnimalsOperator,
    HealthCheckOperator,
    LoadAnimalsBatchOperator,
    TransformAnimalsOperator,
)


class TestExtractAnimalsOperator:
    @patch("plugins.operators.animal_etl_operators.AnimalsAPIHook")
    def test_execute_success(self, mock_hook_class):
        mock_hook = Mock()
        mock_hook.get_all_animals.return_value = [
            {"id": 1, "name": "Lion", "species": "Panthera leo"},
            {"id": 2, "name": "Tiger", "species": "Panthera tigris"},
        ]
        mock_hook_class.return_value = mock_hook

        operator = ExtractAnimalsOperator(task_id="test_extract")
        result = operator.execute(context={})

        assert len(result) == 2
        assert result[0]["name"] == "Lion"
        mock_hook.get_all_animals.assert_called_once()


class TestTransformAnimalsOperator:
    @patch("plugins.operators.animal_etl_operators.AnimalsAPIHook")
    def test_execute_success(self, mock_hook_class):
        mock_context = {"task_instance": Mock()}
        mock_context["task_instance"].xcom_pull.return_value = [
            {"id": 1, "name": "Lion"},
            {"id": 2, "name": "Tiger"},
        ]

        mock_hook = Mock()
        mock_hook.get_animals_details_batch.return_value = [
            Mock(
                id=1,
                name="Lion",
                species="Panthera leo",
                friends="Tiger",
                born_at=None,
            ),
            Mock(
                id=2,
                name="Tiger",
                species="Panthera tigris",
                friends="Lion",
                born_at=None,
            ),
        ]
        mock_hook_class.return_value = mock_hook
        operator = TransformAnimalsOperator(
            task_id="test_transform", batch_size=10
        )

        with (
            patch.object(
                operator.transformer, "transform_animals_batch"
            ) as mock_transform,
            patch.object(
                operator.transformer, "to_dict_list"
            ) as mock_to_dict,
        ):

            mock_transform.return_value = [Mock(), Mock()]
            mock_to_dict.return_value = [{"id": 1}, {"id": 2}]

            result = operator.execute(mock_context)

            assert len(result) == 1
            assert len(result[0]) == 2


class TestHealthCheckOperator:
    @patch("plugins.operators.animal_etl_operators.AnimalsAPIHook")
    def test_execute_healthy(self, mock_hook_class):
        mock_hook = Mock()
        mock_hook.health_check.return_value = True
        mock_hook_class.return_value = mock_hook

        operator = HealthCheckOperator(task_id="test_health_check")
        result = operator.execute(context={})

        assert result is True
        mock_hook.health_check.assert_called_once()

    @patch("plugins.operators.animal_etl_operators.AnimalsAPIHook")
    def test_execute_unhealthy(self, mock_hook_class):
        mock_hook = Mock()
        mock_hook.health_check.return_value = False
        mock_hook_class.return_value = mock_hook

        operator = HealthCheckOperator(task_id="test_health_check")

        with pytest.raises(Exception):
            operator.execute(context={})
