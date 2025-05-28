import json
import os
import tempfile
from typing import Any, Dict, List, Optional

from airflow.exceptions import AirflowException
from airflow.models import BaseOperator
from airflow.utils.context import Context

from config.settings import settings
from plugins.hooks.animals_api_hook import AnimalsAPIHook
from utils.models import AnimalDetail
from utils.transformers import AnimalDataTransformer


class ExtractAnimalsOperator(BaseOperator):
    template_fields = ("animals_api_conn_id",)

    def __init__(
        self, animals_api_conn_id: str = "animals_api_default", **kwargs
    ):
        super().__init__(**kwargs)
        self.animals_api_conn_id = animals_api_conn_id

    def execute(self, context: Context) -> Dict[str, Any]:
        hook = AnimalsAPIHook(animals_api_conn_id=self.animals_api_conn_id)
        animals = hook.get_all_animals()

        temp_dir = tempfile.gettempdir()
        temp_file = os.path.join(
            temp_dir, f"animals_data_{context['run_id']}.json"
        )

        with open(temp_file, "w", encoding="utf-8") as f:
            json.dump(animals, f, ensure_ascii=False, indent=2)

        self.log.info(
            f"Extracted {len(animals)} animals, stored in {temp_file}"
        )

        return {
            "total_animals": len(animals),
            "temp_file": temp_file,
            "status": "completed",
        }


class TransformAnimalsOperator(BaseOperator):
    template_fields = ("batch_size",)

    def __init__(self, batch_size: int = settings.BATCH_SIZE, **kwargs):
        super().__init__(**kwargs)
        self.batch_size = batch_size
        self.transformer = AnimalDataTransformer()

    def execute(self, context: Context) -> Dict[str, Any]:
        extract_result = context["task_instance"].xcom_pull(
            task_ids="extract_animals"
        )

        if not extract_result or "temp_file" not in extract_result:
            raise AirflowException(
                "No animals data received from extract task"
            )

        temp_file = extract_result["temp_file"]

        try:
            with open(temp_file, "r", encoding="utf-8") as f:
                animals_data = json.load(f)
        except FileNotFoundError:
            raise AirflowException(
                f"Animals data file not found: {temp_file}"
            )
        except json.JSONDecodeError as e:
            raise AirflowException(
                f"Invalid JSON in animals data file: {e}"
            )

        self.log.info(
            f"Loaded {len(animals_data)} animals from {temp_file}"
        )

        hook = AnimalsAPIHook()
        animal_ids = [animal["id"] for animal in animals_data]

        all_transformed_batches = []
        total_processed = 0

        for i in range(0, len(animal_ids), self.batch_size):
            batch_ids = animal_ids[i : i + self.batch_size]
            self.log.info(
                f"Processing batch {i//self.batch_size + 1}: IDs {batch_ids[0]} to {batch_ids[-1]}"
            )

            batch_details = hook.get_animals_details_batch(batch_ids)
            transformed_animals = self.transformer.transform_animals_batch(
                batch_details
            )
            animals_api_format = self.transformer.to_api_format(
                transformed_animals
            )

            for j in range(0, len(animals_api_format), 100):
                load_batch = animals_api_format[j : j + 100]

                batch_file = os.path.join(
                    tempfile.gettempdir(),
                    f"load_batch_{len(all_transformed_batches)}_{context['run_id']}.json",
                )

                with open(batch_file, "w", encoding="utf-8") as f:
                    json.dump(load_batch, f, ensure_ascii=False, indent=2)

                all_transformed_batches.append(
                    {
                        "file": batch_file,
                        "count": len(load_batch),
                        "batch_index": len(all_transformed_batches),
                    }
                )

            total_processed += len(transformed_animals)
            self.log.info(
                f"Processed {total_processed}/{len(animal_ids)} animals so far"
            )

        try:
            os.remove(temp_file)
        except OSError:
            self.log.warning(f"Could not remove temp file: {temp_file}")

        self.log.info(
            f"Transformed {total_processed} animals into {len(all_transformed_batches)} load batches"
        )

        return {
            "total_batches": len(all_transformed_batches),
            "total_animals": total_processed,
            "batch_files": [
                batch["file"] for batch in all_transformed_batches
            ],
            "batch_info": all_transformed_batches,
            "status": "completed",
        }


class LoadAnimalsBatchOperator(BaseOperator):
    template_fields = ("animals_api_conn_id", "batch_index")

    def __init__(
        self,
        animals_api_conn_id: str = "animals_api_default",
        batch_index: int = 0,
        **kwargs,
    ):
        super().__init__(**kwargs)
        self.animals_api_conn_id = animals_api_conn_id
        self.batch_index = batch_index

    def execute(self, context: Context) -> Dict[str, Any]:
        transform_result = context["task_instance"].xcom_pull(
            task_ids="transform_animals"
        )

        if not transform_result or "batch_info" not in transform_result:
            self.log.warning("No batch info received from transform task")
            return {"status": "skipped", "reason": "no_batch_info"}

        batch_info_list = transform_result["batch_info"]

        if self.batch_index >= len(batch_info_list):
            self.log.warning(
                f"Batch index {self.batch_index} not found (only {len(batch_info_list)} batches)"
            )
            return {
                "status": "skipped",
                "reason": "batch_index_out_of_range",
            }

        batch_info = batch_info_list[self.batch_index]
        batch_file = batch_info["file"]

        try:
            with open(batch_file, "r", encoding="utf-8") as f:
                batch_animals = json.load(f)
        except FileNotFoundError:
            raise AirflowException(f"Batch file not found: {batch_file}")
        except json.JSONDecodeError as e:
            raise AirflowException(f"Invalid JSON in batch file: {e}")

        if not isinstance(batch_animals, list):
            raise AirflowException(
                f"Batch data is not a list: {type(batch_animals)}"
            )

        if batch_animals and not isinstance(batch_animals[0], dict):
            raise AirflowException(
                f"Batch animals are not objects: {type(batch_animals[0])}"
            )

        hook = AnimalsAPIHook(animals_api_conn_id=self.animals_api_conn_id)
        success = hook.send_animals_to_home(batch_animals)

        try:
            os.remove(batch_file)
        except OSError:
            self.log.warning(f"Could not remove batch file: {batch_file}")

        self.log.info(
            f"Loaded batch {self.batch_index} with {len(batch_animals)} animals"
        )

        return {
            "status": "completed" if success else "failed",
            "batch_index": self.batch_index,
            "animals_count": len(batch_animals),
            "success": success,
        }


class HealthCheckOperator(BaseOperator):
    template_fields = ("animals_api_conn_id",)

    def __init__(
        self, animals_api_conn_id: str = "animals_api_default", **kwargs
    ):
        super().__init__(**kwargs)
        self.animals_api_conn_id = animals_api_conn_id

    def execute(self, context: Context) -> Dict[str, Any]:
        hook = AnimalsAPIHook(animals_api_conn_id=self.animals_api_conn_id)
        is_healthy = hook.health_check()

        if not is_healthy:
            raise AirflowException("Animals API health check failed")

        self.log.info("Animals API is healthy")
        return {"status": "healthy", "timestamp": context["ts"]}
