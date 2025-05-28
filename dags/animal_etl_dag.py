from datetime import datetime, timedelta
from typing import Any, Dict, List

from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator
from airflow.utils.trigger_rule import TriggerRule

from config.settings import settings
from plugins.operators.animal_etl_operators import (
    ExtractAnimalsOperator,
    HealthCheckOperator,
    TransformAnimalsOperator,
)

default_args = {
    "owner": settings.DAG_OWNER,
    "depends_on_past": False,
    "start_date": datetime(2025, 1, 1),
    "email_on_failure": True,
    "email_on_retry": False,
    "email": settings.DAG_EMAIL,
    "retries": 3,
    "retry_delay": timedelta(minutes=5),
    "execution_timeout": timedelta(hours=2),
}

dag = DAG(
    "animal_etl_pipeline_v3",
    default_args=default_args,
    description="ETL pipeline for animal data processing - Fixed data corruption",
    schedule_interval=settings.DAG_SCHEDULE,
    catchup=settings.DAG_CATCHUP,
    max_active_runs=1,
    tags=["etl", "animals", "data-pipeline", "v3-fixed"],
)

extract_animals = ExtractAnimalsOperator(
    task_id="extract_animals",
    dag=dag,
)

transform_animals = TransformAnimalsOperator(
    task_id="transform_animals",
    batch_size=settings.BATCH_SIZE,
    dag=dag,
)


def load_all_batches_fixed(**context):
    import json

    from plugins.hooks.animals_api_hook import AnimalsAPIHook

    transform_result = context["task_instance"].xcom_pull(
        task_ids="transform_animals"
    )

    if not transform_result or "batch_info" not in transform_result:
        print("No batch info received from transform task")
        return {"status": "no_data", "batches_processed": 0}

    batch_info_list = transform_result["batch_info"]
    hook = AnimalsAPIHook()

    successful_batches = 0
    failed_batches = 0
    total_animals = 0

    print(f"DEBUG: Processing {len(batch_info_list)} batches")
    print(
        f"DEBUG: Batch info structure: {batch_info_list[0] if batch_info_list else 'No batches'}"
    )

    for i, batch_info in enumerate(batch_info_list):
        batch_file = batch_info["file"]
        expected_count = batch_info["count"]

        try:
            print(f"DEBUG: Loading batch {i+1} from {batch_file}")

            with open(batch_file, "r", encoding="utf-8") as f:
                batch_content = f.read()
                print(
                    f"DEBUG: Raw file content (first 200 chars): {batch_content[:200]}"
                )

                batch_animals = json.loads(batch_content)
                print(f"DEBUG: Parsed JSON type: {type(batch_animals)}")
                print(
                    f"DEBUG: Batch size: {len(batch_animals) if isinstance(batch_animals, list) else 'Not a list'}"
                )

                if (
                    isinstance(batch_animals, list)
                    and len(batch_animals) > 0
                ):
                    print(f"DEBUG: First animal: {batch_animals[0]}")

            if not isinstance(batch_animals, list):
                print(
                    f"ERROR: Batch data is not a list: {type(batch_animals)}"
                )
                failed_batches += 1
                continue

            if not batch_animals:
                print(f"ERROR: Batch is empty")
                failed_batches += 1
                continue

            first_animal = batch_animals[0]
            if not isinstance(first_animal, dict):
                print(f"ERROR: Animal is not a dict: {type(first_animal)}")
                failed_batches += 1
                continue

            required_fields = ["id", "name", "friends", "born_at"]
            missing_fields = [
                field
                for field in required_fields
                if field not in first_animal
            ]
            if missing_fields:
                print(f"ERROR: Missing fields: {missing_fields}")
                failed_batches += 1
                continue

            print(
                f"Batch {i+1} validation passed - sending {len(batch_animals)} animals"
            )

            success = hook.send_animals_to_home(batch_animals)

            if success:
                successful_batches += 1
                total_animals += len(batch_animals)
                print(f"Batch {i+1} loaded successfully")
            else:
                failed_batches += 1
                print(f"Batch {i+1} failed to load")

        except json.JSONDecodeError as e:
            failed_batches += 1
            print(f"JSON decode error in batch {i+1}: {str(e)}")
            print(f"   File: {batch_file}")

        except Exception as e:
            failed_batches += 1
            print(f"Batch {i+1} failed with error: {str(e)}")
            print(f"   File: {batch_file}")

        finally:
            try:
                import os

                os.remove(batch_file)
                print(f"🧹 Cleaned up {batch_file}")
            except OSError as e:
                print(f"⚠️ Could not remove {batch_file}: {e}")

    result = {
        "status": "completed",
        "total_batches": len(batch_info_list),
        "successful_batches": successful_batches,
        "failed_batches": failed_batches,
        "total_animals": total_animals,
    }

    print(f"Load Summary: {result}")
    return result


load_animals = PythonOperator(
    task_id="load_animals",
    python_callable=load_all_batches_fixed,
    dag=dag,
)


def send_success_notification(**context):
    load_result = context["task_instance"].xcom_pull(
        task_ids="load_animals"
    )

    if load_result:
        print(f"Animal ETL pipeline completed!")
        print(f"Processed {load_result.get('total_animals', 0)} animals")
        print(
            f"Successful batches: {load_result.get('successful_batches', 0)}"
        )
        print(f"Failed batches: {load_result.get('failed_batches', 0)}")
    else:
        print("Pipeline completed (no load results)")

    print(f"Execution date: {context['execution_date']}")

    return {
        "status": "success",
        "execution_date": str(context["execution_date"]),
        "load_summary": load_result,
    }


success_notification = PythonOperator(
    task_id="success_notification",
    python_callable=send_success_notification,
    dag=dag,
)


def cleanup_temp_files(**context):
    import glob
    import os
    import tempfile

    temp_dir = tempfile.gettempdir()
    run_id = context["run_id"]

    pattern = os.path.join(temp_dir, f"*{run_id}*")
    temp_files = glob.glob(pattern)

    for temp_file in temp_files:
        try:
            os.remove(temp_file)
            print(f"Cleaned up temp file: {temp_file}")
        except OSError as e:
            print(f"Could not remove temp file {temp_file}: {e}")

    print(f"Cleanup completed for run {run_id}")
    return {"cleaned_files": len(temp_files)}


cleanup = PythonOperator(
    task_id="cleanup",
    python_callable=cleanup_temp_files,
    trigger_rule=TriggerRule.ALL_DONE,
    dag=dag,
)

(
    extract_animals
    >> transform_animals
    >> load_animals
    >> success_notification
    >> cleanup
)
