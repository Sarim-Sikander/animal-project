import asyncio
import json
from typing import Any, Dict, List, Optional

import httpx
from airflow.exceptions import AirflowException
from airflow.hooks.base import BaseHook
from tenacity import retry, stop_after_attempt, wait_exponential

from config.settings import settings
from utils.exceptions import ExternalAPIException
from utils.models import AnimalDetail, PaginatedResponse


class AnimalsAPIHook(BaseHook):
    conn_name_attr = "animals_api_conn_id"
    default_conn_name = "animals_api_default"
    conn_type = "http"
    hook_name = "Animals API"

    def __init__(self, animals_api_conn_id: str = default_conn_name):
        super().__init__()
        self.animals_api_conn_id = animals_api_conn_id
        self.base_url = settings.ANIMALS_API_BASE_URL
        self.timeout = settings.ANIMALS_API_TIMEOUT

    @retry(
        stop=stop_after_attempt(settings.MAX_RETRIES),
        wait=wait_exponential(multiplier=settings.RETRY_DELAY, max=60),
    )
    def get_animals_page(self, page: int = 1) -> PaginatedResponse:
        self.log.info(f"Fetching animals page {page}")

        try:
            with httpx.Client(
                base_url=self.base_url, timeout=self.timeout
            ) as client:
                response = client.get(
                    "/animals/v1/animals", params={"page": page}
                )
                response.raise_for_status()

                data = response.json()
                return PaginatedResponse(**data)

        except httpx.HTTPStatusError as e:
            if e.response.status_code in [500, 502, 503, 504]:
                raise ExternalAPIException(
                    f"Server error: {e.response.status_code}"
                )
            elif e.response.status_code == 429:
                raise ExternalAPIException("Rate limit exceeded")
            else:
                raise ExternalAPIException(
                    f"HTTP error: {e.response.status_code}"
                )
        except Exception as e:
            raise ExternalAPIException(f"Unexpected error: {str(e)}")

    def get_all_animals(self) -> List[Dict[str, Any]]:
        self.log.info("Starting to fetch all animals")
        all_animals = []
        page = 1

        while True:
            try:
                paginated_response = self.get_animals_page(page)
                page_animals = [
                    animal.dict() for animal in paginated_response.items
                ]
                all_animals.extend(page_animals)

                self.log.info(
                    f"Fetched page {page}: {len(page_animals)} animals"
                )

                if (
                    not paginated_response.has_next
                    or len(page_animals) == 0
                ):
                    break

                page += 1

                if page > 1000:
                    self.log.warning("Reached maximum page limit (1000)")
                    break

            except Exception as e:
                self.log.error(
                    f"Failed to fetch animals page {page}: {str(e)}"
                )
                raise AirflowException(
                    f"Failed to fetch animals: {str(e)}"
                )

        self.log.info(
            f"Completed fetching all animals: {len(all_animals)} total"
        )
        return all_animals

    @retry(
        stop=stop_after_attempt(settings.MAX_RETRIES),
        wait=wait_exponential(multiplier=settings.RETRY_DELAY, max=60),
    )
    def get_animal_detail(self, animal_id: int) -> AnimalDetail:
        try:
            with httpx.Client(
                base_url=self.base_url, timeout=self.timeout
            ) as client:
                response = client.get(f"/animals/v1/animals/{animal_id}")
                response.raise_for_status()

                data = response.json()
                return AnimalDetail(**data)

        except Exception as e:
            raise ExternalAPIException(
                f"Failed to fetch animal {animal_id}: {str(e)}"
            )

    def get_animals_details_batch(
        self, animal_ids: List[int]
    ) -> List[AnimalDetail]:
        self.log.info(f"Fetching details for {len(animal_ids)} animals")

        results = []
        failed_ids = []

        for animal_id in animal_ids:
            try:
                animal_detail = self.get_animal_detail(animal_id)
                results.append(animal_detail)
            except Exception as e:
                failed_ids.append(animal_id)
                self.log.error(
                    f"Failed to fetch animal {animal_id}: {str(e)}"
                )

        if failed_ids:
            self.log.warning(
                f"Failed to fetch {len(failed_ids)} animals: {failed_ids}"
            )

        return results

    @retry(
        stop=stop_after_attempt(settings.MAX_RETRIES),
        wait=wait_exponential(multiplier=settings.RETRY_DELAY, max=60),
    )
    def send_animals_to_home(self, animals: List[Dict[str, Any]]) -> bool:
        if len(animals) > 100:
            raise AirflowException(
                f"Batch size too large: {len(animals)} > 100"
            )

        self.log.info(f"Sending {len(animals)} animals to home endpoint")

        if animals:
            try:
                if isinstance(animals, list) and isinstance(
                    animals[0], dict
                ):
                    sample_data = json.dumps(
                        animals[0], indent=2, default=str
                    )
                    self.log.info(f"Sample animal (first): {sample_data}")
                else:
                    self.log.error(
                        f"Invalid data type: {type(animals)} containing {type(animals[0]) if animals else 'empty'}"
                    )
                    return False
            except Exception as e:
                self.log.error(f"Error logging sample data: {e}")
                self.log.info(f"Raw animals data type: {type(animals)}")
                self.log.info(f"Raw animals content: {str(animals)[:500]}")
                return False

        try:
            payload_json = json.dumps(animals, default=str)
            self.log.info(f"Payload size: {len(payload_json)} characters")
        except Exception as e:
            self.log.error(f"Cannot serialize animals to JSON: {e}")
            return False

        try:
            with httpx.Client(
                base_url=self.base_url, timeout=self.timeout
            ) as client:
                response = client.post("/animals/v1/home", json=animals)

                self.log.info(
                    f"POST response status: {response.status_code}"
                )
                self.log.info(
                    f"POST response headers: {dict(response.headers)}"
                )

                if response.status_code >= 400:
                    self.log.error(
                        f"HTTP Error {response.status_code}: {response.text}"
                    )
                    response.raise_for_status()

                try:
                    response_data = response.json()
                    self.log.info(f"Response data: {response_data}")
                except:
                    self.log.info(f"Response text: {response.text}")

                self.log.info(
                    f"Successfully sent {len(animals)} animals to home"
                )
                return True

        except httpx.HTTPStatusError as e:
            error_details = {
                "status_code": e.response.status_code,
                "response_text": e.response.text,
                "request_url": str(e.request.url),
                "request_method": e.request.method,
            }
            self.log.error(f"HTTP Status Error: {error_details}")
            raise ExternalAPIException(
                f"HTTP {e.response.status_code} error sending animals to home",
                error_code="HTTP_ERROR",
                details=error_details,
            ) from e

        except Exception as e:
            self.log.error(f"Unexpected error: {str(e)}")
            raise ExternalAPIException(
                f"Unexpected error while sending animals to home: {str(e)}",
                error_code="UNEXPECTED_ERROR",
                details={"error": str(e)},
            ) from e

    def health_check(self) -> bool:
        try:
            with httpx.Client(
                base_url=self.base_url, timeout=10
            ) as client:
                response = client.get("/animals/v1/animals?page=1")
                return response.status_code == 200
        except Exception:
            return False
