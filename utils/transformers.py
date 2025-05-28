from typing import Any, Dict, List

from utils.exceptions import DataTransformationException
from utils.models import AnimalDetail, TransformedAnimal


class AnimalDataTransformer:
    @staticmethod
    def transform_animal(animal_detail: AnimalDetail) -> TransformedAnimal:
        try:
            return TransformedAnimal(**animal_detail.dict())
        except Exception as e:
            print(f"Transform failed for animal ID {animal_detail.id}:")
            print(f"   Name: {getattr(animal_detail, 'name', 'N/A')}")
            print(
                f"   Species: {getattr(animal_detail, 'species', 'N/A')}"
            )
            print(
                f"   Friends: {getattr(animal_detail, 'friends', 'N/A')}"
            )
            print(
                f"   Born_at: {getattr(animal_detail, 'born_at', 'N/A')}"
            )
            print(f"   Error: {str(e)}")

            raise DataTransformationException(
                f"Failed to transform animal {animal_detail.id}",
                error_code="TRANSFORMATION_FAILED",
                details={
                    "animal_id": animal_detail.id,
                    "animal_data": animal_detail.dict(),
                    "error": str(e),
                },
            ) from e

    @staticmethod
    def transform_animals_batch(
        animal_details: List[AnimalDetail],
    ) -> List[TransformedAnimal]:
        print(f"Transforming batch of {len(animal_details)} animals")

        transformed_animals = []
        failed_transformations = []
        failure_reasons = {}

        for i, animal_detail in enumerate(animal_details):
            try:
                transformed_animal = (
                    AnimalDataTransformer.transform_animal(animal_detail)
                )
                transformed_animals.append(transformed_animal)
            except DataTransformationException as e:
                animal_id = getattr(animal_detail, "id", f"index_{i}")
                failed_transformations.append(animal_id)

                error_msg = str(e)
                if "born_at" in error_msg.lower():
                    failure_reasons.setdefault("date_parsing", []).append(
                        animal_id
                    )
                elif "friends" in error_msg.lower():
                    failure_reasons.setdefault(
                        "friends_parsing", []
                    ).append(animal_id)
                elif "validation" in error_msg.lower():
                    failure_reasons.setdefault(
                        "validation_error", []
                    ).append(animal_id)
                else:
                    failure_reasons.setdefault("unknown", []).append(
                        animal_id
                    )

                print(f"Skipping animal {animal_id}: {str(e)}")

        if failed_transformations:
            print(f"Transformation Summary:")
            print(f"   Total processed: {len(animal_details)}")
            print(f"   Successful: {len(transformed_animals)}")
            print(f"   Failed: {len(failed_transformations)}")
            print(f"   Failure breakdown:")
            for reason, ids in failure_reasons.items():
                print(f"     {reason}: {len(ids)} animals")
                if len(ids) <= 5:
                    print(f"       IDs: {ids}")
                else:
                    print(
                        f"       IDs: {ids[:5]} ... (and {len(ids)-5} more)"
                    )

        return transformed_animals

    @staticmethod
    def to_api_format(
        transformed_animals: List[TransformedAnimal],
    ) -> List[Dict[str, Any]]:
        result = []

        for i, animal in enumerate(transformed_animals):
            try:
                api_animal = {
                    "id": animal.id,
                    "name": animal.name,
                    "friends": animal.friends,
                    "born_at": None,
                }

                if animal.born_at:
                    api_animal["born_at"] = (
                        animal.born_at.isoformat() + "Z"
                    )

                result.append(api_animal)

            except Exception as e:
                print(
                    f"Failed to convert animal {i} to API format: {str(e)}"
                )
                print(f"   Animal data: {animal}")

        print(f"Converted {len(result)} animals to API format")
        return result

    @staticmethod
    def to_dict_list(
        transformed_animals: List[TransformedAnimal],
    ) -> List[Dict[str, Any]]:
        return AnimalDataTransformer.to_api_format(transformed_animals)
