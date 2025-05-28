from datetime import datetime, timezone
from typing import Any, Dict, List, Optional, Union

from dateutil import parser as date_parser
from pydantic import BaseModel, Field, validator


class AnimalListItem(BaseModel):
    id: int
    name: str
    species: Optional[str] = "Unknown"
    born_at: Optional[Union[str, int, float]] = None

    @validator("born_at", pre=True)
    def normalize_born_at(cls, v):
        if v is None:
            return None
        if isinstance(v, (int, float)):
            try:
                timestamp = v
                if timestamp > 1e10:
                    timestamp = timestamp / 1000
                dt = datetime.fromtimestamp(timestamp)
                return dt.isoformat()
            except (ValueError, OSError) as e:
                print(f"Warning: Could not convert timestamp {v}: {e}")
                return None
        return str(v) if v is not None else None


class AnimalDetail(BaseModel):
    id: int
    name: str
    species: Optional[str] = "Unknown"
    friends: Union[str, List[str]] = Field(default="")
    born_at: Optional[Union[str, int, float]] = None
    habitat: Optional[str] = None
    diet: Optional[str] = None
    conservation_status: Optional[str] = None

    @validator("born_at", pre=True)
    def normalize_born_at(cls, v):
        if v is None:
            return None
        if isinstance(v, (int, float)):
            try:
                timestamp = v
                if timestamp > 1e10:
                    timestamp = timestamp / 1000
                dt = datetime.fromtimestamp(timestamp)
                return dt.isoformat()
            except (ValueError, OSError) as e:
                print(f"Warning: Could not convert timestamp {v}: {e}")
                return None
        return str(v) if v is not None else None

    @validator("friends", pre=True)
    def normalize_friends(cls, v):
        if v is None:
            return ""
        if isinstance(v, list):
            return ",".join(str(item) for item in v if item is not None)
        return str(v) if v is not None else ""


class TransformedAnimal(BaseModel):
    id: int
    name: str
    species: Optional[str] = "Unknown"
    friends: List[str] = Field(default_factory=list)
    born_at: Optional[datetime] = None
    habitat: Optional[str] = None
    diet: Optional[str] = None
    conservation_status: Optional[str] = None

    @validator("friends", pre=True, always=True)
    def parse_friends(cls, v: Union[str, List[str], None]) -> List[str]:
        try:
            if v is None or v == "":
                return []
            if isinstance(v, str):
                if v.strip() == "":
                    return []
                friends = [
                    friend.strip()
                    for friend in v.split(",")
                    if friend.strip()
                ]
                return friends
            if isinstance(v, list):
                return [
                    str(item).strip()
                    for item in v
                    if item is not None and str(item).strip()
                ]
            return [str(v).strip()] if str(v).strip() else []
        except Exception as e:
            print(f"Warning: Error parsing friends '{v}': {e}")
            return []

    @validator("born_at", pre=True)
    def parse_born_at(
        cls, v: Optional[Union[str, int, float]]
    ) -> Optional[datetime]:
        if v is None:
            return None

        try:
            if isinstance(v, (int, float)):
                timestamp = v
                if timestamp == 0:
                    return None
                if timestamp > 1e10:
                    timestamp = timestamp / 1000
                if timestamp > 2147483647:
                    print(
                        f"Warning: Timestamp {v} seems too large, skipping"
                    )
                    return None
                return datetime.fromtimestamp(
                    timestamp, tz=timezone.utc
                ).replace(tzinfo=None)

            if isinstance(v, str):
                v_stripped = v.strip()
                if not v_stripped or v_stripped.lower() in [
                    "null",
                    "none",
                    "",
                ]:
                    return None

                try:
                    parsed_date = date_parser.parse(v_stripped)

                    if parsed_date.tzinfo is None:
                        return parsed_date
                    else:
                        utc_date = parsed_date.astimezone(timezone.utc)
                        return utc_date.replace(tzinfo=None)

                except (ValueError, TypeError) as e:
                    print(
                        f"Warning: Could not parse date string '{v}': {e}"
                    )
                    return None

            return None

        except (ValueError, TypeError, OSError) as e:
            print(
                f"Warning: Could not parse born_at value '{v}' (type: {type(v)}): {e}"
            )
            return None

    @validator("name")
    def validate_name(cls, v):
        if not v or not str(v).strip():
            raise ValueError("Name cannot be empty")
        return str(v).strip()

    @validator("id")
    def validate_id(cls, v):
        if v is None or v < 0:
            raise ValueError(f"Invalid ID: {v}")
        return int(v)


class PaginatedResponse(BaseModel):
    items: List[AnimalListItem]
    page: int
    total_pages: int
    has_next: bool = False

    def __init__(self, **data):
        raw_items = data.get("items", [])
        processed_items = []
        failed_items = []

        for i, item in enumerate(raw_items):
            try:
                if "species" not in item or item["species"] is None:
                    item["species"] = "Unknown"
                processed_items.append(AnimalListItem(**item))
            except Exception as e:
                failed_items.append(
                    {"index": i, "item": item, "error": str(e)}
                )
                print(
                    f"Warning: Failed to parse item {i}: {item}. Error: {e}"
                )
                continue

        if failed_items:
            print(
                f"⚠️  Failed to parse {len(failed_items)} items out of {len(raw_items)}"
            )

        super().__init__(
            items=processed_items,
            page=data.get("page", 1),
            total_pages=data.get("total_pages", 1),
            has_next=data.get("page", 1) < data.get("total_pages", 1),
        )
