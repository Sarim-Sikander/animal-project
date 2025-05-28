from typing import Optional

from pydantic_settings import BaseSettings


class Settings(BaseSettings):
    ANIMALS_API_BASE_URL: str = "http://localhost:3123"
    ANIMALS_API_TIMEOUT: int = 30

    BATCH_SIZE: int = 100
    MAX_CONCURRENT_REQUESTS: int = 10
    MAX_RETRIES: int = 3
    RETRY_DELAY: float = 1.0

    DAG_OWNER: str = "Sarim Sikander"
    DAG_EMAIL: list = ["sarimsikander24@gmail.com"]
    DAG_SCHEDULE: str = "0 2 * * *"
    DAG_CATCHUP: bool = False

    LOG_LEVEL: str = "INFO"

    class Config:
        env_file = ".env"
        case_sensitive = True


settings = Settings()
