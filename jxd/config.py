from pydantic import BaseSettings, Field


class Settings(BaseSettings):
    sportmonks_api_token: str = Field("", env="SPORTMONKS_API_TOKEN")
    database_url: str = Field("sqlite:///data/jxd.sqlite", env="DATABASE_URL")
    requests_per_hour: int = Field(3500, env="REQUESTS_PER_HOUR")
    log_level: str = Field("INFO", env="LOG_LEVEL")

    class Config:
        env_file = ".env"
        env_file_encoding = "utf-8"


settings = Settings()
