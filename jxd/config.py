from pydantic import BaseSettings, Field


class Settings(BaseSettings):
    sportmonks_api_token: str = Field("", env="SPORTMONKS_API_TOKEN")
    sportmonks_base_url: str = Field(
        "https://api.sportmonks.com/v3/football", env="SPORTMONKS_BASE_URL"
    )
    database_url: str = Field("sqlite:///data/jxd.sqlite", env="DATABASE_URL")
    requests_per_hour: int = Field(3500, env="REQUESTS_PER_HOUR")
    log_level: str = Field("INFO", env="LOG_LEVEL")
    bookmaker_id: int = Field(2, env="BOOKMAKER_ID")  # 2 = Bet365 on SportMonks
    use_filters_populate: bool = Field(True, env="USE_FILTERS_POPULATE")
    default_league_ids: str = Field(
        "8,9,72,82,181,208,244,271,301,384,387,444,453,462,501,564,567,573,591,600",
        env="LEAGUE_IDS",
    )

    class Config:
        env_file = ".env"
        env_file_encoding = "utf-8"


def league_ids_from_settings(settings: Settings) -> list[int]:
    """
    Parse comma-separated league IDs into a list of ints.
    """
    raw = settings.default_league_ids
    if not raw:
        return []
    ids = []
    for piece in raw.split(","):
        piece = piece.strip()
        if not piece:
            continue
        try:
            ids.append(int(piece))
        except ValueError:
            continue
    return ids


settings = Settings()
