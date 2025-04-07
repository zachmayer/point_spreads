"""Settings management for the point_spreads package."""

from pathlib import Path
from typing import Any, ClassVar, Optional

from pydantic import Field, field_validator
from pydantic_settings import BaseSettings, SettingsConfigDict


class Settings(BaseSettings):
    """Application settings with configuration from environment variables.

    Note: Settings are loaded once at application startup and cannot be changed.
    """

    # Flag to prevent reinitialization
    _initialized: ClassVar[bool] = False

    # API Keys - Optional for type checking but validated in __init__
    google_api_key: Optional[str] = Field(
        default=None,
        description="API key for Google AI services",
    )

    # Cache settings
    cache_dir: Path = Field(
        default_factory=lambda: Path(__file__).parent.parent.parent / ".cache",
        description="Directory to cache HTML content",
    )

    cache_ttl: int = Field(
        24 * 60 * 60,  # 24 hours in seconds
        description="Time to live for cached items in seconds",
    )

    model_config = SettingsConfigDict(
        env_prefix="POINT_SPREADS_",
        env_file=".env",
        env_file_encoding="utf-8",
        case_sensitive=False,
        extra="ignore",
        validate_default=True,
    )

    @field_validator("cache_dir")
    def create_cache_dir(cls, v: Path) -> Path:
        """Ensure the cache directory exists."""
        v.mkdir(exist_ok=True)
        return v

    def __init__(self, **data: Any) -> None:
        """Initialize settings only once."""
        if Settings._initialized:
            raise RuntimeError("Settings have already been initialized and cannot be changed")

        super().__init__(**data)

        # Validate required fields after initialization
        if not self.google_api_key:
            raise ValueError("No Google API key found. Please set POINT_SPREADS_GOOGLE_API_KEY environment variable.")

        Settings._initialized = True


# Create a global settings instance that loads from environment variables once
try:
    settings = Settings()
except ValueError as e:
    # Re-raise with a clearer error message
    raise ValueError(f"{e} Set this in your environment or .env file before importing this module.") from e
