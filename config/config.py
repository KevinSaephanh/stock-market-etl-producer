from pydantic_settings import BaseSettings


class Settings(BaseSettings):
    ENV: str
    ALPHA_VANTAGE_API_KEY: str
    KAFKA_TOPIC: str
    KAFKA_BOOTSTRAP_SERVERS: str
    KAFKA_USERNAME: str
    KAFKA_PASSWORD: str

    class Config:
        env_file = ".env"


settings = Settings()
