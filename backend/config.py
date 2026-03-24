from pathlib import Path
from pydantic_settings import BaseSettings

# Project root = one level above backend/
_ROOT = Path(__file__).resolve().parent.parent


class Settings(BaseSettings):
    # Neo4j connection — NEO4J_USERNAME (AuraDB style) takes priority over NEO4J_USER
    neo4j_uri: str = "bolt://localhost:7687"
    neo4j_user: str = "neo4j"          # fallback: NEO4J_USER
    neo4j_username: str = ""            # preferred: NEO4J_USERNAME (AuraDB)
    neo4j_password: str = "metaengine123"
    neo4j_database: str = ""            # set for AuraDB named databases

    # Local Docker Neo4j fallback (used when AuraDB is unreachable)
    neo4j_local_uri: str = "bolt://localhost:7687"
    neo4j_local_user: str = "neo4j"
    neo4j_local_password: str = "metaengine123"

    openai_api_key: str = ""
    repo_scan_root: str = str(_ROOT / "sample_repo")

    model_config = {
        "env_file": str(_ROOT / ".env"),
        "case_sensitive": False,
        "extra": "ignore",
    }

    @property
    def effective_user(self) -> str:
        """Return NEO4J_USERNAME when set (AuraDB), otherwise NEO4J_USER."""
        return self.neo4j_username or self.neo4j_user


settings = Settings()

