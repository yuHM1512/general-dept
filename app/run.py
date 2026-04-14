from __future__ import annotations

import uvicorn

from app.settings import settings


def main() -> None:
    uvicorn.run(
        "app.main:app",
        host=settings.host,
        port=settings.port,
        reload=(settings.app_env == "dev"),
        log_level=settings.log_level.lower(),
    )


if __name__ == "__main__":
    main()

