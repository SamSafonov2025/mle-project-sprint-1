# part2_dvc/scripts/data.py
import os
import sys
from pathlib import Path

import pandas as pd
import yaml
from sqlalchemy import create_engine


# Поиск файла .env вверх по дереву директорий
def find_env(start: Path) -> Path:
    p = start.resolve()
    while p != p.parent:
        if (p / ".env").exists():
            return p / ".env"
        p = p.parent
    raise FileNotFoundError("файл .env не найден выше по дереву")


# Чтение пар ключ-значение из .env (без побочных эффектов на окружение процесса)
def load_dotenv(path: Path) -> dict:
    cfg = {}
    for line in path.read_text().splitlines():
        s = line.strip()
        if not s or s.startswith("#") or "=" not in s:
            continue
        k, v = s.split("=", 1)
        cfg[k.strip()] = v.strip().strip('"').strip("'")
    return cfg


def get_data():
    # 1. Прочитать гиперпараметры
    here = Path(__file__).resolve().parent
    with open(here.parent / "params.yaml", "r") as fd:
        params = yaml.safe_load(fd)

    # 2. Подключение к БД
    env = load_dotenv(find_env(here))
    host = env["DB_DESTINATION_HOST"]
    port = env["DB_DESTINATION_PORT"]
    db = env["DB_DESTINATION_NAME"]
    username = env["DB_DESTINATION_USER"]
    password = env["DB_DESTINATION_PASSWORD"]

    conn = create_engine(
        f"postgresql://{username}:{password}@{host}:{port}/{db}"
    )

    # 3. Чтение
    table = params["source_table"]
    data = pd.read_sql(
        f'select * from public."{table}"',
        conn,
        index_col=params["index_col"],
    )
    print(f"data shape = {data.shape}")

    # 4. Сохранение
    out_dir = here.parent / "data"
    os.makedirs(out_dir, exist_ok=True)
    data.to_csv(out_dir / "initial_data.csv")


if __name__ == "__main__":
    get_data()
