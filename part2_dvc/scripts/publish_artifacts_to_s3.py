# part2_dvc/scripts/publish_artifacts_to_s3.py
#
# Зеркалирует артефакты DVC из content-addressed путей (files/md5/...) в
# человекочитаемые пути в том же бакете S3, под префиксом проекта.
#
# Запускать после `dvc push`. Копия выполняется внутри бакета (без скачивания и
# повторной заливки).
#
# Конфигурация (через переменные окружения, fallback — .env проекта):
#   S3_ENDPOINT_URL        https://storage.yandexcloud.net
#   S3_BUCKET_NAME         имя бакета Yandex Cloud
#   AWS_ACCESS_KEY_ID
#   AWS_SECRET_ACCESS_KEY
#
# Использование:
#   cd part2_dvc && python scripts/publish_artifacts_to_s3.py

import os
import sys
from pathlib import Path

import yaml


# Префикс проекта внутри бакета. Должен совпадать с суффиксом
# url в part2_dvc/.dvc/config (s3://<bucket>/<PROJECT_PREFIX>).
PROJECT_PREFIX = "mle-project-sprint-1"


HERE = Path(__file__).resolve().parent
PART2_DVC = HERE.parent


# Поиск файла .env вверх по дереву директорий
def find_env(start: Path):
    p = start.resolve()
    while p != p.parent:
        if (p / ".env").exists():
            return p / ".env"
        p = p.parent
    return None


# Чтение пар ключ-значение из .env (пропускает INI-секции вида [default])
def load_dotenv(path: Path) -> dict:
    cfg = {}
    for line in path.read_text().splitlines():
        s = line.strip()
        if not s or s.startswith("#") or s.startswith("[") or "=" not in s:
            continue
        k, v = s.split("=", 1)
        cfg[k.strip()] = v.strip().strip('"').strip("'")
    return cfg


# Получение значения: сначала из переменных окружения, потом из .env
def cfg_value(name: str, env_cfg: dict) -> str:
    return os.environ.get(name) or env_cfg.get(name, "")


def main():
    # 1. Прочитать конфигурацию (env + .env fallback)
    env_path = find_env(HERE)
    env_cfg = load_dotenv(env_path) if env_path else {}
    if env_path:
        print(f"loaded fallback env from {env_path}")

    endpoint = cfg_value("S3_ENDPOINT_URL", env_cfg)
    bucket = cfg_value("S3_BUCKET_NAME", env_cfg)
    key = cfg_value("AWS_ACCESS_KEY_ID", env_cfg)
    secret = cfg_value("AWS_SECRET_ACCESS_KEY", env_cfg)

    missing = [n for n, v in (
        ("S3_ENDPOINT_URL", endpoint), ("S3_BUCKET_NAME", bucket),
        ("AWS_ACCESS_KEY_ID", key), ("AWS_SECRET_ACCESS_KEY", secret),
    ) if not v]
    if missing:
        print(f"FATAL: не заданы переменные: {missing}", file=sys.stderr)
        sys.exit(1)

    print(f"target: s3://{bucket}/{PROJECT_PREFIX}   endpoint={endpoint}")

    # 2. Подключение к S3 через s3fs
    try:
        import s3fs
    except ImportError:
        print("FATAL: pip install s3fs", file=sys.stderr)
        sys.exit(1)

    # 3. Прочитать dvc.lock и собрать пары (src_key, dst_key) под project-prefix
    lock_path = PART2_DVC / "dvc.lock"
    if not lock_path.exists():
        print(f"FATAL: {lock_path} не найден. Сначала запустите dvc repro.", file=sys.stderr)
        sys.exit(1)

    lock = yaml.safe_load(lock_path.read_text())

    pairs = []
    for stage_name, stage in lock.get("stages", {}).items():
        for out in stage.get("outs", []):
            md5 = out["md5"]
            src = f"{bucket}/{PROJECT_PREFIX}/files/md5/{md5[:2]}/{md5[2:]}"
            dst = f"{bucket}/{PROJECT_PREFIX}/{out['path']}"
            pairs.append((src, dst))

    if not pairs:
        print("в dvc.lock нет outs — нечего публиковать")
        return

    # 4. Серверная копия внутри бакета (без скачивания и перезаливки)
    fs = s3fs.S3FileSystem(endpoint_url=endpoint, key=key, secret=secret)
    copied = skipped = 0
    for src, dst in pairs:
        if not fs.exists(src):
            print(f"  SKIP объекта нет в remote: s3://{src}")
            skipped += 1
            continue
        print(f"  copy: s3://{src}\n     -> s3://{dst}")
        fs.copy(src, dst)
        copied += 1

    # 5. Итог
    print(f"done. copied={copied}, skipped={skipped}")


if __name__ == "__main__":
    main()
