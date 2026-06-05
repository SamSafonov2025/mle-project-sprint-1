# part2_dvc/scripts/publish_artifacts_to_s3.py
#
# Зеркалирует артефакты DVC из content-addressed путей (files/md5/...) в
# человекочитаемые пути в том же бакете S3, под префиксом проекта.
#
# Запускать после `dvc push`. Копия выполняется внутри бакета (без скачивания и
# повторной заливки).
#
# Источник истины по bucket + project-prefix — DVC remote URL из .dvc/config:
#   url = s3://<bucket>/<project-prefix>
# Креды и endpoint — env vars (или .env проекта как fallback):
#   S3_ENDPOINT_URL        https://storage.yandexcloud.net
#   AWS_ACCESS_KEY_ID
#   AWS_SECRET_ACCESS_KEY
#
# Использование:
#   cd part2_dvc && python scripts/publish_artifacts_to_s3.py

import configparser
import os
import sys
from pathlib import Path
from urllib.parse import urlparse

import yaml


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


# Чтение пар ключ-значение из .env
def load_dotenv(path: Path) -> dict:
    cfg = {}
    for line in path.read_text().splitlines():
        s = line.strip()
        if not s or s.startswith("#") or "=" not in s:
            continue
        k, v = s.split("=", 1)
        cfg[k.strip()] = v.strip().strip('"').strip("'")
    return cfg


# Получение значения: сначала из переменных окружения, потом из .env
def cfg_value(name: str, env_cfg: dict) -> str:
    return os.environ.get(name) or env_cfg.get(name, "")


# Чтение URL default DVC remote: s3://bucket/prefix -> (bucket, prefix)
def parse_dvc_remote(dvc_config_path: Path):
    cp = configparser.ConfigParser()
    cp.read(dvc_config_path)
    remote_name = cp.get("core", "remote")
    section = f'remote "{remote_name}"'
    url = cp.get(section, "url")
    u = urlparse(url)
    if u.scheme != "s3":
        raise ValueError(f"DVC remote URL не s3-схема: {url!r}")
    bucket = u.netloc
    prefix = u.path.strip("/")
    return bucket, prefix


def main():
    # 1. Прочитать .env как fallback для env vars (endpoint, creds)
    env_path = find_env(HERE)
    env_cfg = load_dotenv(env_path) if env_path else {}
    if env_path:
        print(f"loaded fallback env from {env_path}")

    endpoint = cfg_value("S3_ENDPOINT_URL", env_cfg)
    key = cfg_value("AWS_ACCESS_KEY_ID", env_cfg)
    secret = cfg_value("AWS_SECRET_ACCESS_KEY", env_cfg)

    # 2. Прочитать bucket + project-prefix из DVC remote (single source of truth)
    try:
        bucket, prefix = parse_dvc_remote(PART2_DVC / ".dvc" / "config")
    except (ValueError, configparser.Error, FileNotFoundError) as e:
        print(f"FATAL: не удалось прочитать DVC remote из .dvc/config: {e}",
              file=sys.stderr)
        sys.exit(1)

    missing = [n for n, v in (
        ("S3_ENDPOINT_URL", endpoint),
        ("AWS_ACCESS_KEY_ID", key),
        ("AWS_SECRET_ACCESS_KEY", secret),
    ) if not v]
    if missing:
        print(f"FATAL: не заданы переменные: {missing}", file=sys.stderr)
        sys.exit(1)

    prefix_display = f"/{prefix}" if prefix else ""
    print(f"target: s3://{bucket}{prefix_display}   endpoint={endpoint}")

    # 3. Подключение к S3 через s3fs
    try:
        import s3fs
    except ImportError:
        print("FATAL: pip install s3fs", file=sys.stderr)
        sys.exit(1)

    # 4. Прочитать dvc.lock и собрать пары (src_key, dst_key) под project-prefix
    lock_path = PART2_DVC / "dvc.lock"
    if not lock_path.exists():
        print(f"FATAL: {lock_path} не найден. Сначала запустите dvc repro.",
              file=sys.stderr)
        sys.exit(1)

    lock = yaml.safe_load(lock_path.read_text())
    prefix_part = f"{prefix}/" if prefix else ""

    pairs = []
    for stage_name, stage in lock.get("stages", {}).items():
        for out in stage.get("outs", []):
            md5 = out["md5"]
            src = f"{bucket}/{prefix_part}files/md5/{md5[:2]}/{md5[2:]}"
            dst = f"{bucket}/{prefix_part}{out['path']}"
            pairs.append((src, dst))

    if not pairs:
        print("в dvc.lock нет outs — нечего публиковать")
        return

    # 5. Серверная копия внутри бакета (без скачивания и перезаливки)
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

    # 6. Итог
    print(f"done. copied={copied}, skipped={skipped}")


if __name__ == "__main__":
    main()
