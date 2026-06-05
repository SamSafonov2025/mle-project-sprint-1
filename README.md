# mle-project-sprint-1

Финальный проект Sprint 1 курса «Инженер машинного обучения с опытом» Яндекс Практикума.

Задача — **регрессия цены квартиры** по объединённым таблицам `buildings` и `flats`:
сборка единого датасета через Airflow, очистка от выбросов и логических ошибок,
обучение базовой модели и подсчёт метрик через DVC-пайплайн с трекингом в MLflow.

## Структура

```
mle-project-sprint-1/
├── .env_template            # шаблон для .env (placeholder-ы заполняются локально)
├── .gitignore
├── README.md
├── requirements.txt
├── notebooks/               # корневые шаблоны Practicum (delete_table, template_notebook)
├── part1_airlfow/           # этап 1 и 2 — Airflow
│   ├── dags/
│   ├── logs/
│   ├── notebooks/
│   └── plugins/
└── part2_dvc/               # этап 3 — DVC pipeline
    ├── cv_results/
    ├── data/
    ├── mlruns/
    ├── models/
    ├── notebooks/
    ├── scripts/
    ├── dvc.yaml
    ├── dvc.lock
    └── params.yaml
```

## Этапы 1 и 2 — Airflow DAGs

| DAG file | DAG id / function | Назначение | Output table |
| --- | --- | --- | --- |
| `part1_airlfow/dags/flats_buildings_etl.py` | `prepare_flats_buildings_dataset` | JOIN `buildings` и `flats` в единый датасет | `public.flats_buildings_raw` |
| `part1_airlfow/dags/clean_flats_buildings.py` | `clean_flats_buildings_dataset` | Дедупликация + обработка выбросов | `public.flats_buildings_clean` |

Общие функции очистки — `part1_airlfow/plugins/cleaning.py`
(`remove_duplicate_flats`, `drop_logical_impossibilities`, `drop_price_outliers`,
`clip_ceiling_height`, end-to-end `clean_flats_buildings`).

Эксплоратив очистки — `part1_airlfow/notebooks/eda_flats_buildings.ipynb`.

### Airflow connections

Перед запуском DAG-ов в Airflow нужно завести два подключения:

| conn_id | Назначение |
| --- | --- |
| `source_db` | источник данных (`buildings`, `flats`) |
| `destination_db` | целевая БД (raw + clean таблицы) |

Хост / порт / логин / пароль берутся из `.env` (`DB_SOURCE_*`, `DB_DESTINATION_*`).

## Этап 3 — DVC pipeline

DVC-пайплайн лежит в `part2_dvc/`. Стадии описаны в `part2_dvc/dvc.yaml`:

| Stage | Script | Output |
| --- | --- | --- |
| `load_data` | `part2_dvc/scripts/data.py` | `part2_dvc/data/initial_data.csv` |
| `fit_model` | `part2_dvc/scripts/fit.py` | `part2_dvc/models/fitted_model.pkl` |
| `evaluate_model` | `part2_dvc/scripts/evaluate.py` | `part2_dvc/cv_results/cv_res.json` |

Параметры — `part2_dvc/params.yaml`. Hash-и артефактов — `part2_dvc/dvc.lock`.
Модель: `sklearn.linear_model.Ridge` поверх `ColumnTransformer`
(`StandardScaler` + `OneHotEncoder`). Метрики — 5-fold cross-validation:
`neg_mean_absolute_error`, `neg_root_mean_squared_error`, `r2`. MLflow runs пишутся
в `part2_dvc/mlruns/` (отдельный experiment, два run-а: `fit_baseline` и `evaluate_cv`).

### Запуск пайплайна

```bash
source .venv/bin/activate
export $(grep -v '^#' .env | grep -v '^\[' | xargs)
cd part2_dvc
dvc repro
dvc push
python scripts/publish_artifacts_to_s3.py
```

`scripts/publish_artifacts_to_s3.py` дублирует объекты внутри бакета:
из DVC content-addressed (`files/md5/...`) — в человекочитаемые ключи
(`models/fitted_model.pkl`, `data/initial_data.csv`, `cv_results/cv_res.json`).
Копирование выполняется на стороне S3 (без скачивания и повторной заливки).

## S3 bucket

Артефакты публикуются в Yandex Cloud Object Storage.

| Параметр | Значение |
| --- | --- |
| Endpoint | `https://storage.yandexcloud.net` |
| Bucket | `s3-student-mle-20260415-5a6a70b312-freetrack` (имя в `.env` → `S3_BUCKET_NAME`) |
| Project prefix | `mle-project-sprint-1/` (внутри бакета — общий префикс всех артефактов проекта) |
| DVC remote | `s3_storage` → `s3://<bucket>/mle-project-sprint-1` (`part2_dvc/.dvc/config`) |

После `dvc push` + `publish_artifacts_to_s3.py` reviewer-friendly артефакты доступны
по ключам:

- модель: `s3://<bucket>/mle-project-sprint-1/models/fitted_model.pkl`
- данные: `s3://<bucket>/mle-project-sprint-1/data/initial_data.csv`
- метрики: `s3://<bucket>/mle-project-sprint-1/cv_results/cv_res.json`

DVC native объекты лежат рядом, по `s3://<bucket>/mle-project-sprint-1/files/md5/…`.
Project-prefix даёт возможность держать в одном бакете несколько спринтов
без коллизий имён.

## .env

Скопировать `.env_template` → `.env` и заполнить:

- `DB_SOURCE_*` — подключение к источнику данных;
- `DB_DESTINATION_*` — подключение к целевой БД;
- `S3_BUCKET_NAME`, `AWS_ACCESS_KEY_ID`, `AWS_SECRET_ACCESS_KEY` — Yandex Cloud;
- опционально: `S3_ENDPOINT_URL=https://storage.yandexcloud.net`,
  `AWS_DEFAULT_REGION=ru-central1`.

`.env` в репозиторий не коммитится (`.gitignore`).

## Версионирование

```bash
# изменения в коде/параметрах
dvc repro                       # пересобрать пайплайн
git add dvc.lock dvc.yaml params.yaml scripts/
git commit -m "..."
dvc push                        # артефакты в S3
git push origin main            # код в GitHub
```
