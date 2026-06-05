# mle-project-sprint-1

Финальный проект Sprint 1 курса «Инженер машинного обучения с опытом» Яндекс Практикума.

Задача — **регрессия цены квартиры** по объединённым таблицам `buildings` и `flats`:
сборка единого датасета через Airflow, очистка от выбросов и логических ошибок,
обучение базовой модели и подсчёт метрик через DVC-пайплайн с трекингом в MLflow.

## Чек-лист для ревьюера

| Что | Где |
| --- | --- |
| ETL DAG file | `part1_airlfow/dags/flats_buildings_etl.py` |
| ETL DAG function | `prepare_flats_buildings_dataset` |
| Cleaning DAG file | `part1_airlfow/dags/clean_flats_buildings.py` |
| Cleaning DAG function | `clean_flats_buildings_dataset` |
| Общие cleaning-функции | `part1_airlfow/plugins/cleaning.py` |
| DVC stage `load_data` | `part2_dvc/scripts/data.py` |
| DVC stage `fit_model` | `part2_dvc/scripts/fit.py` |
| DVC stage `evaluate_model` | `part2_dvc/scripts/evaluate.py` |
| DVC pipeline definition | `part2_dvc/dvc.yaml` |
| DVC params | `part2_dvc/params.yaml` |
| DVC lock (хеши артефактов) | `part2_dvc/dvc.lock` |
| S3 bucket (Yandex Cloud) | `s3-student-mle-20260415-5a6a70b312-freetrack` |
| Project prefix внутри бакета | `mle-project-sprint-1/` |
| Модель в S3 | `s3://s3-student-mle-20260415-5a6a70b312-freetrack/mle-project-sprint-1/models/fitted_model.pkl` |
| Метрики в S3 | `s3://s3-student-mle-20260415-5a6a70b312-freetrack/mle-project-sprint-1/cv_results/cv_res.json` |
| Датасет в S3 | `s3://s3-student-mle-20260415-5a6a70b312-freetrack/mle-project-sprint-1/data/initial_data.csv` |

## Структура

```
mle-project-sprint-1/
├── .env_template            # шаблон для .env
├── README.md
├── requirements.txt
├── notebooks/               # корневые шаблоны Practicum
├── part1_airlfow/           # этапы 1 и 2 — Airflow
│   ├── dags/
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

| DAG file | DAG function | Назначение | Output table |
| --- | --- | --- | --- |
| `part1_airlfow/dags/flats_buildings_etl.py` | `prepare_flats_buildings_dataset` | JOIN `buildings` и `flats` в единый датасет | `public.flats_buildings_raw` |
| `part1_airlfow/dags/clean_flats_buildings.py` | `clean_flats_buildings_dataset` | Дедупликация + обработка выбросов | `public.flats_buildings_clean` |

Общие функции очистки — `part1_airlfow/plugins/cleaning.py`:

- `remove_duplicate_flats` — дедупликация по `flat_id`;
- `drop_logical_impossibilities` — отсечение строк где `kitchen_area > total_area` и `living_area > total_area`;
- `drop_price_outliers(q=0.995)` — отсечение цены выше 99.5-перцентиля (placeholder-значения);
- `clip_ceiling_height(2.0, 5.0)` — высота потолков в физических пределах;
- `clean_flats_buildings` — end-to-end pipeline, склейка всех функций выше.

Эксплоратив очистки — `part1_airlfow/notebooks/eda_flats_buildings.ipynb`.

### Airflow connections

Перед запуском DAG-ов завести два project-scoped подключения. Имена не пересекаются с другими проектами курса.

| conn_id | Назначение |
| --- | --- |
| `mle_project_sprint_1_source_db` | источник (`buildings`, `flats`) |
| `mle_project_sprint_1_destination_db` | целевая БД (`flats_buildings_raw`, `flats_buildings_clean`) |

Параметры подключений (host / port / db / user / password) берутся из `.env`
(`DB_SOURCE_*` и `DB_DESTINATION_*`). В Practicum prod-окружении обе таблицы
проекта живут в личной БД, поэтому оба conn_id фактически указывают на одну и ту же БД.

## Этап 3 — DVC pipeline

DVC-пайплайн лежит в `part2_dvc/`. Стадии описаны в `part2_dvc/dvc.yaml`:

| Stage | Script | Output |
| --- | --- | --- |
| `load_data` | `part2_dvc/scripts/data.py` | `part2_dvc/data/initial_data.csv` |
| `fit_model` | `part2_dvc/scripts/fit.py` | `part2_dvc/models/fitted_model.pkl` |
| `evaluate_model` | `part2_dvc/scripts/evaluate.py` | `part2_dvc/cv_results/cv_res.json` |

Гиперпараметры — `part2_dvc/params.yaml`. Хеши артефактов — `part2_dvc/dvc.lock`.

Модель: `sklearn.linear_model.Ridge` поверх `ColumnTransformer`
(`StandardScaler` на числовых + `OneHotEncoder(drop=if_binary)` на категориальных
+ passthrough на boolean). Целевая переменная — `price`.

Метрики: 5-fold cross-validation,
`neg_mean_absolute_error` / `neg_root_mean_squared_error` / `r2`. MLflow runs
пишутся в `part2_dvc/mlruns/` — отдельный experiment `flats_buildings_baseline`,
два run-а: `fit_baseline` (логирование параметров + train R²) и `evaluate_cv`
(CV-метрики).

## S3 / Yandex Cloud Object Storage

| Параметр | Значение |
| --- | --- |
| Endpoint | `https://storage.yandexcloud.net` |
| Bucket | `s3-student-mle-20260415-5a6a70b312-freetrack` |
| Project prefix | `mle-project-sprint-1/` (внутри бакета — общий префикс всех артефактов этого проекта) |
| DVC remote | `s3_storage` → `s3://s3-student-mle-20260415-5a6a70b312-freetrack/mle-project-sprint-1` (`part2_dvc/.dvc/config`) |

Под префиксом `mle-project-sprint-1/` в бакете лежат:

```
mle-project-sprint-1/
├── files/md5/…                   # DVC content-addressed (после dvc push)
├── data/initial_data.csv         # человекочитаемая копия датасета
├── models/fitted_model.pkl       # человекочитаемая копия модели
└── cv_results/cv_res.json        # человекочитаемая копия CV-метрик
```

Дублирование «files/md5/… → человекочитаемые ключи» делает
`part2_dvc/scripts/publish_artifacts_to_s3.py`: после `dvc push` он копирует
объекты внутри бакета (server-side copy, без скачивания и повторной заливки)
из DVC-раскладки в ожидаемые пути. Это запускается один раз после каждого
успешного `dvc push`. Project-prefix позволяет держать в одном бакете
несколько проектов курса без коллизий имён.

## .env

Скопировать `.env_template` → `.env` и заполнить:

| Переменная | Назначение |
| --- | --- |
| `DB_SOURCE_HOST` / `_PORT` / `_NAME` / `_USER` / `_PASSWORD` | источник (buildings, flats) |
| `DB_DESTINATION_HOST` / `_PORT` / `_NAME` / `_USER` / `_PASSWORD` | целевая БД |
| `S3_ENDPOINT_URL` | `https://storage.yandexcloud.net` |
| `S3_BUCKET_NAME` | имя бакета Yandex Cloud (без префикса проекта) |
| `AWS_ACCESS_KEY_ID` / `AWS_SECRET_ACCESS_KEY` | креды Yandex Cloud |
| `AWS_DEFAULT_REGION` | `ru-central1` (опционально) |

`.env` в репозиторий не коммитится (`.gitignore`).

## Воспроизведение

```bash
# 1. Виртуальное окружение
python3 -m venv .venv
source .venv/bin/activate
pip install --upgrade pip wheel
pip install dvc[s3]==3.55.0 scikit-learn==1.5.2 mlflow==2.16.2 \
            pandas sqlalchemy psycopg2-binary pyyaml s3fs joblib

# 2. .env с реальными значениями (см. таблицу выше)
cp .env_template .env && $EDITOR .env

# 3. Прогон DVC pipeline (читает источник из БД, тренирует, считает метрики)
export $(grep -v '^#' .env | grep -v '^\[' | xargs)
cd part2_dvc
dvc repro

# 4. Публикация артефактов в S3
dvc push                                          # DVC native layout
python scripts/publish_artifacts_to_s3.py         # человекочитаемые копии
```

После шага 4 в Yandex Cloud-бакете под префиксом `mle-project-sprint-1/`
появятся `models/fitted_model.pkl`, `data/initial_data.csv`,
`cv_results/cv_res.json` — это и есть точки входа для ревью.
