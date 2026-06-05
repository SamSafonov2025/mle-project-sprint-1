# part1_airlfow/dags/clean_flats_buildings.py
#
# DAG очистки: читает flats_buildings_raw, чистит, пишет flats_buildings_clean.
#
# Использует функции из part1_airlfow/plugins/cleaning.py через стандартный
# импорт (папка plugins в Airflow добавляется в sys.path).
#
# Telegram: пропуск (нет bot token в окружении).
from __future__ import annotations

import pendulum
import pandas as pd
from airflow.decorators import dag, task
from airflow.providers.postgres.hooks.postgres import PostgresHook

from cleaning import clean_flats_buildings


SOURCE_CONN = "mle_project_sprint_1_source_db"
DESTINATION_CONN = "mle_project_sprint_1_destination_db"
SOURCE_TABLE = "flats_buildings_raw"
DESTINATION_TABLE = "flats_buildings_clean"


# Заглушка для будущей интеграции с Telegram
def telegram_notify(context):
    return None


@dag(
    dag_id="clean_flats_buildings_dataset",
    description="Cleaning: dedup + обработка выбросов в flats_buildings_raw",
    schedule="@once",
    start_date=pendulum.datetime(2026, 6, 1, tz="UTC"),
    catchup=False,
    max_active_runs=1,  # сериализуем запуски, чтобы не было гонок на destination-таблице
    tags=["sprint-1", "etl", "real-estate", "cleaning"],
    default_args={
        "owner": "airflow",
        "on_failure_callback": telegram_notify,
        "on_success_callback": telegram_notify,
    },
)
def clean_flats_buildings_dataset():
    @task()
    def create_table() -> str:
        # 1. Создать целевую таблицу для очищенных данных
        # PRIMARY KEY на flat_id здесь корректен, потому что после dedupe
        # flat_id уникален.
        hook = PostgresHook(postgres_conn_id=DESTINATION_CONN)
        ddl = f"""
            DROP TABLE IF EXISTS public.{DESTINATION_TABLE} CASCADE;
            CREATE TABLE public.{DESTINATION_TABLE} (
                flat_id            BIGINT PRIMARY KEY,
                building_id        BIGINT NOT NULL,
                floor              BIGINT,
                kitchen_area       DOUBLE PRECISION,
                living_area        DOUBLE PRECISION,
                rooms              BIGINT,
                is_apartment       BOOLEAN,
                studio             BOOLEAN,
                total_area         DOUBLE PRECISION,
                price              BIGINT,
                build_year         BIGINT,
                building_type_int  BIGINT,
                latitude           DOUBLE PRECISION,
                longitude          DOUBLE PRECISION,
                ceiling_height     DOUBLE PRECISION,
                flats_count        BIGINT,
                floors_total       BIGINT,
                has_elevator       BOOLEAN
            );
            CREATE INDEX idx_{DESTINATION_TABLE}_building_id
                ON public.{DESTINATION_TABLE} (building_id);
        """
        hook.run(ddl)
        return DESTINATION_TABLE

    @task()
    def extract_and_clean() -> list[tuple]:
        # 2. Прочитать сырой датасет и применить очистку
        # Чтение и очистка объединены в одну задачу, чтобы не передавать
        # большой DataFrame через XCom — отдаём уже компактные кортежи.
        hook = PostgresHook(postgres_conn_id=SOURCE_CONN)
        df = hook.get_pandas_df(f"SELECT * FROM public.{SOURCE_TABLE}")
        print(f"raw: {len(df)} rows, cols={list(df.columns)}")

        # 3. Применить функции очистки из plugins/cleaning.py
        cleaned = clean_flats_buildings(df)
        print(f"clean: {len(cleaned)} rows  (dropped {len(df) - len(cleaned)})")

        # 4. Привести порядок колонок к порядку DDL для последующего insert_rows
        cols_order = [
            "flat_id", "building_id", "floor", "kitchen_area", "living_area",
            "rooms", "is_apartment", "studio", "total_area", "price",
            "build_year", "building_type_int", "latitude", "longitude",
            "ceiling_height", "flats_count", "floors_total", "has_elevator",
        ]
        cleaned = cleaned[cols_order]
        # 5. Преобразовать в нативные Python-типы для драйвера psycopg2
        return [tuple(r) for r in cleaned.itertuples(index=False, name=None)]

    @task()
    def load(rows: list[tuple], table: str) -> int:
        # 6. Загрузка в destination-таблицу
        hook = PostgresHook(postgres_conn_id=DESTINATION_CONN)
        target_fields = [
            "flat_id", "building_id", "floor", "kitchen_area", "living_area",
            "rooms", "is_apartment", "studio", "total_area", "price",
            "build_year", "building_type_int", "latitude", "longitude",
            "ceiling_height", "flats_count", "floors_total", "has_elevator",
        ]
        hook.insert_rows(
            table=f"public.{table}",
            rows=rows,
            target_fields=target_fields,
            commit_every=5000,
        )
        # 7. Верификация: посчитать строки в таблице
        n = hook.get_first(f"SELECT COUNT(*) FROM public.{table}")[0]
        print(f"loaded {n} rows into public.{table}")
        return n

    table = create_table()
    rows = extract_and_clean()
    load(rows, table)


clean_flats_buildings_dataset()
