# part1_airlfow/dags/flats_buildings_etl.py
#
# ETL DAG: собирает таблицы buildings и flats в один датасет.
#
# Читает источник через PostgresHook(conn_id='source_db'), делает SQL JOIN
# buildings.id = flats.building_id и пишет результат в таблицу
# public.flats_buildings_raw через PostgresHook(conn_id='destination_db').
#
# Telegram-уведомления: ПРОПУЩЕНО (нет bot token в окружении). Колбэки
# on_success/on_failure подключены как пустые заглушки — заменить тело
# telegram_notify при появлении токена.
#
# Connections в Airflow (одноразовая настройка в UI или CLI):
#   source_db       -> подключение к источнику (buildings, flats)
#   destination_db  -> подключение к целевой БД (flats_buildings_raw)
from __future__ import annotations

import pendulum
from airflow.decorators import dag, task
from airflow.providers.postgres.hooks.postgres import PostgresHook


SOURCE_CONN = "source_db"
DESTINATION_CONN = "destination_db"
DESTINATION_TABLE = "flats_buildings_raw"


# Заглушка для будущей интеграции с Telegram
def telegram_notify(context):
    return None


@dag(
    dag_id="prepare_flats_buildings_dataset",
    description="ETL: JOIN buildings + flats -> flats_buildings_raw",
    schedule="@once",
    start_date=pendulum.datetime(2026, 6, 1, tz="UTC"),
    catchup=False,
    max_active_runs=1,  # сериализуем запуски, чтобы не было гонок на destination-таблице
    tags=["sprint-1", "etl", "real-estate"],
    default_args={
        "owner": "yp-mle",
        "on_failure_callback": telegram_notify,
        "on_success_callback": telegram_notify,
    },
)
def prepare_flats_buildings_dataset():
    @task()
    def create_table() -> str:
        # 1. Создать целевую таблицу
        # ВАЖНО: flat_id НЕ является PRIMARY KEY — у источника flats.id
        # содержит дубликаты, PK у источника отсутствует. Дедупликация
        # выполняется в DAG-е очистки.
        hook = PostgresHook(postgres_conn_id=DESTINATION_CONN)
        ddl = f"""
            DROP TABLE IF EXISTS public.{DESTINATION_TABLE} CASCADE;
            CREATE TABLE public.{DESTINATION_TABLE} (
                flat_id            BIGINT,
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
            CREATE INDEX idx_{DESTINATION_TABLE}_flat_id
                ON public.{DESTINATION_TABLE} (flat_id);
        """
        hook.run(ddl)
        return DESTINATION_TABLE

    @task()
    def extract() -> list[tuple]:
        # 2. Извлечь и сразу склеить таблицы на стороне SQL
        # Возвращаем список кортежей — компактнее, чем pandas DataFrame для XCom.
        hook = PostgresHook(postgres_conn_id=SOURCE_CONN)
        sql = """
            SELECT
                f.id            AS flat_id,
                f.building_id,
                f.floor,
                f.kitchen_area,
                f.living_area,
                f.rooms,
                f.is_apartment,
                f.studio,
                f.total_area,
                f.price,
                b.build_year,
                b.building_type_int,
                b.latitude,
                b.longitude,
                b.ceiling_height,
                b.flats_count,
                b.floors_total,
                b.has_elevator
            FROM public.flats AS f
            JOIN public.buildings AS b ON b.id = f.building_id;
        """
        rows = hook.get_records(sql)
        print(f"extracted {len(rows)} joined rows")
        return rows

    @task()
    def transform(rows: list[tuple]) -> list[tuple]:
        # 3. Преобразование (для этого этапа — pass-through)
        # Задача оставлена в пайплайне согласно спецификации Practicum
        # (create_table / extract / transform / load) и как место для будущих
        # фичей: можно тут добавить производные колонки.
        print(f"transform: pass-through, {len(rows)} rows")
        return rows

    @task()
    def load(rows: list[tuple], table: str) -> int:
        # 4. Загрузка в destination-таблицу
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
        # 5. Верификация: посчитать строки в таблице
        n = hook.get_first(f"SELECT COUNT(*) FROM public.{table}")[0]
        print(f"loaded {n} rows into public.{table}")
        return n

    table = create_table()
    rows = extract()
    rows_t = transform(rows)
    load(rows_t, table)


prepare_flats_buildings_dataset()
