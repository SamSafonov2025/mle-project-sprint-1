# part1_airlfow/plugins/cleaning.py
#
# Чистые pandas-функции очистки для перехода flats_buildings_raw ->
# flats_buildings_clean.
#
# Используются:
#   - part1_airlfow/notebooks/eda_flats_buildings.ipynb (EDA + sanity check)
#   - part1_airlfow/dags/clean_flats_buildings.py (production-шаг очистки)
#
# Каждая функция принимает DataFrame и возвращает новый DataFrame
# (без мутации входа).
from __future__ import annotations

import pandas as pd


def remove_duplicate_flats(df: pd.DataFrame) -> pd.DataFrame:
    # 1. Удалить дубликаты по flat_id (оставляем первый)
    # У источника flats.id не имел ограничения PRIMARY KEY, поэтому делаем
    # defensively даже если на текущем срезе уникальность уже выполняется.
    return df.drop_duplicates(subset=["flat_id"], keep="first").reset_index(drop=True)


def drop_logical_impossibilities(df: pd.DataFrame) -> pd.DataFrame:
    # 2. Отбросить строки с невозможными значениями площадей
    # Примеры наблюдений: kitchen_area > total_area (2 строки в текущем срезе).
    # living + kitchen <= total НЕ требуется (есть прочие комнаты, балконы и т.п.).
    mask = df["kitchen_area"] <= df["total_area"]
    mask &= df["living_area"] <= df["total_area"]
    return df.loc[mask].reset_index(drop=True)


def clip_ceiling_height(df: pd.DataFrame, lower: float = 2.0, upper: float = 5.0) -> pd.DataFrame:
    # 3. Ограничить высоту потолков физически разумным интервалом
    # В сыром датасете встречается max=27 м — явная ошибка/выброс. Верхнюю
    # границу задаём 5.0 м (лофты, дореволюционные дома).
    out = df.copy()
    out["ceiling_height"] = out["ceiling_height"].clip(lower=lower, upper=upper)
    return out


def drop_price_outliers(df: pd.DataFrame, upper_quantile: float = 0.995) -> pd.DataFrame:
    # 4. Отбросить выбросы по цене выше заданного квантиля
    # В сыром датасете встречается max=9.87 млрд — явная подделка/placeholder.
    # p99 ~ 152 млн — разумная верхняя граница для московской недвижимости.
    cap = df["price"].quantile(upper_quantile)
    return df.loc[df["price"] <= cap].reset_index(drop=True)


def clean_flats_buildings(
    df: pd.DataFrame,
    ceiling_min: float = 2.0,
    ceiling_max: float = 5.0,
    price_upper_quantile: float = 0.995,
) -> pd.DataFrame:
    # End-to-end пайплайн очистки.
    # Порядок:
    #   1. дедупликация по flat_id;
    #   2. удалить строки с невозможными площадями;
    #   3. удалить выбросы по цене;
    #   4. ограничить ceiling_height физическими границами.
    df = remove_duplicate_flats(df)
    df = drop_logical_impossibilities(df)
    df = drop_price_outliers(df, upper_quantile=price_upper_quantile)
    df = clip_ceiling_height(df, lower=ceiling_min, upper=ceiling_max)
    return df
