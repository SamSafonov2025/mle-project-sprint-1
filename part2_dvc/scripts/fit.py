# part2_dvc/scripts/fit.py
import os
from pathlib import Path

import joblib
import mlflow
import mlflow.sklearn
import pandas as pd
import yaml
from sklearn.compose import ColumnTransformer
from sklearn.linear_model import Ridge
from sklearn.pipeline import Pipeline
from sklearn.preprocessing import OneHotEncoder, StandardScaler


def fit_model():
    here = Path(__file__).resolve().parent
    part2_dvc = here.parent

    # 1. Прочитать гиперпараметры
    with open(part2_dvc / "params.yaml", "r") as fd:
        params = yaml.safe_load(fd)

    # 2. Загрузить данные из предыдущего шага
    data = pd.read_csv(
        part2_dvc / "data" / "initial_data.csv",
        index_col=params["index_col"],
    )

    # 3. Разбить признаки на группы
    target = params["target_col"]
    y = data[target]
    X = data.drop(columns=[target])

    num_features = [
        "floor", "kitchen_area", "living_area", "total_area", "build_year",
        "latitude", "longitude", "ceiling_height", "flats_count",
        "floors_total", "rooms",
    ]
    cat_features = ["building_type_int"]
    bool_features = ["is_apartment", "studio", "has_elevator"]

    # 4. Препроцессор
    preprocessor = ColumnTransformer(
        [
            ("num", StandardScaler(), num_features),
            ("cat", OneHotEncoder(drop=params["one_hot_drop"], handle_unknown="ignore"), cat_features),
            ("bool", "passthrough", bool_features),
        ],
        remainder="drop",
        verbose_feature_names_out=False,
    )

    # 5. Модель и пайплайн
    model = Ridge(alpha=params["ridge_alpha"], random_state=params["random_state"])
    pipeline = Pipeline(
        [
            ("preprocessor", preprocessor),
            ("model", model),
        ]
    )

    # 6. Обучить (с логированием параметров и метрик в MLflow)
    mlruns_dir = part2_dvc / "mlruns"
    mlruns_dir.mkdir(parents=True, exist_ok=True)
    mlflow.set_tracking_uri(f"file:{mlruns_dir}")
    mlflow.set_experiment(params["experiment_name"])

    with mlflow.start_run(run_name="fit_baseline") as run:
        mlflow.log_params(
            {
                "ridge_alpha": params["ridge_alpha"],
                "one_hot_drop": params["one_hot_drop"],
                "target_col": target,
                "rows": int(len(data)),
                "model": "sklearn.linear_model.Ridge",
            }
        )
        pipeline.fit(X, y)

        # Оптимистичная оценка R^2 на полной выборке (честная кросс-валидация — в evaluate.py)
        train_r2 = pipeline.score(X, y)
        mlflow.log_metric("train_r2_full", float(train_r2))
        mlflow.log_param("run_id", run.info.run_id)

    # 7. Сохранить
    models_dir = part2_dvc / "models"
    os.makedirs(models_dir, exist_ok=True)
    with open(models_dir / "fitted_model.pkl", "wb") as fd:
        joblib.dump(pipeline, fd)


if __name__ == "__main__":
    fit_model()
