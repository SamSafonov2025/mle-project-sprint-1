# part2_dvc/scripts/evaluate.py
import json
import os
from pathlib import Path

import joblib
import mlflow
import numpy as np
import pandas as pd
import yaml
from sklearn.model_selection import KFold, cross_validate


def evaluate_model():
    here = Path(__file__).resolve().parent
    part2_dvc = here.parent

    # 1. Прочитать гиперпараметры
    with open(part2_dvc / "params.yaml", "r") as fd:
        params = yaml.safe_load(fd)

    # 2. Загрузить данные и обученный пайплайн
    data = pd.read_csv(
        part2_dvc / "data" / "initial_data.csv",
        index_col=params["index_col"],
    )
    with open(part2_dvc / "models" / "fitted_model.pkl", "rb") as fd:
        pipeline = joblib.load(fd)

    target = params["target_col"]
    y = data[target]
    X = data.drop(columns=[target])

    # 3. Кросс-валидация
    cv_strategy = KFold(
        n_splits=params["n_splits"],
        shuffle=True,
        random_state=params["random_state"],
    )
    cv_res = cross_validate(
        pipeline,
        X,
        y,
        cv=cv_strategy,
        n_jobs=params["n_jobs"],
        scoring=params["metrics"],
        return_train_score=False,
    )

    # 4. Агрегировать метрики: оставить среднее по фолдам и округлить
    summary = {}
    for key, value in cv_res.items():
        summary[key] = round(float(np.asarray(value).mean()), 4)

    # 5. Сохранить результат
    out_dir = part2_dvc / "cv_results"
    os.makedirs(out_dir, exist_ok=True)
    with open(out_dir / "cv_res.json", "w") as fd:
        json.dump(summary, fd, indent=2)

    # 6. Дополнительно: залогировать метрики в MLflow (отдельным run в том же эксперименте)
    mlruns_dir = part2_dvc / "mlruns"
    mlflow.set_tracking_uri(f"file:{mlruns_dir}")
    mlflow.set_experiment(params["experiment_name"])
    with mlflow.start_run(run_name="evaluate_cv") as run:
        mlflow.log_params(
            {
                "n_splits": params["n_splits"],
                "metrics": ",".join(params["metrics"]),
            }
        )
        for key, value in summary.items():
            # имена метрик в MLflow не должны содержать точки и пробелы
            safe = key.replace(".", "_").replace(" ", "_")
            mlflow.log_metric(safe, value)


if __name__ == "__main__":
    evaluate_model()
