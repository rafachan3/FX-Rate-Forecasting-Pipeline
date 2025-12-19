import pandas as pd
import numpy as np

import pandas as pd

def load_fx_series(path: str) -> pd.Series:
    with open(path, "r", encoding="utf-8") as f:
        lines = f.readlines()

    header_idx = None
    for i, line in enumerate(lines):
        low = line.strip().lower()
        if "date" in low and "fxusdcad" in low:
            header_idx = i
            break

    if header_idx is None:
        raise ValueError("Could not find header row containing both 'date' and 'FXUSDCAD'.")

    df = pd.read_csv(path, skiprows=header_idx, parse_dates=["date"])
    df = df.sort_values("date").set_index("date")

    # Strip spaces from column names just in case
    df.columns = [c.strip() for c in df.columns]

    rate_col = "FXUSDCAD"
    if rate_col not in df.columns:
        raise ValueError(f"Expected '{rate_col}', found: {list(df.columns)}")

    s = df[rate_col].astype(float).dropna()
    s.name = "usd_cad"
    return s

def naive_forecast(train: pd.Series, horizon: int) -> np.ndarray:
    last_value = train.iloc[-1]
    return np.repeat(last_value, horizon)

def drift_forecast(train: pd.Series, horizon: int) -> np.ndarray:
    y_start = train.iloc[0]
    y_end = train.iloc[-1]
    n = len(train) - 1

    drift = (y_end - y_start) / n
    return np.array([y_end + drift * (h + 1) for h in range(horizon)])

def rolling_backtest(
    series: pd.Series,
    horizon: int = 7,
    min_train_size: int = 252 * 2  # ~2 years
):
    results = []

    for t in range(min_train_size, len(series) - horizon):
        train = series.iloc[:t]
        test = series.iloc[t : t + horizon]

        naive_pred = naive_forecast(train, horizon)
        drift_pred = drift_forecast(train, horizon)

        for h in range(horizon):
            results.append({
                "date": test.index[h],
                "horizon": h + 1,
                "actual": test.iloc[h],
                "naive": naive_pred[h],
                "drift": drift_pred[h],
                "naive_error": test.iloc[h] - naive_pred[h],
                "drift_error": test.iloc[h] - drift_pred[h],
            })

    return pd.DataFrame(results)

def mae(y_true, y_pred) -> float:
    y_true = np.asarray(y_true, dtype=float)
    y_pred = np.asarray(y_pred, dtype=float)
    return float(np.mean(np.abs(y_true - y_pred)))

def mape(y_true, y_pred, eps: float = 1e-12) -> float:
    y_true = np.asarray(y_true, dtype=float)
    y_pred = np.asarray(y_pred, dtype=float)
    denom = np.maximum(np.abs(y_true), eps)
    return float(np.mean(np.abs((y_true - y_pred) / denom)) * 100.0)

def evaluate_by_horizon(results: pd.DataFrame) -> pd.DataFrame:
    rows = []
    for h in sorted(results["horizon"].unique()):
        dfh = results[results["horizon"] == h]

        rows.append({
            "horizon": h,
            "naive_mae": mae(dfh["actual"], dfh["naive"]),
            "drift_mae": mae(dfh["actual"], dfh["drift"]),
            "naive_mape_%": mape(dfh["actual"], dfh["naive"]),
            "drift_mape_%": mape(dfh["actual"], dfh["drift"]),
            "drift_beats_naive_%": float(np.mean(np.abs(dfh["drift_error"]) < np.abs(dfh["naive_error"])) * 100.0),
        })

    out = pd.DataFrame(rows)
    out["mae_improvement_%"] = (out["naive_mae"] - out["drift_mae"]) / out["naive_mae"] * 100.0
    return out

if __name__ == "__main__":
    series = load_fx_series("data/FXUSDCAD-sd-2020-12-24-ed-2025-12-24.csv")
    results = rolling_backtest(series, horizon=7)
    print(results.head())

    results.to_csv("outputs/baseline_backtest_rows.csv", index=False)
