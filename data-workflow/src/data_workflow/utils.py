import numpy as np
import pandas as pd


def bootstrap_diff_means(a: pd.Series,b: pd.Series,n_boot: int = 2000,seed: int = 0,) -> dict[str, float]:

    a_clean = pd.to_numeric(a, errors="coerce").dropna().values
    b_clean = pd.to_numeric(b, errors="coerce").dropna().values

    if len(a_clean) == 0 or len(b_clean) == 0:
        raise ValueError("empty after clean")

    random_gen = np.random.default_rng(seed)

    bootstrap_diffs = np.empty(n_boot)

    for i in range(n_boot):
        sample_a = random_gen.choice(a_clean, size=len(a_clean), replace=True)
        sample_b = random_gen.choice(b_clean, size=len(b_clean), replace=True)
        bootstrap_diffs[i] = sample_a.mean() - sample_b.mean()

    observed = a_clean.mean() - b_clean.mean()
    ci_lower, ci_upper = np.quantile(bootstrap_diffs, [0.025, 0.975])

    return {
        "diff_mean": float(observed),
        "ci_lower": float(ci_lower),
        "ci_upper": float(ci_upper),
    }