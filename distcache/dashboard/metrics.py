import pandas as pd

def distribution_dataframe(distribution):
    total = sum(distribution.values())
    rows = []

    for node, value in distribution.items():
        percent = round(value / total * 100, 2) if total > 0 else 0

        rows.append({
            "Node": node,
            "Load": value,
            "Percent": percent
        })

    return pd.DataFrame(rows)