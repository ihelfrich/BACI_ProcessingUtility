import pandas as pd
import numpy as np
import os
import joblib


def analyze_csv(file_path):
    """Analyze a CSV file and return its structure."""
    df = pd.read_csv(
        file_path, nrows=1000)  # Read first 1000 rows for analysis
    return {
        'columns': list(df.columns),
        'dtypes': df.dtypes.to_dict(),
        'sample': df.head(5).to_dict()
    }


def cached_analyze_csv(file_path):
    """Analyze a CSV file with caching for improved performance."""
    cache_file = f"{file_path}.cache"
    if os.path.exists(cache_file):
        return joblib.load(cache_file)
    result = analyze_csv(file_path)
    joblib.dump(result, cache_file)
    return result


def find_merge_keys(main_structure, auxiliary_structure):
    """Find potential merge keys between main and auxiliary dataframes."""
    main_cols = set(main_structure['columns'])
    aux_cols = set(auxiliary_structure['columns'])

    common_cols = main_cols.intersection(aux_cols)

    potential_renames = []
    for main_col in main_cols:
        for aux_col in aux_cols:
            if main_structure['dtypes'][main_col] == auxiliary_structure['dtypes'][aux_col]:
                if main_structure['sample'][main_col].values() == auxiliary_structure['sample'][aux_col].values():
                    potential_renames.append((main_col, aux_col))

    return list(common_cols), potential_renames


def read_file(file_path):
    """Read different file formats."""
    if file_path.endswith('.csv'):
        return pd.read_csv(file_path)
    elif file_path.endswith('.parquet'):
        return pd.read_parquet(file_path)
    else:
        raise ValueError(f"Unsupported file format: {file_path}")
