import pandas as pd

from models.models import Pipeline
from utils.utils import remove_columns_with_missing, find_missing_cols

if __name__ == "__main__":
    pipline = Pipeline("input/lc_loan_sample.csv")
    pipline.wrangling()