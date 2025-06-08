import pandas as pd
from pandas import DataFrame

def countrows(df: DataFrame) -> int:
	return df.shape[0]

def filter(df: DataFrame, column: str, val) -> DataFrame:
	return df[df[column] == val]