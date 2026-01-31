from pyspark.sql import DataFrame
from pyspark.sql.functions import col, when, regexp_replace, trim


def replace_null_strings(df: DataFrame, null_values: list[str] | None = None) -> DataFrame:
    if null_values is None:
        null_values = ["\\N", "null", "NULL", "None", ""]

    for column in df.columns:
        if df.schema[column].dataType.simpleString() == "string":
            condition = col(column).isin(null_values) | (trim(col(column)) == "")
            df = df.withColumn(column, when(condition, None).otherwise(col(column)))
    return df


def clean_whitespace(df: DataFrame, columns: list[str] | None = None) -> DataFrame:
    if columns is None:
        columns = [
            f.name
            for f in df.schema.fields
            if f.dataType.simpleString() == "string"
        ]
    for column in columns:
        df = df.withColumn(column, trim(regexp_replace(col(column), r"\s+", " ")))
    return df
