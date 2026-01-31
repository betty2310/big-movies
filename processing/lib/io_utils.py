from pyspark.sql import SparkSession, DataFrame
from pyspark.sql.types import StructType
from pyspark.sql.functions import lit, current_date
from datetime import date


def get_spark_session(app_name: str) -> SparkSession:
    return (
        SparkSession.builder.appName(app_name)
        .config("spark.sql.adaptive.enabled", "true")
        .config("spark.sql.parquet.compression.codec", "snappy")
        .getOrCreate()
    )


def read_csv(
    spark: SparkSession,
    path: str,
    schema: StructType,
    header: bool = True,
) -> DataFrame:
    return spark.read.csv(path, schema=schema, header=header)


def read_tsv(
    spark: SparkSession,
    path: str,
    schema: StructType,
    header: bool = True,
) -> DataFrame:
    return spark.read.csv(
        path,
        schema=schema,
        header=header,
        sep="\t",
        nullValue="\\N",
    )


def read_ndjson(
    spark: SparkSession,
    path: str,
    schema: StructType | None = None,
) -> DataFrame:
    reader = spark.read
    if schema:
        reader = reader.schema(schema)
    return reader.json(path)


def write_parquet(
    df: DataFrame,
    path: str,
    mode: str = "overwrite",
    partition_by: list[str] | None = None,
) -> None:
    writer = df.write.mode(mode)
    if partition_by:
        writer = writer.partitionBy(*partition_by)
    writer.parquet(path)


def add_ingest_metadata(df: DataFrame, ingest_date: date | None = None) -> DataFrame:
    if ingest_date:
        return df.withColumn("ingest_date", lit(ingest_date))
    return df.withColumn("ingest_date", current_date())


def get_latest_partition(spark: SparkSession, base_path: str) -> str | None:
    try:
        from py4j.protocol import Py4JJavaError

        fs = spark._jvm.org.apache.hadoop.fs.FileSystem.get(
            spark._jsc.hadoopConfiguration()
        )
        path = spark._jvm.org.apache.hadoop.fs.Path(base_path)
        if not fs.exists(path):
            return None
        statuses = fs.listStatus(path)
        dirs = [
            s.getPath().getName()
            for s in statuses
            if s.isDirectory() and not s.getPath().getName().startswith("_")
        ]
        if not dirs:
            return None
        dirs.sort(reverse=True)
        return f"{base_path}/{dirs[0]}"
    except Exception:
        return None
