from pyspark.sql import SparkSession


def get_spark(app_name: str = "stride-pipeline") -> SparkSession:
    return (
        SparkSession.builder.appName(app_name)
        .master("local[*]")
        .config("spark.sql.session.timeZone", "UTC")
        .config("spark.sql.shuffle.partitions", "8")
        .config("spark.driver.memory", "2g")
        .config("spark.ui.showConsoleProgress", "false")
        .getOrCreate()
    )
