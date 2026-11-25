"""Shared Apache Spark 4.1.4 + Apache Iceberg 1.12.0 + OpenLineage 1.55.0 session builder."""

from __future__ import annotations

import os
from typing import Any

ICEBERG_SPARK_PACKAGE = "org.apache.iceberg:iceberg-spark-runtime-4.1_2.13:1.12.0"
ICEBERG_AWS_PACKAGE = "org.apache.iceberg:iceberg-aws-bundle:1.12.0"
OPENLINEAGE_SPARK_PACKAGE = "io.openlineage:openlineage-spark_2.13:1.55.0"


def build_spark_session(
    app_name: str,
    *,
    extra_conf: dict[str, str] | None = None,
) -> Any:
    """Create a SparkSession wired to Lakekeeper Iceberg REST catalog, RustFS S3, and OpenLineage."""
    from pyspark.sql import SparkSession

    rest_uri = os.environ.get("ICEBERG_REST_URI", "http://lakekeeper:8181/catalog")
    warehouse = os.environ.get("ICEBERG_WAREHOUSE", "fleet")
    s3_endpoint = os.environ.get("S3_ENDPOINT_URL", "http://rustfs:9000")
    s3_key = os.environ.get("S3_ACCESS_KEY_ID", "")
    s3_secret = os.environ.get("S3_SECRET_ACCESS_KEY", "")
    ol_url = os.environ.get("OPENLINEAGE_URL", "http://marquez:5000")
    ol_ns = os.environ.get("OPENLINEAGE_NAMESPACE", "autonomous-fleet-platform")

    builder = (
        SparkSession.builder.appName(app_name)
        .config(
            "spark.jars.packages",
            ",".join([ICEBERG_SPARK_PACKAGE, ICEBERG_AWS_PACKAGE, OPENLINEAGE_SPARK_PACKAGE]),
        )
        .config(
            "spark.sql.extensions",
            "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions",
        )
        .config("spark.sql.defaultCatalog", "fleet")
        .config("spark.sql.catalog.fleet", "org.apache.iceberg.spark.SparkCatalog")
        .config("spark.sql.catalog.fleet.type", "rest")
        .config("spark.sql.catalog.fleet.uri", rest_uri)
        .config("spark.sql.catalog.fleet.warehouse", warehouse)
        .config("spark.sql.catalog.fleet.io-impl", "org.apache.iceberg.aws.s3.S3FileIO")
        .config("spark.sql.catalog.fleet.s3.endpoint", s3_endpoint)
        .config("spark.sql.catalog.fleet.s3.path-style-access", "true")
        .config("spark.sql.catalog.fleet.s3.access-key-id", s3_key)
        .config("spark.sql.catalog.fleet.s3.secret-access-key", s3_secret)
        .config("spark.sql.adaptive.enabled", "true")
        .config("spark.sql.adaptive.skewJoin.enabled", "true")
        .config("spark.sql.shuffle.partitions", os.environ.get("SPARK_SHUFFLE_PARTITIONS", "16"))
        .config("spark.extraListeners", "io.openlineage.spark.agent.OpenLineageSparkListener")
        .config("spark.openlineage.transport.type", "http")
        .config("spark.openlineage.transport.url", ol_url)
        .config("spark.openlineage.namespace", ol_ns)
    )
    for k, v in (extra_conf or {}).items():
        builder = builder.config(k, v)
    return builder.getOrCreate()
