# Pilot environment (sourced). All paths local to the project folder.
export B2=/Users/sajishkrishnan/b2-iceberg-chaos
export JAVA_HOME=/opt/homebrew/opt/openjdk@21
export PATH=$JAVA_HOME/bin:$PATH
export SPARK_HOME=$B2/bin/spark-3.5.3-bin-hadoop3
export PYSPARK_PYTHON=/usr/bin/python3
export MINIO_ROOT_USER=chaos MINIO_ROOT_PASSWORD=chaos-pilot-pw
export SQLITE_CAT=$B2/work/catalog.db
SPARK_CONF=(
 --conf spark.ui.enabled=false
 --conf spark.sql.shuffle.partitions=2
 --conf spark.driver.memory=1g
 --conf spark.sql.extensions=org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions
 --conf spark.sql.catalog.ice=org.apache.iceberg.spark.SparkCatalog
 --conf spark.sql.catalog.ice.catalog-impl=org.apache.iceberg.jdbc.JdbcCatalog
 --conf "spark.sql.catalog.ice.uri=jdbc:postgresql://127.0.0.1:5433/icecat"
  --conf spark.sql.catalog.ice.cache-enabled=false
 --conf spark.sql.catalog.ice.jdbc.user=chaos
 --conf spark.sql.catalog.ice.jdbc.password=x
 --conf spark.sql.catalog.ice.io-impl=org.apache.iceberg.aws.s3.S3FileIO
 --conf spark.sql.catalog.ice.warehouse=s3://warehouse/wh
 --conf spark.sql.catalog.ice.s3.endpoint=http://127.0.0.1:9100
 --conf spark.sql.catalog.ice.s3.path-style-access=true
 --conf spark.sql.catalog.ice.s3.access-key-id=chaos
 --conf spark.sql.catalog.ice.s3.secret-access-key=chaos-pilot-pw
 --conf spark.sql.catalog.ice.client.region=us-east-1
)
