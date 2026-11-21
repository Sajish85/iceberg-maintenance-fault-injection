#!/bin/zsh
# usage: [ICE_VER=1.7.1|1.12.0] [S3A=1] run.sh <py file> [args...]  (exec: PID is the spark-submit launcher; callers killpg the session)
# S3A=1 adds hadoop-aws + S3A conf so Hadoop-FS listing (used by DeleteOrphanFiles) can reach MinIO via the s3:// scheme.
source /Users/sajishkrishnan/b2-iceberg-chaos/scripts/env.sh
MASTER=${MASTER:-local[2]}
ICE_VER=${ICE_VER:-1.7.1}
JARS=$B2/dl/jars/iceberg-spark-runtime-3.5_2.12-$ICE_VER.jar,$B2/dl/jars/iceberg-aws-bundle-$ICE_VER.jar,$B2/dl/jars/postgresql-42.7.4.jar
EXTRA=()
if [[ -n "$S3A" ]]; then
  JARS=$JARS,$B2/dl/s3a/hadoop-aws-3.3.4.jar,$B2/dl/s3a/aws-java-sdk-bundle-1.12.262.jar
  EXTRA=(--conf spark.hadoop.fs.s3.impl=org.apache.hadoop.fs.s3a.S3AFileSystem
         --conf spark.hadoop.fs.s3a.endpoint=http://127.0.0.1:9100 --conf spark.hadoop.fs.s3a.path.style.access=true
         --conf spark.hadoop.fs.s3a.access.key=chaos --conf spark.hadoop.fs.s3a.secret.key=chaos-pilot-pw
         --conf spark.hadoop.fs.s3a.connection.ssl.enabled=false)
fi
exec $SPARK_HOME/bin/spark-submit --master "$MASTER" --jars $JARS \
  --driver-class-path $B2/dl/jars/postgresql-42.7.4.jar "${SPARK_CONF[@]}" "${EXTRA[@]}" "$@"
