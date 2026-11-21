#!/bin/zsh
cd /Users/sajishkrishnan/b2-iceberg-chaos
export SPARK_LOCAL_DIRS=$PWD/work/spark-local-race2 RUN_TAG=-race2
shasum -a 256 -c chaos/FROZEN.sha256 > logs/race2_hash.log 2>&1 || exit 9
for v in 1.12.0 main; do ICE_VER=$v scripts/run.sh chaos/race2.py quota > logs/race2_v$v.log 2>&1; done
echo done > logs/race2_done
