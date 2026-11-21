#!/bin/zsh
# usage: run_version.sh <ICE_VER>   Order: frozen-hash check -> selftest -> control #17202 (gate) -> control #17304 -> matrix
export ICE_VER=$1; B2=/Users/sajishkrishnan/b2-iceberg-chaos; cd $B2
LOG=logs/full_v$ICE_VER.log; echo "[$(date +%H:%M:%S)] start $ICE_VER" >> $LOG
shasum -a 256 -c chaos/FROZEN.sha256 >> $LOG 2>&1 || { echo "FROZEN HASH MISMATCH - refusing to run" >> $LOG; exit 9; }
scripts/run.sh chaos/selftest.py > logs/selftest_v$ICE_VER.log 2>&1
echo "[$(date +%H:%M:%S)] selftest: $(grep SELFTEST logs/selftest_v$ICE_VER.log)" >> $LOG
scripts/run.sh chaos/run_matrix.py control17202 > logs/control17202_v$ICE_VER.log 2>&1; rc=$?
echo "[$(date +%H:%M:%S)] control17202 rc=$rc (0=detected, 3=MISSED)" >> $LOG
if [ $rc -ne 0 ]; then echo "CONTROL MISS - matrix NOT started for $ICE_VER" >> $LOG; exit 3; fi
scripts/run.sh chaos/run_matrix.py control17304 > logs/control17304_v$ICE_VER.log 2>&1
echo "[$(date +%H:%M:%S)] control17304 done" >> $LOG
scripts/run.sh chaos/run_matrix.py matrix > logs/matrix_v$ICE_VER.log 2>&1
echo "[$(date +%H:%M:%S)] matrix finished rc=$?" >> $LOG
