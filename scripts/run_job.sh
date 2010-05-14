#!/bin/bash
export PYTHONPATH=$PYTHONPATH:../src/python
PYTHON=${PYTHON:-python}

HOST=`hostname -f`

MASTER_HOST=${MASTER_HOST:-http://$HOST:9000}

ID=${2:-foo}

${PYTHON} ../src/python/mrry/mercator/cloudscript/interpreter/cluster.py $1 ${MASTER_HOST} $ID