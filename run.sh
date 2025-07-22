#!/bin/sh

cd /opt/flock-dashboard
. ./venv/bin/activate
python server.py --max-age 7200 --replication-dir /storage/openstreetmap/replication/ $@
