#!/bin/bash
cd ~/Bureau/S8/Hidoop_S8_v2/hidoopfonctionnel/src
java src/ordo.WorkerImpl $1 $2
echo "Server Worker $2 restarted on port $1"