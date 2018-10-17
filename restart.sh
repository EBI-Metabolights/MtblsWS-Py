#!/usr/bin/env bash
./stop_server.sh
./start_servers.sh
tail -f ./logs/ws.log
