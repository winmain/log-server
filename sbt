#!/usr/bin/env bash
JPDA_PORT=${JPDA_PORT:-9997}
env sbt \
  -J-Xrunjdwp:transport=dt_socket,server=y,suspend=n,address=${JPDA_PORT} \
  -J-Xms512m \
  -J-Xmx512m
