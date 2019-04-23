#!/usr/bin/env bash

export DISPLAY=:99
Xvfb :99&

service postgresql start

python3 tests.py 127.0.0.1 postgres -v
