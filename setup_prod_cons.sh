#!/usr/bin/env bash

set -e

python --version
pip --version
pip list

sudo pip install pika --upgrade
pip list