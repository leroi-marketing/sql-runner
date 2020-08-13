#!/usr/bin/env bash

set -eo pipefail

cd /app
git fetch origin && git reset origin/master --hard

exec "$@"
