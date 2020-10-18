#!/bin/bash

cd $(dirname "${BASH_SOURCE[0]}") || exit 120
source 'venv/bin/activate'
"$@"

