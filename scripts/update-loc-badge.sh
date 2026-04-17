#!/bin/bash -xe
LOC=$(tokei -o json | jq '.Total.code'| awk '{ printf "%.1fk\n", $1/1000 }')
SHIELDS_IO_URL="https://img.shields.io/badge/code_lines-${LOC}-blue"
curl -L -o docs/loc-badge.svg "${SHIELDS_IO_URL}"
git add docs/loc-badge.svg