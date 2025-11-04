#!/usr/bin/env bash
TOP=/sdf/group/cryoem/exp

thr=$(date -d '2 years ago' +%Y%m%d)

find "$TOP" -mindepth 2 -maxdepth 2 -type d -print0 |
  while IFS= read -r -d '' d; do
    base=${d##*/}
    parent=${d%/*}; parent=${parent##*/}

    if [[ $parent =~ ^[0-9]{6}$ && $base =~ ^([0-9]{8}) ]]; then
      day=${BASH_REMATCH[1]}
      if (( 10#$day < 10#$thr )); then
        printf '%s\n' "$d"
      fi
    fi
  done
