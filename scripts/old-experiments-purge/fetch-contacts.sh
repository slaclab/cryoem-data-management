#!/usr/bin/bash

# Usage:
#   export session="<your_session_cookie>"
#   export webauth_at="<your_webauth_at_cookie>"
#   ./fetch_contacts.sh experiments.txt contacts.txt

set -uo pipefail

infile="${1:-}"
outfile="${2:-}"

if [[ -z "${infile}" || -z "${outfile}" ]]; then
  echo "Usage: $0 <input_file> <output_file>" >&2
  exit 1
fi
if [[ ! -f "${infile}" ]]; then
  echo "Input file not found: ${infile}" >&2
  exit 1
fi

# Ensure cookies are provided
if [[ -z "${session-}" || -z "${webauth_at-}" ]]; then
  echo "Export 'session' and 'webauth_at' environment variables" >&2
  exit 1
fi

# Create/overwrite the output file
: > "${outfile}"

while IFS= read -r item; do
  [[ -z "${item}" || "${item}" =~ ^[[:space:]]*# ]] && continue
  url="https://cryoem-logbook.slac.stanford.edu/lgbk/${item}/ws/info"

  resp="$(curl -sSf -L \
    -b "session=${session} webauth_at=${webauth_at}" \
    "${url}")" || {
      echo "${item} ERROR" >> "${outfile}"
      continue
    }

  success="$(jq -r '.success // "true"' <<< "${clean}" 2>/dev/null || echo "true")"
  if [[ "${success}" != "true" ]]; then
    echo "${item} ERROR" >> "${outfile}"
    continue
  fi

  contact="$(jq -r '.value.contact_info // "N/A"' <<< "${clean}" 2>/dev/null || echo "N/A")"

  echo "${item} ${contact}" >> "${outfile}"
done < "${infile}"

echo "Wrote contacts to: ${outfile}"
