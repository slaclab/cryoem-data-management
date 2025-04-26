#!/bin/bash 

PATTERN=unset
JQ_PATH=$(which jq)
JQ_EXE=${JQ_PATH:-jq}

log_to_file() {
  LOGFILE=$1
  LOGMSG=$2
  if [ -f "${LOGFILE}" ]; then
    echo "${LOGMSG}" >> "${LOGFILE}"
  else
    echo "${LOGTEXT}" > "${LOGFILE}"  
  fi 
}

archive_channels() {
    TOKEN=$1
    CHANNEL_LIST=$2
    log_to_file "# of channels to archive: ${#CHANNEL_LIST[@]}"
    for ch in "${CHANNEL_LIST[@]}"; do 
        CHANNEL_INFO=$(curl "https://slack.com/api/conversations.info?token=${TOKEN}&channel=${ch}")
        CHANNEL_NAME=$(echo "${CHANNEL_INFO}" | "${JQ_EXE}" -r ". | select(.ok == true) | .channel.name")

        log_to_file "Archiving channel ID ${ch}: ${CHANNEL_NAME}"
        RESPONSE=$(curl "https://slack.com/api/conversations.archive?token=${TOKEN}&channel=${ch}")
        if [ $(echo ${RESPONSE} | "${JQ_EXE}" -r ". | select(.ok == false) | .error" ) ]; then
            log_to_file "$(echo ${RESPONSE} | "${JQ_EXE}" -r ". | select(.ok == false) | .error")"    
        fi
        sleep 2
    done

}

usage() {
  echo "note: valid Slack token must be assigned to \$TOKEN environment variable."
  echo "  ex: export TOKEN=\$(vault kv get --field=<field_name> secret/path/to/secret)"
  echo "usage: ./slack_archive_channel.sh <pattern>" 
  exit 1
}

main() {
  if [ "$#" -lt 1 ]; then
    usage
  fi

  PASSED_ARGUMENTS=$(getopt -o p:l:h --long pattern:,logfile:,help -- "$@")
  VALID_ARGUMENTS=$?
  if [ "${VALID_ARGUMENTS}" != "0" ]; then
    usage
  fi

  eval set -- "${PASSED_ARGUMENTS}"
  
  while true; do
    case "$1" in
      -p | --pattern) PATTERN="$2"; shift 2 ;;
      -l | --logfile) LOGFILE="$2"; shift 2 ;;
      -h | --help) usage; shift ; break ;;
      --) shift; break ;;
      *) shift; break ;;
    esac
  done

  if [ -z "${PATTERN}" ]; then
    usage
  fi
  
  if [ -z "${LOGFILE}" ]; then
    LOGFILE=/tmp/slack_archive_channel.log
  fi

  if [ -f "${LOGFILE}" ]; then
    # clear log file before starting
    rm "${LOGFILE}"
  fi

  if [ -z "${TOKEN}" ]; then
    echo "ENV TOKEN not set" 
    exit 1
  fi

  CHANNEL_LIST=()
  NEXT_CURSOR=""
  while true; do
      # save each page of output from the Slack conversations.list api call
      log_to_file "${LOGFILE}" "get response for NEXT_CURSOR = ${NEXT_CURSOR}"
      RESPONSE=$(curl -H "Authorization: Bearer ${TOKEN}" -X POST -d "limit=100" -d "types=private_channel" -d "exclude_archived=true" -d "pretty=1" -d "cursor=${NEXT_CURSOR}" "https://slack.com/api/conversations.list")
      if [ $(echo "${RESPONSE}" | "${JQ_EXE}" ".channels | length") -gt 0 ]; then
          log_to_file "${LOGFILE}" "Found $(echo "${RESPONSE}" | "${JQ_EXE}" ".channels | length") channels..."
          FOUND_CHANNELS=$(echo "${RESPONSE}" | "${JQ_EXE}" -cr -j '"\(.channels[] | select(.name | contains("'"${PATTERN}"'") ) | .id) "' | sed -e 's/[[:space:]]*$//')
          log_to_file "${LOGFILE}" "FOUND_CHANNELS = ${FOUND_CHANNELS[*]}"
          for ch in $(echo "${FOUND_CHANNELS}"); do
              log_to_file "${LOGFILE}" "Adding channel ${ch} to CHANNEL_LIST"
              #set -f
              CHANNEL_LIST+=(${ch/\n// })
              log_to_file "${LOGFILE}" "CHANNEL_LIST = ${CHANNEL_LIST[*]}"
          done
      fi
  
      RESPONSE_METADATA=$(echo "${RESPONSE}" | "${JQ_EXE}" -r ". | select(.response_metadata != \"\" and .response_metadata != null) | .response_metadata")
      if [ "${RESPONSE_METADATA}" != "{}" ]; then
          if [ ! -z $(echo "${RESPONSE_METADATA}" | "${JQ_EXE}" -r ".next_cursor") ]; then
               NEXT_CURSOR=$(echo "${RESPONSE_METADATA}" | "${JQ_EXE}" -r ".next_cursor")
               log_to_file "${LOGFILE}" "set NEXT_CURSOR=${NEXT_CURSOR}"
               continue
          else
               NEXT_CURSOR=""
          fi 
      else
          NEXT_CURSOR=""
      fi
  
      if [ -z "${NEXT_CURSOR}" ]; then
         log_to_file "${LOGFILE}" "Breaking loop..."
         break
      fi
  done
  
  #archive_channels
  log_to_file "${LOGFILE}" "# of channels to archive: ${#CHANNEL_LIST[@]}"
  for ch in "${CHANNEL_LIST[@]}"; do 
      CHANNEL_INFO=$(curl -H "Authorization: Bearer ${TOKEN}" -X POST -d "channel=${ch}"  "https://slack.com/api/conversations.info")
      CHANNEL_NAME=$(echo "${CHANNEL_INFO}" | "${JQ_EXE}" -r ". | select(.ok == true) | .channel.name")
  
      log_to_file "${LOGFILE}" "Archiving channel ID ${ch}: ${CHANNEL_NAME}"
      RESPONSE=$(curl -H "Authorization: Bearer ${TOKEN}" -X POST -d "channel=${ch}" "https://slack.com/api/conversations.archive")
      if [ $(echo ${RESPONSE} | "${JQ_EXE}" -r ". | select(.ok == false) | .error" ) ]; then
          log_to_file "${LOGFILE}" "$(echo ${RESPONSE} | "${JQ_EXE}" -r ". | select(.ok == false) | .error")"    
      fi
      sleep 2
  done
}

set -e
main "$@"
