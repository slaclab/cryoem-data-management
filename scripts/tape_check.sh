#!/bin/bash

MONTH=$1
export TAPE_CHECK_FILE=/tmp/tape_check_${MONTH}

SAMPLE_COUNT=$(find $MONTH -mindepth 2 -maxdepth 2 -type d | sort | wc -l )
HTAR_COUNT=$(find $MONTH -name 'raw.htar' | wc -l )

echo "htar/samples in $MONTH: ${HTAR_COUNT}/${SAMPLE_COUNT}" > ${TAPE_CHECK_FILE}
find $MONTH -mindepth 2 -maxdepth 2 -type d | xargs -I{} sh -c 'hsi "ls -U /cryoEM/exp/{}/raw*tar" 2>&1 | tee -a -- ${TAPE_CHECK_FILE}; if [ $? -ne 0 ]; then echo "sample {} not on tape" >> "${TAPE_CHECK_FILE}" ; fi '

echo -e "\n\n==============\n\n" >> "${TAPE_CHECK_FILE}"
