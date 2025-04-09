#!/usr/bin/env bash

# This script downloads the segments of the Common Crawl dataset for a specific partition.
# It extracts the segment paths from the warc.paths.gz file and downloads the first 3 segments.
# Usage: ./download_segments.sh YYYY-WW
# Example: ./download_segments.sh 2025-13
# Make sure you have wget and gunzip installed

PARTITION=$1
if [[ -z "$PARTITION" ]]; then
    echo "Usage: $0 YYYY-WW"
    exit 1
fi
if ! [[ "$PARTITION" =~ ^[0-9]{4}-[0-9]{2}$ ]]; then
    echo "Invalid partition format. Use YYYY-WW."
    exit 1
fi

WARC_PATH_URL="https://data.commoncrawl.org/crawl-data/CC-MAIN-${PARTITION}/warc.paths.gz"

SEGMENT_BASE_URL="https://data.commoncrawl.org/"

OUTPUT_DIR="/opt/airflow/commoncrawl"

mkdir -p "${OUTPUT_DIR}/segments"

# Download the warc.paths.gz file
wget -P "${OUTPUT_DIR}" "${WARC_PATH_URL}"
# Extract 3 segment paths from the gzipped file
SEGMENTS=$(gunzip -c "${OUTPUT_DIR}/warc.paths.gz" \
  | grep -Eo "crawl-data/CC-MAIN-${PARTITION}/segments/[0-9]+\.[0-9]+/.*\.warc\.gz" \
  | sort -u | head -n 3)


gunzip -c "${OUTPUT_DIR}/warc.paths.gz" | while read -r line; do
    for prefix in $SEGMENTS; do
        if [[ "$line" == "$prefix"* ]]; then
            SEGMENT_URL="${SEGMENT_BASE_URL}${line}"
            echo "Downloading segment: ${SEGMENT_URL}"
            wget -P "${OUTPUT_DIR}/segments" "${SEGMENT_URL}"
        fi
    done
done    

rm -f "${OUTPUT_DIR}/warc.paths.gz"

echo "Done downloading segments."
