#!/bin/bash

# Run filter_warc_by_ids.py on a warc.gz URL

set -euo pipefail

# Command-line arguments
if [ $# -ne 4 ]; then
    echo -e "Usage: $0 FIELD IDS URL OUT" >&2
    exit 1
fi

FIELD="$1"
IDS="$2"
URL="$3"
OUT="$4"

echo "----------------------------------------------------------------------"
echo "START $SLURM_JOBID: $(date): $URL"
echo "----------------------------------------------------------------------"

OUTDIR=$(dirname "$OUT")

mkdir -p "$OUTDIR"

# Create temporary directory and make sure it's wiped on exit
PWD=`pwd -P`
mkdir -p "$PWD/tmp"
TMPDIR=`mktemp -d -p $PWD/tmp`

function on_exit {
    echo "Removing $TMPDIR ..." >&2
    rm -rf "$TMPDIR"
}
trap on_exit EXIT

echo "Downloading \"$URL\" to $TMPDIR ..." >&2
time wget -P "$TMPDIR" --no-verbose --limit-rate=1000k "$URL"

base=$(basename "$URL")
path="$TMPDIR/$base"

echo "Sampling $path ..." >&2
source venv/bin/activate
python filter_warc_by_ids.py -f "$FIELD" "$IDS" "$path" "$OUT"
echo `date` > ${OUT}.completed

echo "Removing $path ..." >&2
rm -rf "$path"

echo "----------------------------------------------------------------------"
echo "END $SLURM_JOBID: $(date): $URL"
echo "----------------------------------------------------------------------"
