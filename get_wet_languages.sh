#!/bin/bash

# Run get_wet_languages.py on a warc.wet.gz URL

set -euo pipefail

# Command-line arguments
if [ $# -ne 2 ]; then
    echo -e "Usage: $0 URL OUT" >&2
    exit 1
fi

URL="$1"
OUT="$2"

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
wget -P "$TMPDIR" --no-verbose --limit-rate=1000k "$URL"

base=$(basename "$URL")
path="$TMPDIR/$base"

echo "Sampling $path ..." >&2
source venv/bin/activate
python get_wet_languages.py "$path" > "$OUT"
echo `date` > ${OUT}.completed

echo "Removing $path ..." >&2
rm -rf "$path"

echo "----------------------------------------------------------------------"
echo "END $SLURM_JOBID: $(date): $URL"
echo "----------------------------------------------------------------------"
