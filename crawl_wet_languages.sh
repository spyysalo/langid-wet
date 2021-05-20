#!/bin/bash

# Run get_wet_languages.py on all warc.wet.gz files in a Common Crawl
# using GREASY.

# Number of nodes to run on
NODES=5

# Base URL for crawls
BASEURL="https://commoncrawl.s3.amazonaws.com"

# Slurm account
ACCOUNT=project_2004407    # FinnGen-data

# Maximum number of GREASY steps to run
MAX_STEPS=20000

set -euo pipefail

# Command-line arguments
if [ $# -lt 1 ]; then
    echo -e "Usage: $0 CRAWL-ID [OUTDIR]" >&2
    echo -e "Example: $0 CC-MAIN-2021-04" >&2
    exit 1
fi

CRAWLID="$1"
if [ $# -ge 2 ]; then
    OUTDIR="$2"
else
    OUTDIR="${CRAWLID}-languages"
fi

if [ -e "$OUTDIR" ]; then
    read -n 1 -r -p "Output directory $OUTDIR exists. Continue? [y/n] "
    echo
    if [[ $REPLY =~ ^[Yy]$ ]]; then
	echo "OK, running for files missing from $OUTDIR."
    else
	echo "Exiting."
	exit 1
    fi
fi

# Create temporary directory and make sure it's wiped on exit
PWD=`pwd -P`
mkdir -p "$PWD/tmp"
TMPDIR=`mktemp -d -p $PWD/tmp`

function on_exit {
    echo "Removing $TMPDIR ..." >&2
    rm -rf "$TMPDIR"
}
trap on_exit EXIT

# Download and unpack wet.paths.gz file
url="$BASEURL/crawl-data/$CRAWLID/wet.paths.gz"
echo "Downloading $url to $TMPDIR" >&2
wget -P "$TMPDIR" "$url"
gunzip "$TMPDIR/wet.paths.gz"

# Get longest common prefix of paths
prefix=$(python commonprefix.py --dir $TMPDIR/wet.paths)

# Create temporary file for GREASY tasklist
PWD=`pwd -P`
mkdir -p "$PWD/tasklists"
TASKLIST=`mktemp -p $PWD/tasklists tasklist.XXX`

# Create tasklist
path_count=$(wc -l < "$TMPDIR/wet.paths")
echo "Creating tasklist $TASKLIST from $path_count paths ..." >&2
count=0
skip=0
while read p; do
    if [ $count -ge $MAX_STEPS ]; then
	echo "MAX_STEPS ($MAX_STEPS) reached, skipping remaining" >&2
	break
    fi
    url="$BASEURL/$p"
    # exclude common prefix and "/wet" suffix from output path
    dir=$(echo $(dirname "$p") | perl -pe 's|'"$prefix"'/?||; s|/wet$||')
    out="$OUTDIR/$dir/$(basename $p .warc.wet.gz).tsv"
    if [ -e "$out" ]; then
	skip=$((skip+1))
	if [ $((skip % 1000)) -eq 0 ]; then
	    echo "Skippped $skip ..." >&2
	fi
    else
	echo "./get_wet_languages.sh $url $out"
	count=$((count+1))
	if [ $((count % 1000)) -eq 0 ]; then
	    echo "Processed $count ..." >&2
	fi
    fi
done < <(cat "$TMPDIR/wet.paths") > "$TASKLIST"

echo "Wrote tasklist with $count tasks, skipped $skip." >&2
if [ $count -eq 0 ]; then
    rm "$TASKLIST"
    echo "All done, exiting without tasklist." >&2
    exit 0
fi

mkdir -p greasy-jobs
JOB_TEMP=`mktemp -u greasy-jobs/greasy-job-XXX.sbatch`

module load greasy

sbatch-greasy $TASKLIST \
    --cores 1 \
    --nodes "$NODES" \
    --time 0:45:00 \
    --account "$ACCOUNT" \
    --file "$JOB_TEMP"

mkdir -p slurm-logs
perl -p -i -e 's|^(#SBATCH -[oe] )|${1} slurm-logs/|' "$JOB_TEMP"
# Puhti-specific adjustment
perl -p -i -e 's/^(#SBATCH -p) small.*/$1 large/' "$JOB_TEMP"

echo "----------------------------------------"
echo " Wrote $JOB_TEMP, starting the job with"
echo "     sbatch $JOB_TEMP"
echo "----------------------------------------"

sbatch "$JOB_TEMP"
