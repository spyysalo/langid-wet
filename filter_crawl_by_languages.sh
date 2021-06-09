#!/bin/bash

# Run filter_warc.py on all warc.gz files with reference to langid
# data generated by crawl_wet_languages.sh

# Number of nodes to run on
NODES=10

# Base URL for crawls
BASEURL="https://commoncrawl.s3.amazonaws.com"

# Base URL for langid data
LANGIDURL="https://a3s.fi/commoncrawl-languages"

# Directory to store langid-filtered IDs in
IDDIR="filtered-ids"

# ID field index in crawl_wet_languages.sh output
IDIDX=1

# Slurm account
ACCOUNT=project_2004407    # FinnGen-data

# Maximum number of GREASY steps to run
MAX_STEPS=20000

set -euo pipefail

# Command-line arguments
if [ $# -lt 2 ]; then
    echo -e "Usage: $0 CRAWL-ID LANGUAGES [OUTDIR]" >&2
    echo -e "Example: $0 CC-MAIN-2021-04 fi,fin" >&2
    exit 1
fi

CRAWLID="$1"
LANGUAGES="$2"
if [ $# -ge 3 ]; then
    OUTDIR="$3"
else
    OUTDIR="${CRAWLID}-filtered"
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
    #rm -rf "$TMPDIR"
}
trap on_exit EXIT

# Download and unpack warc.paths.gz file
url="$BASEURL/crawl-data/$CRAWLID/warc.paths.gz"
echo "Downloading $url to $TMPDIR" >&2
wget -P "$TMPDIR" "$url"
gz="$TMPDIR/warc.paths.gz"
echo "Unpacking $gz" >&2
gunzip "$gz"

# Create file with IDs if necessary
idfile="$IDDIR/${CRAWLID}-${LANGUAGES}.tsv"
if [ -s "$idfile" ]; then
    echo "Using $idfile for filtering" >&2
else
    # Download ${CRAWLID}-languages.tar.gz file
    url="$LANGIDURL/${CRAWLID}-languages.tar.gz"
    echo "Downloading $url to $TMPDIR" >&2
    wget -P "$TMPDIR" "$url"
    tgz="$TMPDIR/${CRAWLID}-languages.tar.gz"
    # Filter to target language(s)
    echo "Filtering $tgz to $idfile"
    mkdir -p $(dirname "$idfile")
    langs=$(echo "$LANGUAGES" | tr ',' '|')
    time tar xzf "$tgz" -O | egrep $'\t''.*'$'\t''.*\b('"$langs"')\b' > "$idfile"
fi

# Get longest common prefix of paths
prefix=$(python commonprefix.py --dir $TMPDIR/warc.paths)

# Create temporary file for GREASY tasklist
PWD=`pwd -P`
mkdir -p "$PWD/tasklists"
TASKLIST=`mktemp -p $PWD/tasklists tasklist.XXX`

# Create tasklist
path_count=$(wc -l < "$TMPDIR/warc.paths")
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
    dir=$(echo $(dirname "$p") | perl -pe 's|'"$prefix"'/?||; s|/warc$||')
    out="$OUTDIR/$dir/$(basename $p)"
    if [ -e "$out" ]; then
	skip=$((skip+1))
	if [ $((skip % 1000)) -eq 0 ]; then
	    echo "Skippped $skip ..." >&2
	fi
    else
	echo "./filter_warc_by_ids.sh $IDIDX $idfile $url $out"
	count=$((count+1))
	if [ $((count % 1000)) -eq 0 ]; then
	    echo "Processed $count ..." >&2
	fi
    fi
done < <(cat "$TMPDIR/warc.paths") > "$TASKLIST"

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
perl -p -i -e 's|^(#SBATCH -[oe] )|${1}slurm-logs/|' "$JOB_TEMP"
# Puhti-specific adjustment
perl -p -i -e 's/^(#SBATCH -p) small.*/$1 large/' "$JOB_TEMP"

echo "----------------------------------------"
echo " Wrote $JOB_TEMP, starting the job with"
echo "     sbatch $JOB_TEMP"
echo "----------------------------------------"

sbatch "$JOB_TEMP"