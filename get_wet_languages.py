#!/usr/bin/env python3

# Get IDs and language information from WET files.

import sys
import gzip
import functools
import logging

import langid
import langdetect
import fasttext
import pycld2
import cld3

from collections import defaultdict
from time import perf_counter
from argparse import ArgumentParser

from warcio.archiveiterator import ArchiveIterator


# Use https://dl.fbaipublicfiles.com/fasttext/supervised-models/lid.176.bin
FASTTEXT_MODEL = fasttext.load_model('lid.176.bin')

# For norm_probs=True
LANGID_IDENTIFIER = langid.langid.LanguageIdentifier.from_modelstring(
   langid.langid.model, norm_probs=True)


def argparser():
    ap = ArgumentParser()
    ap.add_argument('-v', '--verbose', default=False, action='store_true')
    ap.add_argument('wet', nargs='+')
    return ap


def named_timer(name):
    def timer(func):
        @functools.wraps(func)
        def wrapper(*args, **kwargs):
            start = perf_counter()
            value = func(*args, **kwargs)
            named_timer.times[name] += perf_counter() - start
            return value
        return wrapper
    return timer
named_timer.times = defaultdict(int)


def uniq(seq):
    seen = set()
    return [x for x in seq if not (x in seen or seen.add(x))]


@named_timer('langdetect')
def langdetect_langs(text):
    try:
        pred = langdetect.detect_langs(text)
        return ','.join(f'{p.lang}:{p.prob:.5}' for p in pred)
    except:
        return None


@named_timer('langid')
def langid_langs(text):
    global LANGID_IDENTIFIER
    lang, prob = LANGID_IDENTIFIER.classify(text)
    return f'{lang}:{prob:.5}'


@named_timer('fasttext')
def fasttext_langs(text):
    global FASTTEXT_MODEL
    text = text.replace('\n', ' ')
    labels, probs = FASTTEXT_MODEL.predict([text])
    label, prob = labels[0][0], probs[0][0]
    label = label.replace('__label__', '')
    return f'{label}:{prob:.5}'


@named_timer('cld2')
def cld2_langs(text):
    try:
        is_reliable, bytes_found, details = pycld2.detect(text)
        return ','.join(uniq(d[1] for d in details))
    except:
        return None


@named_timer('cld3')
def cld3_langs(text):
    pred = cld3.get_language(text)
    if pred is None:
        return None
    else:
        return f'{pred.language}:{pred.probability:.5}'


def get_record_id(record):
    return record.rec_headers.get_header('WARC-Record-ID')


def get_refers_to(record):
    return record.rec_headers.get_header('WARC-Refers-To')


def get_identified_language(record):
    return record.rec_headers.get_header('WARC-Identified-Content-Language')


def process_stream(flo):
    logging.info(f'START processing {flo.name}')
    for record in ArchiveIterator(flo):
        if record.rec_type != 'conversion':
            continue
        id_ = get_record_id(record)
        logging.info(f'reading {id_}')
        ref_id = get_refers_to(record)
        language = get_identified_language(record)
        content = record.content_stream().read().decode('utf-8')
        detected_languages = [
            langdetect_langs(content),
            langid_langs(content),
            fasttext_langs(content),
            cld2_langs(content),
            cld3_langs(content),
        ]
        detected_str = '\t'.join(str(d) for d in detected_languages)
        print(f'{id_}\t{ref_id}\t{language}\t{detected_str}')
    logging.info(f'END processing {flo.name}')


def main(argv):
    args = argparser().parse_args(argv[1:])

    logging.basicConfig()
    if args.verbose:
        logging.getLogger().setLevel(logging.INFO)

    for fn in args.wet:
        with gzip.open(fn) as f:
            process_stream(f)

    for k, v in sorted(named_timer.times.items()):
        print(f'{k}\t{v}', file=sys.stderr)


if __name__ == '__main__':
    sys.exit(main(sys.argv))
