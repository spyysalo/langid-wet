#!/usr/bin/env python3

# Filter warc file to records with given WARC-Record-ID values.

import sys
import gzip
import logging

from time import time
from functools import wraps
from argparse import ArgumentParser

from warcio import WARCWriter
from warcio.archiveiterator import ArchiveIterator



def argparser():
    ap = ArgumentParser()
    ap.add_argument('ids')
    ap.add_argument('warc_in')
    ap.add_argument('warc_out')
    ap.add_argument('-f', '--field', default=0, type=int)
    ap.add_argument('-v', '--verbose', default=False, action='store_true')
    return ap


def get_record_id(record):
    return record.rec_headers.get_header('WARC-Record-ID')


def filter_warc_stream(ids, warc_in, warc_out):
    writer = WARCWriter(warc_out, gzip=True)

    output, total, errors = 0, 0, 0
    for record in ArchiveIterator(warc_in):
        id_ = get_record_id(record)
        if id_ in ids:
            output += 1
            try:
                writer.write_record(record)
            except Exception as e:
                logging.error(f'failed to write record: {e}')
                errors += 1
        total += 1
        if total % 10000 == 0:
            logging.info(f'processed {total} records, output {output}, '
                         f'{errors} errors')
    print(f'Done, processed {total} records, output {output}, '
          f'{errors} errors')


def timed(f, out=sys.stderr):
    @wraps(f)
    def wrapper(*args, **kwargs):
        start = time()
        result = f(*args, **kwargs)
        print(f'{f.__name__} completed in {time()-start:.1f} sec', file=out)
        return result
    return wrapper


@timed
def load_response_ids(fn, args):
    if not fn.endswith('.gz'):
        xopen = open
    else:
        xopen = lambda p: gzip.open(p, mode='rt', encoding='utf-8')

    ids = set()
    with xopen(fn) as id_in:
        for ln, l in enumerate(id_in, start=1):
            fields = l.rstrip('\n').split('\t')
            id_ = fields[args.field]
            if id_ in ids:
                logging.warning('duplicate id {id_} on line {ln} in {fn}')
            ids.add(id_)
    return ids


def main(argv):
    args = argparser().parse_args(argv[1:])

    logging.basicConfig()
    if args.verbose:
        logging.getLogger().setLevel(logging.INFO)

    ids = load_response_ids(args.ids, args)

    with gzip.open(args.warc_in) as warc_in:
        with open(args.warc_out, 'wb') as warc_out:
            filter_warc_stream(ids, warc_in, warc_out)


if __name__ == '__main__':
    sys.exit(main(sys.argv))
