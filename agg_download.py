import boto
import boto.s3.connection

import sys, time, os, glob
import zlib, codecs
import pickle

import numpy as np
import regex as re
from datetime import datetime
import multiprocessing as mp


def build_prefix(lang, n, version='20120701'):
    """ Builds a prefix for listing keys from the GoogleBooks bucket.
    This is mostly hardcoded and doesn't check if the prefix is valid (returns existing keys)

    Parameters:
    -----------
        lang : 3-letter string for language. It doesn't check if the language actually exists
        n    : the n-gram.
        version: which data version, e.g. '20120701'
    """
    # TODO: you could validate this agains the bucket by checking each prefix
    return '{lang}/googlebooks-{lang}-all-{n}gram-{version}-'.format(lang=lang, n=n, version=version)


def get_s3_bucket():
    """ Returns the bucket object for downloading the files. Currently uses the boto2 interface"""
    conn = boto.connect_s3(anon=True, host='datasets.iccluster.epfl.ch',
                           calling_format=boto.s3.connection.OrdinaryCallingFormat())
    return conn.get_bucket('google-ngrams')


def key_basename(key):
    """ Customised basename for google keys, where the separator is '-'.
    By default, returns the name without extension. Set extension=True to include it """
    return key.name.rsplit('-', 1)[-1].split('.')[0]


def decompress_stream(stream):
    """ Given an iterable stream of gzipped data (such as a `key` in S3 storage), this function returns an iterator
    over the uncompressed and utf-8 decoded data """
    extracter = zlib.decompressobj(16 + zlib.MAX_WBITS)
    decoder   = codecs.getincrementaldecoder('utf-8')()  # note the second () which instantiates the object

    for chunk in stream:
        yield decoder.decode( extracter.decompress(chunk) )

    yield decoder.decode( extracter.flush() , final = True)


def get_entry(word, range_size):
    """ Given `word` returns its clean form and an integer array if it is valid
    or the empty string and None if it is invalid """
    # skip if it's not letters only (and space _ apostrophe) or is "word _ADJ_
    # TODO: check this works for 3-grams etc
    if re.search(r"[^\p{Lu}\p{Ll}_' ]|\b_[A-Z]*_\b", word):
        return "", None

           # remove POS tags             #lower
    return re.sub(r'_[A-Z]*\b', '', word).lower(),\
           np.zeros(range_size, dtype=np.uint32)    # array of counts


def save_counts(counts, save_path):
    """ Serializes the dict of counts """
    with open(save_path, 'wb') as f:
        pickle.dump(counts, f, pickle.HIGHEST_PROTOCOL)


def process_key(key, save_path, start_year=1840, end_year=2001):
    """ Given the keyname, save a dictionary of the wordcount aggregates """

    # setup the counting structure
    range_size = end_year - start_year # open interval
    word_counts = {}

    # since verifying and cleaning an entry is quite expensive (2 RegEx)
    # we do it only once, when we detect the start of a new block
    # `word_prev_line` is used for checking if current line starts the block
    # of a new entry; `word_entry` holds the array for `word_clean`
    word_prev_line   = u"_unk_curr_word_"
    word_clean = u""
    word_entry = None
    word_default_entry = np.zeros(range_size, dtype=np.uint32)

    prev_chunk_end  = u""
    for chunk in decompress_stream(key):
        lines = chunk.split('\n')

        # complete chunk with the ending of previous one
        lines[0] = prev_chunk_end + lines[0]

        # leave last line for next chunk since it may be incomplete
        prev_chunk_end = lines[-1]

        for line in lines[:-1]:
            word_line, rest_line = line.split('\t', 1)

            if word_line != word_prev_line:
                if word_entry is not None and word_entry.sum() > range_size * 35:
                    # we have a valid entry, we add the one up to now. adding because of `.lower()`
                    word_counts[word_clean] = word_entry + word_counts.get(word_clean, word_default_entry)

                word_prev_line = word_line                                     # update the form
                word_clean, word_entry = get_entry(word_prev_line, range_size) # generate new entry

            if not word_clean:
                continue
            # here we can rely on word_entry (it is valid)

            # check if fits our range
            year, count, _ = rest_line.split('\t')
            year = int(year)
            if not start_year <= year < end_year:
                continue

            idx = year - start_year
            word_entry[idx] += int(count)

    # we finished, we save it
    save_counts(word_counts, save_path)
    return key, len(word_counts.keys())


def retry_process_key(*args, **kw_args):
    """ Stupid workaround because MP doesn't support decorators
    Retries `process_key` for 5 times """
    num_tries = 5
    while num_tries:
        try:
            num_tries -= 1
            return process_key(*args, **kw_args)
        except Exception as e:
            # this is usually a timeout exception. The resource could be busy, so wait a bit
            time.sleep(1)
            if not num_tries:
                # we return the exception, so that it can be logged
                return str(e)
    return "Nothing"


# this belongs to global thread
log_file = None
start_time = 0
num_keys, num_kbytes, num_words = 0, 0, 0
def logger(res):
    global log_file, start_time, num_keys, num_kbytes, num_words

    if type(res) is str:
        # it was an error
        print(datetime.now(), res, file=log_file, flush=True)
        print(datetime.now(), res, flush=True)
        return

    time_sofar = int(time.time() - start_time)

    key, key_words = res
    print(key_basename(key), flush=True)

    num_keys   += 1
    num_kbytes += key.size >> 10
    num_words  += key_words

    print("{info} Key {kn} finished. Elapsed: {total}s ({avg} s/key, {b} kbytes/s)"
          " {w} words in {k} keys.".format(
              info = datetime.now(), kn = key_basename(key), total = time_sofar,
              avg=time_sofar // num_keys, b = num_kbytes // time_sofar,
              w=num_words, k=num_keys), file=log_file, flush=True)


def aggregate(lang, n, start_year, end_year):
    """ Downloads filtered aggregates for given years and n """
    year_str  = 'y{}-{}'.format(start_year, end_year)
    root_path = os.path.join('/mnt/cluster-nas/ciprian/n-grams/', lang, year_str, str(n))

    num_files = len(glob.glob(root_path + '/*.pkl'))
    if n == 1 and num_files == 26 or num_files == 26**2:
        print('Data exists at ', root_path)
        return

    if not os.path.exists(root_path):
        ans = 'm'
        while ans not in 'yn' or len(ans) != 1:
            ans = input('Create directory ' + root_path + ' ? (y/n)')
        if ans == 'n':
            return

        os.makedirs(root_path, exist_ok=True)


    global log_file, start_time

    bucket = get_s3_bucket()
    prefix = build_prefix(lang, n)
    log_file = open("{l}{n}{date:%m-%d-%H-%M}.log".format(l=lang, n=n, date=datetime.now()), "a")

    # paralellism
    pool = mp.Pool(20)
    start_time = time.time() - 1 # to avoid 0 time

    # map them out
    for key in sorted(bucket.list(prefix, '-'), key=lambda k: k.size, reverse=True):
        # skip uninteresting files; including "a_"
        name = key_basename(key)
        if not name.isalpha() or name in ['other', 'pos', 'punctuation']:
            continue

        key_path = os.path.join(root_path, name + '.pkl')
        if os.path.exists(key_path):
            print("Exists", name, file=log_file, flush=True)
            continue

        pool.apply_async(retry_process_key, (key, key_path, start_year, end_year), callback=logger)

    # no more tasks
    pool.close()

    # wait for any unfinished ones
    pool.join()
    log_file.close()


if __name__ == "__main__":
    if len(sys.argv) != 5:
        print('Usage: python agg_download lang n start_year end_year')
        print('Nothing done')
        exit()

    aggregate(sys.argv[1], int(sys.argv[2]), int(sys.argv[3]), int(sys.argv[4]))
