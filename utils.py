import numpy as np
import pandas as pd
import numba
import glob, pickle
from os import path


def load_google_counts(lang, n, start_year, end_year):
    """ Get the dictionary of all the words for (lang, n, start, end) from filtered keys on disk """
    year_str = 'y{}-{}'.format(start_year, end_year)
    base_path = "/mnt/cluster-nas/ciprian/n-grams/"
    folder = path.join(base_path, lang, year_str, str(n))
    assert path.isdir(folder), "Invalid path " + folder

    word_counts = {}
    for file in glob.iglob(path.join(folder, "*.pkl")):
        with open(file, 'rb') as f:
            key_wc = pickle.load(f)
        word_counts.update(key_wc)

    return word_counts

def get_filtered_df(counts_df, quantile=None):
    ''' Returns a DataFrame of frequencies with years as rows and words as columns
    If `quantile` is given, only that percentile of most frequent words are kept

    Returns (df, ranks)
    '''
    print("Got %d words" % counts_df.shape[1])

    # find the words based on counts, rather than relative freqs
    word_ranks = counts_df.sum().rank(method='first')
    if quantile:
        assert(0 < quantile < 1)
        words_to_keep = word_ranks > quantile * word_ranks.max()

    # make sure we don't modify the original
    df = counts_df.copy()

    # remove empty years to avoid div by zero
    df = df.loc[counts_df.any(axis='columns'), :]

    # remove 2009 since it's an outlier
    df.drop(2009, axis='rows', inplace=True, errors='ignore')

    # counts -> frequencies
    df = df.div(df.sum(axis='columns'), axis='rows')

    # remove the rest of the words
    if quantile:
        df = df.loc[:, words_to_keep]
        word_ranks = word_ranks[words_to_keep]

    print("Filtered down to %d words" % df.shape[1])

    return df, word_ranks



@numba.jit(nopython=True)
def word_resilience(w):
    res, rmax = 0, 0
    for v in w:
        if v > 0:
            res += 1
        else:
            rmax = max(res, rmax)
            res = 0
    return max(rmax, res)

@numba.jit(nopython=True)
def _nmb_resilience(mat):
    ''' Calculates resiliences for all words in `mat`, which should be a numpy array with years on rows
    and words on columns. Returns np.array with each entry corresponding to the equivalent column in `mat` '''
    N = mat.shape[1]                     # number of words
    result = np.empty(N, dtype=np.uint8) # at most 208 years => uint8
    for i in range(N):
        result[i] = word_resilience(mat[:, i])
    return result

def df_resilience(df):
    ''' Calculates resilience for all words in DataFrame. One word per column.
    Returns pd.Series indexed by words. '''
    result = _nmb_resilience(df.values)
    return pd.Series(result, index=df.columns)

def filter_hapax(df, max_resilience=3):
    ''' Removes the hapax words, which are those with resilience <= max_resilience '''
    ress = df_resilience(df)
    return df.loc[:, ress > max_resilience]


