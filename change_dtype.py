import numpy as np
import pickle
import sys
from os import path
import glob

def main(lang, n, start_year, end_year):
    """ Get the dictionary of all the words for (lang, n, start, end) from filtered keys on disk """
    year_str = 'y{}-{}'.format(start_year, end_year)
    base_path = '/mnt/cluster-nas/ciprian/n-grams/'
    folder = path.join(base_path, lang, year_str, str(n))
    assert path.isdir(folder), 'Invalid path ' + folder

    for i, file in enumerate(glob.iglob(path.join(folder, '*.pkl'))):
        if i % 100 == 0:
            print(lang, n, i, file)
        new_file, ext = path.splitext(file)
        new_file += '_uint' + ext
        with open(file, 'rb') as fin, open(new_file, 'wb') as fout:
            all_counts = pickle.load(fin)
            new_counts = {}

            for word, counts in all_counts.items():
                new_counts[word] = counts.astype(np.uint32)

                # sanity check
                if np.any(new_counts[word] != counts):
                    raise ValueError('Error converting file %s for key %s' % (file, k))

            pickle.dump(new_counts, fout, pickle.HIGHEST_PROTOCOL)



if __name__ == '__main__':
    print('Usage: python change_dtype.py lang n start_year end_year')
    if len(sys.argv) < 5:
        sys.exit()
    main(*sys.argv[1:])


