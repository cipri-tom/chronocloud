#!/usr/bin/python3
# -*- coding: utf-8 -*-

import os
import requests
from bs4 import BeautifulSoup
import zlib
import shutil

def decompress_stream(stream):
    d = zlib.decompressobj(16 + zlib.MAX_WBITS)

    for chunk in stream:
        yield d.decompress(chunk)

    yield d.flush()

def go(langue,n,num_stop=-1):
    ''' Downloads all the n-grams of the given language.
    Parameters:
    -----------
    langue : string -- The exact name of the language from the google page
    n      : int    -- Which n-grams to get [1-5]
    num_stop: int   -- If > 0, will stop after this many files
    '''

    if n > 5:
        print('Invalid n')
        return

    # setup the directory
    langue_name = langue.replace('(','').replace(')','').replace(' ','_')
    path_to_put = os.path.join('/mnt/cluster-nas/ciprian/n-grams', langue_name, '%d_gram' % n)
    if not os.path.exists(path_to_put):
        os.makedirs(path_to_put)

    # get the page with the links
    response = requests.get('http://storage.googleapis.com/books/ngrams/books/datasetsv2.html')
    soup     = BeautifulSoup(response.text,'html.parser')

    # move to the language and N we need
    target = soup.find('h1',text=langue)
    target = target.find_next('b', text='%d-grams' % n).parent # take the parent <p> to limit the children

    count = 0
    for link in target.findChildren('a'):
        # skip the categorical data (_ADJ_ _ADV_ ...) and other, punctuation
        if link.text.startswith('_') and link.text.endswith('_') or \
           link.text in ['other', 'punctuation']:
           continue

        # stream the file and decompress on the fly
        filename = os.path.join(path_to_put, link.text + '.txt')
        req = requests.get(link['href'], stream=True)
        with open(filename, 'wb') as f:
            for chunk in decompress_stream(req.iter_content(chunk_size=2048)):
                if chunk:
                    f.write(chunk)
        print(filename)

        count += 1
        if count == num_stop:
            break

#go('Chinese (simplified)',1)
#go('English Fiction',1)
#go('British English',1)
#go('American English',1)
#go('Russian',1)
#go('Hebrew',1)
#go('Spanish',1)
#go('German',1)
#go('Italian',1)
#go('French',1)
go('English',2)
