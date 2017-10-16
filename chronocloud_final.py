#!/usr/bin/python
# -*- coding: utf-8 -*-

import glob
import pickle
import numpy as np
from datetime import datetime
from functools import partial
from wordcloud import WordCloud
from PIL import Image, ImageFont, ImageDraw
from os import path


def image_from_np(matrix):
    w = matrix.shape[0]
    h = matrix.shape[1]
    x_div = int(1.0 * w / 2)
    y_div = int(1.0 * h / 2)
    img_1 = Image.fromarray(matrix[:x_div, :y_div], 'RGB')
    img_2 = Image.fromarray(matrix[:x_div, y_div:], 'RGB')
    img_3 = Image.fromarray(matrix[x_div:, :y_div], 'RGB')
    img_4 = Image.fromarray(matrix[x_div:, y_div:], 'RGB')
    image = Image.new('RGB', (w, h), (0, 0, 0))
    image.paste(img_1, (0, 0))
    image.paste(img_2, (x_div, 0))
    image.paste(img_3, (0, y_div))
    image.paste(img_4, (x_div, y_div))
    return image


def image_from_np_2(matrix):
    w = matrix.shape[0]
    h = matrix.shape[1]
    x_div = int(1.0 * w / 2)
    y_div = int(1.0 * h / 2)
    img_1 = image_from_np(matrix[:x_div, :y_div])
    img_2 = image_from_np(matrix[:x_div, y_div:])
    img_3 = image_from_np(matrix[x_div:, :y_div])
    img_4 = image_from_np(matrix[x_div:, y_div:])
    image = Image.new('RGB', (w, h), (0, 0, 0))
    image.paste(img_1, (0, 0))
    image.paste(img_2, (x_div, 0))
    image.paste(img_3, (0, y_div))
    image.paste(img_4, (x_div, y_div))
    return image


def generate_date_circle(matrix, dates, angles, pos_dep):
    res = matrix
    n = len(matrix)
    the_font_size = int(0.02 * n)
    the_font = ImageFont.truetype('NotoSans-Regular.ttf', the_font_size)
    for i in range(len(dates)):
        the_text = dates[i]
        image = Image.new('RGB', (n, n), (0, 0, 0))
        draw = ImageDraw.Draw(image)
        w, h = draw.textsize(the_text, the_font)
        pos_x = int(0.5 * n - 0.5 * w)
        pos_y = int((1.0 - pos_dep) * 0.5 * n - 0.5 * h)
        draw.text((pos_x, pos_y), the_text, (255, 255, 255), the_font)
        rotation = image.rotate(angles[i])
        arr = np.asarray(rotation)
        res += arr
        res[res > 255] = 255
    return res


def extract_year(frequencies, year_min):
    """ frequencies should be a numpy array """
    year_idx = np.argmax(frequencies)
    return year_idx + year_min


def extract_frequency(frequencies):
    """ frequencies should be a numpy array """
    return frequencies.sum()


def extract_resilience(frequencies):
    resilience, res_max = 0, 0
    for freq in frequencies:
        if freq != 0.0:
            resilience += 1
        else:
            res_max = max(res_max, resilience)
            resilience = 0
    res_max = max(res_max, resilience)
    return res_max


def color_func(colors, word, font_size, position, orientation, random_state=None, **kwargs):
    return colors[word]


def make_chronocloud(words_carac, n, name, langage):
    data = np.zeros((n, n, 3), dtype=np.uint8)
    dates = [str(year) for year in range(1840, 1991, 10)]
    dates[0] = '2000 | ' + dates[0]
    angles = [0, 337.5, 315.0, 292.5, 270.0, 247.5, 225.0, 202.5, 180.0, 157.5, 135.0, 112.5, 90.0, 67.5, 45.0, 22.5]
    data = generate_date_circle(data, dates, angles, 0.95)
    data = 255 - data
    print('chronocloud legend: done')
    if langage == 'Hebrew':
        the_font = 'NotoSansHebrew-Regular.ttf'
    elif langage == 'Chinese_simplified':
        the_font = 'NotoSansCJKtc-Regular.otf'
    else:
        the_font = 'NotoSans-Regular.ttf'

    # TODO: pack these in a dict `default_params` and pass as wc(..., **default_params)
    param_max_font_size = 0.03 * n
    param_relative_scaling = 0.5
    param_max_words = 5000

    # prepare the mask for the center, which is a ring or radius r_1
    resilience = 150  # for the center
    r_1 = 0.45 * (7 - (resilience / 25)) * (n / 5)
    a, b = n / 2, n / 2
    y, x = np.ogrid[0:n, 0:n]
    condition = (x - a) * (x - a) + (y - b) * (y - b) > r_1 * r_1

    # start with full image
    the_mask = np.zeros((n, n), dtype=np.uint8)
    the_mask[condition] = 255
    x_min = min(np.argwhere(the_mask == 0)[..., 0])
    x_max = max(np.argwhere(the_mask == 0)[..., 0])
    y_min = min(np.argwhere(the_mask == 0)[..., 1])
    y_max = max(np.argwhere(the_mask == 0)[..., 1])

    # but paint only inside the bounding rectangle of interest, because it's faster
    the_real_mask = the_mask[x_min:(x_max + 1), y_min:(y_max + 1)]

    the_words  = {}
    the_colors = {}
    for word in words_carac:
        if words_carac[word][2] >= resilience:
            the_words[word] = words_carac[word][1]
            try:
                the_colors[word] = words_carac[word][3]
            except:
                print(words_carac[word])
    var_1, var_2 = [], []
    if the_words:                        # it has some keys
        color_func_apply = partial(color_func, the_colors)
        wc = WordCloud(
            font_path=the_font,
            mask=the_real_mask,
            color_func=color_func_apply,
            prefer_horizontal=0.5,
            background_color='white',
            max_words=param_max_words,
            stopwords=[],
            relative_scaling=param_relative_scaling,
            max_font_size=param_max_font_size
        )
        wc.generate_from_frequencies(the_words)

        # extract data; need to expand the layout in order to add x_min, y_min
        var_1 += wc.words_
        for i in range(len(wc.layout_)):
            var_2.append((
                    wc.layout_[i][0], wc.layout_[i][1],
                    (wc.layout_[i][2][0] + x_min, wc.layout_[i][2][1] + y_min), wc.layout_[i][3], wc.layout_[i][4]
                )
            )
    print('chronocloud noyau: done')

    # prepare the sectors
    t  = 0.002         # taux ligne
    p1 = 2.41421356237
    p2 = 0.41421356237
    #modele : Z1 (x - Z2 * a) + Z3 (y - Z4 * b) > 0
    c_1  = [[- 1, 1+t,  0, 1  ],    [ p1, 1  ,  1, 1  ]]
    c_2  = [[-p1, 1  , -1, 1  ],    [  1, 1  ,  1, 1  ]]
    c_3  = [[- 1, 1  , -1, 1  ],    [ p2, 1  ,  1, 1  ]]
    c_4  = [[-p2, 1  , -1, 1  ],    [  0, 1  ,  1, 1-t]]
    c_5  = [[  0, 1  , -1, 1+t],    [-p2, 1  ,  1, 1  ]]
    c_6  = [[ p2, 1  , -1, 1  ],    [- 1, 1  ,  1, 1  ]]
    c_7  = [[  1, 1  , -1, 1  ],    [-p1, 1  ,  1, 1  ]]
    c_8  = [[ p1, 1  , -1, 1  ],    [- 1, 1+t,  0, 1  ]]
    c_9  = [[  1, 1-t,  0, 1  ],    [-p1, 1  , -1, 1  ]]
    c_10 = [[ p1, 1  ,  1, 1  ],    [- 1, 1  , -1, 1  ]]
    c_11 = [[  1, 1  ,  1, 1  ],    [-p2, 1  , -1, 1  ]]
    c_12 = [[ p2, 1  ,  1, 1  ],    [  0, 1  , -1, 1+t]]
    c_13 = [[  0, 1  ,  1, 1-t],    [ p2, 1  , -1, 1  ]]
    c_14 = [[-p2, 1  ,  1, 1  ],    [  1, 1  , -1, 1  ]]
    c_15 = [[- 1, 1  ,  1, 1  ],    [ p1, 1  , -1, 1  ]]
    c_16 = [[-p1, 1  ,  1, 1  ],    [  1, 1-t,  0, 1  ]]
    arretes = [c_1, c_2, c_3, c_4, c_5, c_6, c_7, c_8, c_9, c_10, c_11, c_12, c_13, c_14, c_15, c_16]

    # go through each sector[resilience, year]
    for resilience in [125, 100, 75, 50]:
        for years_lim in range(1840, 1991, 10):
            r_1 = 0.45 * (7 - (resilience / 25)) * (n / 5)
            r_2 = 0.45 * (6 - (resilience / 25)) * (n / 5)
            condition_1 = (x - a) * (x - a) + (y - b) * (y - b) > r_1 * r_1
            condition_2 = (x - a) * (x - a) + (y - b) * (y - b) < r_2 * r_2

            indice = (years_lim - 1840) // 10
            z1 = arretes[indice][0][0]
            z2 = arretes[indice][0][1]
            z3 = arretes[indice][0][2]
            z4 = arretes[indice][0][3]
            condition_3 = z1 * (x - z2 * a) + z3 * (y - z4 * b) > 0

            z1 = arretes[indice][1][0]
            z2 = arretes[indice][1][1]
            z3 = arretes[indice][1][2]
            z4 = arretes[indice][1][3]
            condition_4 = z1 * (x - z2 * a) + z3 * (y - z4 * b) > 0

            # TODO: aren't these redundant ?
            the_mask = np.zeros((n, n), dtype=np.uint8)
            the_mask[condition_1] = 255
            the_mask[condition_2] = 255
            the_mask[condition_3] = 255
            the_mask[condition_4] = 255

            x_min = min(np.argwhere(the_mask == 0)[..., 0])
            x_max = max(np.argwhere(the_mask == 0)[..., 0])
            y_min = min(np.argwhere(the_mask == 0)[..., 1])
            y_max = max(np.argwhere(the_mask == 0)[..., 1])
            the_real_mask = the_mask[x_min:(x_max + 1), y_min:(y_max + 1)]

            # TODO: generate the words relevant for this (resilience, year)
            the_words = {}
            the_colors = {}
            for word in words_carac:
                if resilience <= words_carac[word][2] < resilience + 25 and \
                   years_lim  <= words_carac[word][0] < years_lim  + 10:
                    the_words[word]  = words_carac[word][1]
                    the_colors[word] = words_carac[word][3]

            if the_words:
                color_func_apply = partial(color_func, the_colors)
                wc = WordCloud(
                    font_path=the_font,
                    mask=the_real_mask,
                    color_func=color_func_apply,
                    prefer_horizontal=0.5,
                    background_color='white',
                    max_words=param_max_words,
                    stopwords=[],
                    relative_scaling=param_relative_scaling,
                    max_font_size=param_max_font_size
                )
                wc.generate_from_frequencies(the_words)
                var_1 += wc.words_
                for i in range(len(wc.layout_)):
                    var_2.append((
                            wc.layout_[i][0], wc.layout_[i][1],
                            (wc.layout_[i][2][0] + x_min, wc.layout_[i][2][1] + y_min), wc.layout_[i][3], wc.layout_[i][4]
                        )
                    )
        print('chronocloud R=' + str(resilience) + ': done')

    wc_montre = WordCloud(font_path=the_font, background_color='white', width=n, height=n)
    wc_montre.words_  = var_1
    wc_montre.layout_ = var_2
    fichier = open(name + '_chronodata_words_alt.txt', 'w')
    for i in range(len(var_1)):
        fichier.write(str(var_1[i]) + '\n')
    fichier.close()
    fichier = open(name + '_chronodata_layout_alt.txt', 'w')
    for i in range(len(var_2)):
        fichier.write(str(var_2[i]) + '\n')
    fichier.close()
    data_1 = 255 - data
    data_2 = 255 - wc_montre.to_array()
    data = data_1 + data_2
    data[data > 255] = 255
    data = 255 - data
    image_from_np_2(data).save(name + '_chronocloud.png')


def est_alpha(ngram):
    reponse = 1
    decopose = ngram.split(' ')
    for gram in decopose:
        if not gram.isalpha():
            reponse = 0
    return reponse


## removed import journal


# ----------------------------------------------------------------------------------------------------

def freqs_from_counts(word_counts):
    some_value = next(iter(word_counts.values()))
    year_counts = np.zeros(len(some_value), dtype=np.uint64)

    for counts in word_counts.values():
        year_counts += counts

    word_freqs = {}
    for word, counts in word_counts.items():
        word_freqs[word] = counts / year_counts

    return word_freqs


def import_google(lang, n):
    """ reduces range from 1800-2012 to 1840, 2000 """
    from utils import load_google_counts

    word_counts = load_google_counts(lang, n, 1800, 2012)
    # select range 1840 -- 2000
    for word, entry in word_counts.items():
        word_counts[word] = word_counts[word][40:-12+1]
    return freqs_from_counts(word_counts)


# ----------------------------------------------------------------------------------------------------


def go(langue, nbg, resolution):
    ## ONLY WORKS WITH GOOGLE NOW

    debut = datetime.now()
    words_frequencies = import_google(langue, nbg)
    fin = datetime.now()
    print('step 1: done / ' + str(fin - debut))

    # TODO: cleanup to use numpy
    words_carac = {}
    the_max_vec = []
    all_res = [50, 75, 100, 125, 150]
    all_years = [1840, 1850, 1860, 1870, 1880, 1890, 1900, 1910, 1920, 1930, 1940, 1950, 1960, 1970, 1980, 1990]
    for i in range(len(all_res)):
        temp = []
        for j in range(len(all_years)):
            temp.append(0.0)
        the_max_vec.append(temp)
    for word in words_frequencies:
        year_max_res = extract_year(words_frequencies[word], 1840)
        frequency = extract_frequency(words_frequencies[word])
        resilience = extract_resilience(words_frequencies[word])

        words_carac[word] = [year_max_res, frequency, resilience]

        if 50 <= resilience < 75:
            for t in range(len(all_years)):
                if year_max_res >= all_years[t] and year_max_res < all_years[t] + 10:
                    the_max_vec[0][t] = max(the_max_vec[0][t], frequency)
        if 75 <= resilience < 100:
            for t in range(len(all_years)):
                if year_max_res >= all_years[t] and year_max_res < all_years[t] + 10:
                    the_max_vec[1][t] = max(the_max_vec[1][t], frequency)
        if 100 <= resilience < 125:
            for t in range(len(all_years)):
                if year_max_res >= all_years[t] and year_max_res < all_years[t] + 10:
                    the_max_vec[2][t] = max(the_max_vec[2][t], frequency)
        if 125 <= resilience < 150:
            for t in range(len(all_years)):
                if year_max_res >= all_years[t] and year_max_res < all_years[t] + 10:
                    the_max_vec[3][t] = max(the_max_vec[3][t], frequency)
        if 150 <= resilience:
            for t in range(len(all_years)):
                if year_max_res >= all_years[t] and year_max_res < all_years[t] + 10:
                    the_max_vec[4][t] = max(the_max_vec[4][t], frequency)
    the_max_vec = list(filter(lambda a: a != 0, the_max_vec))
    the_mean_vec = []
    for i in range(len(the_max_vec)):
        the_mean_vec.append(1.0 * sum(the_max_vec[i]) / len(the_max_vec[i]))
    the_max = the_mean_vec[3]
    word_to_sort = []
    word_to_sort_freq = []
    for word in words_carac.keys():
        word_to_sort_freq.append(words_carac[word][1])
        word_to_sort.append(word)
    word_to_sort_freq, word_to_sort = zip(*sorted(zip(word_to_sort_freq, word_to_sort), reverse=True))
    for i in range(len(word_to_sort)):
        the_freq = min(word_to_sort_freq[i], the_max)
        the_value = int(250.0 - (250.0 * the_freq / the_max))
        words_carac[word_to_sort[i]].append('hsl(' + str(the_value) + ', 100%, 30%)')
    fin = datetime.now()
    print('step 2: done / ' + str(fin - debut))
    name = langue + '_' + str(nbg) + '_' + str(resolution) + '_final'
    name = 'chrono_images/' + name
    make_chronocloud(words_carac, resolution, name, langue)
    fin = datetime.now()
    print('chronocloud "' + langue + '": done / ' + str(fin - debut))


# go('JDG','French',1,80000)
# go('GDL','French',1,80000)
# go('French_google','French',1,80000)
# go('English_google','English',1,80000)
