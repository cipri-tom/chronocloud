#!/usr/bin/python
# -*- coding: utf-8 -*-

import scipy
import pymysql
import multiprocessing
from functools import partial
from datetime import datetime
from lmfit.models import GaussianModel

def extract_resilience(frequencies,journal):
    if journal=='JDG':
        years=list(range(1826,1998))
        years_remove=[1837,1859,1860,1917,1918,1919]
    if journal=='GDL':
        years=list(range(1798,1998))
        years_remove=[1800,1801,1802,1803]
    resilience=0
    resilience_max=0
    for i in range(len(years)):
        if years[i] not in years_remove:
            if frequencies[i]!=0.0:
                resilience+=1
            else:
                resilience_max=max(resilience_max,resilience)
                resilience=0
    resilience_max=max(resilience_max,resilience)
    return resilience_max

def fit(les_x_init,les_y_init,remove):
    les_x=[]
    les_y=[]
    for i in range(len(les_x_init)):
        if les_x_init[i] not in remove:
            les_x.append(les_x_init[i])
            les_y.append(les_y_init[i])
    x=scipy.asarray(les_x)
    y=scipy.asarray(les_y)
    gmod=GaussianModel()
    param=gmod.guess(y, x=x)
    amplitude=param['amplitude'].value
    center=param['center'].value
    sigma=param['sigma'].value
    the_fit=gmod.fit(y,x=x,amplitude=amplitude,center=center,sigma=sigma)
    best_res=the_fit.chisqr
    amplitude=the_fit.params['amplitude'].value
    center=the_fit.params['center'].value
    sigma=the_fit.params['sigma'].value
    best_sol=[amplitude,center,sigma]
    y_fit=[]
    for i in range(len(les_x_init)):
        y_fit.append(gmod.eval(x=les_x_init[i],amplitude=amplitude,center=center,sigma=sigma))
    return [best_sol,best_res,y_fit]

def gauss_fit(mot,dico,level,gram_tot,cursor,table,journal):
    if journal=='JDG':
        years=list(range(1826,1998))
        years_remove=[1837,1859,1860,1917,1918,1919]
    if journal=='GDL':
        years=list(range(1798,1998))
        years_remove=[1800,1801,1802,1803]
    gram_split=gram_tot.split(' ')
    nbg=len(gram_split)
    the_fit=fit(years,dico[mot][0],years_remove)
    moyenne=sum(dico[mot][0])/len(dico[mot][0])
    moyenne_fit=sum(the_fit[2])/len(the_fit[2])
    val_0=0.0
    val_1=0.0
    val_2=0.0
    for i in range(len(years)):
        if years[i] not in years_remove:
            val_0+=(dico[mot][0][i]-moyenne)*(the_fit[2][i]-moyenne_fit)
            val_1+=(dico[mot][0][i]-moyenne)**2
            val_2+=(the_fit[2][i]-moyenne_fit)**2
    if val_1!=0.0 and val_2!=0.0:
        score=1.0-(val_0**2/(val_1*val_2))
        variables='(nbg,gram'
        for i in range(9):
            variables+=',gram_'+str(i+1)
        variables+=',amplitude,center,sigma,residus,fit_quality)'
        valeurs='('+str(nbg)+',"'+gram_tot+'"'
        for i in range(len(gram_split)):
            valeurs+=',"'+gram_split[i]+'"'
        for i in range(len(gram_split),9):
            valeurs+=',""'
        valeurs+=','+str(the_fit[0][0])
        valeurs+=','+str(the_fit[0][1])
        valeurs+=','+str(the_fit[0][2])
        valeurs+=','+str(the_fit[1])
        valeurs+=','+str(score)+')'
        cursor.execute('INSERT INTO '+table+' '+variables+' VALUES '+valeurs)
        if nbg!=level:
            if dico[mot][1]!={}:
                for gram in dico[mot][1].keys():
                    gauss_fit(gram,dico[mot][1],level,gram_tot+' '+gram,cursor,table,journal)

def go(journal):
    liste_mots=[]
    liste_sommes=[]
    if journal=='JDG':
        for mot in jdg.keys():
            #if extract_resilience(jdg[mot][0],journal)==166:
            if extract_resilience(jdg[mot][0],journal)>=100:
                liste_mots.append(mot)
    if journal=='GDL':
        for mot in gdl.keys():
            #if extract_resilience(gdl[mot][0],journal)==196:
            if extract_resilience(gdl[mot][0],journal)>=100:
                liste_mots.append(mot)
    connection=pymysql.connect(host='cdh-dhlabpc3.epfl.ch',user='',password='',db='ling_cap',charset='utf8')
    cursor=connection.cursor()
    table=journal+'_gaussians'
    compteur=0
    longueur=len(liste_mots)
    if journal=='JDG':
        for mot in liste_mots:
            gauss_fit(mot,jdg,9,mot,cursor,table,journal)
            compteur+=1
            print(journal+' / '+str(compteur)+' / '+str(longueur)+' / '+mot)
    if journal=='GDL':
        for mot in liste_mots:
            gauss_fit(mot,gdl,9,mot,cursor,table,journal)
            compteur+=1
            print(journal+' / '+str(compteur)+' / '+str(longueur)+' / '+mot)
    connection.close()

go('GDL')
go('JDG')
