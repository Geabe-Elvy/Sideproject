import pandas as pd
import numpy as np
import requests
import json
import time as tm
import math
import networkx as nx
import matplotlib.pyplot as plt
from tkinter import filedialog
import pycountry
from fuzzywuzzy import fuzz

# créer une fonction pour calculer la similarité entre deux chaînes de caractères
def similarity(string1, string2):
    return fuzz.token_set_ratio(string1, string2)

axyz.iloc[:,-7:-1] = axyz.iloc[:,-7:-1].apply(lambda x: x.str.upper())

cols = ['sim_o', 'sim_pc', 'sim_xp1', 'sim_xp2', 'sim_xp3', 'sim_xp4', 'sim_xp5']
for i in range(0, 7):
    axyz[cols[i]] = axyz.apply(lambda row: similarity(row['app_name'], row[axyz.columns[9+i]]), axis=1)

good = axyz[axyz[cols[0]] > 60]
for i in range(1, 7):
    good = pd.concat([good, axyz[axyz[cols[i]] > 60]])

good = good[-good.lens_id.isna()].drop_duplicates()
good.lens_id
good.name.drop_duplicates()

X = pd.merge(X, bioT[['name', 'Company', 'prev_comp', 'org_xp1', 'org_xp2', 'org_xp3', 'org_xp4', 'org_xp5']], left_on='inv_name', right_on='name', how='outer')
X = pd.merge(X, bioT[['name_rev', 'Company', 'prev_comp', 'org_xp1', 'org_xp2', 'org_xp3', 'org_xp4', 'org_xp5']], left_on='inv_name', right_on='name_rev', how='outer')
for i in range(3, 11):
    X.iloc[:,i] = X.iloc[:,i].fillna(X.iloc[:,8+i])
X = X.drop(X.iloc[:,-8:].columns, axis=1)

X.iloc[:,-8:] = X.iloc[:,-8:].apply(lambda x: x.str.upper())

cols = ['sim_o', 'sim_pc', 'sim_xp1', 'sim_xp2', 'sim_xp3', 'sim_xp4', 'sim_xp5']
for i in range(0, 7):
    X[cols[i]] = X.apply(lambda row: similarity(row['app_name'], row[X.columns[4+i]]), axis=1)

good2 = X[X[cols[0]] > 60]
for i in range(1, 7):
    good2 = pd.concat([good2, X[X[cols[i]] > 60]])
good2 = good2[-good2.lens_id.isna()].drop_duplicates()


good2 = good2[-good2.lens_id.isin(good.lens_id)]
good = pd.concat([good, good2])
good_id= len(list(good.lens_id.drop_duplicates()))

good.to_csv('C:/Users/vernhes/OneDrive - ENSTA Paris/Bureau/Sideproject/Data2/good_T.csv', index=False)

df_Bio_T = pd.merge(good[['inv_name', 'fam_id', 'lens_id']], df_T[df_T.lens_id.isin(good.lens_id)], on='lens_id')

# Définir une fonction pour inverser les noms et prénoms
def inverser_noms_prenoms(name):
    names = name.split(' ')
    return names[1] + ' ' + names[0]

dico_name = pd.DataFrame(bioT.name.str.upper())
dico_name['good_name'] = bioT.name.str.upper()
dico_name2 = pd.DataFrame(bioT['name'].astype('str').apply(inverser_noms_prenoms).str.upper())
dico_name2['good_name'] = bioT.name.str.upper()
dico_name = pd.concat([dico_name, dico_name2]).drop_duplicates()
dico_name.columns = ['inv_name', 'good_name']
df_Bio_T = df_Bio_T.merge(dico_name, on='inv_name', how='left')
df_Bio_T.inv_name = df_Bio_T.good_name
df_Bio_T = df_Bio_T.drop('good_name', axis=1)
df_Bio_T['inv_name'] = df_Bio_T['inv_name'].apply(lambda x: x.title())

df_Bio_T.to_csv('C:/Users/vernhes/OneDrive - ENSTA Paris/Bureau/Sideproject/Data2/df_Bio_T.csv', index=False)

bioT = bioT.drop(['name', 'name_rev', 'ctry_apl2', 'Name', 'sem_loc', 'sem_city', 'prev_comp'], axis=1)
bioT.columns = ['Conference name', 'date', 'place', 'name', 'Job', 'Company', 'country', 'org_xp1', 'org_xp2', 'org_xp3', 'org_xp4', 'org_xp5']

bioT = pd.merge(bioT, df_bio_S.authors_name.value_counts(), left_on='name', right_on=df_bio_S.authors_name.value_counts().index, how='outer')

T = pd.merge(df_Bio_T.inv_name.value_counts(), df_Bio_T[['inv_name', 'fam_id']].drop_duplicates()['inv_name'].value_counts(), left_on=df_Bio_T.inv_name.value_counts().index, right_on=df_Bio_T[['inv_name', 'fam_id']].drop_duplicates()['inv_name'].value_counts().index)

bioT = pd.merge(bioT, T, left_on='name', right_on='key_0', how='outer')
bioT = bioT.drop('key_0', axis=1)
bioT.columns = bioT.columns.str.replace('authors_name','nb_articles').str.replace('inv_name_y','nb_invention_fam').str.replace('inv_name_x','nb_patent')

bioT.to_csv('C:/Users/vernhes/OneDrive - ENSTA Paris/Bureau/Sideproject/Data2/bioT.csv', index=False)
bioT = pd.read_csv('C:/Users/vernhes/OneDrive - ENSTA Paris/Bureau/Sideproject/Data2/bioT.csv')

bioT.columns = ['Conference name', 'date', 'place', 'name', 'Job', 'Company', 'country',
       'org_xp1', 'org_xp2', 'org_xp3', 'org_xp4', 'org_xp5', 'nb_articles',
       'nb_patent', 'nb_invention_fam']
