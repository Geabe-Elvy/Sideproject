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

bioT = pd.read_csv("C:/Users/vernhes/OneDrive - ENSTA Paris/Bureau/Sideproject/Biotech Conf names.csv")

#Intégrer les données issues du scrapping Google
bioT = pd.merge(bioT, gogole, left_on='SPEAKERS', right_on='name')

A = bioT.SPEAKERS.dropna().str.upper().str.split(' ').str[1:].reset_index(drop=True)
B = bioT.SPEAKERS.dropna().str.upper().str.split(' ').str[0].reset_index(drop=True)
C = bioT.SPEAKERS.dropna().str.upper().reset_index(drop=True)

bioT['name'] = bioT['name_rev'] = np.nan
bioT_name = list([])
for i in range(0, len(A)):
    A[i] = ' '.join(A[i])
    bioT_name.append(C[i])
    bioT['name'][i] = C[i]
    bioT_name.append(A[i] + " " + B[i])
    bioT['name_rev'][i] = A[i] + " " + B[i]

bioT['ctry_apl2'] = np.nan
for i in range(0, len(bioT)):
    try:
        bioT['ctry_apl2'][i] = pycountry.countries.search_fuzzy(bioT.country[i])[0].alpha_2
    except:
        bioT['ctry_apl2'][i] = np.nan
bioT['ctry_apl2'] = bioT['ctry_apl2'].str.replace('UG', 'GB')

# recherche de loc dans les bio venant avec la première série de noms
biog = pd.read_csv("C:/Users/vernhes/OneDrive - ENSTA Paris/Bureau/Sideproject/Biogr_nom_bioT.csv", sep="|")
world_cities_name = pd.read_csv("C:/Users/vernhes/OneDrive - ENSTA Paris/Bureau/S5/Nom villes région pays du monde csv/worldcities.csv")

def search_word(word_list, string):
    found_words = []
    for word in word_list:
        if word in string:
            found_words.append(word)
    return found_words

biog['sem_loc'] = np.nan
for i in range(0, len(biog)):
    try:
        biog['sem_loc'][i] = search_word(world_cities_name.country.drop_duplicates().astype('str'), biog.Biography[i])
    except: i+=1

biog['sem_city'] = np.nan
for i in range(0, len(biog)):
    try:
        biog['sem_city'][i] = search_word(world_cities_name[world_cities_name.population > 1_000_000].city
                                          .drop_duplicates().astype('str'), biog.Biography[i])
    except: i+=1

for i in range(0, len(biog)):
    try :
        for j in range(0, len(biog.sem_loc[i])):
            try:
                biog.sem_loc[i][j] = pycountry.countries.get(name=biog.sem_loc[i][j]).alpha_2
            except: j+=1
    except: i+=1

bioT.loc[0:19, 'ctry_apl2'] = biog.loc[0:19, 'sem_loc']
bioT = pd.merge(bioT, biog[['Name', 'sem_loc']], left_on='name' , right_on='Name', how='outer')

# REQUEST Patents ---
lens_api_url = 'https://api.lens.org/patent/search'
headers = {'Authorization': 'Bearer 52eky7NN6lequIKALGi8XA7CmIS1UAg0EMliDPo15WtSTEC4EnR0',
           'Content-Type': 'application/json'}
size = 100  # 100 lignes par requêtes

payload ={
  "query": {
    "bool": {
      "must": [
        {
          "terms": {
            "inventor.name.exact": bioT_name
          }
        },
        {
        "range": {
            "year_published": {
                "gte": "2015",
                "lte": "2022"
            }
        }
        }
      ]
    }
  },
  "size": 100,
        "scroll": "5m"
}

try:
    response = requests.post(lens_api_url, data=json.dumps(payload), headers=headers)#Si les lignes suivantes fonctionnent correctement alors on ne run pas except
    res_json = response.json()
    data = res_json['data']
    _scroll_id = res_json['scroll_id']
    print(f'Tout est bon !')
except KeyError:
    data = []
    _scroll_id = None
    print('Error: Elastic Search: %s' % str(response.json()))

# SCROLL ---

rows = []
time = 0
n = math.floor(res_json['total']/size)#calcule le nombre de requête nécessaire pour scrap l'intégralité du dataset
t1 = tm.time()
for i in range(0, n):
    print(f'Scrolling {i+1} sur {n} :')
    t3 = tm.time()
    scroll_payload = json.dumps({
        'scroll': '5m',
        'scroll_id': _scroll_id,
    })
    scroll_res = requests.post(
        lens_api_url,
        data=scroll_payload,
        headers=headers
    ).json()
    items = scroll_res['data']
    tm.sleep(4.5)  # Sleep for 4.5 seconds
    rows += items
    t4 = tm.time()
    time += t4 - t3
    print(f'Temps execution : {(t4 - t3):.4}s | restant : {round((((t4 - t3) * n) - time)/60,2)}min')
data = data + rows
t2 = tm.time()
print(f'Temps total extraction : {(t2 - t1)/60:.2} min')

requete_restante = int(response.headers._store['x-rate-limit-remaining-request-per-month'][1]) - n
print(f'Nombre de requêtes restant ce mois-ci : {requete_restante} ')

with open(f'C:/Users/vernhes/OneDrive - ENSTA Paris/Bureau/Sideproject/Data2/saveT.json', 'w') as mon_fichier:
	json.dump(data, mon_fichier)
with open("C:/Users/vernhes/OneDrive - ENSTA Paris/Bureau/Sideproject/Data2/save1.json") as data :
    data_T = json.load(data)
df_T = pd.json_normalize(data_T)

x = list([])
for i in range(0, len(data_T)):
    print(f'Extract {i + 1} sur {len(data_T)}')
    try:
        for j in range(0, len(data_T[i]['biblio']['parties']['inventors'])):
            try:
                x.append([data_T[i]['lens_id'], data_T[i]['biblio']['parties']['inventors'][j]['extracted_name']['value'],
                          data_T[i]['biblio']['parties']['inventors'][j]['residence']])
            except:
                j += 1
    except:
        i += 1
x = pd.DataFrame(x, columns=['lens_id', 'inv_name', 'inv_loc'])

x = x[x['inv_name'].isin(bioT_name)].sort_values(by='inv_name')
xx = pd.merge(x['inv_name'].value_counts(), x.groupby('inv_name')['inv_loc'].nunique(), left_on= x['inv_name'].value_counts().index,
         right_on=x.groupby('inv_name')['inv_loc'].nunique().index, how='outer')

y = list([])
for i in range(0, len(data_T)):
    print(f'Extract {i + 1} sur {len(data_T)}')
    try:
        for j in range(0, len(data_T[i]['families']['extended_family']['members'])):
            try:
                y.append([data_T[i]['lens_id'], data_T[i]['families']['extended_family']['members'][j]['lens_id']])
            except:
                j += 1
    except:
        i += 1
y = pd.DataFrame(y, columns=['lens_id', 'fam_mem'])

ids = np.random.randint(low=1000000, high=9999999, size=len(y.lens_id))
maps = {k: v for k, v in zip(y.lens_id, ids)}
y['id'] = y.lens_id.map(maps)
ids = np.random.randint(low=1000000, high=9999999, size=len(y.lens_id))
maps = {k: v for k, v in zip(y.lens_id, ids)}
y['id2'] = y.lens_id.map(maps)
y['-'] = '-'
y['fam_id'] = y['id'].astype(str) + y['-'] + y['id2'].astype(str)
dico_fam_T = y[-y.fam_mem.duplicated()][['fam_mem', 'fam_id']]

yy = pd.merge(x,dico_fam_T, left_on='lens_id', right_on='fam_mem', how='outer')[['inv_name','fam_id','lens_id','inv_loc']].sort_values(by=['inv_name','fam_id'])
yyy = yy.groupby('inv_name').agg({'fam_id': pd.Series.nunique, 'lens_id': pd.Series.nunique, 'inv_loc': pd.Series.nunique})
yy.columns
z = list([])
for i in range(0, len(data_T)):
    print(f'Extract {i + 1} sur {len(data_T)}')
    try:
        for j in range(0, len(data_T[i]['biblio']['classifications_cpc']['classifications'])):
            try:
                z.append([data_T[i]['lens_id'], data_T[i]['biblio']['classifications_cpc']['classifications'][j]['symbol']])
            except:
                j += 1
    except:
        i += 1
z = pd.DataFrame(z, columns=['lens_id', 'cpc'])

z.cpc = z.cpc.str[:4]
z = z.drop_duplicates()

bioT_CPC = pd.read_csv("C:/Users/vernhes/OneDrive - ENSTA Paris/Bureau/Sideproject/list Cpc.csv")
bioT_CPC["CPC Classification Code"] = bioT_CPC["CPC Classification Code"].str[:4]
bioT_CPC = bioT_CPC.groupby("CPC Classification Code").agg("Document Count").sum().sort_values(ascending=False)

z['is_bioT'] = z.cpc.isin(bioT_CPC.index).replace(True, 1).replace(False, 0)
zz = z.groupby('lens_id').agg("is_bioT").sum()

yz = pd.merge(yy, zz, left_on='lens_id', right_on=zz.index, how='outer')

xyz = pd.merge(yz, bioT[['name', 'country']], left_on='inv_name', right_on='name', how='outer')
xyz = pd.merge(xyz, bioT[['name_rev', 'country']], left_on='inv_name', right_on='name_rev', how='outer')

a = list([])
for i in range(0, len(data_T)):
    print(f'Extract {i + 1} sur {len(data_T)}')
    try:
        for j in range(0, len(data_T[i]['biblio']['parties']['inventors'])):
            try:
                a.append([data_T[i]['lens_id'], data_T[i]['biblio']['parties']['applicants'][j]['extracted_name']['value']])
            except:
                j += 1
    except:
        i += 1
a = pd.DataFrame(a, columns=['lens_id', 'app_name'])

ax = pd.merge(pd.merge(a, x, on='lens_id', how='outer'), bioT[['name', 'Company']], left_on='inv_name', right_on='name', how='outer')
axx = pd.merge(ax, bioT[['name_rev', 'Company']], left_on='inv_name', right_on='name_rev', how='outer')
axxyz = pd.merge(axx, yz, on='lens_id', how='outer').sort_values(by='fam_id')


# REQUEST Articles ---
lens_api_url = 'https://api.lens.org/scholarly/search'
headers = {'Authorization': 'Bearer 52eky7NN6lequIKALGi8XA7CmIS1UAg0EMliDPo15WtSTEC4EnR0',
           'Content-Type': 'application/json'}
size = 100  # 100 lignes par requêtes
df_all = pd.DataFrame([])
extract = list([])
for j in range(0, len(bioT_name)):
    #Chercher tout les brevets entre 90 et 2020 sur les technos mili qui citent des brevets ou de la npl
    payload ={
  "query": {
    "bool": {
      "must": [
          {"match_phrase": {"author.display_name": bioT_name[j]}}
      ],
    "filter": {
      "range": {
        "year_published": {
          "gte": "2015",
          "lte": "2022"
          }
        }
      }
    }
  },
        "size": 100,
        "scroll": "5m"
    }

    try:
        response = requests.post(lens_api_url, data=json.dumps(payload), headers=headers)#Si les lignes suivantes fonctionnent correctement alors on ne run pas except
        res_json = response.json()
        data = res_json['data']
        _scroll_id = res_json['scroll_id']
        print(f'Tout est bon !')
    except KeyError:
        data = []
        _scroll_id = None
        print('Error: Elastic Search: %s' % str(response.json()))
    #------------------------------------------------------------------------------------------------------
    rows = []
    time = 0
    n = math.floor(res_json['total']/size)#calcule le nombre de requête nécessaire pour scrap l'intégralité du dataset
    t1 = tm.time()
    i = 0
    if n < 10 :
        for i in range(0, n):
            print(f'Scrolling {i+1} sur {n} :')
            t3 = tm.time()
            scroll_payload = json.dumps({
                'scroll': '5m',
                'scroll_id': _scroll_id,})
            scroll_res = requests.post(
                lens_api_url,
                data=scroll_payload,
                headers=headers
            ).json()
            items = scroll_res['data']
            tm.sleep(4.5)  # Sleep for 4.5 seconds
            rows += items
            t4 = tm.time()
            time += t4 - t3
            print(f'Temps execution : {(t4 - t3):.4}s | restant : {round((((t4 - t3) * n) - time)/60,2)}min')
        data = data + rows
        #locals()[df_Ba_inv_name_list[i].replace(' ', '_')]
        df = pd.json_normalize(data)
        df['inv_name'] = bioT_name[j]
        df_all = pd.concat([df_all, df])
        t2 = tm.time()
        print(f'Temps total extraction : {(t2 - t1)/60:.2} min')
        extract += data
    else :
        j += 1
    tm.sleep(6)

requete_restante = int(response.headers._store['x-rate-limit-remaining-request-per-month'][1]) - n
print(f'Nombre de requêtes restant ce mois-ci : {requete_restante} ')

with open(f'C:/Users/vernhes/OneDrive - ENSTA Paris/Bureau/Sideproject/Data2/save_articles.json', 'w') as mon_fichier:
	json.dump(extract, mon_fichier)
with open("C:/Users/vernhes/OneDrive - ENSTA Paris/Bureau/Sideproject/Data2/save_articles.json") as data :
    data_S = json.load(data)
df_S = pd.json_normalize(data_S)

data = extract
y2 = list([])
for i in range(0, len(data)):
    print(f'Extract {i + 1} sur {len(data)}')
    try:
        for j in range(0, len(data[i]['authors'])):
            try:
                y2.append([data[i]['lens_id'], data[i]['authors'][j]['first_name'] + " " + data[i]['authors'][j]['last_name']])
            except:
                j += 1
    except:
        i += 1
y2 = pd.DataFrame(y2, columns=['lens_id', 'aut_name'])

i = y2.lens_id.drop_duplicates()[i]
for i in y2.lens_id.drop_duplicates():
    if y2[y2.lens_id == i]['aut_name'].str.upper().isin(bioT_name):
        y2['target_aut'] = y2[y2.lens_id == i]['aut_name'].str.upper().isin(bioT_name)

y2['target_aut'] = y2['aut_name'].str.upper().isin(bioT_name)

