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
bioT = pd.merge(bioT, gogole, left_on='SPEAKERS', right_on='name', how='outer')

bioT = pd.merge(bioT, df_xp_org_full.iloc[:, [0, -5, -4, -3, -2, -1]], left_on='SPEAKERS', right_on='name', how='outer')

#Inversement des nom prénom et majuscule
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

#Code alpha2 pour les noms de pays déjà renseignés
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

#recherche de localisation dans les bios
#Pays
biog['sem_loc'] = np.nan
for i in range(0, len(biog)):
    try:
        biog['sem_loc'][i] = search_word(world_cities_name.country.drop_duplicates().astype('str'), biog.Biography[i])
    except: i+=1

#Ville
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

for i in range(0, len(biog)):
    try :
        for j in range(0, len(biog.sem_city[i])):
            try:
                biog.sem_city[i][j] = world_cities_name[world_cities_name.city == biog.sem_city[i][j]]['iso2'].values[0]
            except: j+=1
    except: i+=1

bioT = pd.merge(bioT, biog[['Name', 'sem_loc', 'sem_city']], left_on='name' , right_on='Name', how='outer')

for i in range(0, len(bioT)):
    try:
        bioT.sem_loc[i] = bioT.sem_loc[i][0]
    except: continue
bioT.sem_loc = [np.nan if x == [] else x for x in bioT.sem_loc]

for i in range(0, len(bioT)):
    try:
        bioT.sem_city[i] = bioT.sem_city[i][0]
    except: continue
bioT.sem_city = [np.nan if x == [] else x for x in bioT.sem_city]

bioT.ctry_apl2 = bioT.ctry_apl2.fillna(bioT['loc_words'])
bioT.ctry_apl2 = bioT.ctry_apl2.fillna(bioT['loc_city_word'])
bioT.ctry_apl2 = bioT.ctry_apl2.fillna(bioT['sem_loc'])

bioT = bioT[['Conference name', 'date', 'place', 'SPEAKERS', 'Job', 'Company','name', 'results','name_rev', 'ctry_apl2']]

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
            "year_published": {# Corriger par earliest
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

X = list([])
for i in range(0, len(data_T)):
    print(f'Extract {i + 1} sur {len(data_T)}')
    try:
        for j in range(0, len(data_T[i]['biblio']['parties']['inventors'])):
            try:
                X.append([data_T[i]['lens_id'], data_T[i]['biblio']['parties']['inventors'][j]['extracted_name']['value'],
                          data_T[i]['biblio']['parties']['applicants'][j]['extracted_name']['value']])
            except:
                j += 1
    except:
        i += 1
X = pd.DataFrame(X, columns=['lens_id', 'inv_name', 'app_name'])
X = X[X['inv_name'].isin(bioT_name)].sort_values(by='inv_name').sort_values(by='lens_id')

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
xs = pd.merge(x['inv_name'].value_counts(), x.groupby('inv_name')['inv_loc'].nunique(), left_on= x['inv_name'].value_counts().index,
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

xy = pd.merge(x,dico_fam_T, left_on='lens_id', right_on='fam_mem', how='outer')[['inv_name','fam_id','lens_id','inv_loc']].sort_values(by=['inv_name','fam_id'])
xys = xy.groupby('inv_name').agg({'fam_id': pd.Series.nunique, 'lens_id': pd.Series.nunique, 'inv_loc': pd.Series.nunique})

#Import des données scrappé manuellement sur le CV des inventeurs
bioT['prev_comp'] = np.nan
bioT.loc[bioT.SPEAKERS == "Mark Cunningham", 'prev_comp'] = "Centocor + Janssen"
bioT.loc[bioT.SPEAKERS == "Daniel Lorenz", 'country'] = "DE"
bioT.loc[bioT.SPEAKERS == "Hiroshi Maeda", 'prev_comp'] = "Dead 2021 sans Linkedin"
bioT.loc[bioT.SPEAKERS == "Heinrich Haas", 'prev_comp'] = "Ribological + Medigene"
bioT.loc[bioT.SPEAKERS == "Lykke Pedersen", 'prev_comp'] = "Hoffman La Roche + Santaris"
bioT.loc[bioT.SPEAKERS == "Carsten Timpe", 'prev_comp'] = "Novartis + Eli Lilly"
bioT.loc[bioT.SPEAKERS == "Carmen Popescu", 'Company'] = "Roquette"
bioT.loc[bioT.Company == "Rice University", 'Company'] = "Rice University William M"
bioT.loc[bioT.Company == "Rice University", 'Company'] = "Rice University William M"
bioT.loc[bioT.SPEAKERS == "Jeff Lloyds", 'prev_comp'] = "Centocor + Janssen"
bioT.loc[bioT.SPEAKERS == "Christian Plank", 'prev_comp'] = "Technische ät München + Research Institute of Molecular Pathology"


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
z['n'] = 1
zs = z.groupby('lens_id').sum()
zs.is_bioT = zs.is_bioT/zs.n

yz = pd.merge(yy, zs.is_bioT, left_on='lens_id', right_on=zs.index, how='outer')

xyz = pd.merge(yz, bioT[['name', 'ctry_apl2']], left_on='inv_name', right_on='name', how='outer')
xyz = pd.merge(xyz, bioT[['name_rev', 'ctry_apl2']], left_on='inv_name', right_on='name_rev', how='outer')
xyz.name = xyz.name.fillna(xyz.name_rev)
xyz.ctry_apl2_x = xyz.ctry_apl2_x.fillna(xyz.ctry_apl2_y)
xyz = xyz.drop(['name_rev','ctry_apl2_y'], axis=1)

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

axyz = pd.merge(pd.merge(xyz, a, on='lens_id', how='outer'), bioT[['name', 'Company', 'prev_comp', 'org_xp1', 'org_xp2', 'org_xp3', 'org_xp4', 'org_xp5']], left_on='inv_name', right_on='name', how='outer')
axyz = pd.merge(axyz, bioT[['name_rev', 'Company', 'prev_comp', 'org_xp1', 'org_xp2', 'org_xp3', 'org_xp4', 'org_xp5']], left_on='inv_name', right_on='name_rev', how='outer')
for i in range(8, 16):
    axyz.iloc[:,i] = axyz.iloc[:,i].fillna(axyz.iloc[:,i+8])
axyz = axyz.drop(axyz.iloc[:,16:24].columns, axis=1)
