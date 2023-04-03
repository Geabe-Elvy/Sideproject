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

S_auth_instit = list([])
for i in range(0, len(data_S)):
    print(f'Extract {i + 1} sur {len(data_S)}')
    try:
        for j in range(0, 10):
            try:
                S_auth_instit.append(
                    [data_S[i]['lens_id'], data_S[i]['authors'][j]['first_name'] + " " + data_S[i]['authors'][j]['last_name'],
                     data[i]['authors'][j]['affiliations'][0]['name']])
            except:
                j += 1
    except:
        i += 1
S_auth_instit = pd.DataFrame(S_auth_instit, columns=['lens_id', 'authors_name', 'affiliation']).drop_duplicates()
S_auth_instit['target_author'] = S_auth_instit.authors_name.str.upper().isin(bioT_name)
S_auth_instit[S_auth_instit.target_author == True]['authors_name'].value_counts()

S_subj = list([])
for i in range(0, len(data_S)):
    print(f'Extract {i + 1} sur {len(data_S)}')
    try:
        for j in range(0, 10):
            try:
                S_subj.append(
                    [data_S[i]['lens_id'], data_S[i]['source']['asjc_codes'][j]])
            except:
                j += 1
    except:
        i += 1
S_subj = pd.DataFrame(S_subj, columns=['lens_id', 'asjc_codes'])
S_subj.loc[:, 'asjc_codes'] = S_subj.asjc_codes.str[:2]

S = pd.merge(S_auth_instit[S_auth_instit.target_author == True], S_subj, on='lens_id', how='outer').drop('target_author', axis=1).drop_duplicates()
Ss = S.groupby('authors_name').agg({'lens_id': 'nunique', 'affiliation': 'nunique', 'asjc_codes': 'nunique'})

S_org = pd.merge(S_auth_instit[S_auth_instit.target_author == True], bioT[['SPEAKERS', 'Company', 'prev_comp', 'org_xp1', 'org_xp2', 'org_xp3', 'org_xp4', 'org_xp5']], left_on='authors_name', right_on='SPEAKERS', how='outer')
S_org.loc[:, 'affiliation'] = S_org.affiliation.str.replace('Universit', '')
S_org.loc[:, 'Company'] = S_org.Company.str.replace('Universit', '')

def similarity(string1, string2):
    return fuzz.token_set_ratio(string1, string2)
cols = ['sim_o', 'sim_pc', 'sim_xp1', 'sim_xp2', 'sim_xp3', 'sim_xp4', 'sim_xp5']
for i in range(0, 7):
    S_org[cols[i]] = S_org.apply(lambda row: similarity(row['affiliation'], row[S_org.columns[5+i]]), axis=1)

good_S = S_org[S_org[cols[0]] > 60]
for i in range(1, 7):
    good_S = pd.concat([good_S, S_org[S_org[cols[i]] > 60]])
good_S = good_S[-good_S.lens_id.isna()].drop_duplicates()

good_S.authors_name.drop_duplicates()
good_S.to_csv('C:/Users/vernhes/OneDrive - ENSTA Paris/Bureau/Sideproject/Data2/good_S.csv', index=False)
df_bio_S = pd.merge(good_S[['authors_name', 'lens_id']], df_S[df_S.lens_id.isin(good_S.lens_id)], on='lens_id')
df_bio_S.to_csv('C:/Users/vernhes/OneDrive - ENSTA Paris/Bureau/Sideproject/Data2/df_bio_S.csv', index=False)

df_bio_S = pd.read_csv('C:/Users/vernhes/OneDrive - ENSTA Paris/Bureau/Sideproject/Data2/df_bio_S.csv')
df_bio_S.dtypes

S = S[-S.lens_id.isin(good_S.lens_id)]
S = S[S.asjc_codes.isin(['13', '27', '30', '16', '25', '15', '22', '24', '11', '28', '31', '23'])]
