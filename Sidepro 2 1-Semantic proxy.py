import pandas as pd
import string
from nltk.corpus import stopwords
from gensim.models import Word2Vec
from sklearn.metrics.pairwise import cosine_similarity
import numpy as np

# Nettoyer le texte
punctuations = string.punctuation
stopwords_list = stopwords.words('english')
stopwords_list.extend(['invention', 'present', 'relates'])

def clean_text(text):
    text = text.lower() # Convertir le texte en minuscules
    text = ''.join(char for char in text if char not in punctuations) # Enlever les ponctuations
    words = text.split() # Séparer les mots
    words = [word for word in words if word not in stopwords_list] # Enlever les stopwords
    return words

# Appliquer la fonction de nettoyage aux données
df_bio = list([])
for i in range(0, len(df_T['abstract'])):
    try:
        df_bio.append([df_T[df_T.lens_id.isin(["049-593-844-756-970", "175-897-333-412-253", "067-624-308-860-223",
                                           "180-448-910-864-274", "133-443-559-116-377"])]['abstract'][i][0]['text']])
    except: i += 1
df_bio = pd.DataFrame(df_bio, columns=['colonne_texte'])

df_t = list([])
for i in range(0, len(df_T['abstract'])):
    try:
        df_t.append([df_T['abstract'][i][0]['text'], df_T['abstract'][i][0]['lang']])
    except: df_t.append([np.nan])
df_t = pd.DataFrame(df_t, columns=['colonne_texte', 'lang'])
df_t['lens_id'] = df_T['lens_id']

df_en = pd.DataFrame(df_t[df_t.lang == 'en']['colonne_texte'], columns=['colonne_texte']).reset_index(drop=True)
df_en['colonne_texte'] = df_en['colonne_texte'].apply(clean_text)
df_bio['colonne_texte'] = df_bio['colonne_texte'].apply(clean_text)

# Ensemble de 100 vecteurs de string
vectors_bio = np.array(df_bio['colonne_texte'])
vectors = np.array(df_en['colonne_texte'])

# Entraînement du modèle Word2Vec
model = Word2Vec(vectors, window=5, min_count=1, workers=4)

similarity = list([])
sim_mean = list([])
for j in range(0, len(df_en['colonne_texte'])):
    print(f"moyen sémantique {j} sur {len(df_en['colonne_texte'])}")
    similarity = list([])
    for i in range(0, len(df_bio)):
        # Vecteurs de strings à comparer
        vec1 = df_en['colonne_texte'][j]
        vec2 = df_bio['colonne_texte'][i]

        # Calcul des vecteurs de Word Embedding moyens pour chaque vecteur de strings
        vec1_emb = np.mean([model.wv.get_vector(word) for word in vec1 if word in model.wv.key_to_index], axis=0)
        vec2_emb = np.mean([model.wv.get_vector(word) for word in vec2 if word in model.wv.key_to_index], axis=0)

        # Calcul de la similarité cosinus entre les deux vecteurs de Word Embedding
        similarity.append(np.dot(vec1_emb, vec2_emb) / (np.linalg.norm(vec1_emb) * np.linalg.norm(vec2_emb)))
    sim_mean.append([j, np.mean(similarity)])

sim_mean = pd.DataFrame(sim_mean)

df_sem_bio = pd.merge(df_en, sim_mean, left_on=df_en.index, right_on=0)[['colonne_texte', 1]]
df_sem_bio['lens_id'] = np.nan
df_sem_bio['lens_id'] = list(df_t[df_t.lang == 'en']['lens_id'])
df_sem_bio = pd.merge(df_t, df_sem_bio, on='lens_id', how='outer')
df_sem_bio.columns = ['colonne_texte_x', 'lang', 'lens_id', 'colonne_texte_y', 'sem_prox']


