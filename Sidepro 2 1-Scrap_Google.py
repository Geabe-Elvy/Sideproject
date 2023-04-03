import requests
import urllib
import pandas as pd
from requests_html import HTML
from requests_html import HTMLSession
world_cities_name = pd.read_csv("C:/Users/vernhes/OneDrive - ENSTA Paris/Bureau/S5/Nom villes région pays du monde csv/worldcities.csv")

def get_source(url):
    """Return the source code for the provided URL.

    Args:
        url (string): URL of the page to scrape.

    Returns:
        response (object): HTTP response object from requests_html.
    """

    try:
        session = HTMLSession()
        response = session.get(url)
        return response

    except requests.exceptions.RequestException as e:
        print(e)

def get_results(query):
    query = urllib.parse.quote_plus(query)
    response = get_source("https://www.google.co.uk/search?q=" + query)

    return response

def parse_results(response):
    css_identifier_result = ".tF2Cxc"
    css_identifier_title = "h2"
    css_identifier_link = ".yuRUbf a"
    css_identifier_text = ".VwiC3b"
    css_identifier_loc = ".MUxGbd"

    results = response.html.find(css_identifier_result)

    output = []

    for result in results:
        try :
            item = {
                'title': result.find(css_identifier_title, first=True).text,
                'link': result.find(css_identifier_link, first=True).attrs['href'],
                'text': result.find(css_identifier_text, first=True).text,
                'loc': result.find(css_identifier_loc, first=True).text
            }
            output.append(item)
        except: continue
    return output

def google_search(query):
    response = get_results(query)
    return parse_results(response)

import requests
from bs4 import BeautifulSoup
def bing_search(query):
    query = urllib.parse.quote_plus(query)
    l = []
    o = {}
    headers = {"User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit / 537.36(KHTML, like Gecko) Chrome / 107.0.0.0 Safari / 537.36"}
    target_url = "https://www.bing.com/search?q="+query
    print(target_url)
    resp = requests.get(target_url, headers=headers)
    soup = BeautifulSoup(resp.text, 'html.parser')
    completeData = soup.find_all("li", {"class": "b_algo"})
    o["Title"] = completeData[0].find("a").text
    o["link"] = completeData[0].find("a").get("href")
    o["Description"] = completeData[0].find("div",{"class": "b_caption"}).text
    o["Position"] = 1
    l.append(o)
    o = {}
    return l

def search_word(word_list, string):
    found_words = []
    for word in word_list:
        if word in string:
            found_words.append(word)
    return found_words

def is_linkedin(word, string):
    if word in string:
        return True
    else :
        return False

list_name = bioT.SPEAKERS
list_orga = bioT.Company

results = list([])
for i in range(0, len(list_name)):
    try:
        results.append(google_search(list_name[i] + " " + list_orga[i]))
    except: results.append(np.nan)

result_links_b = list([])
for i in range(0, len(list_name)):
    try:
        result_links_b.append(bing_search(f"{list_name[i]} {list_orga[i]} linkedin"))
    except: result_links_b.append(np.nan)

for i in range(0, len(list_name)):
    try:
        result_links.append(google_search(f"{list_name[i]} {list_orga[i]} linkedin"))
    except: result_links.append(np.nan)

gogole = pd.DataFrame({'name': list_name, 'results': results})
gogole = gogole.dropna().reset_index(drop=True)

goolink = pd.DataFrame({'name': list_name.dropna(), 'results': result_links})
goolink = goolink.dropna().reset_index(drop=True)

blinglink = pd.DataFrame({'name': list_name.dropna(), 'results': result_links_b})
blinglink = blinglink.dropna().reset_index(drop=True)

bli_link_id = list([])
for i in range(0,len(blinglink)):
    try:
        bli_link_id.append(blinglink.results[i][0]['link'].split('/')[-1])
    except: bli_link_id.append(np.nan)
blinglink['bli_link_id'] = bli_link_id

is_linkedin = list([])
for i in range(0, len(blinglink)):
    try :
        is_linkedin.append(search_word(['linkedin'], blinglink.results[i][0]['link'])[0])
    except: is_linkedin.append(np.nan)
blinglink['is_linkedin'] = is_linkedin
blinglink.loc[blinglink.is_linkedin.isna(), 'bli_link_id'] = np.nan
#correction manuelle
blinglink.loc[blinglink.name == "Ekkehard Leberer", 'bli_link_id'] = blinglink[blinglink.name == "Ekkehard Leberer"]['results'].values[0][0]['link'].split('/')[-2]
blinglink.loc[blinglink.name == "Nicole Datson", 'bli_link_id'] = 'nicole-datson-76929840'
blinglink = blinglink[blinglink.name != "Kritika Khurana"]

links = list([])
for i in range(0, len(gogole)):
    try :
        links.append([goolink.name[i], goolink.results[i][:1][0]['link']])
    except: links.append(np.nan)
links = pd.DataFrame(links, columns=['name', 'link'])

is_linkedin = list([])
for i in range(0, len(gogole)):
    try :
        is_linkedin.append(search_word(['linkedin'], links.link[i])[0])
    except: is_linkedin.append(np.nan)
links['is_linkedin'] = is_linkedin
links['linkedin_id'] = links['link'].str.split('/').str[-1]
links.loc[links.name == "Fabian Stutz", 'linkedin_id'] = links[links.name == "Fabian Stutz"]['link'].str.split('/').str[-2]
links.loc[links.is_linkedin.isna(), 'linkedin_id'] = np.nan
blinglink = blinglink[blinglink['bli_link_id'] != 'nigel-langley-207a9a12']
links = links.sort_values(by='name').reset_index(drop=True)
blinglink = blinglink.sort_values(by='name').reset_index(drop=True)
links['linked_id_bling'] = blinglink['bli_link_id']
links.loc[:,'linkedin_id'] = links.loc[:,'linkedin_id'].fillna(links.loc[:,'linked_id_bling'])
links.loc[links.name == 'Nico Spribille', 'linkedin_id'] = 'nico-spribille'
links.loc[links.name == 'Tamara Phan', 'linkedin_id'] = 'tamara-phan'
linkedin_list = list(links['linkedin_id'].dropna())

for i in range(0, len(gogole)):
    gogole.results[i] = ''.join([d['text'] for d in gogole.results[i][:3]])

loc_words = list([])
for i in range(0, len(gogole.results)):
    try:
        loc_words.append(search_word(world_cities_name.country.drop_duplicates().astype('str'), gogole.results[i]))
    except: i+=1
for i in range(0, len(loc_words)):
    try:
        loc_words[i] = pycountry.countries.search_fuzzy(loc_words[i][0])[0].alpha_2
    except: i+=1

loc_words_2 = list([])
for i in range(0, len(gogole.results)):
    try:
        loc_words_2.append(search_word(world_cities_name[world_cities_name.population > 1_000_000].city.drop_duplicates().astype('str'), gogole.results[i]))
    except: i+=1

loc_words_2_2 = list([])
for i in range(0, len(loc_words_2)):
    try:
        loc_words_2_2.append(world_cities_name[world_cities_name.city == loc_words_2[i][0]]['country'].iloc[0])
    except:
        loc_words_2_2.append(np.nan)

for i in range(0, len(loc_words_2_2)):
    try:
        loc_words_2_2[i] = pycountry.countries.search_fuzzy(loc_words_2_2[i])[0].alpha_2
    except:
        i += 1

gogole['loc_words'] = [np.nan if x == [] else x for x in loc_words]
gogole['loc_city'] = loc_words_2
gogole['loc_city_word'] = loc_words_2_2

#Correction manuelle
gogole.loc[gogole.index == 7, "loc_words"] = "US"

#Scrap Linkedin ---

import pandas as pd

api = Linkedin('glenn.quagmire@24f.fr', 'mire@24f', refresh_cookies=True)
api = Linkedin('eleanor.dunbar@ohur.fr', 'bar@ohur', refresh_cookies=True)
api = Linkedin('betsy.bautista@24f.fr', 'sta@24f', refresh_cookies=True)

api = Linkedin('jeannefournier@point-jaune.com', 'nier@point', refresh_cookies=True)
api = Linkedin('philippariseau@point-jaune.com', 'seau@point', refresh_cookies=True)

#result = pd.json_normalize(api.search_people('Amanda Brooks'))

xp_org = list([])
for i in range(69, len(linkedin_list)):
    print(f'Scrap {i} sur {len(linkedin_list)}')
    result = api.get_profile(linkedin_list[i])
    try:
        for j in range(0, len(result['experience'])):
            xp_org.append([result['firstName'] + " " + result['lastName'], linkedin_list[i], result['experience'][j]['companyName']])
    except : xp_org.append([np.nan, np.nan, np.nan])
tm.sleep(np.random.random()*10)

df_xp_org_full_6 = pd.DataFrame(xp_org, columns=['name', 'linkedin_id', 'org_xp'])
df_xp_org_full = pd.concat([df_xp_org_full, df_xp_org_full_2,df_xp_org_full_4,df_xp_org_full_5,df_xp_org_full_6]).drop_duplicates()


#df_xp_org.loc[df_xp_org.name == "Fabian Stutz", 'linkedin_id'] = "fabian-stutz-pharmabotix"

#df_xp_org_full.to_csv("C:/Users/vernhes/OneDrive - ENSTA Paris/Bureau/Sideproject/df_xp_org_full", index=False)
df_xp_org_full = pd.read_csv("C:/Users/vernhes/OneDrive - ENSTA Paris/Bureau/Sideproject/df_xp_org_full")

df_xp_org_full = df_xp_org_full.groupby('linkedin_id')['org_xp'].apply(list)
df_xp_org_full = pd.DataFrame(df_xp_org_full.values.tolist(), index=df_xp_org_full.index).rename(columns=lambda x: f'org_xp{x+1}')
df_xp_org_full.insert(0, 'linkedin_id', df_xp_org_full.index)
df_xp_org_full = df_xp_org_full.reset_index(drop=True)

df_xp_org_full = pd.merge(links, df_xp_org_full, on='linkedin_id', how='outer')



