import pandas as pd
import time


required_columns = ['person_id', 'family_name', 'first_name', 'name_lang', 'sex', 'birthyear', 'deathyear',
                    'birthplace', 'wikipedia_link', 'wikidata_id', 'created', 'created_by', 'last_modified',
                    'last_modified_by'] # 'wikipedia_link','wikidata_id' are waiting for discussion


# Create sample CSV
df = pd.DataFrame(columns = required_columns)
##########################################################
# Case 1: generally, to show what can autofill do:
# Given information 1: Name + name_lang + Wikipedia link + create time by user + user name.
# Given information 2: Name + name_lang + Wikidata id + create time by user + user name.
##########################################################
df.loc[df.shape[0]] =  ['', 'Gaiman', 'Neil', 'en', '','', '', '', 'https://en.wikipedia.org/wiki/Neil_Gaiman',
                        '', time.strftime("%Y-%m-%d", time.localtime()) , 'QG', '', '']

df.loc[df.shape[0]] =  ['', '盖曼', '尼尔', 'zh', '','', '', '', '',
                        'Q210059', time.strftime("%Y-%m-%d", time.localtime()) , 'QG', '', '']



##########################################################
# Case 2: person already in ReadACT:
# Given information: Name + Wikipedia link + Wikidata id + create time by user + user name.
##########################################################
df.loc[df.shape[0]] =  ['AG0001', '鲁', '迅', 'zh', '', '', '', '',
                        'https://zh.wikipedia.org/wiki/%E9%B2%81%E8%BF%85',
                        'Q23114', time.strftime("%Y-%m-%d", time.localtime()), 'QG', '', '']



##########################################################
# Case 3: person not in ReadACT, but user made typos:
# Given information: Name + name_lang + wrong birthyear + wrong deathyear + Wikipedia link + create time by user + user name.
##########################################################
df.loc[df.shape[0]] = ['', '', 'Sanmao', 'en', '', '0000', '1234', '',
                       'https://zh.wikipedia.org/wiki/%E4%B8%89%E6%AF%9B_(%E4%BD%9C%E5%AE%B6)', '', time.strftime("%Y-%m-%d", time.localtime()), 'QG','', ''] # Correct birthyear and deathyear are 1943 and 1991



##########################################################
# Case 4: person not in ReadACT:
# Given information: Name + name_lang + create time by user + user name.
##########################################################
df.loc[df.shape[0]] = ['', 'Neruda', 'Jan', 'en', '', '', '', '',
                       '', '', time.strftime("%Y-%m-%d", time.localtime()), 'QG','', '']

##########################################################
# Write dataframe into a tab-seperated CSV in the CSV file.
##########################################################
with open('../CSV/Person.csv', 'w') as f:
    f.write(df.to_csv())
