#!/usr/bin/env python
# coding: utf-8

# In[6]:


import os

import pandas as pd
import urllib.request


# In[7]:


url_base = 'https://opendata.dwd.de/climate_environment/CDC/observations_germany/phenology/immediate_reporters/'
url_PH_Beschreibung_Pflanze = url_base + 'crops/' + 'historical/' +  'PH_Beschreibung_Pflanze.txt'
url_PH_Beschreibung_Phase = url_base + 'crops/' + 'historical/' +  'PH_Beschreibung_Phase.txt'
url_PH_Beschreibung_Phaenologie_Stationen_Sofortmelder = url_base + 'crops/' + 'historical/' +  'PH_Beschreibung_Phaenologie_Stationen_Sofortmelder.txt'
url_PH_Beschreibung_Phasendefinition_Sofortmelder_Landwirtschaft_Kulturpflanze = url_base + 'crops/' + 'recent/' +  'PH_Beschreibung_Phasendefinition_Sofortmelder_Landwirtschaft_Kulturpflanze.txt'
url_PH_Beschreibung_Phasendefinition_Sofortmelder_Obst = url_base + 'fruit/' + 'recent/' + 'PH_Beschreibung_Phasendefinition_Sofortmelder_Obst.txt'
url_PH_Beschreibung_Phasendefinition_Sofortmelder_Wildwachsende_Pflanze = url_base + 'wild/' + 'recent/' + 'PH_Beschreibung_Phasendefinition_Sofortmelder_Wildwachsende_Pflanze.txt'

urls = [url_PH_Beschreibung_Pflanze, 
        url_PH_Beschreibung_Phase, 
        url_PH_Beschreibung_Phaenologie_Stationen_Sofortmelder, 
        url_PH_Beschreibung_Phasendefinition_Sofortmelder_Landwirtschaft_Kulturpflanze,
        url_PH_Beschreibung_Phasendefinition_Sofortmelder_Obst,
        url_PH_Beschreibung_Phasendefinition_Sofortmelder_Wildwachsende_Pflanze]


# In[9]:


for url in urls:
    filename = url.split('/')[-1]
    urllib.request.urlretrieve(url, filename)
    plist = []
    with open(filename, 'r', encoding='ISO-8859-1') as file:
        lines = file.read().split('eor;')        
        for line in lines:
            row = [ii.strip() for ii in line.split(';')]
            if row[0] != '':
                plist.append(row)
    df = pd.DataFrame(plist[1:], columns=plist[0])
    df.to_csv(filename.split('.')[0]+'.csv', index=False)
    os.remove(filename)

