# -*- coding: utf-8 -*-
"""
Created on Wed Jun 10 12:07:09 2020

@author: joangj
"""

import requests
from bs4 import BeautifulSoup
import pandas as pd

def getWords(text,words):
    counter = 0
    for word in words:
        counter += text.count(word)
    return counter

def getText(soup):
    return(soup.find("div",  class_ = "ep-detail-body").text)

def getLinks(soup):
    listoflinks = set()
    for link in soup.find("div",  class_ = "ep-detail-body").find_all("a", href = True):
        if link["href"].startswith("https://www.elperiodico.com/es/"):
            listoflinks.add((link["href"]))
    return(listoflinks)


listofwords = ["economía","teletrabajo","PIB","crisis","empleo"]

df = pd.DataFrame(columns = ["from","to"])

link = "https://www.elperiodico.com/es/economia/20200610/la-ocde-augura-una-caida-del-pib-de-hasta-el-144-en-espana-7993510"

page = requests.get(link)
soup = BeautifulSoup(page.content, 'html.parser')

text = getText(soup)

count = getWords(text,listofwords)

links = getLinks(soup)

for l in links:
    df.append(pd.DataFrame([link,l],columns = ["from","to"]) )


print(df)