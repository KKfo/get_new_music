#!/bin/env python

import sqlite3
import os.path
import concurrent.futures
import io
import re
import json
import time
from bs4 import BeautifulSoup
from requests import Session
import requests
from time import sleep

NWORKERS = 5
WEBSITE='http://pitchfork.com/reviews/best/tracks/'
FROM = 1
TO = 190
def initDb(dbconn):
    c = dbconn.cursor()
    c.execute('CREATE TABLE IF NOT EXISTS \
    pitchfork_best_new_tracks( \
    Artist TEXT NOT NULL, \
    Song TEXT, \
    Date TEXT, \
    Reviewer TEXT, \
    Review TEXT, \
    Label TEXT, \
    Media TEXT);')
    return c

def saveData(dbconn,pages,c):
    for page in pages:
        for e in page:
            c.execute("INSERT INTO  pitchfork_best_new_tracks(\
            Artist, Song ,Date , Label, Reviewer ,Review , Media) \
            VALUES (?,?,?,?,?,?,?);",
                      (e['artist'] or 'none', 
                       e['song'] or 'none',
                       e['date'] or 'none',
                       e['label'] or 'none',
                       e['reviewer'] or 'none',
                       e['review'] or 'none', 
                       e['media'] or 'none'))
    print('saving data...')
    dbconn.commit()

def doExtract(html):
    if not html:
        return []
    s = BeautifulSoup(html,'lxml')
    ds = s.find_all('div', class_='info')[0:5]
    info = [{
        'artist' : d.h1 and d.h1.find_all('span',class_='artist') and d.h1.find_all('span',class_='artist')[0].string.strip(),
        'song' : d.h1 and d.h1.find_all('span',class_='title') and d.h1.find_all('span',class_='title')[0].string.strip(),
        'reviewer' : d.h4 and d.h4.string.split(';')[0].replace('By','').strip(),
        'review' : "".join([e.strip()+' ' for e in d.find_all('div', class_='editorial') and d.find_all('div', class_='editorial')[0].text.split('\n')]).strip(),
        'label' : d.h3 and d.h3.string.strip(),
        'date': d.h4 and d.h4.string.split(';')[1].strip(),
        'media' : d.div and d.div.find_all('iframe') and d.div.find_all('iframe')[0].get('src') or 'None'}
            for d in ds]
    return info

def extractData(executor, html_pages):
    future_to_page = {executor.submit(doExtract, page) for page in html_pages}
    data = [future.result() for future in concurrent.futures.as_completed(future_to_page)]
    return data

def getHtml(u):
    sleep(0.05)
    return requests.get(u).text

def getPages(executor):
    future_to_page = {executor.submit(getHtml, WEBSITE+str(uri)): uri for uri in range(FROM,TO)}
    pages = [print('Got %s' % future_to_page[future]) or future.result()
             for future in concurrent.futures.as_completed(future_to_page)]
    return pages

def main():
    dbconn = sqlite3.connect('data/pitchfork_music.db')
    ex = concurrent.futures.ThreadPoolExecutor(max_workers=NWORKERS)
    c = initDb(dbconn)
    html_pages = getPages(ex)
    data = extractData(ex, html_pages)
    saveData(dbconn,data,c)
    dbconn.close()
    
if __name__ == '__main__':
    main()
