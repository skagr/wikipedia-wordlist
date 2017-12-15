"""Download Wikipedia archive and save unique words in plain text"""
#from bs4 import BeautifulSoup
import bz2
import os
import requests
import re

def main(url):
    local_filename = url.split('/')[-1] + '.txt'
    decompressor = bz2.BZ2Decompressor()
    words = set()
    chunks = 0
    text = ''
    r = requests.get(url, stream=True)

    for chunk in r.iter_content(chunk_size=100*1024): 
        if chunk: # filter out keep-alive new chunks
            chunks += 1
            data = decompressor.decompress(chunk) 
            #soup = BeautifulSoup(data, 'xml')
            #text = soup.get_text()
            text = ' '.join([text, data.decode('utf-8')])
            if text and re.search(r'[^\w]', text[-1]):  # Don't break on word character
#                print(f'text: {text[:90]}')
                text = re.sub(r'http.*\s', ' ', text)  # remove URLs
                text = re.sub(r'[^\w]+', ' ', text)  # remove non-word characters
                text = text.lstrip()
                new_words = [w for w in text.split() if re.search(r'\w', w)]
                words = words.union(set(new_words))
                print(f'chunks: {chunks}\twords found: {len(words)}\ttext: {text[:60]}')
                text = ''

#            if chunks > 10:
#                break

    with open(local_filename, 'w') as f:
        for w in sorted(words):
            f.write(w)
            f.write('\n')

if __name__ == '__main__':
    url = 'https://dumps.wikimedia.org/nlwiki/latest/nlwiki-latest-pages-articles1.xml.bz2'
    main(url)
