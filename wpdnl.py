"""Download Wikipedia archive and save unique words in plain text"""
# from bs4 import BeautifulSoup
import bz2
import os
import requests
import re


def main(url, mode=None):
    re_breaking_char = re.compile(r'[\s\.,;!?>]$|\]\]$')
    re_open_paren = re.compile(r'\([^\)]*$')
    re_open_curly = re.compile(r'\{[^\}]*$')
    re_open_bracket = re.compile(r'\[[^\]]*$')
    re_open_angle_b = re.compile(r'<[^>]*$')
    re_tag = re.compile(r'<.*?>')
    re_tag_bestand = re.compile(r'\[\[Bestand:.*?\]\]')
    re_url = re.compile(r'http[^ ]*')
    re_html_ent = re.compile(r'\&[a-z]+\;')
    re_non_word = re.compile(r'[^0-9a-zA-ZÀ-ÿ]+')

    data_filename = url.split('/')[-1].split('.')[0] + '.data'
    words_filename = url.split('/')[-1].split('.')[0] + '.wordlist'

    def get_words(text):

        # if (mode == 'DEBUG') and ('ACfU3U1TkEpCpPIKHqumJoAl0KLvY8VB' in text):
        #     with open('test.txt', 'w') as f:
        #         f.write(text)

        t = text  # copy
        t = re.sub(re_url, ' ', t)
        t = re.sub(re_tag, ' ', t)
        t = re.sub(re_tag_bestand, ' ', t)
        t = re.sub(re_html_ent, ' ', t)
        t = re.sub(re_non_word, ' ', t)
        t = t.lstrip()
        return set(
            [w for w in t.split()
             if re.search(r'^[A-zÀ-ÿ]', w)  # Start with letter
             ]
        )

    def show_progress(c, w, t, done=False):
        text = t.lstrip()
        if text or done:
            print(f'chunks: {c:,}\twords: {len(w):,}\ttext: {text[:60]}...')

    words = set()
    chunks = 0
    text = ''
    decompressor = bz2.BZ2Decompressor()
    r = requests.get(url, stream=True)

    if mode == 'DEBUG':
        try:
            os.remove(data_filename)
        except FileNotFoundError:
            pass

    for chunk in r.iter_content(chunk_size=4096):
        if chunk:  # filter out keep-alive new chunks
            chunks += 1
            data = decompressor.decompress(chunk)

            try:
                text = ' '.join([text, data.decode('utf-8')])
            except UnicodeDecodeError:
                pass

            # Don't break on word character or open bracket
            if text and re.search(re_breaking_char, text) \
                    and not re.search(re_open_paren, text) \
                    and not re.search(re_open_curly, text) \
                    and not re.search(re_open_bracket, text) \
                    and not re.search(re_open_angle_b, text):
                words = get_words(text).union(words)
                show_progress(chunks, words, text)
                text = ''

            if mode == 'DEBUG':
                with open(data_filename, 'ab') as f:
                    f.write(data)
                if chunks > 999:
                    break

    words = get_words(text).union(words)
    show_progress(chunks, words, text, True)

    with open(words_filename, 'w') as f:
        for w in sorted(words):
            f.write(w)
            f.write('\n')


if __name__ == '__main__':
    domain = 'https://dumps.wikimedia.org'
    url = '/nlwiki/latest/nlwiki-latest-pages-articles1.xml.bz2'

    # main(domain + url, mode='DEBUG')
    main(domain + url)
