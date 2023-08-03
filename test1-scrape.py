from bs4 import BeautifulSoup
import requests

url_buscape = ["http://www.buscape.com.br/avermedia-game-capture-hd.html?pos=1",
            "http://www.buscape.com.br/skil-3610-bancada.html#precos",
            "http://www.buscape.com.br/battlefield-4-xbox-360-dvd.html?pos=3"]

url_zoom = ["http://www.zoom.com.br/serras-eletricas/serra-circular-de-bancada-skil-3610?__zaf_=_q:serra%20circular||_o:4",
            "http://www.zoom.com.br/ar-condicionado/ar-condicionado-split-lg-12-000btus-ts-c122h4w0?__zaf_=split-hi-wall-convencional||_o:4",
            "http://www.zoom.com.br/jogos-xbox-360/jogo-battlefield-4-xbox-360-ea"]


def buscape(url):
    page = requests.get(url)
    soup = BeautifulSoup(page.text, 'html.parser')
    preco = []
    loja = []
    desc = []
    for link in soup.find_all('div', {'class': 'product-offers__price price'}):
        preco.append(link.text)
    for link in soup.find_all('div', {'class': 'offers-list__col small-7 medium-4 large-4 columns'}):
        for a in link.find_all('a'):
            desc.append(a.get("data-ga-label"))
            loja.append(a.get("data-ga-action"))
            break
    return zip(loja, desc, preco)


def zoom(url):
    page = requests.get(url)
    soup = BeautifulSoup(page.text, 'html.parser')
    preco = []
    loja = []
    desc = []
    for link in soup.find_all('p', {'class': 'offer-name'}):
        desc.append(link.text.replace('\t', '').replace('\n', '').replace('\r', ''))
    for link in soup.find_all('div', {'class': 'price-container'}):
        preco.append(link.text.replace('\t', '').replace('\n', '').replace('\r', '').replace('ou', ' ou '))
    for link in soup.find_all('p', {'class': 'store'}):
        for a in link.find_all('img'):
            loja.append(a.get('alt')[11:])
    if len(desc) == 0:
        for link in soup.find_all('div', {'class': 'product-img'}):
                for a in link.find_all('img'):
                    if len(desc)<len(preco):
                        desc.append(a.get('alt'))
    if len(loja) == 0:
        for link in soup.find_all('div', {'class': 'store-info'}):
                for a in link.find_all('img'):
                    if len(loja) < len(preco):
                        loja.append(a.get('alt'))
    return zip(loja, desc, preco)


for i in url_buscape:
    print i
    for resp in buscape(i):
        print('\nLoja: ' + resp[0] + '\nDescricao: ' + resp[1] + ' <> Preco: ' + resp[2])
    print '--------------------------------------------------------'


for i in url_zoom:
    print i
    for resp in zoom(i):
        print('\nLoja: ' + resp[0] + '\nDescricao: ' + resp[1] + ' <> Preco: ' + resp[2])
    print '--------------------------------------------------------'

