from bs4 import BeautifulSoup
from lxml import etree
import requests

headers = {
    'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,image/apng,*/*;q=0.8,application/signed-exchange;v=b3;q=0.7',
    'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/113.0.0.0 Safari/537.36'
}

url_main = 'https://minzdrav.gov.ru'
url_standart = 'ministry/61/22/stranitsa-979/stranitsa-983/1-standarty-pervichnoy-mediko-sanitarnoy-pomoschi'

page = requests.get(f'{url_main}/{url_standart}', headers=headers)
soup = BeautifulSoup(page.text, "html.parser")
dom = etree.HTML(str(soup))

xpath_urls = dom.xpath('//*[@id="mz-container"]/div[1]/div/article/div[1]/div/a')

category_pages = []
for url in xpath_urls:
    category_pages.append(
        {
            'name': url.text,
            'url': url.get('href'),
            'order': [],
        }
    )

for page_info in category_pages:
    page = requests.get(f'{url_main}/{page_info["url"]}', headers=headers)
    soup = BeautifulSoup(page.text, "html.parser")
    extra_words = 'prikaz-ministerstva-zdravoohraneniya-rossiyskoy-federatsii'

    url_order = [url.get('href') for url in soup.findAll('a') if extra_words in str(url.get('href'))]

    for url in url_order:
        page_order = requests.get(f'{url}', headers=headers)
        soup_order = BeautifulSoup(page_order.text, "html.parser")
        loader_info = soup_order.find('div', class_='document_title').find('a')

        if loader_info:
            filename = loader_info.get('download')
            load_url = loader_info.get('href')
            filedata = requests.get(f'https:{load_url}', headers=headers)
            with open(f'loaded/{filename}', 'wb') as f:
                f.write(filedata.content)