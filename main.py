import json
import csv
import asyncio
import logging
from bs4 import BeautifulSoup
from playwright.async_api import async_playwright
import aiofiles
import re

logging.basicConfig(filename='yandex.log', filemode='w', level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

BASE_URL = "https://market.yandex.ru"

async def get_content(page, url):
    try:
        await page.goto(url)
        html = await page.content()
        soup = BeautifulSoup(html, 'html.parser')

        name = soup.find('h1').get_text(strip=True)
        logger.info(f"Processing item: {name}")  # Log group information

        price_tag = soup.find("span", string="Цена с картой Яндекс Пэй:")
        price_parent = price_tag.find_parent("h3")
        price = price_parent.text.strip().replace('Цена с картой Яндекс Пэй:', '').strip() if price_parent else "None"
        availability_meta = soup.find('meta', itemprop='availability')
        cards = []
#<meta itemprop="availability" content="https://schema.org/InStock">
        try:
            card = {
                'Ссылка': url,
                'Наличие': availability_meta["content"] if availability_meta else "Unknown",
                'Наименование': soup.find('h1').get_text(strip=True),
                'Цена с картой Яндекс Пэй': price,
            }
            cards.append(card)
        except Exception as e:
            logger.error(f"Error processing item: {e}")
        return cards

    except Exception as e:
        logger.error(f"Error getting content from {url}: {e}")
        return []

headers = {'User-Agent':'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) \
AppleWebKit/537.36 (KHTML, like Gecko) Chrome/113.0.0.0 Safari/537.36',
'Accept':'text/html,application/xhtml+xml,application/xml;q=0.9,\
image/avif,image/webp,image/apng,*/*;q=0.8,application/signed-exchange;v=b3;\
q=0.7'}


async def run(playwright):
    user_data_dir = "/playwright"
    browser = await playwright.chromium.launch_persistent_context(
        user_data_dir,
        # headless=True,
        headless=False,
        slow_mo=3000,
        proxy={
            "server": "",
            "username": "",
            "password": ""
        },
        extra_http_headers=headers,
        args=[
            "--disable-popup-blocking",
            "--disable-gpu"
        ],
        ignore_default_args=["--mute-audio"]
    )

    page = await browser.new_page()

    try:
        with open('links.json', 'r') as f:
            data = json.load(f)

        # Get the list of URLs from the loaded data
        urls = data['urls']

        async with aiofiles.open('output.csv', mode='w', newline='', encoding='utf-8-sig') as csvfile:
            fieldnames = ['Ссылка', 'Наличие', 'Наименование', 'Цена с картой Яндекс Пэй']
            writer = csv.DictWriter(csvfile, fieldnames=fieldnames, delimiter=';')

            await csvfile.write(';'.join(fieldnames) + '\n')  # Write headers

            for url_index, url in enumerate(urls):  # Add URL index
                logger.info(f"Processing URL {url_index + 1}/{len(urls)}: {url}")  # Log the current URL
                cards = await get_content(page, url)
                for card_index, card in enumerate(cards):  # Add card index
                    await writer.writerow(card)
    finally:
        await browser.close()


async def main():
    async with async_playwright() as playwright:
        await run(playwright)

if __name__ == "__main__":
    asyncio.run(main())
