# Selenium Web Scraper

## Özellikler
- Dinamik ve kolayca uyarlanabilir scraping
- Proxy, user-agent, headless, scroll, captcha tespiti, paralel scraping
- Otomatik test altyapısı

## Kurulum
```bash
pip install -r requirements.txt
```

## Kullanım
1. `config.json` dosyasını düzenle.
2. `python scrape.py` ile çalıştır.
3. Testler için: `pytest test_scrape.py`

## WebDriver
Webdriver otomatik olarak indirilir (`webdriver-manager` ile).
