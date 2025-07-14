import random
import time
import pandas as pd
import logging
from selenium import webdriver
from selenium.webdriver.common.by import By
from selenium.webdriver.chrome.options import Options
from selenium.common.exceptions import NoSuchElementException, ElementClickInterceptedException, WebDriverException
from functools import wraps
import threading
from queue import Queue
import json
import os
from selenium import webdriver
from webdriver_manager.chrome import ChromeDriverManager

# =====================
# CONFIG YÜKLEME
# =====================
CONFIG_FILE = "config.json"
def load_config():
    if not os.path.exists(CONFIG_FILE):
        # Örnek config oluştur
        example = {
            "TARGET_SITE": "https://okmarts.com/categories/index",
            "CATEGORY_SELECTOR": "div.type-list-out",
            "CATEGORY_NAME_SELECTOR": "p",
            "CATEGORY_URL_SELECTOR": "a.inner",
            "CATEGORY_CSV": "okmarts.csv",
            "PRODUCT_SELECTOR": "div.goods",
            "PRODUCT_NAME_SELECTOR": "h3.overflow-text-2",
            "PRODUCT_PRICE_SELECTOR": "div.price",
            "PRODUCT_IMAGE_SELECTOR": "img.goods_img",
            "NEXT_BUTTON_SELECTOR": "a.next",
            "PRODUCT_CSV": "products.csv",
            "PROXIES": [None],
            "USER_AGENTS": [
                "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/115.0.0.0 Safari/537.36"
            ],
            "HEADLESS": True,
            "SCROLL_PAUSE": 2,
            "SCROLL_MAX_ATTEMPTS": 20
        }
        with open(CONFIG_FILE, "w", encoding="utf-8") as f:
            json.dump(example, f, indent=2, ensure_ascii=False)
        print(f"Örnek config.json oluşturuldu. Lütfen ayarları düzenleyin ve tekrar çalıştırın.")
        exit(1)
    with open(CONFIG_FILE, "r", encoding="utf-8") as f:
        return json.load(f)

config = load_config()

# Config'ten parametreleri oku
globals().update(config)

# =====================
# LOGGING AYARLARI
# =====================
logging.basicConfig(
    filename='scraper.log',
    filemode='a',
    format='%(asctime)s - %(levelname)s - %(message)s',
    level=logging.INFO
)
logger = logging.getLogger()

# =====================
# PROXY VE USER-AGENT LİSTELERİ
# =====================
PROXIES = [
    # Örnek: "http://username:password@proxyserver:port",
    # "http://proxy2:port",
    # Boş bırakılırsa proxy kullanılmaz
    None
]
USER_AGENTS = [
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/115.0.0.0 Safari/537.36",
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/605.1.15 (KHTML, like Gecko) Version/15.1 Safari/605.1.15",
    "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36",
    # Listeyi istediğin kadar uzatabilirsin
]

# =====================
# HEADLESS MOD PARAMETRESİ
# =====================
HEADLESS = True  # True: Tarayıcı arka planda (görünmez) çalışır, False: Görünür

# =====================
# OTOMATİK YENİDEN DENEYEN (RETRY) DECORATOR
# =====================
def retry(ExceptionToCheck, tries=3, delay=2, backoff=2):
    def deco_retry(f):
        @wraps(f)
        def f_retry(*args, **kwargs):
            mtries, mdelay = tries, delay
            while mtries > 1:
                try:
                    return f(*args, **kwargs)
                except ExceptionToCheck as e:
                    logger.warning(f"{f.__name__} hata: {e}, tekrar denenecek ({mtries-1} kaldı)")
                    time.sleep(mdelay)
                    mtries -= 1
                    mdelay *= backoff
            return f(*args, **kwargs)
        return f_retry
    return deco_retry

# =====================
# CAPTCHA/BOT KORUMASI TESPİTİ
# =====================
def is_captcha_page(driver):
    page_source = driver.page_source.lower()
    captcha_keywords = [
        'captcha', 'recaptcha', 'g-recaptcha', 'hcaptcha', 'cloudflare', 'are you human', 'verify you are a human',
        'please verify', 'robot', 'protection', 'security check', 'cf-chl-captcha', 'cf-captcha-container'
    ]
    for keyword in captcha_keywords:
        if keyword in page_source:
            logger.warning(f"Captcha/bot koruması tespit edildi: {keyword}")
            print("UYARI: Captcha veya bot koruması tespit edildi!")
            return True
    return False

# =====================
# KOLAYCA DEĞİŞTİRİLEBİLİR PARAMETRELER
# =====================
TARGET_SITE = "https://okmarts.com/categories/index"  # Ana kategori sayfası
CATEGORY_SELECTOR = "div.type-list-out"               # Kategori kutusu
CATEGORY_NAME_SELECTOR = "p"                          # Kategori adı
CATEGORY_URL_SELECTOR = "a.inner"                     # Kategori linki
CATEGORY_CSV = "okmarts.csv"                          # Kategori CSV dosyası

PRODUCT_SELECTOR = "div.goods"                        # Ürün kutusu
PRODUCT_NAME_SELECTOR = "h3.overflow-text-2"           # Ürün adı
PRODUCT_PRICE_SELECTOR = "div.price"                   # Fiyat
PRODUCT_IMAGE_SELECTOR = "img.goods_img"               # Resim
NEXT_BUTTON_SELECTOR = "a.next"                        # Sonraki sayfa butonu
PRODUCT_CSV = "products.csv"                           # Ürün CSV dosyası

# =====================
# VERİ TEMİZLİĞİ VE DOĞRULAMA
# =====================
def clean_and_validate_data(data, required_fields=None):
    """
    data: Liste (dict)
    required_fields: Zorunlu alanlar (ör: ["category", "url"])
    - Boş, None veya eksik alanlı kayıtları atar
    - Tekrar eden kayıtları atar
    - Tüm string alanlarda strip uygular
    """
    if not data:
        return []
    cleaned = []
    seen = set()
    for row in data:
        # Alanları temizle
        row_clean = {k: (v.strip() if isinstance(v, str) else v) for k, v in row.items()}
        # Zorunlu alan kontrolü
        if required_fields and not all(row_clean.get(f) for f in required_fields):
            logger.warning(f"Eksik alanlı kayıt atlandı: {row_clean}")
            continue
        # Tekrar kontrolü (tuple olarak hashlenebilir)
        row_tuple = tuple(row_clean.get(f) for f in (required_fields or row_clean.keys()))
        if row_tuple in seen:
            logger.info(f"Tekrar eden kayıt atlandı: {row_clean}")
            continue
        seen.add(row_tuple)
        cleaned.append(row_clean)
    return cleaned

# =====================
# CSV YAZMA
# =====================
def write(data, file_name, required_fields=None):
    cleaned_data = clean_and_validate_data(data, required_fields)
    if cleaned_data:
        df_data = pd.DataFrame(cleaned_data)
        df_data.to_csv(file_name, index=False, encoding="utf-8")
        logger.info(f"{file_name} dosyasına {len(cleaned_data)} temiz kayıt yazıldı.")
    else:
        logger.warning(f"{file_name} için yazılacak temiz veri yok.")

# =====================
# KATEGORİLERİ ÇEKME
# =====================
@retry(Exception, tries=2, delay=2)
def scrape_categories():
    data = []
    driver = get_webdriver()
    try:
        driver.get(TARGET_SITE)
        logger.info(f"Kategori sayfası açıldı: {TARGET_SITE}")
        time.sleep(8)
        if is_captcha_page(driver):
            logger.error("Captcha/bot koruması nedeniyle scraping durduruldu (kategori).");
            return
        elements = driver.find_elements(By.CSS_SELECTOR, CATEGORY_SELECTOR)
        for element in elements:
            try:
                category = element.find_element(By.CSS_SELECTOR, CATEGORY_NAME_SELECTOR).text.strip()
                category_url = element.find_element(By.CSS_SELECTOR, CATEGORY_URL_SELECTOR).get_attribute("href")
                if not category or not category_url:
                    logger.warning(f"Eksik kategori veya url: {category}, {category_url}")
                    continue
                logger.info(f"Kategori bulundu: {category} - {category_url}")
                data.append({"category": category, "url": category_url})
            except Exception as e:
                logger.error(f"Kategori çekme hatası: {e}")
        write(data, CATEGORY_CSV, required_fields=["category", "url"])
    except Exception as e:
        logger.error(f"scrape_categories genel hata: {e}")
        raise
    finally:
        driver.quit()
        logger.info("WebDriver kapatıldı (kategori çekme)")

def scroll_to_bottom(driver, pause_time=2, max_attempts=20):
    """
    Sayfanın sonuna kadar scroll eder. Her scroll sonrası yeni içerik yüklenip yüklenmediğini kontrol eder.
    """
    last_height = driver.execute_script("return document.body.scrollHeight")
    attempts = 0
    while attempts < max_attempts:
        driver.execute_script("window.scrollTo(0, document.body.scrollHeight);")
        time.sleep(pause_time)
        new_height = driver.execute_script("return document.body.scrollHeight")
        if new_height == last_height:
            logger.info(f"Scroll işlemi tamamlandı. Toplam deneme: {attempts}")
            break
        last_height = new_height
        attempts += 1
    if attempts == max_attempts:
        logger.warning("Scroll işlemi max denemeye ulaştı.")

# =====================
# ÜRÜNLERİ ÇEKME (PAGINATION DAHİL)
# =====================
def scrape_products_worker(row, data_queue):
    driver = get_webdriver()
    try:
        driver.get(row["url"])
        logger.info(f"[Thread] Kategoriye girildi: {row['category']} - {row['url']}")
        time.sleep(3)
        if is_captcha_page(driver):
            logger.error(f"[Thread] Captcha/bot koruması nedeniyle scraping durduruldu (ürün) - {row['category']}");
            return
        # Dinamik içerik/sonsuz scroll desteği
        scroll_to_bottom(driver, pause_time=2, max_attempts=20)
        while True:
            try:
                elements = driver.find_elements(By.CSS_SELECTOR, PRODUCT_SELECTOR)
                for element in elements:
                    try:
                        product_name = element.find_element(By.CSS_SELECTOR, PRODUCT_NAME_SELECTOR).text.strip()
                        price = element.find_element(By.CSS_SELECTOR, PRODUCT_PRICE_SELECTOR).text.strip()
                        image = element.find_element(By.CSS_SELECTOR, PRODUCT_IMAGE_SELECTOR).get_attribute("src")
                        if not product_name or not price or not image:
                            logger.warning(f"[Thread] Eksik ürün verisi: {product_name}, {price}, {image}")
                            continue
                        data_queue.put({
                            "category": row["category"],
                            "product_name": product_name,
                            "price": price,
                            "image": image
                        })
                    except Exception as e:
                        logger.error(f"[Thread] Ürün çekme hatası: {e}")
                next_button = driver.find_element(By.CSS_SELECTOR, NEXT_BUTTON_SELECTOR)
                if "disabled" in next_button.get_attribute("class"):
                    logger.info("[Thread] Sonraki sayfa yok, kategori tamamlandı.")
                    break
                next_button.click()
                logger.info("[Thread] Sonraki sayfaya geçildi.")
                time.sleep(random.randint(3, 6))
            except NoSuchElementException:
                logger.info("[Thread] Daha fazla sayfa yok.")
                break
            except ElementClickInterceptedException as e:
                logger.warning(f"[Thread] Tıklama hatası: {e}")
                time.sleep(3)
        logger.info(f"[Thread] Bitti: {row['category']}")
    except Exception as e:
        logger.error(f"[Thread] scrape_products_worker hata: {e}")
    finally:
        driver.quit()
        logger.info("[Thread] WebDriver kapatıldı (ürün çekme)")

@retry(Exception, tries=2, delay=2)
def scrape_products():
    data_queue = Queue()
    threads = []
    try:
        category_urls = pd.read_csv(CATEGORY_CSV)
        for i, row in category_urls.iterrows():
            t = threading.Thread(target=scrape_products_worker, args=(row, data_queue))
            threads.append(t)
            t.start()
        for t in threads:
            t.join()
        # Tüm threadlerden gelen verileri topla
        data = []
        while not data_queue.empty():
            data.append(data_queue.get())
        write(data, PRODUCT_CSV, required_fields=["category", "product_name", "price", "image"])
        logger.info("Tüm ürünler çekildi! (paralel)")
    except Exception as e:
        logger.error(f"scrape_products genel hata: {e}")
        raise

# =====================
# KULLANIM
# =====================
if __name__ == "__main__":
    scrape_categories()
    scrape_products()

# Not: Başta tanımlı selector ve URL'leri değiştirerek başka siteler için kolayca uyarlanabilir.
