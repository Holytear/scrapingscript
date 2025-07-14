import pytest
import os
import json
from scrape import load_config, clean_and_validate_data

def test_load_config_creates_file(tmp_path, monkeypatch):
    # Geçici config dosyası yolu
    config_path = tmp_path / "config.json"
    monkeypatch.setattr("scrape.CONFIG_FILE", str(config_path))
    # Dosya yoksa oluşturmalı ve çıkmalı
    with pytest.raises(SystemExit):
        load_config()
    assert config_path.exists()
    with open(config_path, "r", encoding="utf-8") as f:
        data = json.load(f)
    assert "TARGET_SITE" in data

def test_clean_and_validate_data_basic():
    data = [
        {"category": "A", "url": "x"},
        {"category": "A", "url": "x"},  # tekrar
        {"category": "B", "url": "y"},
        {"category": "", "url": "z"},   # eksik
        {"category": None, "url": "z"},   # eksik
        {"category": "C", "url": None},  # eksik
    ]
    cleaned = clean_and_validate_data(data, required_fields=["category", "url"])
    assert len(cleaned) == 2
    assert all(row["category"] in ["A", "B"] for row in cleaned)

def test_clean_and_validate_data_strip():
    data = [{"category": "  A  ", "url": "  x  "}]
    cleaned = clean_and_validate_data(data, required_fields=["category", "url"])
    assert cleaned[0]["category"] == "A"
    assert cleaned[0]["url"] == "x"

# Web scraping fonksiyonları için smoke test (mock)
def test_scrape_functions_exist():
    import scrape
    assert hasattr(scrape, "scrape_categories")
    assert hasattr(scrape, "scrape_products") 