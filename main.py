from src.extractors.extractor_template import extract

if __name__ == "__main__":
    dane = extract()
    print(f"Main odebrał {len(dane)} rekordów.")

