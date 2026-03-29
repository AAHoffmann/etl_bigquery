from src.extract.class_extract import Extract


def main():
    extract = Extract()

    url = "https://fakestoreapi.com/products"

    extract.extract_api_and_save_to_csv(url, None, "teste")

