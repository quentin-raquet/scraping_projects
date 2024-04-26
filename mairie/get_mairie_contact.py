import tarfile
import requests
import pandas as pd


def download_file(url: str, file_path: str) -> None:
    """
    Download a file from the specified URL and save it to the given file path.

    Args:
        url (str): The URL of the file to download.
        file_path (str): The path where the downloaded file should be saved.
    """
    response = requests.get(url)
    if response.status_code == 200:
        with open(file_path, "wb") as file:
            file.write(response.content)
        print("File downloaded successfully.")
    else:
        print("Failed to download the file.")


def extract_files(file_path: str) -> None:
    """
    Extract all files from a tar.bz2 archive.

    Args:
        file_path (str): The path to the tar.bz2 file.
    """
    with tarfile.open(file_path, "r:bz2") as tar:
        tar.extractall(path="data/")


def clean_data(input_path: str) -> None:
    """
    Clean the data and save it to a CSV file.
    """
    data = pd.read_json(input_path).service
    data = pd.DataFrame(data.tolist())
    new_cols_df = pd.json_normalize(data["pivot"])[0]
    new_cols_df = pd.DataFrame(
        [
            {"type_service_local": "", "code_insee_commune": []} if value is None else value
            for value in new_cols_df.tolist()
        ]
    )
    mask = new_cols_df.type_service_local == "mairie"
    data = data.loc[mask, ["nom", "adresse_courriel", "telephone"]]
    data.nom = data.nom.apply(lambda x: x.replace("Mairie - ", "") if x else None)
    data.adresse_courriel = data.adresse_courriel.apply(lambda x: x[0] if x else None)
    data.telephone = data.telephone.apply(lambda x: x[0]["valeur"] if x else None)
    data = data.rename(
        columns={"nom": "nom_mairie", "adresse_courriel": "email", "telephone": "phone"}
    )
    return data


if __name__ == "__main__":
    # Specify the URL of the file you want to download
    url = "https://www.data.gouv.fr/fr/datasets/r/73302880-e4df-4d4c-8676-1a61bb997f3d"

    # Specify the path where you want to save the downloaded file
    file_path = "data/mairie.tar.bz2"

    # Download the file
    download_file(url, file_path)

    # Extract the files from the tar.bz2 archive
    extract_files(file_path)

    # Clean the data and save it to a CSV file
    input_path = "data/2024-04-26_060518-data.gouv_local.json"
    data = clean_data(input_path)

    # Save the cleaned data to a CSV file
    output_path = "data/mairie_contact.csv"
    data.to_csv(output_path, index=False)
