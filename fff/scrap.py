# This script scrapes the FFF website to get the list
# of clubs based on the given latitude and longitude.
import requests
import json
from typing import Optional, Generator
import pandas as pd
from bs4 import BeautifulSoup


def get_list_club(id: int, lat: float, long: float) -> dict:
    """
    Get the list of clubs based on the given latitude, and longitude.

    Args:
        id (int): The ID of the lat long file.
        lat (float): The latitude of the location.
        long (float): The longitude of the location.

    Returns:
        dict: The JSON response containing the list of clubs.
    """
    url = "https://www.fff.fr/api/find-club"
    params = {
        "find_club[licenceType1][]": "FC",
        "find_club[licenceType1][]": "FL",
        "find_club[licenceType1][]": "FH",
        "find_club[licenceType2][]": "F11",
        "find_club[licenceType2][]": "F8",
        "find_club[licenceType2][]": "FS",
        "find_club[licenceType2][]": "BS",
        "find_club[latitude]": lat,
        "find_club[longitude]": long,
        "find_club[radius]": "5",
    }
    res = requests.post(url, data=params)
    if res.status_code != 200:
        raise Exception(f"Bad request status code {res.status_code}, id : {id}, lat : {lat}, long : {long}")
    return res.json()


def clean_club(list_club_json: list[dict]) -> list[dict]:
    """Clean the club JSON data.

    Args:
        list_club_json (list): List of club JSON data.

    Returns:
        list: List of cleaned club JSON data.
    """
    base_url = "https://www.fff.fr/competition/club/"
    get_info_url = lambda x: f"{base_url}{x["cl_cod"]}-{x["cl_nom_slug"]}/information.html"
    def clean_item(item):
        item["info_url"] = get_info_url(item)
        for key in ["cl_cod", "cl_nom_slug", "logo", "cl_geo_location"]:
            del item[key]
        return item
    list_club_json = list(map(clean_item, list_club_json))
    return list_club_json


def get_club_contacts(info_url: str) -> dict:
    """Get the club contacts based on the given info URL.

    Args:
        info_url (str): Information URL of the club.

    Returns:
        dict: Contact information of the club.
    """
    page = requests.get(info_url)
    soup = BeautifulSoup(page.content, "lxml")
    contacts = {}
    for item in soup.find("section", class_="staff container").find_all(
        "section", class_="legende_map"
    ):
        head = item.find_all("p", class_=["margin_b8", "bold"])
        title = head[0].get_text(strip=True)
        name = head[1].get_text(strip=True)
        contacts[title] = {"name": name}
        for elem in item.find_all("p", class_="margin_b8", partial=False):
            if "bold" in elem["class"]:
                continue
            text = elem.get_text(strip=True)
            text_s = text.split(" : ")
            contacts[title][text_s[0]] = text_s[1]
    return contacts


def add_contacts(list_club_json: list[dict]) -> list[dict]:
    """Add contacts to the club JSON data."""
    def add_elem(club):
        club["contact"] = get_club_contacts(club["info_url"])
        return club
    return list(map(add_elem, list_club_json))



def get_main_contact(contact: dict) -> Optional[str]:
    """Get the main contact from the given contact information.

    Args:
        contact (dict): Contact information of the club.

    Returns:
        str: Main contact of the club.
    """
    titles = [
        "CORRESPONDANT",
        "PRESIDENT",
        "VICE PRESIDENT",
        "SECRETAIRE GENERAL",
        "TRESORIER",
        "RESPONSABLE TECHNIQUE JEUNES",
        "DIRECTEUR TECHNIQUE"
    ]
    emails = ["Email principal", "Email officiel", "Email autre"]
    for title in titles:
        if title in contact.keys():
            contact_keys = contact[title].keys()
            match = [key for key in emails if key in contact_keys]
            if len(match) > 0:
                return contact[title][match[0]]
    other_titles = [elem for elem in contact.keys() if elem not in titles]
    for title in other_titles:
        if title in contact.keys():
            contact_keys = contact[title].keys()
            match = [key for key in emails if key in contact_keys]
            if len(match) > 0:
                return contact[title][match[0]]
    return None


def add_main_email(list_club_json: list[dict]) -> list[dict]:
    """Add the main email to the club JSON data."""
    def add_elem(club):
        club["main_email"] = get_main_contact(club["contact"])
        return club
    return list(map(add_elem, list_club_json))


def prepare_json(i: int, lat: float, long: float) -> list[dict]:
    """Prepare the JSON data based on the given latitude and longitude."""
    list_club_json = get_list_club(i, lat, long)
    list_club_json = clean_club(list_club_json)
    list_club_json = add_contacts(list_club_json)
    list_club_json = add_main_email(list_club_json)
    return list_club_json


def save_list_club(list_club_json: list[dict], output_name: str ="data.json") -> None:
    """Save the list of clubs to the given output file."""
    with open(output_name, "w", encoding="utf-8") as f:
        json.dump(list_club_json, f, ensure_ascii=False, indent=4)



def read_lat_long(csv_path: str, skiprows: Optional[int]=None) -> Generator[int, tuple[float, float]]:
    """Read the latitude and longitude from the given CSV file."""
    cities = pd.read_csv(csv_path)
    if isinstance(skiprows, int):
        ids_to_remove = list(range(skiprows))
        ids_to_keep = [id for id in cities.index if id not in ids_to_remove]
        cities = cities.filter(items=ids_to_keep, axis=0)
    for i, (lat, long) in cities.iterrows():
        yield i, (lat, long)


def write_json_file(generator: Generator[int, tuple[float, float]], file_path: str) -> None:
    """
    Write data from a generator to a JSON file.

    Args:
        generator: A generator that yields tuples of latitude and longitude values.
        file_path (str): The path to the JSON file.

    Raises:
        Exception: If there is an error while writing the data to the JSON file.

    """
    try:
        # Open the JSON file in append mode
        with open(file_path, 'a') as json_file:
            for i, (lat, long) in generator:
                try:
                    data = prepare_json(i, lat, long)
                    json.dump(data, json_file)
                    print(f"Data ({i}-{lat}-{long}) successfully written to JSON file.")
                except Exception as e:
                    print(f"Data ({i}-{lat}-{long}) failed")
                    print("Error:", e)
                    pass
    except Exception as e:
        print("Error:", e)


def process_club_data(input_path: str, output_path: str, skiprows: int) -> None:
    """
    Process club data from the given input file and save it to the output file.

    Args:
        input_path (str): The path to the input file.
        output_path (str): The path to the output file.
        skiprows (int): The number of rows to skip in the input file.

    Returns:
        None
    """
    lat_long = read_lat_long(input_path, skiprows=skiprows)
    write_json_file(lat_long, output_path)


if __name__ == "__main__":
    input_path = "data/lat_long.csv"
    output_path = "data/fff_clubs.json"
    process_club_data(input_path, skiprows=None)
