import requests
import json
import pandas as pd
from bs4 import BeautifulSoup


def get_list_club(id, lat, long):
    url = "https://www.fff.fr/api/find-club"
    params = {
        "find_club[licenceType1][]": "FC",
        "find_club[licenceType1][]": "FL",
        "find_club[licenceType1][]": "FH",
        "find_club[licenceType2][]": "F11",
        "find_club[licenceType2][]": "F8",
        "find_club[licenceType2][]": "FS",
        "find_club[licenceType2][]": "BS",
        # "find_club[position]": "Paris 75011",
        "find_club[latitude]": lat,
        "find_club[longitude]": long,
        "find_club[radius]": "5",
    }
    res = requests.post(url, data=params)
    if res.status_code != 200:
        raise Exception(f"Bad request status code {res.status_code}, id : {id}, lat : {lat}, long : {long}")
    return res.json()

def clean_club(list_club_json):
    base_url = "https://www.fff.fr/competition/club/"
    get_info_url = lambda x: f"{base_url}{x["cl_cod"]}-{x["cl_nom_slug"]}/information.html"
    def clean_item(item):
        item["info_url"] = get_info_url(item)
        for key in ["cl_cod", "cl_nom_slug", "logo", "cl_geo_location"]:
            del item[key]
        return item
    list_club_json = list(map(clean_item, list_club_json))
    return list_club_json


def get_club_contacts(info_url):
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


def add_contacts(list_club_json):
    def add_elem(club):
        club["contact"] = get_club_contacts(club["info_url"])
        return club
    return list(map(add_elem, list_club_json))



def get_main_contact(contact):
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


def add_main_email(list_club_json):
    def add_elem(club):
        club["main_email"] = get_main_contact(club["contact"])
        return club
    return list(map(add_elem, list_club_json))


def prepare_json(i, lat, long):
    list_club_json = get_list_club(i, lat, long)
    list_club_json = clean_club(list_club_json)
    list_club_json = add_contacts(list_club_json)
    list_club_json = add_main_email(list_club_json)
    return list_club_json


def save_list_club(list_club_json, output_name="data.json"):
    with open(output_name, "w", encoding="utf-8") as f:
        json.dump(list_club_json, f, ensure_ascii=False, indent=4)



def read_lat_long(csv_path, skiprows=None):
    cities = pd.read_csv(csv_path)
    if isinstance(skiprows, int):
        ids_to_remove = list(range(skiprows))
        ids_to_keep = [id for id in cities.index if id not in ids_to_remove]
        cities = cities.filter(items=ids_to_keep, axis=0)
    for i, (lat, long) in cities.iterrows():
        yield i, (lat, long)


def write_json_file(generator, file_path):
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



if __name__ == "__main__":
    input_path = "data/lat_long.csv"
    output_path = "data/fff_clubs.json"
    lat_long = read_lat_long(input_path, skiprows=None)
    write_json_file(lat_long, output_path)
