import pandas as pd

if __name__ == "__main__":
    cities = pd.read_csv(
        "https://static.data.gouv.fr/resources/villes-de-france/20220928-173607/cities.csv"
    )
    cities = cities.loc[
        ~cities.region_name.isin(
            [
                "polynésie-française",
                "la réunion",
                "guadeloupe",
                "martinique",
                "nouvelle-calédonie",
                "guyane",
                "mayotte",
                "saint-pierre-et-miquelon",
                "wallis-et-futuna",
            ]
        ),
        ["latitude", "longitude"],
    ]
    cities.to_csv("data/lat_long_temp.csv", index=False)