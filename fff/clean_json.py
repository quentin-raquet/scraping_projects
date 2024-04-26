# Clean json file after scraping
import argparse
import pandas as pd

if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument(
        "--file_path", default="data/fff_clubs.json", type=str, required=False
    )
    args = parser.parse_args()

    with open(args.file_path, "r") as file:
        filedata = file.read()

    filedata = filedata.replace("[]", "").replace("][", ",")

    split_file = args.file_path.split(".")
    clean_file = f"{split_file[0]}_cleaned.{split_file[1]}"

    with open(clean_file, "w") as file:
        file.write(filedata)

    data = pd.read_json(clean_file)
    data.drop_duplicates(subset=["cl_nom", "info_url"], inplace=True)
    data = data.loc[data.main_email.notnull()]
    data.to_csv("data/fff_clubs.csv", index=False)
