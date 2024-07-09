import csv
import json
import os
import re
from pathlib import Path

import casanova
import click
import geocoder
from rich.progress import BarColumn, MofNCompleteColumn, Progress, TimeElapsedColumn
from viapy.api import ViafAPI

GEO = os.environ["GEONAMES"]


@click.group
def cli():
    pass


INPUT = Path("viaf_ids.csv")
OUT_DIR = Path("viaf_results")


@cli.command
def get_viaf():
    api = ViafAPI()
    finished_results = [i.stem for i in OUT_DIR.iterdir()]

    total = casanova.count(INPUT)
    with open(INPUT, "r") as f, Progress(
        BarColumn(), MofNCompleteColumn(), TimeElapsedColumn()
    ) as p:
        t = p.add_task("", total=total)
        reader = casanova.reader(f)
        for id in reader.cells("VIAF"):
            if id in finished_results:
                p.advance(t)

            else:
                uri = api.uri_from_id(id) + "/"
                results = api.search(id)
                p.advance(t)
                item = [i for i in results if i.uri == uri][0]

                keys = [k for k in item.keys()]
                values = [v for v in item.values()]
                d = {k: v for k, v in zip(keys, values)}
                data = d["recordData"]
                with open(OUT_DIR.joinpath(f"{id}.json"), "w") as f:
                    json.dump(data, f, indent=4, ensure_ascii=False)


class Data:
    def __init__(self, record: dict) -> None:
        self.record = record

    def parse_500(
        self, x500: dict, marc_tag: str, marc_code: str, marc_text: str
    ) -> str | None:
        df = x500["datafield"]
        if df["@tag"] == marc_tag:
            subfields = df["subfield"]
            if isinstance(subfields, dict):
                subfields = [subfields]
            for sf in subfields:
                code, text = sf["@code"], sf["#text"]
                if marc_code == code and marc_text == text:
                    return df["normalized"]

    @property
    def x500s(self) -> list:
        if self.record.get("x500s"):
            x = self.record["x500s"]["x500"]
            if isinstance(x, dict):
                return [x]
            else:
                return x
        else:
            return []

    @property
    def x400s(self) -> list:
        if self.record.get("x400s"):
            x = self.record["x400s"]["x400"]
            if isinstance(x, dict):
                return [x]
            else:
                return x
        else:
            return []

    @property
    def orta(self) -> dict | None:
        for x500 in self.x500s:
            normalized = self.parse_500(
                x500, marc_tag="551", marc_code="4", marc_text="orta"
            )
            if normalized:
                return normalized

    @property
    def geow(self) -> dict | None:
        for x500 in self.x500s:
            normalized = self.parse_500(
                x500, marc_tag="551", marc_code="4", marc_text="geow"
            )
            if normalized:
                return normalized

    @property
    def isni(self) -> str | None:
        for x400 in self.x400s:
            if (
                x400["datafield"]["@tag"] == "410"
                and x400["datafield"]["@dtype"] == "MARC21"
            ):
                if x400["sources"]["s"] == "ISNI":
                    return x400["sources"]["sid"].split("ISNI|")[-1]

    @property
    def names(self) -> list:
        l = []
        headings = self.record["mainHeadings"]["data"]
        if isinstance(headings, dict):
            headings = [headings]
        for heading in headings:
            l.append(heading["text"])
        return l


@cli.command()
def get_geonames():
    d = {}
    for file in OUT_DIR.iterdir():
        with open(file, "r") as f:
            record = json.load(f)
            data = Data(record)
            name = data.names[0]
            all_names = set(data.names)
            all_names.remove(name)
            clean_name = re.sub(pattern=r"\(.*\)", repl="", string=name)
            clean_name = re.sub(pattern=r"\.$", repl="", string=clean_name)

            space = data.orta
            pd = {
                "id": None,
                "name": "",
                "admin1": "",
                "point": "",
                "country": "",
            }
            country = data.geow
            if space:
                p = geocoder.geonames(space, key=GEO, featureClass="P")
                print(p.address)
                p_id = p.geonames_id
                pr = geocoder.geonames(p_id, method="details", key=GEO)
                pd = {
                    "id": p_id,
                    "name": pr.address,
                    "admin1": pr.admin2,
                    "point": f"POINT({pr.lng} {pr.lat})",
                    "country": pr.country,
                }
                country = pr.country

            d.update(
                {
                    file.stem: {
                        "name": clean_name.strip(),
                        "alternativeNames": list(all_names),
                        "ISNI": data.isni,
                        "space": space,
                        "cityGEO": pd,
                        "geo": country,
                    }
                }
            )
    with open("repository_names.json", "w") as f:
        json.dump(d, f, ensure_ascii=False, indent=4)


@cli.command
@click.argument("city-name")
def city(city_name):
    p = geocoder.geonames(city_name, key=GEO, featureClass="P")
    p_id = p.geonames_id
    pr = geocoder.geonames(p_id, method="details", key=GEO)
    pd = {
        "id": p_id,
        "name": pr.address,
        "admin1": pr.state,
        "point": f"POINT({pr.lng} {pr.lat})",
        "country": pr.country,
    }
    s = json.dumps(pd)
    q = json.dumps(json.loads(s), indent=2, ensure_ascii=False)
    from pprint import pprint

    pprint(q)


@cli.command
@click.argument("p-id")
def city_id(p_id):
    p_id = int(p_id)
    pr = geocoder.geonames(p_id, method="details", key=GEO)
    pd = {
        "id": p_id,
        "name": pr.address,
        "admin1": pr.state,
        "point": f"POINT({pr.lng} {pr.lat})",
        "country": pr.country,
    }
    s = json.dumps(pd)
    q = json.dumps(json.loads(s), indent=2, ensure_ascii=False)
    from pprint import pprint

    pprint(q)


MANUALLY_CLEANED_REPOS_WITH_VIAF_AND_GEONAMES = "repository_names_working.json"


@cli.command
def pull_cities():
    geonames = {}
    with open(MANUALLY_CLEANED_REPOS_WITH_VIAF_AND_GEONAMES, "r") as f:
        data = json.load(f)
        for metadata in data.values():
            geo = metadata["cityGEO"]
            id = geo["id"]
            if geo["country"] == "The Netherlands":
                geo.update({"country": "Netherlands"})
            geonames.update({id: geo})
    with open("unique_cities.csv", "w") as of:
        writer = csv.DictWriter(
            of, fieldnames=["id", "name", "admin1", "point", "country"]
        )
        writer.writeheader()
        for geo in geonames.values():
            writer.writerow(geo)


@cli.command
def pull_repos():
    places = {}
    with open("places_added_09072024.csv", "r") as f:
        reader = csv.DictReader(f)
        for row in reader:
            geonames_id = int(row["GeoNames ID"])
            h_id = row['\ufeff"Place H-ID"']
            if places.get(geonames_id):
                raise KeyError
            places.update(
                {
                    geonames_id: {
                        "geo": geonames_id,
                        "H-ID": h_id,
                        "title": row["rec_Title"],
                    }
                }
            )

    repo_names = []
    with open(MANUALLY_CLEANED_REPOS_WITH_VIAF_AND_GEONAMES, "r") as f:
        data = json.load(f)
        for id, metadata in data.items():
            name = metadata["name"]
            alt_names = "|".join(metadata["alternativeNames"])
            isni = metadata["ISNI"]
            geo_id = metadata["cityGEO"]["id"]
            city_h_id = places[geo_id]["H-ID"]
            repo_names.append(
                {
                    "name": name,
                    "alt names": alt_names,
                    "City H-ID": city_h_id,
                    "VIAF ID": id,
                    "ISNI": isni,
                }
            )

    with open("unique_viaf_repos.csv", "w") as of:
        writer = csv.DictWriter(
            of, fieldnames=["name", "alt names", "City H-ID", "VIAF ID", "ISNI"]
        )
        writer.writeheader()
        for repo in repo_names:
            writer.writerow(repo)


if __name__ == "__main__":
    cli()
