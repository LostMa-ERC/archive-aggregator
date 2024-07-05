import os

import requests
from requests import RequestException
from rich.progress import (
    BarColumn,
    MofNCompleteColumn,
    Progress,
    SpinnerColumn,
    TextColumn,
    TimeElapsedColumn,
)

from dataclasses import dataclass

from database import DBConnection
from create_statements import WIKIDATA_CREATE_STATEMENT, GEONAMES_CREATE_STATEMENT

USER = os.environ["MYSQL_USER"]
PASS = os.environ["MYSQL_PASS"]

URL = "https://query.wikidata.org/sparql"
with open("wikidata-archives.txt", "r") as f:
    QUERY_ARCHIVE = f.read()
with open("wikidata-libraries.txt", "r") as f:
    QUERY_LIBRARY = f.read()


@dataclass
class SPARQLResults:
    id: str | None = None
    viafID: str | None = None
    label: str | None = None
    point: str | None = None
    continentWikiID: str | None = None
    continentWikiLabel: str | None = None
    countryWikiID: str | None = None
    countryWikiLabel: str | None = None
    cityWikiID: str | None = None
    cityGeoName: str | None = None
    cityPoint: str | None = None
    cityWikiLabel: str | None = None
    localityWikiID: str | None = None
    localityWikiLabel: str | None = None

    @property
    def __list__(self) -> list:
        return list(self.__dict__.values())

    @property
    def names(self) -> str:
        return ", ".join(self.__dict__.keys())

    @property
    def place_holders(self) -> str:
        return ", ".join("%s" for _ in range(len(self.__dict__.keys())))

    @classmethod
    def convert_columns(cls, row: dict) -> "SPARQLResults":
        return SPARQLResults(
            id=row["item"],
            viafID=row["viaf"],
            label=row["itemLabel"],
            point=row["itemPoint"],
            continentWikiID=row["continent"],
            continentWikiLabel=row["continentLabel"],
            countryWikiID=row["country"],
            countryWikiLabel=row["countryLabel"],
            cityWikiID=row["city"],
            cityGeoName=row["cityGeoName"],
            cityPoint=row["cityPoint"],
            cityWikiLabel=row["cityLabel"],
            localityWikiID=row["locality"],
            localityWikiLabel=row["localityLabel"],
        )


WIKIDATA_INSERT_STATEMENT = f"""
REPLACE INTO wikidata (archive, {SPARQLResults().names})
VALUES (%s, {SPARQLResults().place_holders})
"""
GEONAMES_INSERT_STATEMENT = """
REPLACE INTO geonames_cities (id)
VALUES (%s)
"""


def call_wikidata(query: str, archive: bool = True):
    with Progress(
        TextColumn("{task.description}"), SpinnerColumn(), TimeElapsedColumn()
    ) as p:
        t = "Libraries"
        if archive:
            t = "Archives"
        p.add_task(f"Requesting {t}...")
        response = requests.get(URL, params={"format": "json", "query": query})
        if not response.status_code == 200:
            raise RequestException(response=response)
        else:
            return response.json()["results"]["bindings"]


def get_wikidata(results: dict):
    for result in results:
        binding = {k: v["value"] for k, v in result.items()}
        yield binding


def insert_data(results: list, archive: bool = True):
    total = len(results)

    with DBConnection(username=USER, password=PASS) as db, Progress(
        TextColumn("Inserting..."),
        BarColumn(),
        MofNCompleteColumn(),
        TimeElapsedColumn(),
    ) as p:
        t = p.add_task("", total=total)

        # Insert the GeoNames IDs (foreign key for wikidata table)
        for binding in get_wikidata(results):
            converted_binding = SPARQLResults.convert_columns(binding)
            try:
                db.commit(
                    operation=GEONAMES_INSERT_STATEMENT,
                    seq_of_params=[converted_binding.cityGeoName],
                    commit_later=True,
                )
            except Exception as e:
                print(converted_binding)
                raise e
        db.connector.commit()

        # With the foreign keys committed, insert wikidata
        for binding in get_wikidata(results):
            converted_binding = SPARQLResults.convert_columns(binding)
            data = [archive]
            data.extend(converted_binding.__list__)
            try:
                db.commit(
                    operation=WIKIDATA_INSERT_STATEMENT,
                    seq_of_params=data,
                    commit_later=True,
                )
            except Exception as e:
                print(data)
                raise e
            p.advance(t)
        db.connector.commit()


def main():
    with DBConnection(username=USER, password=PASS) as db:
        db.commit("drop table if exists wikidata")
        db.commit("drop table if exists geonames_cities")

        db.commit(GEONAMES_CREATE_STATEMENT)
        db.commit(WIKIDATA_CREATE_STATEMENT)

    archives = call_wikidata(QUERY_ARCHIVE)
    insert_data(archives)


if __name__ == "__main__":
    main()
