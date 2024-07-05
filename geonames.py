import os
import requests
from rich.progress import (
    Progress,
    BarColumn,
    MofNCompleteColumn,
    TimeElapsedColumn,
    TextColumn,
)

from database import DBConnection

USER = os.environ["MYSQL_USER"]
PASS = os.environ["MYSQL_PASS"]
GEO = os.environ["GEONAMES"]
ID_COL = "cityGeoName"

SELECT_STATEMENT = f"""
SELECT DISTINCT w.{ID_COL}
FROM wikidata w
LEFT JOIN geonames_cities gc ON gc.id = w.{ID_COL}
WHERE gc.id IS NULL
"""
INSERT_STATEMENT = f"""
REPLACE INTO geonames_cities(id, toponymName, asciiName, lat, lng, adminName1, adminName2, adminName3, countryName)
VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s)
"""


def request_geonames(geonameID) -> dict | None | RuntimeWarning:
    url = "http://api.geonames.org/getJSON?formatted=true&geonameId={}&username={}&style=full".format(
        geonameID, GEO
    )
    try:
        r = requests.get(url)
    except Exception as e:
        raise e
    d = r.json()

    # Check the validity of the response
    message = None
    if d.get("status") and d["status"].get("message"):
        message = d["status"]["message"]
    if r.status_code != 200:
        if message and message == "the geoname feature does not exist.":
            print("Bad GeoNames ID", geonameID)
            return None
        else:
            raise RuntimeError(r.status_code)
    elif message and message.startswith("the hourly limit"):
        print(message)
        return message
    else:
        return d


with DBConnection(username=USER, password=PASS) as db, Progress(
    TextColumn("{task.description}"),
    BarColumn(),
    MofNCompleteColumn(),
    TimeElapsedColumn(),
) as p:
    ids = db.select(operation=SELECT_STATEMENT)
    gt = p.add_task("Calling GeoNames...", total=len(ids))
    st = p.add_task("Inserting Data...", total=len(ids))
    for id in ids:
        r = request_geonames(geonameID=id[0])
        if isinstance(r, str):
            print(r)
            break
        elif r:
            p.advance(gt)
            data = (
                id[0],
                r["toponymName"],
                r["asciiName"],
                r["lat"],
                r["lng"],
                r["adminName1"],
                r["adminName2"],
                r["adminName3"],
                r["countryName"],
            )
            try:
                db.commit(
                    operation=INSERT_STATEMENT,
                    seq_of_params=data,
                    commit_later=True,
                )
            except Exception as e:
                print(data)
                print(INSERT_STATEMENT)
                raise e
            p.advance(st)
    db.connector.commit()
