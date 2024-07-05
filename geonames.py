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
SELECT id FROM geonames_cities WHERE toponymName IS NULL
"""
INSERT_STATEMENT = f"""
UPDATE geonames_cities
SET
    toponymName = %s, 
    asciiName = %s, 
    lat = %s, 
    lng = %s, 
    adminName1 = %s, 
    adminName2 = %s,
    adminName3 = %s, 
    countryName = %s
WHERE id = %s
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
                r["toponymName"],
                r["asciiName"],
                r["lat"],
                r["lng"],
                r["adminName1"],
                r["adminName2"],
                r["adminName3"],
                r["countryName"],
                id[0],
            )
            data = [i if i != "" else None for i in data]
            try:
                db.commit(
                    operation=INSERT_STATEMENT,
                    seq_of_params=data,
                    commit_later=True,
                )
            except Exception as e:
                print(data)
                print(INSERT_STATEMENT)
                print(INSERT_STATEMENT % data)
                raise e
            p.advance(st)
    db.connector.commit()
