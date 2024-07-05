GEONAMES_CREATE_STATEMENT = """
CREATE TABLE IF NOT EXISTS geonames_cities (
    id VARCHAR(250), 
    toponymName VARCHAR(250),
    asciiName VARCHAR(250),
    lat DECIMAL(10,5),
    lng DECIMAL(10,5),
    adminName1 VARCHAR(250),
    adminName2 VARCHAR(250),
    adminName3 VARCHAR(250),
    countryName VARCHAR(250),
    PRIMARY KEY (id)
)
"""
WIKIDATA_CREATE_STATEMENT = """
CREATE TABLE IF NOT EXISTS wikidata (
    archive BOOL,
    id VARCHAR(250),
    viafID VARCHAR(250),
    label VARCHAR(250),
    point VARCHAR(250),
    continentWikiID VARCHAR(250),
    continentWikiLabel VARCHAR(250),
    countryWikiID VARCHAR(250),
    countryWikiLabel VARCHAR(250),
    cityWikiID VARCHAR(250),
    cityGeoName VARCHAR(250),
    cityPoint VARCHAR(250),
    cityWikiLabel VARCHAR(250),
    localityWikiID VARCHAR(250),
    localityWikiLabel VARCHAR(250),
    PRIMARY KEY (id),
    FOREIGN KEY (cityGeoName) REFERENCES geonames_cities (id)
)
"""
