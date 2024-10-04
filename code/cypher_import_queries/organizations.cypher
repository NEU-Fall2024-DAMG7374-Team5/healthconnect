//Create nodes
LOAD CSV WITH HEADERS FROM 'file:///organizations.csv' AS row

MERGE (org:Organization {id: row.Id})

SET org.orgnizationName = COALESCE(row.NAME, org.orgnizationName),
    org.address = COALESCE(row.ADDRESS, org.address),
    org.zip = COALESCE(row.ZIP, org.zip),
    org.lat = COALESCE(toFloat(row.LAT), org.lat),
    org.lon = COALESCE(toFloat(row.LON), org.lon),
    org.phone = COALESCE(row.PHONE, org.phone),
    org.revenue = COALESCE(row.REVENUE, org.revenue),
    org.utilization = COALESCE(toFloat(row.UTILIZATION), org.utilization)

MERGE (c:City {name: COALESCE(row.CITY, 'Unknown')})
MERGE (s:State {name: COALESCE(row.STATE, 'Unknown')});


// Create index
CREATE INDEX organization_index FOR (org:Organization) ON (org.id);

//Create relationships
:auto CALL {
LOAD CSV WITH HEADERS FROM 'file:///organizations.csv' AS row

MATCH (org:Organization {id: row.Id})
MATCH (c:City {name: COALESCE(row.CITY, 'Unknown')})

MERGE (org)-[:OPERATES_IN]->(c)

} IN TRANSACTIONS OF 100 ROWS