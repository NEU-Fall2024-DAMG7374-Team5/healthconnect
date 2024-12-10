//Create nodes
LOAD CSV WITH HEADERS FROM 'file:///patients.csv' AS row

MERGE (p:Patient {id: row.Id})

SET p.birthdate = COALESCE(date(row.BIRTHDATE), p.birthdate),
    p.deathdate = COALESCE(row.DEATHDATE, p.deathdate),
    p.ssn = COALESCE(row.SSN, p.ssn),
    p.drivers = COALESCE(row.DRIVERS, p.drivers),
    p.passport = COALESCE(row.PASSPORT, p.passport),
    p.prefix = COALESCE(row.PREFIX, p.prefix),
    p.firstName = COALESCE(row.FIRST, p.firstName),
    p.middleName = COALESCE(row.MIDDLE, p.middleName),
    p.lastName = COALESCE(row.LAST, p.lastName),
    p.suffix = COALESCE(row.SUFFIX, p.suffix),
    p.maidenName = COALESCE(row.MAIDEN, p.maidenName),
    p.maritalStatus = COALESCE(row.MARITAL, p.maritalStatus),
    p.birthplace = COALESCE(row.BIRTHPLACE, p.birthplace),
    p.address = COALESCE(row.ADDRESS, p.address),
    p.zip = COALESCE(row.ZIP, p.zip),
    p.lat = COALESCE(toFloat(row.LAT), p.lat),
    p.lon = COALESCE(toFloat(row.LON), p.lon),
    p.healthcareExpenses = COALESCE(toFloat(row.HEALTHCARE_EXPENSES), p.healthcareExpenses),
    p.healthcareCoverage = COALESCE(toFloat(row.HEALTHCARE_COVERAGE), p.healthcareCoverage),
    p.income = COALESCE(toFloat(row.INCOME), p.income)

MERGE (c:City {name: COALESCE(row.CITY, 'Unknown')})
MERGE (co:County {name: COALESCE(row.COUNTY, 'Unknown')})
MERGE (s:State {name: COALESCE(row.STATE, 'Unknown')})
MERGE (g:Gender {type: COALESCE(row.GENDER, 'Unknown')})
MERGE (r:Race {type: COALESCE(row.RACE, 'Unknown')})
MERGE (e:Ethnicity {type: COALESCE(row.ETHNICITY, 'Unknown')});



// Create index
CREATE INDEX patient_index FOR (p:Patient) ON (p.id);
CREATE INDEX city_index FOR (c:City) ON (c.name);
CREATE INDEX county_index FOR (co:County) ON (co.name);
CREATE INDEX state_index FOR (st:State) ON (st.name);
CREATE INDEX gender_index FOR (g:Gender) ON (g.type);
CREATE INDEX race_index FOR (r:Race) ON (r.type);
CREATE INDEX ethnicity_index FOR (e:Ethnicity) ON (e.type);



//Create relationships
:auto CALL {
LOAD CSV WITH HEADERS FROM 'file:///patients.csv' AS row

MATCH (p:Patient {id: row.Id})
MATCH (c:City {name: COALESCE(row.CITY, 'Unknown')})
MATCH (co:County {name: COALESCE(row.COUNTY, 'Unknown')})
MATCH (s:State {name: COALESCE(row.STATE, 'Unknown')})
MATCH (g:Gender {type: COALESCE(row.GENDER, 'Unknown')})
MATCH (r:Race {type: COALESCE(row.RACE, 'Unknown')})
MATCH (e:Ethnicity {type: COALESCE(row.ETHNICITY, 'Unknown')})

MERGE (p)-[:LIVES_IN]->(c)
MERGE (c)-[:PART_OF]->(co)
MERGE (co)-[:PART_OF]->(s)
MERGE (p)-[:HAS_GENDER]->(g)
MERGE (p)-[:HAS_RACE]->(r)
MERGE (p)-[:HAS_ETHNICITY]->(e)
} IN TRANSACTIONS OF 100 ROWS