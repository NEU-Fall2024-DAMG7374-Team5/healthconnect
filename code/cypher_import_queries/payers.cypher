// Create nodes
LOAD CSV WITH HEADERS FROM 'file:///payers.csv' AS row

MERGE (pay:Payer {id: row.Id})

SET pay.payerName = COALESCE(row.NAME, pay.payerName),
    pay.address = COALESCE(row.ADDRESS, pay.address),
    pay.zip = COALESCE(row.ZIP, pay.zip),
    pay.phone = COALESCE(row.PHONE, pay.phone),
    pay.amountCovered = COALESCE(toFloat(row.AMOUNT_COVERED), pay.amountCovered),
    pay.amountUncovered = COALESCE(toFloat(row.AMOUNT_UNCOVERED), pay.amountUncovered),
    pay.revenue = COALESCE(toFloat(row.REVENUE), pay.revenue),
    pay.coveredEncounters = COALESCE(toInteger(row.COVERED_ENCOUNTERS), pay.coveredEncounters),
    pay.uncoveredEncounters = COALESCE(toInteger(row.UNCOVERED_ENCOUNTERS), pay.uncoveredEncounters),
    pay.coveredMedications = COALESCE(toInteger(row.COVERED_MEDICATIONS), pay.coveredMedications),
    pay.uncoveredMedications = COALESCE(toInteger(row.UNCOVERED_MEDICATIONS), pay.uncoveredMedications),
    pay.coveredProcedures = COALESCE(toInteger(row.COVERED_PROCEDURES), pay.coveredProcedures),
    pay.uncoveredProcedures = COALESCE(toInteger(row.UNCOVERED_PROCEDURES), pay.uncoveredProcedures),
    pay.coveredImmunizations = COALESCE(toInteger(row.COVERED_IMMUNIZATIONS), pay.coveredImmunizations),
    pay.uncoveredImmunizations = COALESCE(toInteger(row.UNCOVERED_IMMUNIZATIONS), pay.uncoveredImmunizations),
    pay.uniqueCustomers = COALESCE(toInteger(row.UNIQUE_CUSTOMERS), pay.uniqueCustomers),
    pay.qolsAvg = COALESCE(toFloat(row.QOLS_AVG), pay.qolsAvg),
    pay.memberMonths = COALESCE(toInteger(row.MEMBER_MONTHS), pay.memberMonths)

MERGE (c:City {name: COALESCE(row.CITY, 'Unknown')})
MERGE (s:State {name: COALESCE(row.STATE_HEADQUARTERED, 'Unknown')})
MERGE (o:Ownership {type: COALESCE(row.OWNERSHIP, 'Unknown')});

// Create index
CREATE INDEX payer_index FOR (pay:Payer) ON (pay.id);

// Create relationships
:auto CALL {
LOAD CSV WITH HEADERS FROM 'file:///payers.csv' AS row

MATCH (pay:Payer {id: row.Id})
MATCH (c:City {name: COALESCE(row.CITY, 'Unknown')})
MATCH (s:State {name: COALESCE(row.STATE_HEADQUARTERED, 'Unknown')})
MATCH (o:Ownership {type: COALESCE(row.OWNERSHIP, 'Unknown')})

MERGE (pay)-[:HEADQUARTERED_IN]->(s)
MERGE (pay)-[:LOCATED_IN]->(c)
MERGE (pay)-[:OWNERSHIP_TYPE]->(o)

} IN TRANSACTIONS OF 100 ROWS