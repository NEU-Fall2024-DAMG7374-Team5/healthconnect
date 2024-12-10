MATCH (p:Patient)
WHERE p.id IN ['7d5f37d2-57ee-e895-2f1c-f1701e53ac29', '1c98c1a4-bafa-fc4d-deea-915afcd5c856']
RETURN p, 
       [(p)-[:LIVES_IN]->(c:City) | c] AS city, 
       [(c)-[:PART_OF]->(co:County) | co] AS county,
       [(co)-[:PART_OF]->(s:State) | s] AS state,
       [(p)-[:HAS_GENDER]->(g:Gender) | g] AS gender,
       [(p)-[:HAS_RACE]->(r:Race) | r] AS race,
       [(p)-[:HAS_ETHNICITY]->(e:Ethnicity) | e] AS ethnicity