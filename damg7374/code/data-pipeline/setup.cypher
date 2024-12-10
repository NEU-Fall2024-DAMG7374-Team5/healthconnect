CREATE INDEX patientIndex IF NOT EXISTS FOR (p:Patient) ON (p.Id);

CREATE INDEX ethnicityIndex IF NOT EXISTS FOR (e:Ethnicity) ON (e.NAME);
CREATE INDEX raceIndex IF NOT EXISTS FOR (r:Race) ON (r.NAME);
CREATE INDEX genderIndex IF NOT EXISTS FOR (g:Gender) ON (g.NAME);

CREATE INDEX addressIndex IF NOT EXISTS FOR (a:Address) ON (a.NAME, a.ZIPCODE);
CREATE INDEX cityIndex IF NOT EXISTS FOR (c:City) ON (c.NAME);
CREATE INDEX countyIndex IF NOT EXISTS FOR (co:County) ON (co.NAME);
CREATE INDEX stateIndex IF NOT EXISTS FOR (s:State) ON (s.NAME);


CREATE INDEX providerIndex IF NOT EXISTS FOR (pr:Provider) ON (pr.Id);
CREATE INDEX organizationIndex IF NOT EXISTS FOR (org:Organization) ON (org.Id);
CREATE INDEX payerIndex IF NOT EXISTS FOR (pay:Payer) ON (pay.Id);

CREATE INDEX encounterIndex IF NOT EXISTS FOR (enc:Encounter) ON (enc.Id);
CREATE INDEX claimIndex IF NOT EXISTS FOR (claim:Claim) ON (claim.Id);
CREATE INDEX claimTransactionIndex IF NOT EXISTS FOR (claimTransaction:ClaimTransaction) ON (claimTransaction.Id);

CREATE INDEX snomedctIndex IF NOT EXISTS FOR (snomedct:SNOMEDCT) ON (snomedct.CODE);
CREATE INDEX rxnormIndex IF NOT EXISTS FOR (rxnorm:RXNORM) ON (rxnorm.CODE);
CREATE INDEX cvxIndex IF NOT EXISTS FOR (cvx:CVX) ON (cvx.CODE);
CREATE INDEX loincIndex IF NOT EXISTS FOR (loinc:LOINC) ON (loinc.CODE);
CREATE INDEX dicomdcmIndex IF NOT EXISTS FOR (dicomdcm:DICOMDCM) ON (dicomdcm.CODE);
CREATE INDEX dicomsopIndex IF NOT EXISTS FOR (dicomsop:DICOMSOP) ON (dicomsop.CODE);

CREATE INDEX encounterClassIndex IF NOT EXISTS FOR (encclass:EncounterClass) ON (encclass.NAME);
CREATE INDEX careplanIndex IF NOT EXISTS FOR (crp:Careplan) ON (crp.Id);
CREATE INDEX procedureIndex IF NOT EXISTS FOR (proc:Procedure) ON (proc.PROCEDURE_STARTED_ON, proc.PROCEDURE_STOPPED_ON, proc.ENCOUNTER, proc.CODE);
CREATE INDEX deviceIndex IF NOT EXISTS FOR (dev:Device) ON (dev.DEVICE_USAGE_STARTED_ON, dev.DEVICE_USAGE_STOPPED_ON, dev.ENCOUNTER, dev.CODE, dev.UDI);
CREATE INDEX allergyIndex IF NOT EXISTS FOR (alg:Allergy) ON (alg.ALLERGY_STARTED_ON, alg.ALLERGY_STOPPED_ON, alg.ENCOUNTER, alg.CODE, alg.SYSTEM);
CREATE INDEX medicationIndex IF NOT EXISTS FOR (med:Medication) ON (med.MEDICATION_STARTED_ON, med.MEDICATION_STOPPED_ON, med.PAYER, med.ENCOUNTER, med.CODE);



DROP INDEX patientIndex;

DROP INDEX ethnicityIndex;
DROP INDEX raceIndex;
DROP INDEX genderIndex;

DROP INDEX addressIndex;
DROP INDEX cityIndex;
DROP INDEX countyIndex;
DROP INDEX stateIndex;


DROP INDEX providerIndex;
DROP INDEX organizationIndex;
DROP INDEX payerIndex;

DROP INDEX encounterIndex;
DROP INDEX claimIndex;
DROP INDEX claimTransactionIndex;

DROP INDEX snomedctIndex;
DROP INDEX rxnormIndex;
DROP INDEX cvxIndex;
DROP INDEX loincIndex;
DROP INDEX dicomdcmIndex;
DROP INDEX dicomsopIndex;

DROP INDEX encounterClassIndex;
DROP INDEX careplanIndex;
DROP INDEX procedureIndex;
DROP INDEX deviceIndex;
DROP INDEX allergyIndex;
DROP INDEX medicationIndex;


