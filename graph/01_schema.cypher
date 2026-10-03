// =====================================================================
// AML-Guard graph schema + ontology  (idempotent - safe to re-run)
// =====================================================================

// ---- Constraints (also create the backing indexes) ----
CREATE CONSTRAINT customer_id     IF NOT EXISTS FOR (n:Customer)      REQUIRE n.customer_id   IS UNIQUE;
CREATE CONSTRAINT account_id      IF NOT EXISTS FOR (n:Account)       REQUIRE n.account_id    IS UNIQUE;
CREATE CONSTRAINT txn_id          IF NOT EXISTS FOR (n:Transaction)   REQUIRE n.txn_id        IS UNIQUE;
CREATE CONSTRAINT device_id       IF NOT EXISTS FOR (n:Device)        REQUIRE n.device_id     IS UNIQUE;
CREATE CONSTRAINT address_id      IF NOT EXISTS FOR (n:Address)       REQUIRE n.address_id    IS UNIQUE;
CREATE CONSTRAINT jurisdiction    IF NOT EXISTS FOR (n:Jurisdiction)  REQUIRE n.code          IS UNIQUE;
CREATE CONSTRAINT typology_id     IF NOT EXISTS FOR (n:Typology)      REQUIRE n.id            IS UNIQUE;
CREATE CONSTRAINT indicator_id    IF NOT EXISTS FOR (n:RiskIndicator) REQUIRE n.id            IS UNIQUE;
CREATE CONSTRAINT ontology_class  IF NOT EXISTS FOR (n:OntologyClass) REQUIRE n.name          IS UNIQUE;

// ---- Indexes for hot-path lookups ----
CREATE INDEX txn_ts IF NOT EXISTS FOR (t:Transaction) ON (t.ts);

// ---- Ontology: class hierarchy (what kinds of things exist) ----
// ShellCompany is an INFERRED class: it is never asserted in the data; agents derive it
// from graph evidence (shared address, high-risk jurisdiction, pass-through behaviour).
UNWIND [
  {name:'Party',        parent:null},
  {name:'Person',       parent:'Party'},
  {name:'Organization', parent:'Party'},
  {name:'ShellCompany', parent:'Organization'},
  {name:'Account',      parent:null},
  {name:'Transaction',  parent:null},
  {name:'Device',       parent:null},
  {name:'Address',      parent:null},
  {name:'Jurisdiction', parent:null}
] AS c
MERGE (k:OntologyClass {name:c.name})
WITH k, c WHERE c.parent IS NOT NULL
MERGE (p:OntologyClass {name:c.parent})
MERGE (k)-[:SUBCLASS_OF]->(p);

// ---- Ontology: laundering typologies and the risk indicators that evidence them ----
UNWIND [
  {id:'STRUCTURING', name:'Structuring',
   description:'Repeated transfers kept just below a reporting threshold to avoid detection.',
   weight:0.70, indicators:['JUST_BELOW_THRESHOLD','HIGH_VELOCITY']},
  {id:'FAN_IN', name:'Smurfing / Fan-in',
   description:'Many unrelated accounts funnel small amounts into one collector account.',
   weight:0.75, indicators:['MANY_TO_ONE','SHARED_DEVICE','HIGH_VELOCITY']},
  {id:'ROUND_TRIP', name:'Round-tripping',
   description:'Funds travel through a ring of accounts and return to the origin.',
   weight:0.85, indicators:['CYCLE','LAYERED_HOPS']},
  {id:'RAPID_PASS_THROUGH', name:'Rapid pass-through',
   description:'Funds received and forwarded within seconds, minus a small fee.',
   weight:0.80, indicators:['RAPID_PASS_THROUGH','HIGH_RISK_JURISDICTION']},
  {id:'SHELL_LAYERING', name:'Shell-company layering',
   description:'Multi-hop chain of shell entities ending in a high-risk jurisdiction.',
   weight:0.90, indicators:['LAYERED_HOPS','SHARED_ADDRESS','HIGH_RISK_JURISDICTION']}
] AS t
MERGE (ty:Typology {id:t.id})
  SET ty.name = t.name, ty.description = t.description, ty.base_weight = t.weight
WITH ty, t
UNWIND t.indicators AS ind
MERGE (i:RiskIndicator {id:ind})
MERGE (ty)-[:INDICATED_BY]->(i);
