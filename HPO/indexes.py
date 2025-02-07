from neo4j import Session

def create_indexes(session: Session):  
    indexes = [
        "CREATE FULLTEXT INDEX phenotype_search IF NOT EXISTS FOR (p:Phenotype) ON EACH [p.name, p.definition, p.synonyms]",
        "CREATE FULLTEXT INDEX disease_search IF NOT EXISTS FOR (d:Disease) ON EACH [d.name]",
        "CREATE INDEX IF NOT EXISTS FOR (p:Phenotype) ON (p.category)",  
        "CREATE INDEX IF NOT EXISTS FOR (p:Phenotype) ON (p.prevalence)"
    ]

    for index in indexes:
        try:  
            session.run(index)
        except Exception as e:
            print(f"Warning: Index creation failed: {e}")