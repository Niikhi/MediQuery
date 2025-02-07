from neo4j import Session

def create_constraints(session: Session):
    constraints = [
        "CREATE CONSTRAINT IF NOT EXISTS FOR (p:Phenotype) REQUIRE p.id IS UNIQUE",
        "CREATE CONSTRAINT IF NOT EXISTS FOR (d:Disease) REQUIRE d.id IS UNIQUE",
        "CREATE CONSTRAINT IF NOT EXISTS FOR (p:Phenotype) REQUIRE (p.id, p.name) IS NODE KEY"  
    ]

    for constraint in constraints:
        try:
            session.run(constraint)
        except Exception as e:
            print(f"Warning: Constraint creation failed: {e}")