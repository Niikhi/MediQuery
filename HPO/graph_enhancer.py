# File Name: `graph_enhancer.py`
from neo4j import GraphDatabase
from tqdm import tqdm

class GraphEnhancer:
    def __init__(self, driver: GraphDatabase):
        self.driver = driver

    def create_symptom_relationships(self):
        print("\nCreating symptom associations...")
        try:
            with self.driver.session() as session:
                print("Getting phenotypes...")
                phenotypes = session.run("""
                    MATCH (p:Phenotype)
                    RETURN p.id as id
                    ORDER BY p.id
                """).values()
                
                total = len(phenotypes)
                batch_size = 100
                relationships_created = 0
                
                print(f"Processing {total} phenotypes in batches of {batch_size}...")
                with tqdm(total=total) as pbar:
                    for i in range(0, total, batch_size):
                        batch = phenotypes[i:i + batch_size]
                        phenotype_ids = [p[0] for p in batch]
                        
                        result = session.run("""
                            UNWIND $phenotype_ids as pid1
                            MATCH (p1:Phenotype {id: pid1})
                            MATCH (p1)<-[:HAS_PHENOTYPE]-(d:Disease)-[:HAS_PHENOTYPE]->(p2:Phenotype)
                            WHERE p1 <> p2
                            WITH p1, p2, COUNT(d) as co_occurrences
                            WHERE co_occurrences >= 5
                            WITH p1, p2, co_occurrences
                            MATCH (p1)<-[r1:HAS_PHENOTYPE]-()
                            WITH p1, p2, co_occurrences, COUNT(r1) as total_occurrences
                            MERGE (p1)-[r:ASSOCIATED_WITH]->(p2)
                            SET r.co_occurrence_count = co_occurrences,
                                r.correlation_strength = co_occurrences * 1.0 / total_occurrences,
                                r.created_at = datetime()
                            RETURN count(r) as created
                        """, {'phenotype_ids': phenotype_ids})
                        
                        relationships_created += result.single()['created']
                        pbar.update(len(batch))
                        pbar.set_description(f"Created {relationships_created} relationships")
                
            print(f"\nTotal symptom associations created: {relationships_created}")
            
        except Exception as e:
            print(f"Error creating symptom relationships: {e}")
            raise

    def create_symptom_clusters(self):
        print("\nCreating symptom clusters...")
        try:
            with self.driver.session() as session:
                session.run("""
                    MATCH (p:Phenotype)
                    WITH p.category as category, COLLECT(p) as symptoms
                    WITH category, symptoms, SIZE(symptoms) as cluster_size
                    UNWIND symptoms as symptom
                    SET symptom.cluster_id = category,
                        symptom.cluster_size = cluster_size
                """)
                
                session.run("""
                    MATCH (p1:Phenotype)-[r:ASSOCIATED_WITH]->(p2:Phenotype)
                    WHERE p1.cluster_id <> p2.cluster_id
                    SET r.cross_cluster = true,
                        r.cluster_connection_strength = r.co_occurrence_count * 1.0 /
                            (p1.cluster_size + p2.cluster_size)
                """)
            print("Symptom clusters created")
        except Exception as e:
            print(f"Error creating symptom clusters: {e}")
            raise

    def apply_graph_algorithms(self):
        print("\nApplying graph algorithms...")
        try:
            # Configure longer timeout for these operations
            with self.driver.session(default_access_mode="WRITE") as session:
                print("Calculating degree centrality...")
                session.run("""
                    CALL apoc.periodic.iterate(
                        'MATCH (p:Phenotype) RETURN p',
                        'MATCH (p)<-[r:HAS_PHENOTYPE]-() 
                        WITH p, COUNT(r) as degree 
                        SET p.degree_centrality = degree',
                        {batchSize: 1000, parallel: false}
                    )
                """)

                print("Calculating betweenness score...")
                session.run("""
                    CALL apoc.periodic.iterate(
                        'MATCH (p:Phenotype) RETURN p',
                        'OPTIONAL MATCH path=(p)-[:ASSOCIATED_WITH*..3]-(other:Phenotype)
                        WITH p, COUNT(DISTINCT other) as reach
                        SET p.betweenness_score = reach',
                        {batchSize: 1000, parallel: false}
                    )
                """)

                print("Identifying hub nodes...")
                session.run("""
                    CALL apoc.periodic.iterate(
                        'MATCH (p:Phenotype) RETURN p',
                        'MATCH (p)-[r:ASSOCIATED_WITH]-()
                        WITH p, COUNT(r) as connections
                        SET p.is_hub = CASE WHEN connections > 10 THEN true ELSE false END',
                        {batchSize: 1000, parallel: false}
                    )
                """)

                print("Calculating clustering coefficients...")
                session.run("""
                    CALL apoc.periodic.iterate(
                        'MATCH (p:Phenotype) RETURN p',
                        'MATCH (p)-[:ASSOCIATED_WITH]-(neighbor)
                        WITH p, COLLECT(neighbor) as neighbors, COUNT(neighbor) as degree
                        MATCH (n1:Phenotype)-[:ASSOCIATED_WITH]-(n2:Phenotype)
                        WHERE n1 IN neighbors AND n2 IN neighbors AND n1 <> n2
                        WITH p, degree, COUNT(*) as triangles
                        SET p.cluster_coefficient = CASE
                            WHEN degree <= 1 THEN 0
                            ELSE (2.0 * triangles) / (degree * (degree - 1))
                        END',
                        {batchSize: 500, parallel: false}
                    )
                """)
                
            print("Graph algorithms applied successfully")
        except Exception as e:
            print(f"Error applying graph algorithms: {e}")
            raise

def add_phenotype_metrics(self):
    print("\nAdding phenotype metrics...")
    try:
        with self.driver.session(default_access_mode="WRITE") as session:
            print("Calculating phenotype metrics...")
            session.run("""
                CALL apoc.periodic.iterate(
                    'MATCH (p:Phenotype) RETURN p',
                    'MATCH (p)<-[hp:HAS_PHENOTYPE]-()
                     WITH p, COUNT(DISTINCT hp) as prevalence
                     MATCH (p)-[aw:ASSOCIATED_WITH]-()
                     WITH p, prevalence, COUNT(DISTINCT aw) as connectivity
                     OPTIONAL MATCH (p)-[ia:IS_A*]->()
                     WITH p, prevalence, connectivity, COUNT(DISTINCT ia) as hierarchical_level
                     SET 
                         p.prevalence = prevalence,
                         p.connectivity = connectivity,
                         p.specificity = CASE WHEN prevalence > 0 THEN 1.0 / prevalence ELSE 0 END,
                         p.hierarchical_level = hierarchical_level,
                         p.leaf_node = NOT EXISTS((p)<-[:IS_A]-())',
                    {batchSize: 1000, parallel: false}
                )
            """)
        print("Phenotype metrics added successfully")
    except Exception as e:
        print(f"Error adding phenotype metrics: {e}")
        raise