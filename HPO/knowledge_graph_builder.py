from neo4j import GraphDatabase
from config import Config
from constraints import create_constraints
from indexes import create_indexes
from phenotype import PhenotypeProcessor
from disease import DiseaseProcessor
from graph_enhancer import GraphEnhancer
import time

class MedicalKnowledgeGraphBuilder:
    def __init__(self, uri: str, user: str, password: str):
        self.driver = GraphDatabase.driver(uri, auth=(user, password))

    def close(self):
        self.driver.close()

    def clear_database(self):
        print("Clearing existing database...")
        with self.driver.session() as session:
            session.run("MATCH (n) DETACH DELETE n")
        print("Database cleared successfully")

    def create_schema(self):
        print("Creating schema...")
        with self.driver.session() as session:
            create_constraints(session)
            create_indexes(session)

    def print_database_stats(self):
        print("\n=== Database Statistics ===")
        try:
            with self.driver.session() as session:
                basic_stats = session.run("""
                    CALL apoc.meta.stats()
                    YIELD nodeCount, relCount, labels, relTypes
                    RETURN nodeCount, relCount, labels, relTypes
                """).single()
                
                print("\nNode Counts:")
                for label, count in basic_stats['labels'].items():
                    print(f"{label}: {count:,}")
                
                print("\nRelationship Counts:")
                for rel_type, count in basic_stats['relTypes'].items():
                    print(f"{rel_type}: {count:,}")
                
                clustering = session.run("""
                    MATCH (p:Phenotype)
                    RETURN
                        COUNT(p) as total_symptoms,
                        SUM(CASE WHEN p.is_hub THEN 1 ELSE 0 END) as hub_symptoms,
                        AVG(p.cluster_coefficient) as avg_clustering,
                        AVG(p.degree_centrality) as avg_centrality
                """).single()
                
                print("\nGraph Analytics:")
                print(f"Total Symptoms: {clustering['total_symptoms']:,}")
                print(f"Hub Symptoms: {clustering['hub_symptoms']:,}")
                print(f"Average Clustering Coefficient: {clustering['avg_clustering']:.3f}")
                print(f"Average Degree Centrality: {clustering['avg_centrality']:.3f}")
                
                self._print_sample_data(session)
                
        except Exception as e:
            print(f"Error printing database stats: {e}")

    def _print_sample_data(self, session):
        try:
            sample_phenotype = session.run("""
                MATCH (p:Phenotype)
                RETURN p.name AS name,
                       p.id AS id,
                       p.category as category,
                       p.degree_centrality as centrality
                LIMIT 1
            """).single()
            
            sample_disease = session.run("""
                MATCH (d:Disease)
                RETURN d.name AS name,
                       d.id AS id,
                       size((d)-[:HAS_PHENOTYPE]->()) as symptom_count
                LIMIT 1
            """).single()
            
            print("\nSample Data:")
            if sample_phenotype:
                print(f"\nSample Phenotype:")
                print(f"- Name: {sample_phenotype['name']}")
                print(f"- ID: {sample_phenotype['id']}")
                print(f"- Category: {sample_phenotype['category']}")
                print(f"- Centrality: {sample_phenotype['centrality']}")
            
            if sample_disease:
                print(f"\nSample Disease:")
                print(f"- Name: {sample_disease['name']}")
                print(f"- ID: {sample_disease['id']}")
                print(f"- Associated Symptoms: {sample_disease['symptom_count']}")
        except Exception as e:
            print(f"Error printing sample data: {e}")

def main():
    builder = MedicalKnowledgeGraphBuilder(
        uri=Config.NEO4J_URI,
        user=Config.NEO4J_USER,
        password=Config.NEO4J_PASSWORD
    )
    
    try:
        builder.clear_database()
        builder.create_schema()
        
        start_time = time.time()
        
        phenotype_processor = PhenotypeProcessor(builder.driver, Config.BATCH_SIZE)
        phenotype_processor.process_obo_file("hp.obo")
        
        disease_processor = DiseaseProcessor(builder.driver, Config.BATCH_SIZE)
        disease_processor.process_phenotype_annotations("phenotype.hpoa")
        
        print("\nEnhancing graph structure...")
        graph_enhancer = GraphEnhancer(builder.driver)
        graph_enhancer.create_symptom_relationships()
        graph_enhancer.create_symptom_clusters()
        graph_enhancer.apply_graph_algorithms()
        graph_enhancer.add_phenotype_metrics()
        
        elapsed_time = time.time() - start_time
        print(f"\nTotal processing time: {elapsed_time:.2f} seconds")
        builder.print_database_stats()
        
        print("\nGraph construction completed successfully!")
        
    except Exception as e:
        print(f"Error during graph construction: {str(e)}")
        raise
    finally:
        builder.close()

if __name__ == "__main__":
    main()