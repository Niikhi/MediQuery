from neo4j import GraphDatabase
from typing import Dict, List, Set
import csv
from tqdm import tqdm
import time

class MedicalKnowledgeGraphBuilder:
    def __init__(self, uri: str, user: str, password: str):
        self.driver = GraphDatabase.driver(uri, auth=(user, password))
        self.stats = {'nodes': {}, 'relationships': {}}
        self.batch_size = 1000

    def close(self):
        self.driver.close()

    def clear_database(self):
        """Clear all data from database"""
        print("Clearing existing database...")
        with self.driver.session() as session:
            session.run("MATCH (n) DETACH DELETE n")
        print("Database cleared successfully")

    def create_advanced_schema(self):
        """Create advanced Neo4j schema with constraints and indexes"""
        print("Creating advanced schema...")
        with self.driver.session() as session:
            # Node Keys and Constraints
            constraints = [
                """CREATE CONSTRAINT IF NOT EXISTS FOR (p:Phenotype) 
                   REQUIRE p.id IS UNIQUE""",
                """CREATE CONSTRAINT IF NOT EXISTS FOR (d:Disease) 
                   REQUIRE d.id IS UNIQUE""",
                """CREATE CONSTRAINT IF NOT EXISTS FOR (p:Phenotype) 
                   REQUIRE (p.id, p.name) IS NODE KEY"""
            ]
            
            # Full-text indexes for better search
            indexes = [
                """CREATE FULLTEXT INDEX phenotype_search IF NOT EXISTS
                   FOR (p:Phenotype) ON EACH [p.name, p.definition, p.synonyms]""",
                """CREATE FULLTEXT INDEX disease_search IF NOT EXISTS
                   FOR (d:Disease) ON EACH [d.name]""",
                """CREATE INDEX IF NOT EXISTS FOR (p:Phenotype) 
                   ON (p.category)""",
                """CREATE INDEX IF NOT EXISTS FOR (p:Phenotype)
                   ON (p.prevalence)"""
            ]

            for constraint in constraints:
                try:
                    session.run(constraint)
                except Exception as e:
                    print(f"Warning: Constraint creation failed: {e}")

            for index in indexes:
                try:
                    session.run(index)
                except Exception as e:
                    print(f"Warning: Index creation failed: {e}")

    def _determine_category(self, hpo_id: str) -> str:
        """Determine phenotype category based on HPO ID"""
        try:
            id_num = int(hpo_id.split(':')[1]) if ':' in hpo_id else 0
            categories = {
                (0, 1999): 'Morphology',
                (2000, 2999): 'Neurological',
                (3000, 3999): 'Behavioral',
                (4000, 4999): 'Metabolic',
                (5000, 5999): 'Cardiovascular',
                (6000, 6999): 'Respiratory',
                (7000, 7999): 'Digestive',
                (8000, 8999): 'Musculoskeletal',
                (9000, 9999): 'Immunological',
                (10000, 10999): 'Growth',
                (11000, 11999): 'Cellular',
                (12000, 12999): 'Constitutional'
            }
            
            for (start, end), category in categories.items():
                if start <= id_num <= end:
                    return category
            return 'Other'
        except Exception:
            return 'Other'

    def count_file_lines(self, file_path: str) -> int:
        """Count lines in a file for progress monitoring"""
        try:
            with open(file_path, 'r', encoding='utf-8') as f:
                return sum(1 for _ in f)
        except Exception as e:
            print(f"Error counting lines in file: {e}")
            return 0

    def process_obo_file(self, file_path: str):
        """Process HPO terms from .obo file with enhanced properties"""
        print("\nProcessing HPO terms (phenotypes)...")
        terms_processed = 0
        relationships_created = 0
        
        try:
            with self.driver.session() as session:
                current_term = {}
                total_lines = self.count_file_lines(file_path)
                batch = []
                
                with tqdm(total=total_lines, desc="Processing HPO terms") as pbar:
                    with open(file_path, 'r', encoding='utf-8') as file:
                        for line in file:
                            pbar.update(1)
                            line = line.strip()
                            
                            if line == '[Term]':
                                if current_term and 'id' in current_term:
                                    category = self._determine_category(current_term.get('id', ''))
                                    
                                    # Add node to batch
                                    batch.append({
                                        'id': current_term.get('id', ''),
                                        'name': current_term.get('name', ''),
                                        'definition': current_term.get('def', '').split('"')[1] if 'def' in current_term else '',
                                        'synonyms': current_term.get('synonym', []),
                                        'category': category,
                                        'comment': current_term.get('comment', ''),
                                        'xrefs': current_term.get('xref', [])
                                    })
                                    terms_processed += 1
                                    
                                    # Process IS_A relationships
                                    if 'is_a' in current_term:
                                        for parent in current_term['is_a']:
                                            parent_id = parent.split(' ')[0]
                                            batch.append({
                                                'child_id': current_term['id'],
                                                'parent_id': parent_id,
                                                'type': 'relationship'
                                            })
                                            relationships_created += 1
                                
                                if len(batch) >= self.batch_size:
                                    self._process_batch(session, batch)
                                    batch = []
                                
                                current_term = {}
                            elif ':' in line:
                                key, value = line.split(':', 1)
                                key = key.strip()
                                value = value.strip()
                                if key in current_term:
                                    if isinstance(current_term[key], list):
                                        current_term[key].append(value)
                                    else:
                                        current_term[key] = [current_term[key], value]
                                else:
                                    current_term[key] = value
                
                # Process remaining batch
                if batch:
                    self._process_batch(session, batch)
            
            print(f"\nProcessed {terms_processed} phenotype terms")
            print(f"Created {relationships_created} IS_A relationships")
            self.stats['nodes']['Phenotype'] = terms_processed
            self.stats['relationships']['IS_A'] = relationships_created
            
        except Exception as e:
            print(f"Error processing OBO file: {e}")
            raise

    def _process_batch(self, session, batch):
        """Process a batch of nodes and relationships"""
        try:
            # Split batch into nodes and relationships
            nodes = [item for item in batch if 'type' not in item]
            relationships = [item for item in batch if item.get('type') == 'relationship']
            
            if nodes:
                session.run("""
                    UNWIND $nodes AS node
                    MERGE (p:Phenotype {id: node.id})
                    SET p.name = node.name,
                        p.definition = node.definition,
                        p.synonyms = node.synonyms,
                        p.category = node.category,
                        p.comment = node.comment,
                        p.xrefs = node.xrefs,
                        p.created_at = datetime(),
                        p.modified_at = datetime()
                """, {'nodes': nodes})
            
            if relationships:
                session.run("""
                    UNWIND $rels AS rel
                    MATCH (child:Phenotype {id: rel.child_id})
                    MATCH (parent:Phenotype {id: rel.parent_id})
                    MERGE (child)-[r:IS_A]->(parent)
                    SET r.confidence_score = 1.0,
                        r.relationship_type = 'direct',
                        r.created_at = datetime()
                """, {'rels': relationships})
        
        except Exception as e:
            print(f"Error processing batch: {e}")
            raise

    def process_phenotype_annotations(self, file_path: str):
        """Process disease-phenotype associations with batch processing"""
        print("\nProcessing disease-phenotype associations...")
        diseases_processed = 0
        relationships_created = 0
        
        try:
            with self.driver.session() as session:
                total_lines = self.count_file_lines(file_path)
                batch = []
                
                with tqdm(total=total_lines, desc="Processing disease annotations") as pbar:
                    with open(file_path, 'r', encoding='utf-8') as file:
                        next(file)  # Skip header
                        for line in file:
                            pbar.update(1)
                            if line.startswith('#'):
                                continue
                            
                            parts = line.strip().split('\t')
                            if len(parts) >= 4:
                                annotation = {
                                    'disease_id': parts[0],
                                    'disease_name': parts[1],
                                    'phenotype_id': parts[3],
                                    'evidence': parts[5] if len(parts) > 5 else '',
                                    'frequency': parts[8] if len(parts) > 8 else '',
                                    'onset': parts[9] if len(parts) > 9 else ''
                                }
                                batch.append(annotation)
                                diseases_processed += 1
                                relationships_created += 1

                                if len(batch) >= self.batch_size:
                                    self._process_annotation_batch(session, batch)
                                    batch = []

                    # Process remaining batch
                    if batch:
                        self._process_annotation_batch(session, batch)

            print(f"\nProcessed {diseases_processed} disease-phenotype associations")
            self.stats['nodes']['Disease'] = diseases_processed
            self.stats['relationships']['HAS_PHENOTYPE'] = relationships_created
            
        except Exception as e:
            print(f"Error processing phenotype annotations: {e}")
            raise


    def _process_annotation_batch(self, session, batch):
        """Process a batch of disease-phenotype annotations"""
        try:
            session.run("""
                UNWIND $annotations AS annotation
                MERGE (d:Disease {id: annotation.disease_id})
                SET d.name = annotation.disease_name,
                    d.modified_at = datetime()
                WITH d, annotation
                MATCH (p:Phenotype {id: annotation.phenotype_id})
                MERGE (d)-[r:HAS_PHENOTYPE]->(p)
                SET r.evidence = annotation.evidence,
                    r.frequency = annotation.frequency,
                    r.onset = annotation.onset,
                    r.created_at = datetime()
            """, {'annotations': batch})
        except Exception as e:
            print(f"Error processing annotation batch: {e}")
            raise


    def create_symptom_relationships(self):
        """Create ASSOCIATED_WITH relationships between commonly co-occurring symptoms in batches"""
        print("\nCreating symptom associations...")
        try:
            with self.driver.session() as session:
                # First, get all phenotypes
                print("Getting phenotypes...")
                phenotypes = session.run("""
                    MATCH (p:Phenotype)
                    RETURN p.id as id
                    ORDER BY p.id
                """).values()
                
                total = len(phenotypes)
                batch_size = 100  # Process 100 phenotypes at a time
                relationships_created = 0
                
                print(f"Processing {total} phenotypes in batches of {batch_size}...")
                with tqdm(total=total) as pbar:
                    for i in range(0, total, batch_size):
                        batch = phenotypes[i:i + batch_size]
                        phenotype_ids = [p[0] for p in batch]
                        
                        # Process each batch
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
                        
                        # Update progress
                        relationships_created += result.single()['created']
                        pbar.update(len(batch))
                        pbar.set_description(f"Created {relationships_created} relationships")
                
            print(f"\nTotal symptom associations created: {relationships_created}")
            
        except Exception as e:
            print(f"Error creating symptom relationships: {e}")
            raise

    def create_symptom_clusters(self):
        """Create symptom clusters based on co-occurrence patterns"""
        print("\nCreating symptom clusters...")
        try:
            with self.driver.session() as session:
                # Label clusters based on category and connectivity
                session.run("""
                    MATCH (p:Phenotype)
                    WITH p.category as category, COLLECT(p) as symptoms
                    WITH category, symptoms, SIZE(symptoms) as cluster_size
                    UNWIND symptoms as symptom
                    SET symptom.cluster_id = category,
                        symptom.cluster_size = cluster_size
                """)
                
                # Add cross-cluster relationship strength
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
        """Apply graph algorithms for enhanced analytics"""
        print("\nApplying graph algorithms...")
        try:
            with self.driver.session() as session:
                # Calculate degree centrality
                session.run("""
                    MATCH (p:Phenotype)
                    WITH p, COUNT((p)<-[:HAS_PHENOTYPE]-()) as degree
                    SET p.degree_centrality = degree
                """)

                # Calculate betweenness centrality approximation
                session.run("""
                    MATCH (p:Phenotype)
                    OPTIONAL MATCH path=(p)-[:ASSOCIATED_WITH*..3]-(other:Phenotype)
                    WITH p, COUNT(DISTINCT other) as reach
                    SET p.betweenness_score = reach
                """)

                # Identify hub symptoms
                session.run("""
                    MATCH (p:Phenotype)-[r:ASSOCIATED_WITH]-()
                    WITH p, COUNT(r) as connections
                    SET p.is_hub = CASE WHEN connections > 10 THEN true ELSE false END
                """)

                # Calculate clustering coefficients
                session.run("""
                    MATCH (p:Phenotype)-[:ASSOCIATED_WITH]-(neighbor)
                    WITH p, COLLECT(neighbor) as neighbors, COUNT(neighbor) as degree
                    MATCH (n1:Phenotype)-[:ASSOCIATED_WITH]-(n2:Phenotype)
                    WHERE n1 IN neighbors AND n2 IN neighbors AND n1 <> n2
                    WITH p, degree, COUNT(*) as triangles
                    SET p.cluster_coefficient = CASE
                        WHEN degree <= 1 THEN 0
                        ELSE (2.0 * triangles) / (degree * (degree - 1))
                    END
                """)
            print("Graph algorithms applied")
        except Exception as e:
            print(f"Error applying graph algorithms: {e}")
            raise

    def add_phenotype_metrics(self):
        """Add additional metrics to phenotype nodes"""
        print("\nAdding phenotype metrics...")
        try:
            with self.driver.session() as session:
                session.run("""
                    MATCH (p:Phenotype)
                    SET p.prevalence = size((p)<-[:HAS_PHENOTYPE]-()),
                        p.connectivity = size((p)-[:ASSOCIATED_WITH]-()),
                        p.specificity = 1.0 / size((p)<-[:HAS_PHENOTYPE]-()),
                        p.hierarchical_level = size((p)-[:IS_A*]->()),
                        p.leaf_node = NOT EXISTS((:<-[:IS_A]-()))
                """)
            print("Phenotype metrics added")
        except Exception as e:
            print(f"Error adding phenotype metrics: {e}")
            raise

    def print_database_stats(self):
        """Print comprehensive database statistics"""
        print("\n=== Database Statistics ===")
        try:
            with self.driver.session() as session:
                # Basic node and relationship counts
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
                
                # Clustering statistics
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
                
                # Sample data
                self._print_sample_data(session)
                
        except Exception as e:
            print(f"Error printing database stats: {e}")

    def _print_sample_data(self, session):
        """Print sample nodes from the database"""
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
        uri="neo4j://localhost:7687",
        user="neo4j",
        password="innovitcrota"  
    )
    
    try:
        # Clear existing data and create advanced schema
        builder.clear_database()
        builder.create_advanced_schema()
        
        start_time = time.time()
        
        # Process core files
        builder.process_obo_file("hp.obo")  # Update with your file path
        builder.process_phenotype_annotations("phenotype.hpoa")  # Update with your file path
        
        # Create additional relationships and metrics
        print("\nEnhancing graph structure...")
        builder.create_symptom_relationships()
        builder.create_symptom_clusters()
        
        # Apply graph algorithms and metrics
        builder.apply_graph_algorithms()
        builder.add_phenotype_metrics()
        
        # Print final statistics
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