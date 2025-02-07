from neo4j import GraphDatabase
from tqdm import tqdm

class DiseaseProcessor:
    def __init__(self, driver: GraphDatabase, batch_size: int):
        self.driver = driver
        self.batch_size = batch_size

    def count_file_lines(self, file_path: str) -> int:
        try:
            with open(file_path, 'r', encoding='utf-8') as f:
                return sum(1 for _ in f)
        except Exception as e:
            print(f"Error counting lines in file: {e}") 
            return 0

    def process_phenotype_annotations(self, file_path: str):
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

                    if batch:
                        self._process_annotation_batch(session, batch)

            print(f"\nProcessed {diseases_processed} disease-phenotype associations")
            
        except Exception as e:
            print(f"Error processing phenotype annotations: {e}")
            raise

    def _process_annotation_batch(self, session, batch):
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