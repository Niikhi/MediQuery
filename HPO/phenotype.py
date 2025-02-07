from neo4j import GraphDatabase
from tqdm import tqdm

class PhenotypeProcessor:
    def __init__(self, driver: GraphDatabase, batch_size: int):
        self.driver = driver
        self.batch_size = batch_size
        self.categories = [
            ('HP:0000005', 'HP:0001999', 'Inheritance'),
            ('HP:0000118', 'HP:0001416', 'Morphology'),
            ('HP:0000707', 'HP:0001626', 'Nervous System'),
            ('HP:0001574', 'HP:0001780', 'Integument'),
            ('HP:0000478', 'HP:0000612', 'Eye'),
            ('HP:0000598', 'HP:0000706', 'Ear'), 
            ('HP:0000818', 'HP:0000933', 'Skeletal system'),
            ('HP:0001197', 'HP:0001574', 'Connective Tissue'),
            ('HP:0001507', 'HP:0001573', 'Growth'), 
            ('HP:0001574', 'HP:0001780', 'Constitutional Symptom'),
            ('HP:0001781', 'HP:0002060', 'Limbs'),
            ('HP:0002061', 'HP:0003128', 'Musculature'), 
            ('HP:0000152', 'HP:0000589', 'Head/Neck'),
            ('HP:0001608', 'HP:0001621', 'Voice'),
            ('HP:0001626', 'HP:0001869', 'Cardiovascular'),
            ('HP:0001871', 'HP:0002087', 'Blood'), 
            ('HP:0002086', 'HP:0002250', 'Respiratory'),
            ('HP:0002242', 'HP:0003011', 'Abdomen'), 
            ('HP:0000118', 'HP:0000118', 'Genitourinary system'),
            ('HP:0001939', 'HP:0001939', 'Metabolism/Homeostasis'),
            ('HP:0001871', 'HP:0002597', 'Endocrine'),
            ('HP:0002715', 'HP:0004431', 'Immunology'),
            ('HP:0004325', 'HP:0004332', 'Oncology'), 
            ('HP:0032223', 'HP:0033127', 'Multisystem Disorder')
        ]

    def _determine_category(self, hpo_id: str) -> str:
        """Determine phenotype category based on HPO ID"""
        if not hpo_id:
            return 'Other'
        
        for start, end, category in self.categories:
            if start <= hpo_id <= end:
                return category
        
        return 'Other'

    def count_file_lines(self, file_path: str) -> int:
        try:
            with open(file_path, 'r', encoding='utf-8') as f:
                return sum(1 for _ in f)
        except Exception as e:
            print(f"Error counting lines in file: {e}") 
            return 0

    def process_obo_file(self, file_path: str):
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
                
                if batch:
                    self._process_batch(session, batch)
            
            print(f"\nProcessed {terms_processed} phenotype terms")
            print(f"Created {relationships_created} IS_A relationships")
            
        except Exception as e:
            print(f"Error processing OBO file: {e}")
            raise

    def _process_batch(self, session, batch):
        try:
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