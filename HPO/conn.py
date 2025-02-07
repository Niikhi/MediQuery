import networkx as nx
from collections import defaultdict

class HPOParser:
    def __init__(self):
        self.graph = nx.DiGraph()
        self.terms = {}  # Store term information
        self.disease_symptoms = defaultdict(list)  # Disease to symptoms mapping
        self.symptom_diseases = defaultdict(list)  # Symptom to diseases mapping
        
    def parse_obo(self, file_path):
        """Parse the hp.obo file to extract terms and relationships"""
        current_term = None
        with open(file_path, 'r', encoding='utf-8') as file:
            for line in file:
                line = line.strip()
                if line == '[Term]':
                    if current_term:
                        self.terms[current_term['id']] = current_term
                    current_term = {}
                elif line.startswith('id: '):
                    current_term['id'] = line[4:]
                elif line.startswith('name: '):
                    current_term['name'] = line[6:]
                elif line.startswith('def: '):
                    current_term['definition'] = line[5:].split('"')[1]
                elif line.startswith('is_a: '):
                    if 'is_a' not in current_term:
                        current_term['is_a'] = []
                    current_term['is_a'].append(line[6:].split(' !')[0])
                    
        # Add the last term
        if current_term:
            self.terms[current_term['id']] = current_term
            
        # Build the graph structure
        for term_id, term in self.terms.items():
            self.graph.add_node(term_id, 
                              name=term.get('name', ''),
                              definition=term.get('definition', ''))
            if 'is_a' in term:
                for parent in term['is_a']:
                    self.graph.add_edge(term_id, parent)

    def parse_phenotype_annotations(self, file_path):
        """Parse the phenotype.hpoa file for disease-symptom relationships"""
        with open(file_path, 'r', encoding='utf-8') as file:
            next(file)  # Skip header
            for line in file:
                if line.startswith('#'):
                    continue
                parts = line.strip().split('\t')
                if len(parts) >= 4:
                    disease_id = parts[0]
                    hpo_id = parts[3]
                    
                    # Store bidirectional relationships
                    self.disease_symptoms[disease_id].append(hpo_id)
                    self.symptom_diseases[hpo_id].append(disease_id)
                    
    def get_term_info(self, term_id):
        """Get detailed information about a term"""
        return self.terms.get(term_id, {})
    
    def get_ancestors(self, term_id):
        """Get all ancestors of a term (parent terms)"""
        try:
            return list(nx.ancestors(self.graph, term_id))
        except nx.NetworkXError:
            return []
            
    def get_descendants(self, term_id):
        """Get all descendants of a term (child terms)"""
        try:
            return list(nx.descendants(self.graph, term_id))
        except nx.NetworkXError:
            return []
            
    def get_diseases_for_symptom(self, symptom_id):
        """Get all diseases associated with a symptom"""
        return self.symptom_diseases.get(symptom_id, [])
        
    def get_symptoms_for_disease(self, disease_id):
        """Get all symptoms associated with a disease"""
        return self.disease_symptoms.get(disease_id, [])
        
    def get_related_symptoms(self, symptom_id):
        """Get symptoms that commonly co-occur with the given symptom"""
        related = defaultdict(int)
        diseases = self.get_diseases_for_symptom(symptom_id)
        
        for disease in diseases:
            disease_symptoms = self.get_symptoms_for_disease(disease)
            for symptom in disease_symptoms:
                if symptom != symptom_id:
                    related[symptom] += 1
                    
        return dict(related)

# Example usage
def main():
    parser = HPOParser()
    
    # Parse the files
    parser.parse_obo('hp.obo')
    parser.parse_phenotype_annotations('phenotype.hpoa')
    
    # Example queries
    term_id = 'HP:0002315'  # Example HPO ID
    term_info = parser.get_term_info(term_id)
    print(f"Term info: {term_info}")
    
    ancestors = parser.get_ancestors(term_id)
    print(f"Parent terms: {ancestors}")
    
    diseases = parser.get_diseases_for_symptom(term_id)
    print(f"Associated diseases: {diseases}")
    
    related = parser.get_related_symptoms(term_id)
    print(f"Related symptoms: {related}")

if __name__ == "__main__":
    main()