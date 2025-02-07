import requests
from bs4 import BeautifulSoup
import json
import time
from typing import Dict, List, Optional

class MayoClinicScraper:
    def __init__(self):
        self.base_url = "https://www.mayoclinic.org"
        self.headers = {
            'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36',
            'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,*/*;q=0.8'
        }

    def _make_request(self, url: str) -> Optional[str]:
        """Make HTTP request with error handling and rate limiting"""
        try:
            print(f"Fetching: {url}")
            response = requests.get(url, headers=self.headers, timeout=10)
            response.raise_for_status()
            time.sleep(2)  # Rate limiting
            return response.text
        except requests.RequestException as e:
            print(f"Error fetching {url}: {str(e)}")
            return None

    def _get_text_until_next_header(self, start_element, stop_tags=['h2', 'h3']) -> List[str]:
        """Extract text content until next header"""
        content = []
        current = start_element.find_next()
        while current and current.name not in stop_tags:
            if current.name == 'p':
                text = current.text.strip()
                if text:
                    content.append(text)
            current = current.find_next_sibling()
        return content

    def get_overview_section(self, main_content) -> str:
        """Extract the Overview section properly from all possible structures"""
        overview_section = main_content.find(['h2', 'h3'], string=lambda x: x and 'Overview' in x)
        
        # If overview found, extract content
        if overview_section:
            print("[INFO] Found 'Overview' section using heading tag")
            return ' '.join(self._get_text_until_next_header(overview_section))

        # Alternative search: check `section` elements
        overview_section = main_content.find('section', {'aria-labelledby': 'overview'})
        if overview_section:
            print("[INFO] Found 'Overview' section using section[aria-labelledby='overview']")
            return ' '.join(p.text.strip() for p in overview_section.find_all('p'))

        # Alternative search: check div elements
        overview_section = main_content.find('div', class_='cmp-text__rich-content')
        if overview_section:
            print("[INFO] Found 'Overview' section using div.cmp-text__rich-content")
            return ' '.join(p.text.strip() for p in overview_section.find_all('p'))

        print("[ERROR] 'Overview' section not found in any structure")
        return ''




    def get_symptoms_section(self, main_content) -> List[str]:
        """Extract Symptoms from both structures"""
        symptoms = []

        # ✅ First, try old structure (h2 + ul list)
        symptoms_section = main_content.select_one("h2:contains('Symptoms'), h3:contains('Symptoms')")
        if symptoms_section:
            current = symptoms_section.find_next('ul')
            if current:
                symptoms.extend([li.text.strip() for li in current.find_all('li')])

        # ✅ If not found, try new structure (aria-labelledby="symptoms")
        if not symptoms:
            symptoms_section = main_content.find('section', {'aria-labelledby': 'symptoms'})
            if symptoms_section:
                text_content = symptoms_section.find('div', class_='cmp-text__rich-content')
                if text_content:
                    symptoms.extend([p.text.strip() for p in text_content.find_all('p')])
                symptoms.extend([li.text.strip() for li in symptoms_section.find_all('li')])

        return symptoms





    def get_causes_section(self, main_content) -> str:
        """Extract the full Causes section as a single description"""
        causes_section = main_content.find(['h2', 'h3'], string=lambda x: x and 'Causes' in x)
        
        if causes_section:
            print("[INFO] Found 'Causes' section")
            return ' '.join(self._get_text_until_next_header(causes_section))

        print("[WARNING] 'Causes' section not found")
        return ''


    def get_risk_factors_section(self, main_content) -> List[str]:
        """Extract risk factors, ensuring we capture the correct information."""
        risk_factors = []
        risk_section = main_content.find(['h2', 'h3'], string=lambda x: x and 'Risk factors' in x)
        
        if risk_section:
            risk_factors.extend(self._get_text_until_next_header(risk_section))

            # Extract bullet points, if available
            ul_element = risk_section.find_next('ul')
            if ul_element:
                risk_factors.extend([li.text.strip() for li in ul_element.find_all('li')])

        # Ensure no symptom-like data is included
        risk_factors = [rf for rf in risk_factors if 'Hearing loss' not in rf and 'Facial numbness' not in rf]

        return risk_factors








    def get_prevention_section(self, main_content) -> List[str]:
        """Extract prevention methods, but gracefully handle missing sections"""
        prevention_methods = []
        prevention_section = main_content.find(['h2', 'h3'], string=lambda x: x and 'Prevention' in x)

        if prevention_section:
            prevention_methods.extend(self._get_text_until_next_header(prevention_section))
            ul_element = prevention_section.find_next('ul')
            if ul_element:
                prevention_methods.extend([li.text.strip() for li in ul_element.find_all('li')])
        else:
            print("[INFO] No 'Prevention' section found. Setting to empty list.")

        return prevention_methods



    def get_doctor_visit_info(self, main_content) -> str:
        """Extract the 'When to see a doctor' section"""
        doctor_section = main_content.find(['h2', 'h3'], string=lambda x: x and ('When to see' in x and 'doctor' in x))
        
        if doctor_section:
            return ' '.join(self._get_text_until_next_header(doctor_section))
        
        print("[WARNING] 'When to see a doctor' section not found")
        return ''


    def get_disease_details(self, url: str) -> Optional[Dict]:
        """Extract all disease information from page"""
        html = self._make_request(url)
        if not html:
            return None

        soup = BeautifulSoup(html, 'html.parser')
        # main_content = soup.find('div', class_='content')
        
        main_content = soup.find('main')
        if not main_content:
            main_content = soup.find('article')
        if not main_content:
            main_content = soup.find('div', {'data-testid': 'cmp-section'})

        if not main_content:
            print("[ERROR] Main content div not found in any structure. Dumping raw HTML:")
            return None

        details = {
            'description': '',
            'symptoms': [],
            'causes': {
                'description': '',
                'main_causes': [],
                'triggers': []
            },
            'risk_factors': [],
            'prevention': [],
            'when_to_see_doctor': ''
        }

        # Get each section
        details['description'] = self.get_overview_section(main_content)
        details['symptoms'] = self.get_symptoms_section(main_content)
        details['causes'] = self.get_causes_section(main_content)
        details['risk_factors'] = self.get_risk_factors_section(main_content)
        details['prevention'] = self.get_prevention_section(main_content)
        details['when_to_see_doctor'] = self.get_doctor_visit_info(main_content)

        return details

    def get_diseases_from_letter(self, letter: str) -> List[Dict]:
        """Get list of diseases starting with specified letter"""
        url = f"{self.base_url}/diseases-conditions/index?letter={letter}"
        html = self._make_request(url)
        if not html:
            return []

        soup = BeautifulSoup(html, 'html.parser')
        diseases = []

        disease_links = soup.select('a[href*="/diseases-conditions/"][href*="/symptoms-causes/syc-"]')
        
        for link in disease_links:
            name = link.text.strip()
            if name and not name.lower() == 'see':
                disease_data = {
                    'name': name,
                    'url': self.base_url + link['href'] if link['href'].startswith('/') else link['href']
                }
                print(f"Found disease: {name}")
                diseases.append(disease_data)

        return diseases

    def scrape_diseases(self, limit: int = 5) -> List[Dict]:
        """Main scraping function with progress tracking"""
        all_diseases = []
        print(f"\nStarting to scrape first {limit} diseases...")
        
        diseases = self.get_diseases_from_letter('A')
        if not diseases:
            print("No diseases found")
            return []

        diseases = diseases[:limit]
        total = len(diseases)
        
        for i, disease in enumerate(diseases, 1):
            print(f"\nProcessing {i}/{total}: {disease['name']}")
            details = self.get_disease_details(disease['url'])
            if details:
                disease.update(details)
                all_diseases.append(disease)
                print(f"✓ Successfully processed {disease['name']}")
                print(f"  Description length: {len(details['description'])} chars")
                print(f"  Symptoms found: {len(details['symptoms'])}")
                print(f"  Causes description length: {len(details['causes'])} chars")
                print(f"  Risk factors found: {len(details['risk_factors'])}")
                print(f"  Prevention methods: {len(details['prevention'])}")
                if details['when_to_see_doctor']:
                    print(f"  Doctor visit info: Found")
            else:
                print(f"✗ Failed to get details for {disease['name']}")

        return all_diseases

def main():
    scraper = MayoClinicScraper()
    diseases_data = scraper.scrape_diseases(limit=15)
    
    if diseases_data:
        output_file = 'diseases_data.json'
        with open(output_file, 'w', encoding='utf-8') as f:
            json.dump(diseases_data, f, indent=4, ensure_ascii=False)
        print(f"\nSuccessfully scraped {len(diseases_data)} diseases")
        print(f"Data saved to {output_file}")
    else:
        print("\nNo data was scraped. Please check the errors above.")

if __name__ == "__main__":
    main()