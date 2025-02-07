import requests
import json
import time
import sys
from typing import Dict, List, Optional
from base64 import b64encode

class ICD11Fetcher:
    def __init__(self, client_id: str, client_secret: str):
        self.client_id = client_id
        self.client_secret = client_secret
        self.token = None
        self.base_url = "https://id.who.int/icd/release/11/2024-01/mms"
        self.processed_count = 0

    def get_token(self) -> str:
        """Get access token using Basic Auth"""
        try:
            token_url = "https://icdaccessmanagement.who.int/connect/token"
            
            # Create basic auth header
            credentials = f"{self.client_id}:{self.client_secret}"
            encoded_credentials = b64encode(credentials.encode('utf-8')).decode('utf-8')
            
            headers = {
                'Authorization': f'Basic {encoded_credentials}',
                'Content-Type': 'application/x-www-form-urlencoded'
            }
            
            data = {
                'grant_type': 'client_credentials',
                'scope': 'icdapi_access'
            }
            
            print("Attempting to get token with Basic Auth...")
            response = requests.post(token_url, headers=headers, data=data)
            
            print(f"Token response status: {response.status_code}")
            print(f"Token response headers: {dict(response.headers)}")
            print(f"Token response content: {response.text[:200]}")
            
            if response.status_code != 200:
                print(f"Error getting token. Full response: {response.text}")
                sys.exit(1)
                
            token_data = response.json()
            if 'access_token' not in token_data:
                print(f"No access token in response: {token_data}")
                sys.exit(1)
                
            return token_data['access_token']
            
        except Exception as e:
            print(f"Error during token request: {str(e)}")
            sys.exit(1)

    def make_request(self, endpoint: str) -> Dict:
        """Make API request"""
        if not self.token:
            self.token = self.get_token()
            
        headers = {
            'Authorization': f'Bearer {self.token}',
            'Accept': 'application/json',
            'API-Version': 'v2',
            'Accept-Language': 'en'
        }
        
        url = f"{self.base_url}/{endpoint}"
        print(f"Making request to: {url}")
        
        response = requests.get(url, headers=headers)
        
        if response.status_code == 401:
            print("Token expired, getting new token...")
            self.token = self.get_token()
            return self.make_request(endpoint)
        
        return response.json() if response.status_code == 200 else {}

    def process_entity(self, entity_id: str, depth: int = 0) -> Dict:
        """Process entity and its children"""
        self.processed_count += 1
        print(f"{'  ' * depth}Processing entity {entity_id} (Total: {self.processed_count})")
        
        entity_data = self.make_request(f"entity/{entity_id}")
        
        processed_entity = {
            'id': entity_id,
            'code': entity_data.get('code'),
            'title': self._extract_value(entity_data.get('title')),
            'definition': self._extract_value(entity_data.get('definition')),
            'children': [],
            'inclusions': [],
            'exclusions': []
        }
        
        # Get children
        children_data = self.make_request(f"entity/{entity_id}/children")
        if children_data and 'child' in children_data:
            children = children_data['child']
            if isinstance(children, dict):
                children = [children]
            
            for child in children:
                if isinstance(child, dict) and '@id' in child:
                    child_id = child['@id'].split('/')[-1]
                    child_data = self.process_entity(child_id, depth + 1)
                    processed_entity['children'].append(child_data)
        
        return processed_entity

    def _extract_value(self, data):
        if isinstance(data, dict):
            return data.get('@value')
        return data

def validate_credentials(client_id: str, client_secret: str) -> bool:
    """Validate that credentials are in the correct format"""
    if len(client_id) < 10 or len(client_secret) < 10:
        print("Error: Credentials seem too short. Please check them.")
        return False
    return True

def main():
    # Your WHO API credentials here
    client_id = "eec18e8b-e16f-47b7-b8b1-cd2433e1d110_7ed6dc8b-358f-4ff0-ba27-251ac895d8c4"
    client_secret = "hWICxs1NXn0lzVUT0hrsOWTuReSzFDXAM2Rj63epBks"
    
    if not validate_credentials(client_id, client_secret):
        return
    
    fetcher = ICD11Fetcher(client_id, client_secret)
    root_id = "1435254666"  # ID for infectious diseases chapter
    
    print("Starting ICD-11 data fetch...")
    processed_data = fetcher.process_entity(root_id)
    
    print(f"\nTotal entities processed: {fetcher.processed_count}")
    
    print("Saving data...")
    with open('icd11_raw.json', 'w', encoding='utf-8') as f:
        json.dump(processed_data, f, ensure_ascii=False, indent=2)
    
    print("Done!")

if __name__ == "__main__":
    main()