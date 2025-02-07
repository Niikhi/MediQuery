from selenium import webdriver
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from bs4 import BeautifulSoup
import json
import time
from urllib.parse import urljoin
from selenium.webdriver.chrome.service import Service
from webdriver_manager.chrome import ChromeDriverManager

class CDCScraper:
    def __init__(self):
        self.base_url = "https://www.cdc.gov"
        self.topics_url = "https://www.cdc.gov/health-topics.html"
        # Initialize Selenium WebDriver
        options = webdriver.ChromeOptions()
        options.add_argument('--headless')  # Run in headless mode
        self.driver = webdriver.Chrome(service=Service(ChromeDriverManager().install()), options=options)
        self.topics_data = {}

    def get_all_topics(self):
        """Get all topics from A-Z index page"""
        self.driver.get(self.topics_url)
        # Wait for the content to load
        WebDriverWait(self.driver, 10).until(
            EC.presence_of_element_located((By.CLASS_NAME, "az-content"))
        )
        
        # Get the page source after JavaScript has loaded
        soup = BeautifulSoup(self.driver.page_source, 'html.parser')
        
        topic_links = []
        # The topics are organized by letter in char-block divs
        for letter_block in soup.find_all('div', class_='char-block'):
            letter = letter_block.get('data-id', '')
            if letter and letter != 'd-none':  # Skip hidden blocks
                links = letter_block.find_all('a')
                
                for link in links:
                    href = link.get('href')
                    if href and ('cdc.gov' in href or href.startswith('/')):
                        full_url = href if href.startswith('http') else urljoin(self.base_url, href)
                        topic_links.append({
                            'letter': letter,
                            'title': link.text.strip(),
                            'url': full_url
                        })
                        
        return topic_links

    def scrape_topic_page(self, url):
        """Scrape individual topic page"""
        try:
            self.driver.get(url)
            # Wait for main content to load
            WebDriverWait(self.driver, 10).until(
                EC.presence_of_element_located((By.TAG_NAME, "main"))
            )
            
            soup = BeautifulSoup(self.driver.page_source, 'html.parser')
            
            topic_data = {
                'url': url,
                'title': '',
                'key_points': [],
                'sections': [],
                'related_links': [],
                'statistics': []
            }

            # Get title
            title = soup.find('h1')
            if title:
                topic_data['title'] = title.text.strip()

            # Get main content
            content = soup.find('main')
            if content:
                # Find key points
                for h2 in content.find_all(['h2', 'div'], class_='Key points'):
                    key_points = h2.find_next('ul')
                    if key_points:
                        topic_data['key_points'] = [li.text.strip() for li in key_points.find_all('li')]
                
                # Find sections
                current_section = None
                for elem in content.find_all(['h2', 'h3', 'p', 'ul', 'ol']):
                    if elem.name in ['h2', 'h3']:
                        if current_section:
                            topic_data['sections'].append(current_section)
                        current_section = {
                            'title': elem.text.strip(),
                            'content': []
                        }
                    elif current_section:
                        if elem.name == 'p':
                            current_section['content'].append({
                                'type': 'text',
                                'content': elem.text.strip()
                            })
                        elif elem.name in ['ul', 'ol']:
                            current_section['content'].append({
                                'type': 'list',
                                'items': [li.text.strip() for li in elem.find_all('li')]
                            })

                if current_section:
                    topic_data['sections'].append(current_section)

                # Find related links
                for link in content.find_all('a'):
                    href = link.get('href')
                    if href and ('cdc.gov' in href or href.startswith('/')) and '#' not in href:
                        full_url = href if href.startswith('http') else urljoin(self.base_url, href)
                        topic_data['related_links'].append({
                            'text': link.text.strip(),
                            'url': full_url
                        })

            return topic_data

        except Exception as e:
            print(f"Error scraping {url}: {str(e)}")
            return None

    def scrape_all_topics(self):
        """Scrape all topics"""
        topics = self.get_all_topics()
        print(f"Found {len(topics)} topics to scrape")
        
        for i, topic in enumerate(topics):
            print(f"Scraping {i+1}/{len(topics)}: {topic['title']}")
            
            topic_data = self.scrape_topic_page(topic['url'])
            if topic_data:
                self.topics_data[topic['url']] = topic_data
            
            # Save progress periodically
            if i % 10 == 0:
                self.save_data(f"cdc_topics_progress_{i}.json")
            
            # Be nice to CDC's servers
            time.sleep(2)
        
        # Save final data
        self.save_data("cdc_topics_final.json")
        self.driver.quit()

    def save_data(self, filename):
        """Save scraped data to JSON file"""
        with open(filename, 'w', encoding='utf-8') as f:
            json.dump(self.topics_data, f, indent=2, ensure_ascii=False)

# Run the scraper
if __name__ == "__main__":
    scraper = CDCScraper()
    scraper.scrape_all_topics()