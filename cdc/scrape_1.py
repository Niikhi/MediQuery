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
        options = webdriver.ChromeOptions()
        # options.add_argument('--headless')  # Commented out for debugging
        self.driver = webdriver.Chrome(service=Service(ChromeDriverManager().install()), options=options)
        self.topics_data = {}
        self.relationships = {}
        self.topics = []  # Store all topics found

    def get_all_topics(self):
        """Get all topics from A-Z index page"""
        self.driver.get(self.topics_url)
        
        # Wait for the A-Z strip to load
        WebDriverWait(self.driver, 10).until(
            EC.presence_of_element_located((By.CLASS_NAME, "az-strip"))
        )
        
        topic_links = []
        letters = "ABCDEFGHIJKLMNOPQRSTUVWXYZ"
        
        # Click each letter and get topics
        for letter in letters:
            print(f"Getting topics for letter {letter}...")
            try:
                # Find and click the letter button
                letter_button = self.driver.find_element(By.CSS_SELECTOR, f'.az-strip__item[data-id="{letter}"]')
                self.driver.execute_script("arguments[0].click();", letter_button)
                
                # Wait for topics to load
                time.sleep(1)  # Give time for content to update
                
                # Get the content for this letter
                letter_content = self.driver.find_element(By.CSS_SELECTOR, f'.char-block[data-id="{letter}"]')
                links = letter_content.find_elements(By.TAG_NAME, "a")
                
                for link in links:
                    href = link.get_attribute('href')
                    if href and ('cdc.gov' in href or href.startswith('/')):
                        full_url = href if href.startswith('http') else urljoin(self.base_url, href)
                        text = link.text.strip()
                        if text:  # Only add if there's actual text
                            topic_links.append({
                                'letter': letter,
                                'title': text,
                                'url': full_url
                            })
                            print(f"Found topic: {text}")
                
            except Exception as e:
                print(f"Error processing letter {letter}: {str(e)}")
                continue
        
        self.topics = topic_links
        return topic_links

    def extract_relationships(self, content_element, base_topic_url):
        relationships = []
        
        # Find all links in paragraphs and list items
        for element in content_element.find_elements(By.CSS_SELECTOR, 'p, li'):
            try:
                context = element.text.strip()  # Get the surrounding text for context
                
                # Find links within this element
                for link in element.find_elements(By.TAG_NAME, 'a'):
                    href = link.get_attribute('href')
                    if href and ('cdc.gov' in href or href.startswith('/')):
                        full_url = href if href.startswith('http') else urljoin(self.base_url, href)
                        # Only include if it's a condition/disease page
                        if '/about/' in full_url or '/index.html' in full_url:
                            relationships.append({
                                'url': full_url,
                                'text': link.text.strip(),
                                'context': context,
                                'source_url': base_topic_url
                            })
            except Exception as e:
                print(f"Error extracting relationships from element: {str(e)}")
                continue
        
        return relationships

    def scrape_topic_page(self, url):
        try:
            self.driver.get(url)
            print(f"Scraping: {url}")
            
            # Wait for main content
            WebDriverWait(self.driver, 10).until(
                EC.presence_of_element_located((By.TAG_NAME, "main"))
            )
            
            topic_data = {
                'url': url,
                'title': '',
                'key_points': [],
                'sections': [],
                'related_conditions': [],
                'risk_factors': [],
                'symptoms': [],
                'treatments': []
            }

            # Get title
            try:
                title = self.driver.find_element(By.TAG_NAME, "h1")
                topic_data['title'] = title.text.strip()
            except:
                print("No title found")

            # Get main content
            main_content = self.driver.find_element(By.TAG_NAME, "main")
            
            # Extract relationships before removing any sections
            related_conditions = self.extract_relationships(main_content, url)
            topic_data['related_conditions'] = related_conditions
            self.relationships[url] = related_conditions

            # Process sections
            sections = main_content.find_elements(By.CSS_SELECTOR, 'h2, h3')
            current_section = None
            
            for section in sections:
                section_title = section.text.strip()
                
                # Skip Resources section
                if 'Resources' in section_title:
                    continue
                
                current_section = {
                    'title': section_title,
                    'content': [],
                    'type': 'general'
                }
                
                # Determine section type
                lower_title = section_title.lower()
                if 'risk factors' in lower_title:
                    current_section['type'] = 'risk_factors'
                elif 'symptoms' in lower_title or 'signs' in lower_title:
                    current_section['type'] = 'symptoms'
                elif 'treatment' in lower_title:
                    current_section['type'] = 'treatments'
                
                # Get content until next section
                next_element = section
                while True:
                    try:
                        next_element = next_element.find_element(By.XPATH, "following-sibling::*[1]")
                        if next_element.tag_name in ['h2', 'h3']:
                            break
                        
                        if next_element.tag_name == 'p':
                            current_section['content'].append({
                                'type': 'text',
                                'content': next_element.text.strip()
                            })
                        elif next_element.tag_name in ['ul', 'ol']:
                            items = [li.text.strip() for li in next_element.find_elements(By.TAG_NAME, 'li')]
                            current_section['content'].append({
                                'type': 'list',
                                'items': items
                            })
                            
                            # Add items to appropriate categories
                            if current_section['type'] in ['risk_factors', 'symptoms', 'treatments']:
                                topic_data[current_section['type']].extend(items)
                                
                    except:
                        break
                
                if current_section['content']:
                    topic_data['sections'].append(current_section)

            return topic_data

        except Exception as e:
            print(f"Error scraping {url}: {str(e)}")
            return None

    def scrape_all_topics(self):
        topics = self.get_all_topics()
        print(f"Found {len(topics)} topics to scrape")
        
        for i, topic in enumerate(topics):
            print(f"Scraping {i+1}/{len(topics)}: {topic['title']}")
            
            topic_data = self.scrape_topic_page(topic['url'])
            if topic_data:
                self.topics_data[topic['url']] = topic_data
                
                # Save progress every 10 topics
                if (i + 1) % 10 == 0:
                    self.save_final_data("cdc_health_topics_data_in_progress.json")
                    print(f"\nProgress saved: {i+1} topics processed")
                    self.check_progress()
            
            time.sleep(2)
        
        # Save final data
        self.save_final_data("cdc_health_topics_data_final.json")
        print("\nScraping complete! Data saved to cdc_health_topics_data_final.json")
        self.check_progress("cdc_health_topics_data_final.json")
        self.driver.quit()

    def save_final_data(self, filename):
        final_data = {
            'topics': self.topics_data,
            'relationships': self.relationships,
            'metadata': {
                'total_topics_processed': len(self.topics_data),
                'total_topics_found': len(self.topics),
                'total_relationships': sum(len(rels) for rels in self.relationships.values()),
                'scrape_date': time.strftime('%Y-%m-%d %H:%M:%S'),
                'scraping_complete': filename.endswith('final.json')
            }
        }
        
        with open(filename, 'w', encoding='utf-8') as f:
            json.dump(final_data, f, indent=2, ensure_ascii=False)

    def check_progress(self, filename="cdc_health_topics_data_in_progress.json"):
        try:
            with open(filename, 'r', encoding='utf-8') as f:
                data = json.load(f)
                print("\nCurrent Progress:")
                print(f"Topics processed: {data['metadata']['total_topics_processed']}/{data['metadata']['total_topics_found']}")
                print(f"Total relationships found: {data['metadata']['total_relationships']}")
                print(f"Last update: {data['metadata']['scrape_date']}")
                
                # Print sample of last 5 processed topics
                print("\nLast 5 processed topics:")
                topics = list(data['topics'].items())[-5:]
                for url, topic in topics:
                    print(f"- {topic['title']}")
                    if topic['related_conditions']:
                        print(f"  Related conditions: {len(topic['related_conditions'])}")
                        
        except FileNotFoundError:
            print("No progress file found yet")
        except Exception as e:
            print(f"Error reading progress: {str(e)}")

def main():
    scraper = CDCScraper()
    try:
        scraper.scrape_all_topics()
    except KeyboardInterrupt:
        print("\nScraping interrupted by user. Saving current progress...")
        scraper.save_final_data("cdc_health_topics_data_interrupted.json")
        print("Progress saved. You can resume later by modifying the script to start from where you left off.")
    except Exception as e:
        print(f"\nAn error occurred: {str(e)}")
        print("Saving current progress...")
        scraper.save_final_data("cdc_health_topics_data_error.json")
    finally:
        scraper.driver.quit()

if __name__ == "__main__":
    main()