import requests
import xml.etree.ElementTree as ET
from urllib.parse import urlparse
import time
import concurrent.futures

# Configuration
SITEMAP_URL = "https://website.ca/sitemap.xml"
PURGE_BASE = "https://website.ca/purge"
REQUEST_DELAY = 0.5  # Seconds between requests
WARMER_THREADS = 5   # Concurrent threads for cache warming
NAMESPACE = {'ns': 'http://www.sitemaps.org/schemas/sitemap/0.9'}

def fetch_sitemap(url):
    """Fetch and parse sitemap XML"""
    print(f"Fetching sitemap: {url}")
    response = requests.get(url)
    response.raise_for_status()
    return ET.fromstring(response.content)

def process_sitemap(sitemap_url, site_domain):
    """Recursively process sitemap or sitemap index"""
    print(f"\nProcessing: {sitemap_url}")
    sitemap = fetch_sitemap(sitemap_url)
    page_urls = []
    
    # Check if it's a sitemap index
    if sitemap.tag == f'{{{NAMESPACE["ns"]}}}sitemapindex':
        print("Found sitemap index, processing sub-sitemaps...")
        for sitemap_element in sitemap.findall('.//ns:sitemap', NAMESPACE):
            sub_sitemap_url = sitemap_element.find('ns:loc', NAMESPACE).text
            page_urls.extend(process_sitemap(sub_sitemap_url, site_domain))
            
    # Check if it's a regular sitemap
    elif sitemap.tag == f'{{{NAMESPACE["ns"]}}}urlset':
        print("Found URL set, extracting page URLs...")
        for url_element in sitemap.findall('.//ns:url', NAMESPACE):
            page_url = url_element.find('ns:loc', NAMESPACE).text
            parsed = urlparse(page_url)
            if parsed.netloc == site_domain:
                page_urls.append(page_url)
                
    print(f"Found {len(page_urls)} page URLs in {sitemap_url}")
    return page_urls

def generate_purge_urls(page_urls):
    """Generate purge URLs from page URLs"""
    purge_urls = []
    
    for url in page_urls:
        parsed = urlparse(url)
        path = parsed.path if parsed.path else '/'
        purge_url = f"{PURGE_BASE}{path}"
        
        # Preserve query strings if present
        if parsed.query:
            purge_url += f"?{parsed.query}"
            
        purge_urls.append(purge_url)
    
    return purge_urls

def send_purge_requests(urls):
    """Send purge requests with rate limiting"""
    print(f"\nStarting purge process for {len(urls)} pages...")
    success_count = 0
    failed_urls = []
    
    for i, url in enumerate(urls):
        try:
            response = requests.get(url)
            status = response.status_code
            
            if status == 200:
                print(f"[{i+1}/{len(urls)}] PURGED: {url}")
                success_count += 1
            else:
                print(f"[{i+1}/{len(urls)}] FAILED ({status}): {url}")
                failed_urls.append(url)
            
            time.sleep(REQUEST_DELAY)
        except Exception as e:
            print(f"[{i+1}/{len(urls)}] ERROR: {url} - {str(e)}")
            failed_urls.append(url)
    
    print(f"\nPurge complete! Success: {success_count}/{len(urls)}")
    print(f"Failed: {len(urls) - success_count}/{len(urls)}")
    return failed_urls

def warm_cache(urls, threads=WARMER_THREADS):
    """Warm cache by visiting URLs with concurrent threads"""
    print(f"\nStarting cache warming for {len(urls)} pages...")
    print(f"Using {threads} concurrent threads")
    
    success_count = 0
    failed_urls = []
    
    def visit_page(url):
        nonlocal success_count
        try:
            response = requests.get(url, timeout=10)
            if response.status_code == 200:
                return url, True
            return url, False
        except Exception:
            return url, False
    
    with concurrent.futures.ThreadPoolExecutor(max_workers=threads) as executor:
        future_to_url = {executor.submit(visit_page, url): url for url in urls}
        for i, future in enumerate(concurrent.futures.as_completed(future_to_url)):
            url = future_to_url[future]
            try:
                visited_url, success = future.result()
                if success:
                    success_count += 1
                    print(f"[{i+1}/{len(urls)}] WARMED: {visited_url}")
                else:
                    print(f"[{i+1}/{len(urls)}] FAILED: {visited_url}")
                    failed_urls.append(visited_url)
            except Exception as e:
                print(f"[{i+1}/{len(urls)}] ERROR: {url} - {str(e)}")
                failed_urls.append(url)
    
    print(f"\nCache warming complete! Success: {success_count}/{len(urls)}")
    print(f"Failed: {len(urls) - success_count}/{len(urls)}")
    return failed_urls

if __name__ == "__main__":
    try:
        # Get site domain for filtering
        site_domain = urlparse(PURGE_BASE).netloc
        
        # Process all sitemaps recursively
        print("="*60)
        print("SITEMAP PROCESSING PHASE")
        print("="*60)
        all_page_urls = process_sitemap(SITEMAP_URL, site_domain)
        
        if not all_page_urls:
            print("No page URLs found for purging")
            exit()
            
        print(f"\nTotal pages found: {len(all_page_urls)}")
        purge_targets = generate_purge_urls(all_page_urls)
        
        # Purge phase
        print("\n" + "="*60)
        print("CACHE PURGE PHASE")
        print("="*60)
        purge_failures = send_purge_requests(purge_targets)
        
        # Warm cache phase
        print("\n" + "="*60)
        print("CACHE WARMING PHASE")
        print("="*60)
        warm_failures = warm_cache(all_page_urls)
        
        # Final report
        print("\n" + "="*60)
        print("FINAL REPORT")
        print("="*60)
        print(f"Total pages processed: {len(all_page_urls)}")
        print(f"Purge failures: {len(purge_failures)}")
        print(f"Cache warming failures: {len(warm_failures)}")
        
        if purge_failures:
            print("\nPurge failures (retry these manually):")
            for url in purge_failures:
                print(f"- {url}")
        
        if warm_failures:
            print("\nCache warming failures (retry these manually):")
            for url in warm_failures:
                print(f"- {url}")
        
        print("\nOperation completed!")
        
    except requests.exceptions.RequestException as e:
        print(f"Network error: {str(e)}")
    except ET.ParseError as e:
        print(f"XML parsing error: {str(e)}")
    except KeyboardInterrupt:
        print("\nOperation cancelled by user")
    except Exception as e:
        print(f"Unexpected error: {str(e)}")
