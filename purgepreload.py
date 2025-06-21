import requests
import xml.etree.ElementTree as ET
from urllib.parse import urlparse
import time
import concurrent.futures
import sys
import argparse

# Default configuration (can be overridden by command-line args)
REQUEST_DELAY = 0.5  # Seconds between purge requests
WARMER_THREADS = 2   # Concurrent threads for cache warming
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

def generate_purge_urls(page_urls, purge_base):
    """Generate purge URLs from page URLs"""
    purge_urls = []
    
    for url in page_urls:
        parsed = urlparse(url)
        path = parsed.path if parsed.path else '/'
        purge_url = f"{purge_base}{path}"
        
        # Preserve query strings if present
        if parsed.query:
            purge_url += f"?{parsed.query}"
            
        purge_urls.append(purge_url)
    
    return purge_urls

def send_purge_requests(urls, delay):
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
            
            time.sleep(delay)
        except Exception as e:
            print(f"[{i+1}/{len(urls)}] ERROR: {url} - {str(e)}")
            failed_urls.append(url)
    
    print(f"\nPurge complete! Success: {success_count}/{len(urls)}")
    print(f"Failed: {len(urls) - success_count}/{len(urls)}")
    return failed_urls

def warm_cache(urls, threads):
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

def main():
    parser = argparse.ArgumentParser(
        description='Cache Manager: Purge and warm cache for a website',
        formatter_class=argparse.ArgumentDefaultsHelpFormatter
    )
    parser.add_argument('domain', help='Domain name (e.g., landgraflawncare.ca)')
    parser.add_argument('--protocol', choices=['http', 'https'], default='https',
                       help='Website protocol')
    parser.add_argument('--delay', type=float, default=REQUEST_DELAY,
                       help='Delay between purge requests (seconds)')
    parser.add_argument('--threads', type=int, default=WARMER_THREADS,
                       help='Concurrent threads for cache warming')
    parser.add_argument('--skip-purge', action='store_true',
                       help='Skip the cache purge phase')
    parser.add_argument('--skip-warm', action='store_true',
                       help='Skip the cache warming phase')
    
    args = parser.parse_args()
    
    # Set up URLs
    base_url = f"{args.protocol}://{args.domain}"
    sitemap_url = f"{base_url}/sitemap.xml"
    purge_base = f"{base_url}/purge"
    site_domain = urlparse(base_url).netloc
    
    try:
        # Process all sitemaps recursively
        print("="*60)
        print("SITEMAP PROCESSING PHASE")
        print("="*60)
        all_page_urls = process_sitemap(sitemap_url, site_domain)
        
        if not all_page_urls:
            print("No page URLs found for processing")
            return
            
        print(f"\nTotal pages found: {len(all_page_urls)}")
        purge_targets = generate_purge_urls(all_page_urls, purge_base)
        
        purge_failures = []
        warm_failures = []
        
        # Purge phase
        if not args.skip_purge:
            print("\n" + "="*60)
            print("CACHE PURGE PHASE")
            print("="*60)
            purge_failures = send_purge_requests(purge_targets, args.delay)
        else:
            print("\nSkipping cache purge phase")
        
        # Warm cache phase
        if not args.skip_warm:
            print("\n" + "="*60)
            print("CACHE WARMING PHASE")
            print("="*60)
            warm_failures = warm_cache(all_page_urls, args.threads)
        else:
            print("\nSkipping cache warming phase")
        
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
        sys.exit(1)
    except ET.ParseError as e:
        print(f"XML parsing error: {str(e)}")
        sys.exit(1)
    except KeyboardInterrupt:
        print("\nOperation cancelled by user")
        sys.exit(1)
    except Exception as e:
        print(f"Unexpected error: {str(e)}")
        sys.exit(1)

if __name__ == "__main__":
    main()
