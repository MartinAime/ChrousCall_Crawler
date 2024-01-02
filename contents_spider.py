import os
from urllib.parse import urljoin, urlparse
import scrapy
from scrapy.linkextractors import LinkExtractor
from scrapy.spiders import CrawlSpider, Rule

class TestSpider(CrawlSpider):
    name = "test"
    allowed_domains = ["hitachi.co.jp"]
    start_urls = [
        "https://www.hitachi.co.jp/New/cnews/index.html",
        "https://www.hitachi.co.jp/IR/index.html",
        "https://www.hitachi.co.jp/sustainability/index.html"
    ]

    # List of URLs to skip
    skip_urls = [
        "https://itpfdoc.hitachi.co.jp/manuals",
        "https://itpfdoc.hitachi.co.jp/Pages",
        "https://www.hitachi.co.jp/support/inquiry/index.html",
        "https://www.hitachi.co.jp/recruit/index.html"
        # Add more URLs to skip as needed
    ]

    rules = (
        Rule(LinkExtractor(), callback='parse_page', follow=True),
    )

    def parse_page(self, response):
        # Check if the current URL should be skipped
        if self.should_skip_url(response.url):
            self.logger.info(f"Skipping URL: {response.url}")
            return

        # Extract site name and section from the URL
        site_name, section = self.extract_site_and_section(response.url)

        # Create a folder inside "storage" for each site and section
        site_section_folder_path = os.path.join("storage", site_name, section)
        os.makedirs(site_section_folder_path, exist_ok=True)

        # Save the current page content in the site-specific folder
        filename = response.url.split("/")[-1]
        with open(os.path.join(site_section_folder_path, filename), 'wb') as f:
            f.write(response.body)

        # Follow links to other pages within the allowed domain
        for next_page_link in self.extract_links(response, 'a::attr(href)'):
            yield response.follow(next_page_link, callback=self.parse_page)

        # Download all files within the allowed domain
        for file_url in self.extract_links(response, 'a::attr(href)'):
            yield scrapy.Request(file_url, callback=self.download_file)

    def download_file(self, response):
        # Check if the current URL should be skipped
        if self.should_skip_url(response.url):
            self.logger.info(f"Skipping URL: {response.url}")
            return

        # Extract site name and section from the URL
        site_name, section = self.extract_site_and_section(response.url)

        # Extract filename from the URL
        filename = response.url.split("/")[-1]

        # Check if the file has a ".exe" extension and skip downloading it
        if filename.endswith(".exe"):
            self.logger.info(f"Skipping download of {filename} due to '.exe' extension")
            return

        # Specify the absolute path for file storage ("storage/site_name/section")
        file_directory = os.path.join("storage", site_name, section)

        # Create the directory if it doesn't exist
        os.makedirs(file_directory, exist_ok=True)

        # Save the file in the specified directory
        with open(os.path.join(file_directory, filename), 'wb') as f:
            f.write(response.body)

    def extract_site_and_section(self, url):
        # Extract site name from the URL
        site_name = urlparse(url).hostname

        # Extract section from the URL (e.g., "/products", "/services", etc.)
        path_elements = urlparse(url).path.split("/")
        section = path_elements[1] if len(path_elements) > 1 else "default"

        return site_name, section

    def extract_links(self, response, css_selector):
        return [response.urljoin(link) for link in response.css(css_selector).extract()]

    def should_skip_url(self, url):
        # Check if the URL should be skipped based on the predefined list
        return url in self.skip_urls
