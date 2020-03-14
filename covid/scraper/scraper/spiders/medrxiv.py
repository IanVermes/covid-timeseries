import scrapy
import datetime

TITLE_SELECTOR = "span.highwire-cite-title > a > span.highwire-cite-title::text"
DOI_SELECTOR = "span.highwire-cite-metadata-doi::text"
URL_SELECTOR = 'span.highwire-cite-title > a::attr("href")'
DATE_FORMAT = "%B %d, %Y"

DATE_SELECTOR = "div.pane-content h3.highwire-list-title::text"
BOOKMARK = datetime.datetime(year=2020, month=3, day=6)


class MedRXIVSpider(scrapy.Spider):
    name = "medrxiv"
    start_urls = ["https://www.medrxiv.org/content/early/recent?page=0"]

    def parse(self, response):
        # Find all listed articles
        for section in response.css("div.pane-content > div.highwire-list-wrapper"):
            for article in section.css("div > ul > li"):
                data = self._list_item_parser(article)
                yield data

        # Find the ling to the next page
        next_page = response.css(
            'div.highwire-list.page-group-last.item-list > ul > li > a::attr("href")'
        ).get()

        # Decide if following to next page
        if self._follow_predicate(response) and next_page is not None:
            self.logger.info(f"Follow to next page: {next_page}")
            yield response.follow(next_page, callback=self.parse)
        else:
            self.logger.info(
                f"Do not follow to next page, bookmark reached: {BOOKMARK}"
            )

    def _follow_predicate(self, response):
        date_string = response.css(DATE_SELECTOR).get()
        if not date_string:
            self.logger.error("No title date found on page!")
            return false
        date = datetime.datetime.strptime(date_string, DATE_FORMAT)
        # return date > BOOKMARK
        return False

    def _is_article_revision(self, url):
        return url is not None and url.endswith("v1")

    def _list_item_parser(self, item):
        # Extract data
        data = {
            "title": item.css(TITLE_SELECTOR).get(),
            "doi": item.css(DOI_SELECTOR).get(),
            "url": item.css(URL_SELECTOR).get(),
        }
        # Clean data
        for key in data.keys():
            value = data[key]
            if value:
                data[key] = value.strip()
        # Concat URL with domain
        url = data.get("url")
        if url:
            data["url"] = "https://www.medrxiv.org" + url
        # Add `is_revision` key and boolean value
        data["is_revision"] = self._is_article_revision(data.get("url"))
        return data
