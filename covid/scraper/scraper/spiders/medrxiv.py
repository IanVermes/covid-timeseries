import scrapy

TITLE_SELECTOR = "span.highwire-cite-title > a > span.highwire-cite-title::text"
DOI_SELECTOR = "span.highwire-cite-metadata-doi::text"
URL_SELECTOR = 'span.highwire-cite-title > a::attr("href")'


class MedRXIVSpider(scrapy.Spider):
    name = "medrxiv"
    start_urls = ["https://www.medrxiv.org/content/early/recent?page=0"]

    def parse(self, response):
        for section in response.css("div.pane-content > div.highwire-list-wrapper"):
            for article in section.css("div > ul > li"):
                data = self._list_item_parser(article)
                yield data

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
        return data
