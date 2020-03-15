import scrapy
import datetime

from urllib.parse import urlparse
from crossref.restful import Works


# "List of Sections" constants
SECTION_SELECTOR = "div.pane-content > div.highwire-list-wrapper"
NEXT_PAGE_SELECTOR = (
    'div.highwire-list.page-group-last.item-list > ul > li > a::attr("href")'
)
DATE_FORMAT = "%B %d, %Y"

# "Section of Article items" constants
ARTICLE_SELECTOR = "div > ul > li"
SECTION_DATE_SELECTOR = "h3.highwire-list-title::text"

# "Article item" constants
TITLE_SELECTOR = "span.highwire-cite-title > a > span.highwire-cite-title::text"
DOI_SELECTOR = "span.highwire-cite-metadata-doi::text"
URL_SELECTOR = 'span.highwire-cite-title > a::attr("href")'

# "Article page" constants
POSTED_SELECTOR = "ul.issue-toc-list li:first-child"
POSTED_DATE_FORMAT = "%Y-%m-%d"

# BOOKMARK is cursor that tracks just how far back we should scrape each time
BOOKMARK = datetime.datetime(
    year=2020, month=1, day=1
)  # TODO factor bookmark into its own logic


class ArchiveSpiderBase:
    def parse(self, response):
        # Find all listed articles
        for section in response.css(SECTION_SELECTOR):
            section_date = self._get_section_date(section)
            for article in section.css(ARTICLE_SELECTOR):
                data = self._list_item_parser(article, section_date)
                yield data

        # Find the ling to the next page
        next_page = response.css(NEXT_PAGE_SELECTOR).get()

        # Decide if following to next page
        if self._is_page_new(section_date) and next_page is not None:
            self.logger.info(f"Follow to next page: {next_page}")
            yield response.follow(next_page, callback=self.parse)
        else:
            self.logger.info(
                f"Do not follow to next page, bookmark reached: {BOOKMARK}"
            )

    def _is_page_new(self, date):
        return date > BOOKMARK

    def _get_section_date(self, section):
        date_string = section.css(SECTION_DATE_SELECTOR).get()
        if not date_string:
            self.logger.error("No title date found on page!")
            raise ValueError("stale section date selector")
        return datetime.datetime.strptime(date_string, DATE_FORMAT)

    def _is_article_revision(self, url):
        return url is not None and not url.endswith("v1")

    def _list_item_parser(self, item, section_date):
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
        # Clean DOI
        doi = data.get("doi")
        if doi:
            parsed_doi = urlparse(doi).path.strip("/")
            data["doi"] = parsed_doi
        # Concat URL with domain
        url = data.get("url")
        if url:
            data["url"] = self.domain + url
        # Add `id` key and string value
        data = self._add_id(data)
        # Add `is_revision` key and boolean value
        data["is_revision"] = self._is_article_revision(data.get("url"))
        # Add `posted` key and datetime value by scraping the original page
        data_or_request = self._do_posted_date(data, section_date)
        return data_or_request

    def _do_posted_date(self, data, section_date):
        # Fallback posted dates
        # 1) If the article is `v1` use the section date
        # 2) If the article is a revision visit get posted date from DOI
        # 3) Fallback to the date on the article info page
        # 4) Fallback to the date on the website
        #
        # Why complexity? Reduce the amount of slow HTTP requests. DOI request
        #    returns JSON and is faster than scraping another HTML page. Why not scrape
        #    the v1 for the date in article info? Its the same as the section date in
        #    the list, so is already available.
        if data.get("is_revision"):
            doi = data.get("doi")
            if doi:
                posted_date = self._posted_date_from_doi(doi)
            else:
                posted_date = None

            if not posted_date:
                article_info_url = self._make_article_info_url(data["url"])
                request = scrapy.Request(
                    article_info_url,
                    callback=self.parse_article_page,
                    cb_kwargs=dict(data=data),
                )
                return request
            else:
                date = posted_date
        else:
            date = section_date

        data = self._add_posted_date(data, date)
        return data

    def _make_article_info_url(self, url):
        return url + ".article-info"

    def parse_article_page(self, response, data):
        self.logger.debug(f"visited={response.url}")
        epoch_seconds = response.css(POSTED_SELECTOR).attrib.get("date")
        if not epoch_seconds:
            self.logger.error("No info date found on page!")
            raise ValueError(
                "parse_article_page has stale selectors or is a v1 article"
            )
        else:
            epoch_seconds = int(epoch_seconds)
        date = datetime.datetime.fromtimestamp(epoch_seconds)
        data = self._add_posted_date(data, date)
        yield data

    def _add_posted_date(self, data, date):
        data["posted"] = date.strftime(POSTED_DATE_FORMAT)
        return data

    def _add_id(self, data):
        doi = data["doi"]
        _ignore, article_id = doi.split("/", maxsplit=1)
        data["id"] = self.id_prefix + "_" + article_id
        return data

    def _posted_date_from_doi(self, doi):
        # Handle bad DOI or absent DOIs
        doi_data = self.works.doi(doi)
        if doi_data:
            date_dict = doi_data.get("posted", {})
        else:
            date_dict = {}

        # Returns a dict like, but not all keys may be present
        # {'date-parts': [[2020, 3, 13]],
        # 'date-time': '2020-03-13T17:24:23Z',
        # 'timestamp': 1584120263000}
        if "date-parts" in date_dict:
            date_parts = date_dict["date-parts"][0]
            date_string = "-".join(map(str, date_parts))
            posted_date = datetime.datetime.strptime(date_string, POSTED_DATE_FORMAT)
        elif "date-time" in date_dict:
            datetime_string = date_dict["date-time"].strip("Z")
            posted_date = datetime.datetime.fromisoformat(datetime_string)
        elif "timestamp" in date_dict:
            epoch_seconds = date_dict["timestamp"]
            posted_date = datetime.datetime.fromtimestamp(epoch_seconds)
        else:
            posted_date = None
        return posted_date


class MedRXIVSpider(ArchiveSpiderBase, scrapy.Spider):
    name = "medrxiv"
    start_urls = ["https://www.medrxiv.org/content/early/recent?page=0"]
    domain = "https://www.medrxiv.org"
    id_prefix = "medrxiv"
    works = Works()


class BioRXIVSpider(ArchiveSpiderBase, scrapy.Spider):
    name = "biorxiv"
    start_urls = ["https://www.biorxiv.org/content/early/recent?page=0"]
    domain = "https://www.biorxiv.org"
    id_prefix = "biorxiv"
    works = Works()
