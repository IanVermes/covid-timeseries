import scrapy
import json
import datetime

POSTED_DATE_FORMAT = "%Y-%m-%d"

# BOOKMARK is cursor that tracks just how far back we should scrape each time
BOOKMARK = datetime.datetime(
    year=2020, month=1, day=1
)  # TODO factor bookmark into its own logic


class ChemRXIVSpider(scrapy.Spider):
    name = "chemrxiv"
    start_urls = [
        "https://chemrxiv.org/api/institutions/259/items?types=&licenses=&orderBy=published_date&orderType=desc&limit=40&search=&categories=&itemTypes=articles"
    ]
    id_prefix = "chemrxiv"

    def parse(self, response):
        # Chem archrive features an infinite scrolling site that makes a JSON request
        # for 40 new items upon each scrolling event. The first request is without a
        # cursor query. The first response returns 40 items + a cursor. Subsequent
        # requests need this cursor.

        json_data = json.loads(response.body_as_unicode())
        cursor = self._extract_cursor(json_data)
        article_stubs = self._extract_stubs(json_data)

        dates = []
        for stub in article_stubs:
            data = self._process_stub(stub)
            dates.append(self._get_publication_date(stub))
            yield data

        if dates:
            oldest_date = min(dates)
        else:
            oldest_date = None

        next_page = self._next_json_page(cursor)

        if oldest_date is not None and self._is_page_new(oldest_date):
            self.logger.info(f"Follow to next page: {next_page}")
            yield response.follow(next_page, callback=self.parse)
        else:
            self.logger.info(
                f"Do not follow to next page, bookmark reached: {BOOKMARK}"
            )

    def _extract_cursor(self, json_data):
        return json_data["cursor"]

    def _extract_stubs(self, json_data):
        return json_data["items"]

    def _process_stub(self, stub_data):
        data = {
            "title": self._get_article_title(stub_data),
            "url": self._get_article_url(stub_data),
            "posted": self._get_article_posted_date(stub_data),
            "is_revision": self._get_revision_status(stub_data),
            "id": self._get_article_id(stub_data),
        }
        return data

    def _get_article_title(self, stub_data):
        return stub_data["data"]["title"]

    def _get_article_url(self, stub_data):
        return stub_data["data"]["publicUrl"]

    def _get_article_posted_date(self, stub_data):
        date_string = stub_data["data"]["timeline"]["posted"]
        date_string = date_string.strip("Z")
        date_time = datetime.datetime.fromisoformat(date_string)
        date = date_time.strftime(POSTED_DATE_FORMAT)
        return date

    def _get_revision_status(self, stub_data):
        version = stub_data["data"]["version"]
        return version > 1

    def _get_article_id(self, stub_data):
        return self.id_prefix + "_" + str(stub_data["data"]["id"])

    def _get_publication_date(self, stub_data):
        date_string = stub_data["data"]["publishedDate"]
        date_string = date_string.strip("Z")
        return datetime.datetime.fromisoformat(date_string)

    def _is_page_new(self, date):
        return date > BOOKMARK

    def _next_json_page(self, cursor):
        base = self.start_urls[0]
        return base + f"&cursor={cursor}"

