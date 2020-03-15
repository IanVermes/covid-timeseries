import scrapy
import datetime
from lxml import etree

POSTED_DATE_FORMAT = "%Y-%m-%d"

# BOOKMARK is cursor that tracks just how far back we should scrape each time
BOOKMARK = datetime.datetime(
    year=2020, month=1, day=1
)  # TODO factor bookmark into its own logic


class ChinaXIVSpider(scrapy.Spider):
    name = "chinaxiv"
    start_urls = [
        "http://www.chinaxiv.org/oai/OAIHandler?verb=ListRecords&metadataPrefix=oai_eprint"
    ]
    id_prefix = "chinaxiv"

    def parse(self, response):
        # China archrive features an API site that makes an XML request for 100 new
        # items. The first request is without a cursor query. The first response returns
        # 100 items + a cursor. Subsequent requests need this cursor.

        xml_root = etree.fromstring(response.body)
        self.nsmap = {k if k else "ns": v for k, v in xml_root.nsmap.items()}

        cursor = self._extract_cursor(xml_root)
        article_stubs = self._extract_stubs(xml_root)

        dates = []
        for stub in article_stubs:
            data = self._process_stub(stub)
            dates.append(self._get_publication_date(stub))
            yield data

        if dates:
            oldest_date = min(dates)
        else:
            oldest_date = None

        next_page = self._next_xml_page(cursor)

        if oldest_date is not None and self._is_page_new(oldest_date):
            self.logger.info(f"Follow to next page: {next_page}")
            yield response.follow(next_page, callback=self.parse)
        else:
            self.logger.info(
                f"Do not follow to next page, bookmark reached: {BOOKMARK}"
            )

    def _extract_cursor(self, xml_root):
        elements = xml_root.xpath("//ns:resumptionToken/text()", namespaces=self.nsmap)
        try:
            cursor = elements[0]
        except IndexError:
            cursor = None
        return cursor

    def _extract_stubs(self, xml_root):
        return xml_root.xpath("//ns:ListRecords/ns:record", namespaces=self.nsmap)

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
        title = stub_data.xpath(
            "ns:metadata//*[local-name()='title']/text()", namespaces=self.nsmap
        ).pop()
        return title

    def _get_article_url(self, stub_data):
        # We use the namespace XML wildcard because the namespace for this tag was not
        # declared in the document header.
        url = stub_data.xpath(
            "ns:metadata//*[local-name()='url']/text()", namespaces=self.nsmap
        ).pop()
        return url

    def _get_article_posted_date(self, stub_data):
        date_time = self._get_publication_date(stub_data)  # TODO placeholder date
        date = date_time.strftime(POSTED_DATE_FORMAT)
        return date

    def _get_revision_status(self, stub_data):
        return False

    def _get_article_id(self, stub_data):
        article_id = stub_data.xpath(
            "ns:metadata//*[local-name()='id']/text()", namespaces=self.nsmap
        ).pop()
        return self.id_prefix + "_" + article_id

    def _get_publication_date(self, stub_data):
        date_string = stub_data.xpath(
            "ns:metadata//*[local-name()='createtime']/text()", namespaces=self.nsmap
        ).pop()
        date_string = date_string.strip("Z")
        return datetime.datetime.fromisoformat(date_string)

    def _is_page_new(self, date):
        return date > BOOKMARK

    def _next_xml_page(self, cursor):
        if cursor:
            base = "http://www.chinaxiv.org/oai/OAIHandler?verb=ListRecords"
            return base + f"&resumptionToken={cursor}"
        else:
            return None
