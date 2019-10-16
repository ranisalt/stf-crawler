import re
from urllib.parse import (
    parse_qs,
    quote,
    unquote,
    urlencode,
    urlparse,
    urlunparse,
)

from scrapy import Spider
from scrapy.http import Request
from scrapy.selector import Selector

par = re.compile(r"(?<=\.)\s*$", flags=re.MULTILINE)
ref = re.compile(r"<(https?:[^>]*)>")


def repl_ref(match):
    return quote(match.group(0))


class JurisSpider(Spider):
    name = "juris"
    allowed_domains = ["stf.jus.br"]

    def __init__(self, url):
        self.start_url = url
        self.components = urlparse(self.start_url)
        query = parse_qs(self.components[4])
        query.pop("pagina", None)
        self.query = query

    def rebuild(self, query):
        return urlunparse(
            (
                *self.components[:4],
                urlencode(query, doseq=True),
                *self.components[5:],
            )
        )

    def start_requests(self):
        def parse_start_url(res):
            match = re.search(
                r"\d+\s*/\s*(\d+)",
                res.selector.css(
                    "#divNaoImprimir > table:nth-child(1) td:nth-child(3)::text"
                ).extract_first(),
            )
            if not match:
                return

            for i in range(1, int(match.group(1)) + 1):
                yield Request(self.rebuild({**self.query, "pagina": i}))

        yield Request(self.rebuild(self.query), callback=parse_start_url)

    def parse(self, res):
        for d in (
            Selector(text=ref.sub(repl_ref, res.text))
            .xpath(
                u'string(//p[strong[text()="Doutrina"]]/following-sibling::pre)'
            )
            .extract()
        ):
            yield {"lines": d.strip()}
