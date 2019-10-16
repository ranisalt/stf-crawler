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

ref = re.compile(r"<(https?:[^>]*)>")


def rebuild(components, query):
    return urlunparse(
        (*components[:4], urlencode(query, doseq=True), *components[5:])
    )


def repl(match):
    return quote(match.group(0))


class JurisSpider(Spider):
    name = "juris"
    allowed_domains = ["stf.jus.br"]

    def __init__(self, url):
        self.start_url = url

    def start_requests(self):
        components = urlparse(self.start_url)
        query = parse_qs(components[4])
        query.pop("pagina")

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
                yield Request(rebuild(components, {**query, "pagina": i}))

        yield Request(rebuild(components, query), callback=parse_start_url)

    def parse(self, res):
        for d in (
            Selector(text=ref.sub(repl, res.text))
            .xpath(
                u'//p[strong[text()="Doutrina"]]/following-sibling::pre/text()'
            )
            .extract()
        ):
            for s in d.splitlines():
                yield {"line": unquote(s.strip())}
