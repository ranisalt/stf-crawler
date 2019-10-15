# -*- coding: utf-8 -*-
from urllib.parse import parse_qs, urlencode, urlparse, urlunparse

import scrapy
from scrapy.selector import Selector


class JurisSpider(scrapy.Spider):
    name = "juris"
    allowed_domains = ["stf.jus.br"]
    base_url = ""
    # output = []

    def __init__(self, url):
        scheme, netloc, path, params, query, *_ = urlparse(url)
        self.base_url = (scheme, netloc, path)
        self.query = parse_qs(query)

    def start_requests(self):
        yield self.make_requests_from_url(self.make_url(1))

    def make_url(self, page: int):
        return urlunparse(
            (
                *self.base_url,
                "",
                urlencode({**self.query, "pagina": page}, doseq=True),
                "",
            )
        )

    def parse(self, response):
        hxs = Selector(response)

        if len(hxs.xpath(u'//a[text()=" Próximo >>"]')) > 0:
            _, _, _, _, query, *_ = urlparse(response.url)
            page = int(parse_qs(query)["pagina"][0])
            yield self.make_requests_from_url(self.make_url(page + 1))

        doutrinas = hxs.xpath(
            u'//p[strong[text()="Doutrina"]]/following-sibling::pre/text()'
        ).extract()
        for d in doutrinas:
            for s in d.splitlines():
                yield {"line": s.strip()}
