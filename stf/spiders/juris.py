# -*- coding: utf-8 -*-
import scrapy
from scrapy.selector import Selector


class JurisSpider(scrapy.Spider):
    name = "juris"
    allowed_domains = ["stf.jus.br"]
    base_url = ''
    output = []

    def __init__(self, flt):
        self.base_url = 'http://www.stf.jus.br/portal/jurisprudencia/listarJurisprudencia.asp?s1=%s&pagina=%s&base=baseAcordaos' % (flt, '%d')
        self.start_urls = (
            self.base_url % (1, ),
        )
        self.page = 1

    def parse(self, response):
        hxs = Selector(response)

        if len(hxs.xpath(u'//a[text()=" Próximo >>"]')) > 0:
            self.page += 1
            yield self.make_requests_from_url(self.base_url % self.page)

        doutrinas = hxs.xpath(u'//p[strong[text()="Doutrina"]]/following-sibling::pre/text()').extract()
        for d in doutrinas:
            for s in d.splitlines():
                yield {
                    'line': s.strip(),
                }
