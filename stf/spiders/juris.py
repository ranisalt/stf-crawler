import json
import math
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
from scrapy.http import JsonRequest, Response
from scrapy.selector import Selector

PER_PAGE = 150
MAX_RESULT = 10000


def make_params(query: str, page: int, date_from: str, date_to: str):
    date = {}
    if date_from:
        date["gte"] = date_from
    if date_to:
        date["lte"] = date_to

    from_ = page * PER_PAGE
    size = min(MAX_RESULT - from_, PER_PAGE)

    return {
        "query": {
            "function_score": {
                "functions": [
                    {
                        "exp": {
                            "julgamento_data": {
                                "origin": "now",
                                "scale": "47450d",
                                "offset": "1095d",
                                "decay": 0.1,
                            }
                        }
                    },
                    {
                        "filter": {"term": {"orgao_julgador.keyword": "Tribunal Pleno"}},
                        "weight": 1.15,
                    },
                    {
                        "filter": {"term": {"is_repercussao_geral": "true"}},
                        "weight": 1.1,
                    },
                ],
                "query": {
                    "bool": {
                        "filter": [
                            {
                                "query_string": {
                                    "default_operator": "AND",
                                    "fields": [
                                        "acordao_ata^3",
                                        "documental_acordao_mesmo_sentido_lista_texto",
                                        "documental_doutrina_texto",
                                        "documental_indexacao_texto",
                                        "documental_jurisprudencia_citada_texto",
                                        "documental_legislacao_citada_texto",
                                        "documental_observacao_texto",
                                        "documental_publicacao_lista_texto",
                                        "documental_tese_tema_texto^3",
                                        "documental_tese_texto^3",
                                        "ementa_texto^3",
                                        "ministro_facet",
                                        "revisor_processo_nome",
                                        "orgao_julgador",
                                        "partes_lista_texto",
                                        "procedencia_geografica_completo",
                                        "processo_classe_processual_unificada_extenso",
                                        "titulo^6",
                                        "colac_numero",
                                        "colac_pagina",
                                        "decisao_texto^2",
                                        "documental_decisao_mesmo_sentido_lista_texto",
                                        "processo_precedente_texto",
                                        "sumula_texto^3",
                                        "conteudo_texto",
                                    ],
                                    "query": query,
                                    "type": "cross_fields",
                                    "fuzziness": "AUTO:4,7",
                                    "analyzer": "legal_search_analyzer",
                                    "quote_analyzer": "standard_analyzer",
                                    "quote_field_suffix": ".standard",
                                }
                            },
                            {
                                "range": {
                                    "julgamento_data": {
                                        "format": "ddMMyyyy",
                                        **date,
                                    }
                                }
                            },
                        ],
                        "must": [],
                        "should": [
                            {
                                "query_string": {
                                    "default_operator": "AND",
                                    "fields": [
                                        "acordao_ata^3",
                                        "documental_doutrina_texto",
                                        "documental_indexacao_texto",
                                        "documental_jurisprudencia_citada_texto",
                                        "documental_observacao_texto",
                                        "documental_tese_tema_texto^3",
                                        "documental_tese_texto^3",
                                        "ementa_texto^3",
                                        "titulo^6",
                                        "decisao_texto^2",
                                        "sumula_texto^3",
                                        "conteudo_texto",
                                    ],
                                    "query": query,
                                    "tie_breaker": 1,
                                    "fuzziness": "AUTO:4,7",
                                    "analyzer": "legal_search_analyzer",
                                    "quote_analyzer": "standard_analyzer",
                                    "quote_field_suffix": ".standard",
                                }
                            },
                            {
                                "query_string": {
                                    "default_operator": "and",
                                    "type": "phrase",
                                    "tie_breaker": 1,
                                    "phrase_slop": 20,
                                    "fields": [
                                        "acordao_ata^3",
                                        "documental_tese_tema_texto^3",
                                        "documental_tese_texto^3",
                                        "ementa_texto^3",
                                        "decisao_texto^2",
                                        "conteudo_texto",
                                    ],
                                    "query": query,
                                    "fuzziness": "AUTO:4,7",
                                    "analyzer": "legal_search_analyzer",
                                    "quote_analyzer": "standard_analyzer",
                                    "quote_field_suffix": ".standard",
                                }
                            },
                            {
                                "query_string": {
                                    "default_operator": "and",
                                    "type": "phrase",
                                    "tie_breaker": 1,
                                    "phrase_slop": 5,
                                    "fields": [
                                        "documental_acordao_mesmo_sentido_lista_texto",
                                        "documental_doutrina_texto",
                                        "documental_indexacao_texto",
                                        "documental_jurisprudencia_citada_texto",
                                        "documental_legislacao_citada_texto",
                                        "documental_observacao_texto",
                                        "partes_lista_texto",
                                        "processo_precedente_texto",
                                        "documental_decisao_mesmo_sentido_lista_texto",
                                    ],
                                    "query": query,
                                    "fuzziness": "AUTO:4,7",
                                    "analyzer": "legal_search_analyzer",
                                    "quote_analyzer": "standard_analyzer",
                                    "quote_field_suffix": ".standard",
                                }
                            },
                        ],
                    }
                },
            }
        },
        "_source": [
            # "base",
            # "_id",
            # "id",
            # "dg_unique",
            # "titulo",
            # "ministro_facet",
            # "procedencia_geografica_completo",
            # "procedencia_geografica_pais_sigla",
            # "procedencia_geografica_uf_sigla",
            # "procedencia_geografica_uf_extenso",
            # "processo_codigo_completo",
            # "processo_classe_processual_unificada_extenso",
            # "processo_classe_processual_unificada_classe_sigla",
            # "processo_classe_processual_unificada_incidente_sigla",
            # "processo_numero",
            # "julgamento_data",
            # "publicacao_data",
            # "is_decisao_presidencia",
            # "relator_processo_nome",
            # "presidente_nome",
            # "relator_decisao_nome",
            # "acordao_ata",
            # "decisao_texto",
            # "partes_lista_texto",
            # "acompanhamento_processual_url",
            # "dje_url",
            # "documental_publicacao_lista_texto",
            # "documental_decisao_mesmo_sentido_lista_texto",
            # "documental_decisao_mesmo_sentido_is_secundario",
            # "documental_legislacao_citada_texto",
            # "documental_indexacao_texto",
            # "documental_observacao_texto",
            "documental_doutrina_texto",
            # "externo_seq_objeto_incidente",
            # "dg_atualizado_em",
            # "informativo_nome",
            # "informativo_numero",
            # "informativo_url",
            # "periodo_inicio_data",
            # "periodo_fim_data",
            # "conteudo_texto",
            # "conteudo_html",
            # "processo_lista_texto",
            # "sumula_numero",
            # "orgao_julgador",
            # "is_vinculante",
            # "sumula_texto",
            # "processo_precedente_texto",
            # "processo_precedente_html",
            # "processo_classe_processual_unificada_sigla",
            # "is_questao_ordem",
            # "is_repercussao_geral_admissibilidade",
            # "is_repercussao_geral_merito",
            # "is_repercussao_geral",
            # "is_processo_antigo",
            # "is_colac",
            # "colac_numero",
            # "colac_pagina",
            # "revisor_processo_nome",
            # "relator_acordao_nome",
            # "julgamento_is_sessao_virtual",
            # "republicacao_data",
            # "ementa_texto",
            # "inteiro_teor_url",
            # "documental_acordao_mesmo_sentido_lista_texto",
            # "documental_acordao_mesmo_sentido_is_secundario",
            # "documental_jurisprudencia_citada_texto",
            # "documental_assunto_texto",
            # "documental_tese_tipo",
            # "documental_tese_texto",
            # "documental_tese_tema_texto",
            # "old_seq_colac",
            # "old_seq_repercussao_geral",
            # "old_seq_sjur",
        ],
        "aggs": {
            "base_agg": {
                "filters": {
                    "filters": {
                        "acordaos": {"match": {"base": "acordaos"}},
                        "sumulas": {"match": {"base": "sumulas"}},
                        "decisoes": {"match": {"base": "decisoes"}},
                        "informativos": {"match": {"base": "informativos"}},
                    }
                }
            },
            "is_repercussao_geral_agg": {
                "filters": {
                    "filters": {
                        "true": {"match": {"is_repercussao_geral": "true"}},
                        "false": {"match": {"is_repercussao_geral": "false"}},
                    }
                }
            },
            "is_repercussao_geral_admissibilidade_agg": {
                "filters": {
                    "filters": {
                        "true": {"match": {"is_repercussao_geral_admissibilidade": "true"}},
                        "false": {"match": {"is_repercussao_geral_admissibilidade": "false"}},
                    }
                }
            },
            "is_repercussao_geral_merito_agg": {
                "filters": {
                    "filters": {
                        "true": {"match": {"is_repercussao_geral_merito": "true"}},
                        "false": {"match": {"is_repercussao_geral_merito": "false"}},
                    }
                }
            },
            "is_questao_ordem_agg": {
                "filters": {
                    "filters": {
                        "true": {"match": {"is_questao_ordem": "true"}},
                        "false": {"match": {"is_questao_ordem": "false"}},
                    }
                }
            },
            "is_colac_agg": {
                "filters": {
                    "filters": {
                        "true": {"match": {"is_colac": "true"}},
                        "false": {"match": {"is_colac": "false"}},
                    }
                }
            },
            "orgao_julgador_agg": {
                "aggs": {
                    "orgao_julgador_agg": {
                        "terms": {
                            "field": "orgao_julgador.keyword",
                            "size": 200,
                            "execution_hint": "map",
                        }
                    }
                },
                "filter": {"bool": {"must": [{"term": {"base": "acordaos"}}]}},
            },
            "ministro_facet_agg": {
                "aggs": {
                    "ministro_facet_agg": {
                        "terms": {
                            "field": "ministro_facet.keyword",
                            "size": 200,
                            "execution_hint": "map",
                        }
                    }
                },
                "filter": {"bool": {"must": [{"term": {"base": "acordaos"}}]}},
            },
            "processo_classe_processual_unificada_classe_sigla_agg": {
                "aggs": {
                    "processo_classe_processual_unificada_classe_sigla_agg": {
                        "terms": {
                            "field": "processo_classe_processual_unificada_classe_sigla.keyword",
                            "size": 200,
                            "execution_hint": "map",
                        }
                    }
                },
                "filter": {"bool": {"must": [{"term": {"base": "acordaos"}}]}},
            },
            "procedencia_geografica_uf_sigla_agg": {
                "aggs": {
                    "procedencia_geografica_uf_sigla_agg": {
                        "terms": {
                            "field": "procedencia_geografica_uf_sigla",
                            "size": 200,
                            "execution_hint": "map",
                        }
                    }
                },
                "filter": {"bool": {"must": [{"term": {"base": "acordaos"}}]}},
            },
        },
        "size": size,
        "from": from_,
        "post_filter": {"bool": {"must": [{"term": {"base": "acordaos"}}], "should": []}},
        "sort": [{"_score": "desc"}],
        # "highlight": {
        #     "highlight_query": {
        #         "bool": {
        #             "filter": [
        #                 {
        #                     "query_string": {
        #                         "default_operator": "AND",
        #                         "fields": [
        #                             "acordao_ata^3",
        #                             "documental_acordao_mesmo_sentido_lista_texto",
        #                             "documental_doutrina_texto",
        #                             "documental_indexacao_texto",
        #                             "documental_jurisprudencia_citada_texto",
        #                             "documental_legislacao_citada_texto",
        #                             "documental_observacao_texto",
        #                             "documental_publicacao_lista_texto",
        #                             "documental_tese_tema_texto^3",
        #                             "documental_tese_texto^3",
        #                             "ementa_texto^3",
        #                             "ministro_facet",
        #                             "revisor_processo_nome",
        #                             "orgao_julgador",
        #                             "partes_lista_texto",
        #                             "procedencia_geografica_completo",
        #                             "processo_classe_processual_unificada_extenso",
        #                             "titulo^6",
        #                             "colac_numero",
        #                             "colac_pagina",
        #                             "decisao_texto^2",
        #                             "documental_decisao_mesmo_sentido_lista_texto",
        #                             "processo_precedente_texto",
        #                             "sumula_texto^3",
        #                             "conteudo_texto",
        #                         ],
        #                         "query": query,
        #                         "type": "cross_fields",
        #                         "fuzziness": "AUTO:4,7",
        #                         "analyzer": "legal_search_analyzer",
        #                         "quote_analyzer": "standard_analyzer",
        #                         "quote_field_suffix": ".standard",
        #                     }
        #                 }
        #             ],
        #             "must": [],
        #             "should": [
        #                 {
        #                     "query_string": {
        #                         "default_operator": "AND",
        #                         "fields": [
        #                             "acordao_ata^3",
        #                             "documental_doutrina_texto",
        #                             "documental_indexacao_texto",
        #                             "documental_jurisprudencia_citada_texto",
        #                             "documental_observacao_texto",
        #                             "documental_tese_tema_texto^3",
        #                             "documental_tese_texto^3",
        #                             "ementa_texto^3",
        #                             "titulo^6",
        #                             "decisao_texto^2",
        #                             "sumula_texto^3",
        #                             "conteudo_texto",
        #                         ],
        #                         "query": query,
        #                         "tie_breaker": 1,
        #                         "fuzziness": "AUTO:4,7",
        #                         "analyzer": "legal_search_analyzer",
        #                         "quote_analyzer": "standard_analyzer",
        #                         "quote_field_suffix": ".standard",
        #                     }
        #                 },
        #                 {
        #                     "query_string": {
        #                         "default_operator": "and",
        #                         "type": "phrase",
        #                         "tie_breaker": 1,
        #                         "phrase_slop": 20,
        #                         "fields": [
        #                             "acordao_ata^3",
        #                             "documental_tese_tema_texto^3",
        #                             "documental_tese_texto^3",
        #                             "ementa_texto^3",
        #                             "decisao_texto^2",
        #                             "conteudo_texto",
        #                         ],
        #                         "query": query,
        #                         "fuzziness": "AUTO:4,7",
        #                         "analyzer": "legal_search_analyzer",
        #                         "quote_analyzer": "standard_analyzer",
        #                         "quote_field_suffix": ".standard",
        #                     }
        #                 },
        #                 {
        #                     "query_string": {
        #                         "default_operator": "and",
        #                         "type": "phrase",
        #                         "tie_breaker": 1,
        #                         "phrase_slop": 5,
        #                         "fields": [
        #                             "documental_acordao_mesmo_sentido_lista_texto",
        #                             "documental_doutrina_texto",
        #                             "documental_indexacao_texto",
        #                             "documental_jurisprudencia_citada_texto",
        #                             "documental_legislacao_citada_texto",
        #                             "documental_observacao_texto",
        #                             "partes_lista_texto",
        #                             "processo_precedente_texto",
        #                             "documental_decisao_mesmo_sentido_lista_texto",
        #                         ],
        #                         "query": query,
        #                         "fuzziness": "AUTO:4,7",
        #                         "analyzer": "legal_search_analyzer",
        #                         "quote_analyzer": "standard_analyzer",
        #                         "quote_field_suffix": ".standard",
        #                     }
        #                 },
        #             ],
        #         }
        #     },
        #     "number_of_fragments": 64,
        #     "fragment_size": 300,
        #     "order": "score",
        #     "pre_tags": ["<em>"],
        #     "post_tags": ["</em>"],
        #     "fields": {
        #         "ementa_texto": {
        #             "fragment_size": 2400,
        #             "matched_fields": ["ementa_texto", "ementa_texto.standard"],
        #             "type": "fvh",
        #         },
        #         "sumula_texto": {
        #             "number_of_fragments": 0,
        #             "matched_fields": ["sumula_texto", "sumula_texto.standard"],
        #             "type": "fvh",
        #         },
        #         "conteudo_texto": {
        #             "fragment_size": 1200,
        #             "matched_fields": ["conteudo_texto", "conteudo_texto.standard"],
        #             "type": "fvh",
        #         },
        #         "acordao_ata": {
        #             "fragment_size": 600,
        #             "matched_fields": ["acordao_ata", "acordao_ata.standard"],
        #             "type": "fvh",
        #         },
        #         "decisao_texto": {
        #             "fragment_size": 1200,
        #             "matched_fields": ["decisao_texto", "decisao_texto.standard"],
        #             "type": "fvh",
        #         },
        #         "documental_tese_texto": {
        #             "fragment_size": 2000,
        #             "matched_fields": [
        #                 "documental_tese_texto",
        #                 "documental_tese_texto.standard",
        #             ],
        #             "type": "fvh",
        #         },
        #         "documental_tese_tema_texto": {
        #             "fragment_size": 2000,
        #             "matched_fields": [
        #                 "documental_tese_tema_texto",
        #                 "documental_tese_tema_texto.standard",
        #             ],
        #             "type": "fvh",
        #         },
        #         "documental_observacao_texto": {
        #             "matched_fields": [
        #                 "documental_observacao_texto",
        #                 "documental_observacao_texto.standard",
        #             ],
        #             "type": "fvh",
        #         },
        #         "documental_indexacao_texto": {
        #             "matched_fields": [
        #                 "documental_indexacao_texto",
        #                 "documental_indexacao_texto.standard",
        #             ],
        #             "type": "fvh",
        #         },
        #         "documental_legislacao_citada_texto": {
        #             "matched_fields": [
        #                 "documental_legislacao_citada_texto",
        #                 "documental_legislacao_citada_texto.standard",
        #             ],
        #             "type": "fvh",
        #         },
        #         "documental_jurisprudencia_citada_texto": {
        #             "matched_fields": [
        #                 "documental_jurisprudencia_citada_texto",
        #                 "documental_jurisprudencia_citada_texto.standard",
        #             ],
        #             "type": "fvh",
        #         },
        #         "documental_doutrina_texto": {
        #             "matched_fields": [
        #                 "documental_doutrina_texto",
        #                 "documental_doutrina_texto.standard",
        #             ],
        #             "type": "fvh",
        #         },
        #         "partes_lista_texto": {
        #             "matched_fields": [
        #                 "partes_lista_texto",
        #                 "partes_lista_texto.standard",
        #             ],
        #             "type": "fvh",
        #         },
        #         "documental_publicacao_lista_texto": {
        #             "matched_fields": [
        #                 "documental_publicacao_lista_texto",
        #                 "documental_publicacao_lista_texto.standard",
        #             ],
        #             "type": "fvh",
        #         },
        #         "documental_acordao_mesmo_sentido_lista_texto": {
        #             "matched_fields": [
        #                 "documental_acordao_mesmo_sentido_lista_texto",
        #                 "documental_acordao_mesmo_sentido_lista_texto.standard",
        #             ],
        #             "type": "fvh",
        #         },
        #         "documental_decisao_mesmo_sentido_lista_texto": {
        #             "matched_fields": [
        #                 "documental_decisao_mesmo_sentido_lista_texto",
        #                 "documental_decisao_mesmo_sentido_lista_texto.standard",
        #             ],
        #             "type": "fvh",
        #         },
        #         "processo_precedente_texto": {
        #             "matched_fields": [
        #                 "processo_precedente_texto",
        #                 "processo_precedente_texto.standard",
        #             ],
        #             "type": "fvh",
        #         },
        #         "procedencia_geografica_completo": {
        #             "matched_fields": [
        #                 "procedencia_geografica_completo",
        #                 "procedencia_geografica_completo.standard",
        #             ],
        #             "type": "fvh",
        #         },
        #     },
        # },
        # "track_total_hits": "true",
    }


double_nl = re.compile("\r?\n\r?\n")
nl = re.compile("\r?\n")


class JurisSpider(Spider):
    name = "juris"
    allowed_domains = ["jurisprudencia.stf.jus.br"]
    base_url = "https://jurisprudencia.stf.jus.br/api/search/search"

    def __init__(
        self,
        query: str,
        date_from: str = "",
        date_to: str = "",
        *args,
        **kwargs,
    ):
        super().__init__(*args, **kwargs)
        self.query = query
        self.date_from = "".join(reversed(date_from.split("-")))
        self.date_to = "".join(reversed(date_to.split("-")))

    def make_params(self, page: int):
        return make_params(self.query, page, self.date_from, self.date_to)

    def start_requests(self):
        def parse_start_url(res: Response):
            total_hits = res.json().get("result", {}).get("hits", {}).get("total", {}).get("value")
            for i in range(0, math.ceil(total_hits / PER_PAGE)):
                yield JsonRequest(url=f"{self.base_url}#page={i}", data=self.make_params(i), errback=self.error)

        yield JsonRequest(
            url=self.base_url,
            callback=parse_start_url,
            data={
                **self.make_params(0),
                # "track_total_hits": "true"
            },
        )

    def error(self, err):
        breakpoint()

    def parse(self, res):
        hits = res.json().get("result", {}).get("hits", {}).get("hits", [])
        for hit in hits:
            item = hit.get("_source", {}).get("documental_doutrina_texto")
            if item:
                entries = [entry.strip() for entry in double_nl.split(item)]
                yield {"lines": entries}
