# -*- coding: utf-8 -*-

# Define your item pipelines here
#
# Don't forget to add your pipeline to the ITEM_PIPELINES setting
# See: http://doc.scrapy.org/en/latest/topics/item-pipeline.html

from __future__ import print_function


class StfPipeline(object):
    def process_item(self, item, spider):
        spider.output.append(item['line'].encode('utf8'))
        return item
