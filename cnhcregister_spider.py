import datetime
import time
import csv
import re
import scrapy
import hashlib
import requests
import traceback
from random import randint
from lxml.html import fromstring
from scrapy.crawler import CrawlerProcess
import copy


PROXY = '125.27.10.209:59790'


class ExtractItem(scrapy.Item):
    name = scrapy.Field()
    registration = scrapy.Field()
    telephone = scrapy.Field()
    address = scrapy.Field()
    discipline = scrapy.Field()
    website = scrapy.Field()


class CNHCRegisterSpider(scrapy.Spider):
    name = "cnhcregister_spider"
    allowed_domains = ["cnhcregister.org.uk"]
    headers = {
        'user-agent': 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_14_2) '
                      'AppleWebKit/537.36 (KHTML, like Gecko) '
                      'Chrome/72.0.3626.119 Safari/537.36',
    }
    params = {
        "int_NbSearchResultsShown": "50",
        "int_NbPageResultsShown": "50",
        "sName": "a",
        "sTownCity": "",
        "nTherapy": "0",
        "nDistance": "",
        "sPostcode": "",
        "btnSubmit": "Search"
    }
    url = "https://www.cnhcregister.org.uk/newsearch/index.cfm"

    def start_requests(self,):
        yield scrapy.FormRequest(
            url=self.url,
            formdata=self.params,
            callback=self.parse,
            headers=self.headers
        )

    def parse(self, response):
        results = response.xpath(
            '//div[contains(@class, "searchResultLine1")]')
        for result in results:
            item = ExtractItem()
            name = result.xpath(
                'div/span[@class="registrantName"]/text()').extract_first()
            item['name'] = name

            registration = result.xpath(
                'div[strong[text()="Registration:"]]/'
                '/text()').extract()
            item['registration'] = registration[-1].strip()\
                if registration else ""

            discipline = result.xpath(
                'div[strong[text()="Disciplines:"]]/text()').extract()
            item['discipline'] = discipline[-1].strip() if discipline else ""

            address = result.xpath(
                'following-sibling::div[1][contains(@class, '
                '"searchResultLine2")]/div[contains(@class,"addressLine")]/'
                'text()').extract()
            item['address'] = '; '.join([
                i.replace('\n', '').replace('\t', '').strip()
                for i in address if 'ADDRESS DETAIL' not in i
            ])

            telephone = result.xpath(
                'following-sibling::div[1][contains(@class, '
                '"searchResultLine2")]/div[contains(text(), "Phone :")]'
                '/text()').re(r'Phone : (.*)')
            item['telephone'] = '; '.join(telephone) if telephone else ""

            website = result.xpath(
                'following-sibling::div[1][contains(@class, '
                '"searchResultLine2")]//a[not(text()="View on a Map")]'
                '/@href').extract()
            item['website'] = '; '.join(website)
            yield item

        load_more = response.xpath('//input[@id="btnLoadMore"]')
        if load_more:
            paginated_value = int(self.params["int_NbSearchResultsShown"]) + 50
            self.params.update({
                "int_NbSearchResultsShown": str(paginated_value)
            })
            print("self.params")
            print(self.params['int_NbSearchResultsShown'])
            yield scrapy.FormRequest(
                url=self.url,
                formdata=self.params,
                callback=self.parse,
                headers=self.headers,

            )


def run_spider(no_of_threads, request_delay):
    settings = {
        "DOWNLOADER_MIDDLEWARES": {
            'scrapy.downloadermiddlewares.useragent.UserAgentMiddleware': None,
            'scrapy_fake_useragent.middleware.RandomUserAgentMiddleware': 400,
            'scrapy.downloadermiddlewares.retry.RetryMiddleware': 90,
            'rotating_proxies.middlewares.RotatingProxyMiddleware': 610,
            'rotating_proxies.middlewares.BanDetectionMiddleware': 620,
        },
        'ITEM_PIPELINES': {
            'pipelines.ExtractPipeline': 300,
        },
        'DOWNLOAD_DELAY': request_delay,
        'CONCURRENT_REQUESTS': no_of_threads,
        'CONCURRENT_REQUESTS_PER_DOMAIN': no_of_threads,
        'RETRY_HTTP_CODES': [403, 429, 500, 503],
        'ROTATING_PROXY_LIST': PROXY,
        'ROTATING_PROXY_BAN_POLICY': 'pipelines.BanPolicy',
        'RETRY_TIMES': 10,
        'LOG_ENABLED': True,

    }
    process = CrawlerProcess({
        'ITEM_PIPELINES': {
            'pipelines.ExtractPipeline': 300,
        },
        'DOWNLOAD_DELAY': request_delay,
        'CONCURRENT_REQUESTS': no_of_threads,
        'CONCURRENT_REQUESTS_PER_DOMAIN': no_of_threads,
    })
    process.crawl(CNHCRegisterSpider)
    process.start()

if __name__ == '__main__':
    no_of_threads = 40
    request_delay = 0.1
    run_spider(no_of_threads, request_delay)
