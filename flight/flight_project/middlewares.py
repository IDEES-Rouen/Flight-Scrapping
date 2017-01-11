from scrapy.conf import settings


# usage of privoxy
class ProxyMiddleware(object):
    def process_request(self, request, spider):
        request.meta['proxy'] = settings.get('HTTPS_PROXY')