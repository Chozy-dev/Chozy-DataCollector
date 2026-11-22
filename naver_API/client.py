"""Separate SearchAd HMAC and API HUB credentials. No hidden HTTP retries."""
import base64
import hashlib
import hmac
import os
import time
import requests
from product_data_collector.common.rate_limiter import IntervalRateLimiter


class NaverAPIError(RuntimeError):
    pass


class NaverRateLimited(NaverAPIError):
    pass


class NaverClient:
    def __init__(self, ad_key, ad_secret, customer, hub_id, hub_key, config, session=None):
        self.ad_key, self.ad_secret, self.customer = ad_key, ad_secret, customer
        self.hub_id, self.hub_key = hub_id, hub_key
        self.session = session or requests.Session()
        self.ad_limiter = IntervalRateLimiter(config.ad_interval_seconds)
        self.trend_limiter = IntervalRateLimiter(config.trend_interval_seconds)

    @classmethod
    def from_env(cls, config):
        names = ('NAVER_AD_API_KEY', 'NAVER_AD_SECRET_KEY', 'NAVER_AD_CUSTOMER_ID', 'NAVER_HUB_KEY_ID', 'NAVER_HUB_KEY')
        if not all(os.environ.get(n) for n in names):
            raise ValueError('Missing NAVER SearchAd or HUB credentials')
        return cls(*(os.environ[n] for n in names), config)

    def _request(self, method, url, **kwargs):
        try:
            response = self.session.request(method, url, timeout=(10, 60), allow_redirects=False, **kwargs)
        except requests.RequestException:
            raise NaverAPIError('Naver transport failure') from None
        if response.status_code == 429:
            raise NaverRateLimited('Naver rate limited; stop and resume after review')
        if response.status_code != 200:
            raise NaverAPIError('Naver HTTP failure')
        try:
            data = response.json()
        except ValueError:
            raise NaverAPIError('Naver invalid JSON') from None
        if not isinstance(data, dict) or any(k in data for k in ('error', 'errorCode', 'errMsg', 'errId')):
            raise NaverAPIError('Naver provider error')
        return data

    def keywords(self, event, month):
        self.ad_limiter.wait()
        stamp, path = str(int(time.time() * 1000)), '/keywordstool'
        signature = base64.b64encode(hmac.new(self.ad_secret.encode(), f'{stamp}.GET.{path}'.encode(), hashlib.sha256).digest()).decode()
        return self._request('GET', 'https://api.searchad.naver.com' + path,
            headers={'X-Timestamp': stamp, 'X-API-KEY': self.ad_key, 'X-Customer': self.customer, 'X-Signature': signature},
            params={'event': event, 'month': month, 'showDetail': 1})

    def trend(self, body):
        self.trend_limiter.wait()
        return self._request('POST', 'https://naverapihub.apigw.ntruss.com/search-trend/v1/search',
            headers={'X-NCP-APIGW-API-KEY-ID': self.hub_id, 'X-NCP-APIGW-API-KEY': self.hub_key, 'Content-Type': 'application/json'}, json=body)
