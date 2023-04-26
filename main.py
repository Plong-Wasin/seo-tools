import requests
from sklearn.feature_extraction.text import TfidfVectorizer
from sklearn.metrics.pairwise import cosine_similarity
import time
import asyncio
import aiohttp
from urllib.parse import urljoin, urldefrag, unquote, quote, urlsplit
import urllib.robotparser
import logging
import tracemalloc
import pandas as pd
from hashlib import md5
import cProfile
import pstats
from lxml import etree, cssselect, html
from protego import Protego
import json


class Parser():
    def __init__(self, html_string) -> None:
        self.dochtml = html.fromstring(html_string)

    def parse(self):
        select = cssselect.CSSSelector(
            'a:not(a[rel="nofollow"])')
        # select = cssselect.CSSSelector(
        #     'a[href]:not(a[rel="nofollow"],[href^="javascript:"])')
        urls = [el.get('href') for el in select(self.dochtml)]
        select = cssselect.CSSSelector('[src]:not(form)')
        urls.extend(el.get('src') for el in select(self.dochtml))
        urls = list(
            filter(lambda x: x and not x.startswith('javascript:'), urls))
        return urls

    def get_canonical_link(self):
        """Find the canonical link element on the page and return its href attribute.

        Returns:
            The href attribute of the first 'link' element with a 'canonical' 'rel'
            attribute on the page, or False if there is no such element.

        """
        element = cssselect.CSSSelector(
            'link[rel="canonical"]')
        if element is None:
            return False
        return element.get('href')

    def is_follow(self):
        """
        Check if the page should be followed by search engine bots.

        :return: True if the page should be followed, False if it should not.
        :rtype: bool
        """
        select = cssselect.CSSSelector(
            'meta[name="robots"][content*="nofollow"]')
        element = select(self.dochtml)
        if element:
            return False
        return True

    def is_index(self):
        """Check if the page should be indexed by search engines.

        This function looks for a <meta> tag with the attribute name="robots"
        and content containing the string "noindex". If such a tag is found
        in the document represented by the `dochtml` instance variable, this
        function returns False, indicating that the page should not be indexed.
        Otherwise, it returns True.

        Returns:
            bool: True if the page should be indexed, False otherwise.
        """
        select = cssselect.CSSSelector(
            'meta[name="robots"][content*="noindex"]')
        element = select(self.dochtml)
        if element:
            return False
        return True


class UrlParser:
    """
    A class for parsing URLs into their component parts.
    """
    @ staticmethod
    def get_root_url(url):
        """
        Given a URL, returns the root URL by extracting the scheme and netloc.

        :param url: A string representing the URL to extract the root from.
        :return: A string representing the root URL.
        """
        return urlsplit(url).scheme + '://' + urlsplit(url).netloc

    @ staticmethod
    def encode_url_once(url):
        """
        Encodes the given URL only once, if it is not already encoded.

        Args:
            url (str): The URL to be encoded.

        Returns:
            str: The encoded URL.

        """
        parser = UrlParser()
        is_encoded = parser.is_url_already_encoded(url)
        if is_encoded:
            return url
        return parser.encode_url(url)

    @ staticmethod
    def encode_url(url):
        """
        Encodes a given URL by replacing non-ASCII characters in the domain component
        with their IDNA encoding and encoding the path component while preserving the
        path segments.

        Args:
            url (str): The URL to encode.

        Returns:
            str: The encoded URL.
        """
        url_components = urlsplit(url)
        domain = url_components.netloc
        path = url_components.path

        # Encode the non-ASCII characters in the domain component using the idna encoding method
        encoded_domain = domain.encode('idna').decode()

        # Encode the path component while preserving the path segments
        encoded_path = "/".join(quote(segment) for segment in path.split("/"))

        # Replace the domain and path components in the URL with the encoded versions
        encoded_url = url.replace(
            domain, encoded_domain).replace(path, encoded_path)
        return encoded_url

    @ staticmethod
    def decode_url(url):
        """
        Decodes a given URL by replacing Punycode-encoded domain and URL-encoded path components.

        Args:
            url (str): The URL to decode.

        Returns:
            str: The decoded URL.
        """
        if '%' not in url:
            return url
        url_components = urlsplit(url)
        domain = url_components.netloc
        path = url_components.path

        # Decode the Punycode-encoded domain component using the idna decoding method
        decoded_domain = domain.encode().decode('idna')

        # Decode the URL-encoded path component
        decoded_path = "/".join(unquote(segment)
                                for segment in path.split("/"))

        # Replace the domain and path components in the URL with the decoded versions
        decoded_url = url.replace(
            domain, decoded_domain).replace(path, decoded_path)
        return decoded_url

    @ staticmethod
    def is_url_already_encoded(url):
        """
        Check if the given URL is already URL encoded.

        Args:
            url (str): The URL to check for encoding.

        Returns:
            bool: True if the URL is already encoded, False otherwise.
        """
        decoded_text = unquote(url)
        if url == decoded_text:
            return False
        return True


class Crawler:
    # format_processors = {
    #     'xml': XMLWriter,
    #     'txt': TextWriter
    # }

    robots_txt = {}
    exclude_urls = []

    def __init__(
        self, url: str, out_file: str = 'sitemap.xml', out_format: str = 'xml',
        maxtasks: int = 10, todo_queue_backend=set, done_backend=dict,
        http_request_options=None, limit=0,
    ):
        """
        Crawler constructor
        :param rooturl: root url of site
        :type rooturl: str
        :param out_file: file to save sitemap result
        :type out_file: str
        :param out_format: sitemap type [xml | txt]. Default xml
        :type out_format: str
        :param maxtasks: maximum count of tasks. Default 100
        :type maxtasks: int
        """
        self.url = UrlParser.decode_url(url)
        self.rooturl = f'{urlsplit(url).scheme}://{urlsplit(url).netloc}'
        self.todo_queue = todo_queue_backend()
        self.busy = dict()
        self.done = pd.DataFrame(columns=['url',
                                          'indexability',
                                          'indexability_status',
                                          'response_code',
                                          'content_type',
                                          'response_time',
                                          'hash'])
        self.tasks = set()
        self.sem = asyncio.Semaphore(maxtasks)
        self.http_request_options = http_request_options or {}
        self.http_request_options['allow_redirects'] = False
        self.links = list()
        self.errors = set()
        # connector stores cookies between requests and uses connection pool
        self.session = aiohttp.ClientSession()
        self.limit = limit
        self.check_done = set()
        # self.writer = self.format_processors.get(out_format)(out_file)

    def set_parser(self, parser_class):
        self.parser = parser_class

    def is_done(self, url):
        for d in self.done.values():
            if (url == d['url'] or url == d['url_decode']):
                return True
        return False

    async def run(self):
        """
        Main function to start parsing site
        :return:
        """
        t = asyncio.ensure_future(self.addurls([(self.url, self.rooturl)]))
        await asyncio.sleep(1)
        busy = dict()
        count = 0
        while self.busy and count < 300:
            await asyncio.sleep(1)
            if self.busy == busy:
                count += 1
            else:
                count = 0
                busy = self.busy.copy()
        await t
        await self.session.close()
        for k, v in self.busy.items():
            self.append_done(url=k, indexability=False,
                             indexability_status="Always busy")
        self.done.to_csv('export.csv', index=False)
        self.done.reset_index().to_feather('export.ftr')
        df = pd.DataFrame(self.links, columns=['from', 'to'])
        df.to_csv('links.csv', index=False)

    def check_allow_crawl(self, url):
        """
        Check if crawling the given URL is allowed, according to the robots.txt file of the site it belongs to.
        Returns True if crawling is allowed, False otherwise.
        """
        try:
            root_url = UrlParser.get_root_url(url)
            if self.robots_txt.get(root_url) is None:
                response = requests.get(
                    f'{root_url}/robots.txt', timeout=1, allow_redirects=False)
                if not response.ok:
                    self.robots_txt[root_url] = True
                    return True
                self.robots_txt[root_url] = response.text
                rp = Protego.parse(response.text)
            elif self.robots_txt[root_url] is True:
                return True
            rp = Protego.parse(self.robots_txt[root_url])
            return rp.can_fetch('*', url)
        except Exception:
            return True

    # async def check_allow_crawl(self, url):
    #     """
    #     Check if crawling the given URL is allowed, according to the robots.txt file of the site it belongs to.
    #     Returns True if crawling is allowed, False otherwise.
    #     """
    #     try:
    #         root_url = UrlParser.get_root_url(url)
    #         if self.rp.get(root_url) is None:
    #             response = await self.session.get(
    #                 f'{root_url}/robots.txt', timeout=1, allow_redirects=False)
    #             if not response.ok:
    #                 self.rp[root_url] = True
    #                 return True
    #             rp = Protego.parse(response.text)
    #             self.rp[root_url] = rp
    #         elif self.rp[root_url] is True:
    #             return True
    #         rp = self.rp.get(UrlParser().get_root_url(url))
    #         return rp.can_fetch('*', url)
    #     except Exception as exc:
    #         print(exc)
    #         return True

    # def check_allow_crawl_sync(self, url):
    #     return asyncio.Task(self.check_allow_crawl(url))

    async def addurls(self, urls, force=False):
        """
        Add urls in queue and run process to parse
        :param urls:
        :return:
        """
        for url, parenturl in urls:
            url = UrlParser.decode_url(url)
            parenturl = UrlParser.decode_url(parenturl)
            if not (url.startswith('https://') or url.startswith('http://')):
                url = urljoin(parenturl, url)
            url, frag = urldefrag(url)
            self.links.append([parenturl, url])
            if (
                (
                    parenturl.startswith(self.rooturl) or
                    url.startswith(self.rooturl)
                ) and
                not any(exclude_part in url for exclude_part in self.exclude_urls) and
                url not in self.busy and
                url not in self.check_done and
                url not in self.todo_queue and
                (len(self.check_done) + len(self.busy) + len(self.todo_queue)
                 <= self.limit or not self.limit)
                or force
            ):
                if self.check_allow_crawl(url):
                    self.todo_queue.add(url)
                    # Acquire semaphore
                    await self.sem.acquire()
                    # Create async task
                    task = asyncio.ensure_future(self.process(url))
                    # Add collback into task to release semaphore
                    task.add_done_callback(lambda t: self.sem.release())
                    # Callback to remove task from tasks
                    task.add_done_callback(self.tasks.remove)
                    # Add task into tasks
                    self.tasks.add(task)

                else:
                    self.append_done(
                        url=url, indexability_status="Block by robots.txt")

    async def process(self, url):
        """
        Process single url
        :param url:
        :return:
        """
        print('remaining: ', len(self.todo_queue))
        print('processing:', unquote(url))
        # remove url from basic queue and add it into busy list
        if url in self.busy:
            self.busy[url] += 1
        else:
            self.busy[url] = 1
        if url in self.todo_queue:
            self.todo_queue.remove(url)
        try:
            # await response
            start = time.time()
            resp = await self.session.get(url, **self.http_request_options)
            stop = time.time()
        except asyncio.exceptions.TimeoutError as exc:
            if url not in self.errors:
                self.errors.add(url)
                self.enqueue_url(url)
            else:
                self.append_done(
                    url=url, indexability_status=type(exc).__name__)
            # get response url
        except Exception as exc:
            print(exc)
            # on any exception mark url as BAD
            print('...', url, 'has error', repr(str(exc)))
            error_msg = repr(str(exc)) if repr(
                str(exc)) else type(exc).__name__
            self.append_done(
                url=url, indexability_status=error_msg)
        else:
            # only url with status == 200 and content type == 'text/html' parsed
            if (resp.status == 200 and
                    ('text/html' in resp.headers.get('content-type'))
                    and str(url).startswith(self.rooturl)):
                data = (await resp.read()).decode('utf-8', 'replace')
                parser = Parser(data)
                urls = parser.parse()
                if parser.is_follow():
                    addurls = self.addurls([(u, url) for u in urls])
                    asyncio.Task(addurls)
                indexability_status = None
                is_index = parser.is_index()
                if not is_index:
                    indexability_status = 'noindex'
                self.append_done(resp,
                                 url,
                                 indexability=is_index,
                                 indexability_status=indexability_status,
                                 hash_val=md5(data.encode()).hexdigest(),
                                 response_time=stop-start,
                                 dom=parser)
            elif 300 <= resp.status < 400:
                redirect_url = resp.headers.get('location')
                self.append_done(response=resp, url=url,
                                 indexability=False,
                                 indexability_status=resp.reason,
                                 response_time=stop-start)
                if redirect_url:
                    self.add_redirect_url(url, redirect_url)
            elif resp.status > 400:
                self.append_done(response=resp,
                                 url=url,
                                 indexability=False,
                                 indexability_status=resp.reason,
                                 response_time=stop-start)
            else:
                self.append_done(resp,
                                 url,
                                 indexability=True,
                                 response_time=stop-start)

            # even if we have no exception, we can mark url as good
            resp.close()
        try:
            self.busy[url] -= 1
            if not self.busy[url]:
                del self.busy[url]
        except Exception as exc:
            print(url)
        logging.info(len(self.done), 'completed tasks,', len(self.tasks),
                     'still pending, todo_queue', len(self.todo_queue))

    def add_redirect_url(self, url, redirect_url):
        addurls = self.addurls([(redirect_url, url)])
        asyncio.Task(addurls)

    def enqueue_url(self, url):
        """
        Adds a URL to the list of URLs to be processed.

        Args:
            url (str): The URL to be added.
        """
        addurls = self.addurls([('', url)], True)
        asyncio.Task(addurls)

    def append_done(self,
                    response=None,
                    url: str = '',
                    indexability: bool = False,
                    indexability_status: str = None,
                    hash_val: str = None,
                    response_time: float = None,
                    dom=None):
        """
        Append a row to the `done` dataframe with information about a URL that has been processed.
        """
        url_decoded = UrlParser.decode_url(url)
        self.check_done.add(url_decoded)
        self.done = pd.concat([self.done, pd.DataFrame({
            "url": url_decoded,
            "indexability": indexability,
            "indexability_status": indexability_status,
            "content_type": response.headers.get('content-type') if response else None,
            "response_code": response.status if response else None,
            "hash": hash_val,
            "response_time": response_time,
            "redirected_url": response.headers.get('location') if response else None,
        }, index=[0])])

    def set_exclude_url(self, urls_list):
        self.exclude_urls = urls_list

    def runsync(self):
        """
        Runs the `run` coroutine synchronously using an event loop.
        """
        loop = asyncio.get_event_loop()
        loop.run_until_complete(self.run())


profiler = cProfile.Profile()
profiler.enable()

tracemalloc.start()
c = Crawler('https://teroasia.com/',
            limit=100, http_request_options={"timeout": 60}, maxtasks=100)
try:
    with open('robots.txt.json', 'r', encoding="utf-8") as f:
        c.robots_txt = json.load(f)
except FileNotFoundError:
    pass
c.runsync()
with open('robots.txt.json', 'w', encoding="utf-8") as f:
    json.dump(c.robots_txt, f)
mem = tracemalloc.get_traced_memory()
# print as mb
print(f"Memory usage: {mem[0] / 10**6} MB")
print(f"Memory peak: {mem[1] / 10**6} MB")
# stopping the library
tracemalloc.stop()
profiler.disable()
stats = pstats.Stats(profiler)

stats.dump_stats("results.prof")
