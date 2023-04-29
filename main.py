from sklearn.feature_extraction.text import TfidfVectorizer
from sklearn.metrics.pairwise import cosine_similarity
import time
import asyncio
import aiohttp
from urllib.parse import urljoin, urldefrag, unquote, quote, urlsplit
import logging
import tracemalloc
import pandas as pd
from hashlib import md5
import cProfile
import pstats
from lxml import cssselect, html
from protego import Protego
import json
from icecream import ic


class Parser():
    def __init__(self, html_string) -> None:
        """
        Initialize a new instance of the class.

        Args:
            html_string (str): The HTML string to parse.

        Returns:
            None
        """
        self.dochtml = html.fromstring(html_string)

    def parse(self):
        """
        Parses urls from a given HTML document.

        Returns:
        - urls(list): A list of urls extracted from the HTML document.
        """
        select = cssselect.CSSSelector(
            'a:not(a[rel="nofollow"]),link[ref="canonical"]')
        urls = [el.get('href') for el in select(self.dochtml)]
        select = cssselect.CSSSelector('[src]:not(form)')
        urls.extend(el.get('src') for el in select(self.dochtml))
        urls = list(
            filter(lambda x: x and not x.startswith('javascript:'), urls))
        return urls

    def get_canonical_url(self):
        """Find the canonical link element on the page and return its href attribute.

        Returns:
            The href attribute of the first 'link' element with a 'canonical' 'rel'
            attribute on the page, or False if there is no such element.

        """
        select = cssselect.CSSSelector(
            'link[rel="canonical"]')
        element = select(self.dochtml)
        if not element:
            return None
        return UrlParser.decode_url(element[0].get('href'))

    def is_follow(self):
        """
        Check if the page should be followed by search engine bots.

        : return: True if the page should be followed, False if it should not.
        : rtype: bool
        """
        select = cssselect.CSSSelector(
            'meta[name="robots"][content*="nofollow"]')
        element = select(self.dochtml)
        if element:
            return False
        return True

    def is_index(self):
        """
        This function looks for a < meta > tag with the attribute name = "robots"
        and content containing the string "noindex". If such a tag is found
        in the document represented by the `dochtml` instance variable, this
        function returns False, indicating that the page should not be indexed.
        Otherwise, it returns True.
        """
        selector = cssselect.CSSSelector(
            'meta[name="robots"][content*="noindex"]')
        elements = selector(self.dochtml)
        if elements:
            return False
        return True

    def get_meta(self):
        """
        Extracts meta tags from self.dochtml and returns a dictionary.
        """
        select = cssselect.CSSSelector(
            'meta[name="robots"][content],title,meta[name="description"][content],meta[property^="og:"][content]')
        elements = select(self.dochtml)
        return_value = {}
        for element in elements:
            if element.tag == 'title':
                return_value['title'] = element.text
            else:
                key = element.get("property") or element.get("name")
                value = element.get("content")
                if value.startswith('http'):
                    value = UrlParser.decode_url(value)
                return_value[key] = value
        return return_value


class UrlParser:
    """
    A class for parsing URLs into their component parts.
    """
    @ staticmethod
    def get_root_url(url):
        """
    Given a URL, returns the root URL by extracting the scheme and netloc.

    : param url: A string representing the URL to extract the root from.
    : return: A string representing the root URL.
     """
        return urlsplit(url).scheme + '://' + urlsplit(url).netloc

    @ staticmethod
    def encode_url_once(url):
        """
      Encodes the given URL only once, if it is not already encoded.

       Args:
            url(str): The URL to be encoded.

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
            url(str): The URL to encode.

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
            url(str): The URL to decode.

        Returns:
            str: The decoded URL.
        """
        if '%' not in url:
            return url
        try:
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
        except UnicodeDecodeError:
            return url

    @ staticmethod
    def is_url_already_encoded(url):
        """
        Check if the given URL is already URL encoded.

        Args:
            url(str): The URL to check for encoding.

        Returns:
            bool: True if the URL is already encoded, False otherwise.
        """
        decoded_text = unquote(url)
        if url == decoded_text:
            return False
        return True


class Crawler:
    robots_txt = {}
    rp = {}
    exclude_urls = []
    session = None
    done_df = pd.DataFrame()
    links_df = pd.DataFrame()
    robots_txt_busy = set()

    def __init__(
        self, url: str,
        maxtasks: int = 10,
        http_request_options: dict = None,
        limit: int = 0,
        busy_timeout: int = 300,
        allow_external: bool = True
    ):
        """
        Initializes a web crawler instance.
        Args:
            url (str): The starting URL for the crawler.
            maxtasks (int, optional): The maximum number of tasks to run in parallel.
            http_request_options (dict, optional): Additional options to pass to the HTTP request.
            limit (int, optional): The maximum number of pages to crawl.
            busy_timeout (int, optional): The maximum time a task can be busy before being cancelled.
            allow_external (bool, optional): Whether or not to follow external links.
        """
        self.url = url
        self.rooturl = f'{urlsplit(url).scheme}://{urlsplit(url).netloc}'
        self.todo_queue = set()
        self.busy = dict()
        self.done = pd.DataFrame(columns=['url',
                                          'indexability',
                                          'indexability_status',
                                          'response_code',
                                          'content_type',
                                          'hash'])
        self.done = list()
        self.tasks = set()
        self.sem = asyncio.Semaphore(maxtasks)
        self.http_request_options = http_request_options or {}
        self.http_request_options['allow_redirects'] = False
        self.links = list()
        self.errors = set()
        # connector stores cookies between requests and uses connection pool
        self.limit = limit
        self.check_done = set()
        self.busy_timeout = busy_timeout
        self.allow_external = allow_external

    async def run(self):
        """
        Main function to start parsing site
        : return:
        """
        self.session = aiohttp.ClientSession()
        t = asyncio.ensure_future(self.addurls([(self.url, self.rooturl)]))
        await asyncio.sleep(1)
        busy = dict()
        count = 0
        while self.busy and count < self.busy_timeout:
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
        self.done_df = pd.DataFrame(self.done)

        self.links_df = pd.DataFrame(
            self.links, columns=['from', 'to']).drop_duplicates()

    async def check_allow_crawl(self, url):
        """
        Check if crawling the given URL is allowed, according to the robots.txt file of the site it belongs to.
        """
        root_url = UrlParser.get_root_url(url)
        while root_url in self.robots_txt_busy:
            await asyncio.sleep(1)
        if not self.rp.get(root_url):
            if self.robots_txt.get(root_url) is None:
                self.robots_txt_busy.add(root_url)
                response_text = await self._get_robots_txt(root_url)
                self.robots_txt_busy.remove(root_url)
                if not response_text:
                    self.robots_txt[root_url] = True
                    return True
                self.robots_txt[root_url] = response_text
            if self.robots_txt[root_url] is True:
                return True
            self.rp[root_url] = Protego.parse(
                self.robots_txt[root_url])
        return self.rp[root_url].can_fetch('*', url)

    async def _get_robots_txt(self, url):
        try:
            response = await self.session.get(
                f'{url}/robots.txt', timeout=5, allow_redirects=False)
        except (asyncio.exceptions.TimeoutError, aiohttp.client_exceptions.ClientConnectorError):
            return False
        if not response.ok:
            return False
        return await response.text()

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
            if self.should_crowl(force, url, parenturl):
                if await self.check_allow_crawl(url):
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

    def should_crowl(self, force, url, parenturl):
        """
        Determines whether a URL should be crawled, based on various criteria.
        """
        # If we're forcing the crawl, then we should crawl the URL.
        if force:
            return True
        # If we're out of available slots, then don't crawl.
        if not self._has_available_slots():
            return False
        # If the URL isn't available, then don't crawl.
        if not self._is_url_available(url):
            return False
        # If the URL isn't in the domain, then don't crawl.
        if not self._is_url_in_domain(url, parenturl):
            return False
        # If the URL contains an excluded part, then don't crawl.
        if self._has_excluded_part(url):
            return False
        # If we've made it this far, then we should crawl the URL.
        return True

    def _is_url_available(self, url):
        """Checks if a given url is available, i.e. not in any of the queues"""
        return url not in self.busy and url not in self.check_done and url not in self.todo_queue

    def _has_available_slots(self):
        """Checks if there are available slots for urls to be checked"""
        return len(self.check_done) + len(self.busy) + len(self.todo_queue) < self.limit or not self.limit

    def _is_url_in_domain(self, url, parenturl):
        """Checks if a given url is in the same domain as the parenturl"""
        return parenturl.startswith(self.rooturl) or url.startswith(self.rooturl)

    def _has_excluded_part(self, url):
        """Checks if a given url contains any part that should be excluded"""
        return any(exclude_part in url for exclude_part in self.exclude_urls)

    async def process(self, url):
        """
        Process single url
        """
        # print('remaining: ', len(self.todo_queue))
        # print('processing:', unquote(url))
        # remove url from basic queue and add it into busy list
        self.busy[url] = self.busy.get(url, 0) + 1
        if url in self.todo_queue:
            self.todo_queue.remove(url)
        try:
            # await response
            resp = await self.session.get(url, **self.http_request_options)
        except asyncio.exceptions.TimeoutError as exc:
            self._on_timeout_error(url, exc)
        except aiohttp.client_exceptions.ClientConnectorError as exc:
            self._on_connection_error(url, exc)
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
                canonical_url = parser.get_canonical_url()
                if canonical_url:
                    if canonical_url != url:
                        indexability_status = 'Canonicalised'
                        is_index = False
                self.append_done(resp,
                                 url,
                                 indexability=is_index,
                                 indexability_status=indexability_status,
                                 hash_val=md5(data.encode()).hexdigest(),
                                 dom=parser)
            elif 300 <= resp.status < 400:
                redirect_url = resp.headers.get('location')
                self.append_done(response=resp, url=url,
                                 indexability=False,
                                 indexability_status=resp.reason)
                if redirect_url:
                    self.add_url(redirect_url, url)
            elif resp.status > 400:
                self.append_done(response=resp,
                                 url=url,
                                 indexability=False,
                                 indexability_status=resp.reason)
            else:
                self.append_done(resp,
                                 url,
                                 indexability=True)

            # even if we have no exception, we can mark url as good
            resp.close()
        self.busy[url] -= 1
        if self.busy[url] == 0:
            del self.busy[url]
        logging.info(len(self.done), 'completed tasks,', len(self.tasks),
                     'still pending, todo_queue', len(self.todo_queue))

    def _on_connection_error(self, url, exc):
        print(exc)
        # on any exception mark url as BAD
        print('...', url, 'has error', repr(str(exc)))
        error_msg = repr(str(exc)) if repr(
            str(exc)) else type(exc).__name__
        self.append_done(
            url=url, indexability_status=error_msg)

    def _on_timeout_error(self, url, exc):
        if url not in self.errors:
            self.errors.add(url)
            self.add_url(url)
        else:
            self.append_done(
                url=url, indexability_status=type(exc).__name__)

    def add_url(self, url, parent_url=''):
        """
        Adds a URL to the crawler's queue for later processing.
        """
        addurls = self.addurls([(url, parent_url)])
        asyncio.Task(addurls)

    def append_done(self,
                    response=None,
                    url: str = '',
                    indexability: bool = False,
                    indexability_status: str = None,
                    hash_val: str = None,
                    dom=None):
        """
        Appends information about the given URL to the 'done' list of this object.
        """
        self.check_done.add(UrlParser.decode_url(url))
        meta = dom.get_meta() if dom else {}
        self.done.append({
            "url": UrlParser.decode_url(url),
            "indexability": indexability,
            "indexability_status": indexability_status,
            "content_type": response.headers.get('content-type') if response else None,
            "response_code": response.status if response else None,
            "hash": hash_val,
            "redirected_url": response.headers.get('location') if response else None,
            "canonical_url": dom.get_canonical_url() if dom else None,
            **meta
        })

    def set_exclude_url(self, urls_list):
        """
        Set the list of URLs to exclude from crawling.
        """
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
c = Crawler('https://www.thailandpostmart.com/',
            limit=100, http_request_options={"timeout": 60}, maxtasks=100,
            allow_external=True
            )
c.set_exclude_url(['/app/tag/name', '/search/allproducts/',
                  ".pdf", ".jpg", ".zip", 'mod_resize.index', '.png'])
c.runsync()
c.done_df.to_csv('export.csv', index=False)
c.links_df.to_csv('links.csv', index=False)

mem = tracemalloc.get_traced_memory()

# print as mb
print(f"Memory usage: {mem[0] / 10**6} MB")
print(f"Memory peak: {mem[1] / 10**6} MB")
# stopping the library
tracemalloc.stop()
profiler.disable()
stats = pstats.Stats(profiler)

stats.dump_stats("results.prof")
