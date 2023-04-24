import requests
from sklearn.feature_extraction.text import TfidfVectorizer
from sklearn.metrics.pairwise import cosine_similarity
import time
import asyncio
import aiohttp
from urllib.parse import urljoin, urldefrag, urlparse, unquote, quote
import urllib.robotparser
from bs4 import BeautifulSoup
import logging
import json
import tracemalloc
import pandas as pd
import encodings.idna
from hashlib import md5
import cProfile
import pstats
from lxml import etree, cssselect, html


# class Parser:

#     def __init__(self, text):
#         self.soup = BeautifulSoup(text, 'html.parser')

#     def parse(self):
#         elements = self.soup.select(
#             'a[href]:not(a[rel="nofollow"],[href^="javascript:"])')
#         urls = [element.get('href') for element in elements]
#         elements = self.soup.select('[src]:not(form)')
#         urls.extend(element.get('src') for element in elements)
#         return urls

#     def get_canonical_link(self):
#         element = self.soup.find('link', {'rel': 'canonical'})
#         if element is None:
#             return False
#         return element.get('href')
class Parser():
    def __init__(self, html_string) -> None:
        self.dochtml = html.fromstring(html_string)

    def parse(self):
        # select = cssselect.CSSSelector(
        # 'a:not(a[rel="nofollow"])')

        select = cssselect.CSSSelector(
            'a[href]:not(a[rel="nofollow"],[href^="javascript:"])')
        urls = [el.get('href') for el in select(self.dochtml)]
        select = cssselect.CSSSelector('[src]:not(form)')
        urls.extend(el.get('src') for el in select(self.dochtml))
        return urls

    def get_canonical_link(self):
        element = cssselect.CSSSelector(
            'link[rel="canonical"]')
        if element is None:
            return False
        return element.get('href')


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
        return urlparse(url).scheme + '://' + urlparse(url).netloc

    @ staticmethod
    def encode_url_once(url):
        """
        Encodes the given URL only once, if it is not already encoded.

        Args:
            url (str): The URL to be encoded.

        Returns:
            str: The encoded URL.

        """
        if UrlParser().is_url_already_encoded(url):
            return url
        return UrlParser().encode_url(url)

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
        url_components = urlparse(url)
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
        url_components = urlparse(url)
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

    rp = {}
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
        self.url = UrlParser.encode_url_once(url)
        self.rooturl = f'{urlparse(url).scheme}://{urlparse(url).netloc}'
        self.todo_queue = todo_queue_backend()
        self.busy = set()
        self.done = pd.DataFrame(columns=['url',
                                          'url_encode',
                                          'indexability',
                                          'indexability_status',
                                          'response_code',
                                          'content_type',
                                          'response_time',
                                          'hash'])
        self.tasks = set()
        self.sem = asyncio.Semaphore(maxtasks)
        self.http_request_options = http_request_options or {}
        self.links = list()
        self.errors = set()
        # connector stores cookies between requests and uses connection pool
        self.session = aiohttp.ClientSession()
        self.limit = limit
        # self.writer = self.format_processors.get(out_format)(out_file)

    def robots_txt(self, url):
        """
        Parses the `robots.txt` file for the given `rooturl` and returns a
        `RobotFileParser` object.

        :return: `RobotFileParser` object that was created after parsing
                `robots.txt` file.
        """
        rp = urllib.robotparser.RobotFileParser()
        response = requests.get(
            urljoin(UrlParser.get_root_url(url), 'robots.txt'))
        url = urljoin(UrlParser.get_root_url(response.url), 'robots.txt')
        rp.set_url(url)
        rp.read()
        return rp

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
        while self.busy:
            await asyncio.sleep(1)
        await t
        await self.session.close()
        self.done.to_excel('export.xlsx', index=False)
        # json_object = json.dumps(self.done, indent=4, ensure_ascii=False)
        # with open('json.json', 'w') as f:
        #     f.write(json_object)
        # json_object = json.dumps(self.links, indent=4, ensure_ascii=False)
        # with open('json2.json', 'w') as f:
        #     f.write(json_object)

        # await self.writer.write([key for key, value in self.done.items() if value])

    def check_allow_crawl(self, url):
        try:
            if self.rp.get(UrlParser().get_root_url(url)) is None:
                self.rp[UrlParser().get_root_url(url)] = self.robots_txt(url)
            rp = self.rp.get(UrlParser().get_root_url(url))
            return rp.can_fetch('*', url)
        except Exception:
            return False

    async def addurls(self, urls, force=False):
        """
        Add urls in queue and run process to parse
        :param urls:
        :return:
        """
        for url, parenturl in urls:
            url = UrlParser.encode_url_once(url)
            parenturl = UrlParser.encode_url_once(parenturl)
            url = urllib.parse.urljoin(parenturl, url)
            url, frag = urldefrag(url)
            self.links.append([parenturl, url])
            if (parenturl.startswith(self.rooturl) and
                        not any(exclude_part in url for exclude_part in self.exclude_urls) and
                        url not in self.busy and
                        url not in self.done.url_encode and
                        url not in self.todo_queue and
                        (len(self.done) + len(self.busy) + len(self.todo_queue)
                         < self.limit or not self.limit)
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
        self.busy.add(url)
        try:
            self.todo_queue.remove(url)
        except Exception:
            pass
        try:
            # await response
            start = time.time()
            resp = await self.session.get(url, **self.http_request_options)
            stop = time.time()
            for history in resp.history:
                current_url = str(history.url)
                self.append_done(history, current_url,
                                 indexability_status="Redirected")
            new_url = str(resp.url)
            # get response url
        except Exception as exc:
            print(exc)
            print(str(exc))
            # on any exception mark url as BAD
            print('...', url, 'has error', repr(str(exc)))
            if url not in self.errors:
                self.errors.add(url)
                asyncio.Task(self.addurls([('', url)], True))
            else:
                error = repr(str(exc))
                if error == '':
                    error = 'Unknown'
                self.append_done(
                    url=url, indexability_status=error)
        else:
            # only url with status == 200 and content type == 'text/html' parsed
            if (resp.status == 200 and
                    ('text/html' in resp.headers.get('content-type'))):
                data = (await resp.read()).decode('utf-8', 'replace')
                parser = Parser(data)
                urls = parser.parse()
                asyncio.Task(self.addurls([(u, url) for u in urls]))
                self.append_done(resp, new_url, indexability=True,
                                 hash_val=md5(data.encode()).hexdigest(), response_time=stop-start)
            else:
                self.append_done(resp, new_url, indexability=True,
                                 response_time=stop-start)

            # even if we have no exception, we can mark url as good
            resp.close()

        if url in self.busy:
            self.busy.remove(url)
        logging.info(len(self.done), 'completed tasks,', len(self.tasks),
                     'still pending, todo_queue', len(self.todo_queue))

    def append_done(self, response=None, url='', indexability=False, indexability_status=None, hash_val=None, response_time=None):
        self.done = pd.concat([self.done, pd.DataFrame({
            "url": UrlParser.decode_url(url),
            "url_encode": UrlParser.encode_url_once(url),
            "indexability": indexability,
            "indexability_status": indexability_status,
            "content_type": response.headers.get('content-type') if response else None,
            "response_code": response.status if response else None,
            "hash": hash_val,
            "response_time": response_time
        }, index=[0])])

    def set_exclude_url(self, urls_list):
        self.exclude_urls = urls_list

    def runsync(self):
        """
        Runs the `run` coroutine synchronously using an event loop.
        """
        loop = asyncio.get_event_loop()
        loop.run_until_complete(self.run())


def check_duplicate(text1, text2):
    html1 = requests.get(text1).text
    html2 = requests.get(text2).text
    vectorizer = TfidfVectorizer().fit_transform([html1, html2])
    similarity = cosine_similarity(vectorizer[0], vectorizer[1])
    return similarity[0][0]


profiler = cProfile.Profile()
profiler.enable()

tracemalloc.start()
c = Crawler('https://www.thailandpostmart.com/',
            limit=10, http_request_options={"timeout": 10}, maxtasks=100)

# cProfile.run("c.runsync()")
c.runsync()
# print(UrlParser().decode_url(
# "https://www.thailandpostmart.com/product/1013460000224/ก/"))
mem = tracemalloc.get_traced_memory()
# print as mb
print(f"Memory usage: {mem[0] / 10**6} MB")
print(f"Memory peak: {mem[1] / 10**6} MB")
# stopping the library
tracemalloc.stop()
profiler.disable()
stats = pstats.Stats(profiler).sort_stats('cumtime')
stats.print_stats('run_sync')
# start = time.time()
# a = check_duplicate('https://www.thailandpostmart.com/product/1013460000224/',
#                     'https://www.thailandpostmart.com/product/1013460000224/')
# stop = time.time()
# print(stop - start)
# # print typeof a
# print(a)
