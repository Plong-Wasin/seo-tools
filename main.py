import requests
from sklearn.feature_extraction.text import TfidfVectorizer
from sklearn.metrics.pairwise import cosine_similarity
import time
import aiohttp
import asyncio
from urllib.parse import urljoin, urldefrag, urlparse, unquote
import urllib.robotparser
from bs4 import BeautifulSoup
import logging
import json
import tracemalloc


class Parser:
    def __init__(self, text):
        self.soup = BeautifulSoup(text, 'html.parser')

    def parse(self):
        elements = self.soup.select(
            'a[href]:not(a[rel="nofollow"],[href^="javascript:"])')
        urls = [element.get('href') for element in elements]
        elements = self.soup.select('[src]:not(form)')
        urls.extend(element.get('src') for element in elements)
        return urls

    def get_canonical_link(self):
        return self.soup.find('link', {'rel': 'canonical'}).get('href')


class Crawler:

    # format_processors = {
    #     'xml': XMLWriter,
    #     'txt': TextWriter
    # }

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
        self.url = url
        self.rooturl = f'{urlparse(url).scheme}://{urlparse(url).netloc}'
        self.todo_queue = todo_queue_backend()
        self.busy = set()
        self.done = done_backend()
        self.tasks = set()
        self.sem = asyncio.Semaphore(maxtasks)
        self.http_request_options = http_request_options or {}
        self.rp = {self.rooturl: self.robots_txt()}
        self.links = list()
        self.errors = set()
        # connector stores cookies between requests and uses connection pool
        self.session = aiohttp.ClientSession()
        self.limit = limit
        # self.writer = self.format_processors.get(out_format)(out_file)

    def robots_txt(self):
        rp = urllib.robotparser.RobotFileParser()
        rp.set_url(urljoin(self.rooturl, 'robots.txt'))
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
        json_object = json.dumps(self.done, indent=4, ensure_ascii=False)
        with open('json.json', 'w') as f:
            f.write(json_object)
        json_object = json.dumps(self.links, indent=4, ensure_ascii=False)
        with open('json2.json', 'w') as f:
            f.write(json_object)

        # await self.writer.write([key for key, value in self.done.items() if value])

    def check_allow_crawl(self, url):
        try:
            if self.rp.get(self.get_root_url(url)) is None:
                self.rp[self.get_root_url(url)] = self.robots_txt()
            rp = self.rp.get(self.get_root_url(url))
            return rp.can_fetch('*', url)
        except Exception:
            return False

    def get_root_url(self, url):
        return urlparse(url).scheme + '://' + urlparse(url).netloc

    async def addurls(self, urls):
        """
        Add urls in queue and run process to parse
        :param urls:
        :return:
        """
        for url, parenturl in urls:
            url = urllib.parse.urljoin(parenturl, url)
            url, frag = urllib.parse.urldefrag(url)
            self.links.append([parenturl, url])
            if (parenturl.startswith(self.rooturl) and
                    not any(exclude_part in url for exclude_part in self.exclude_urls) and
                    url not in self.busy and
                    url not in self.done and
                    url not in self.todo_queue and
                    not self.is_done(url) and
                    (len(self.done) + len(self.busy) + len(self.todo_queue)
                     < self.limit or not self.limit)
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
                    self.done[url] = {
                        'url': url,
                        'url_decode': unquote(url),
                        'indexability': False,
                        'indexability_status': 'Block by robots.txt'
                    }

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
        self.todo_queue.remove(url)

        try:
            # await response
            resp = await self.session.get(url, **self.http_request_options)
            for history in resp.history:
                current_url = str(history.url)
                self.done[current_url] = {
                    "url": current_url,
                    "url_decode": unquote(current_url),
                    "indexability": False,
                    "indexability_status": 'Redirected',
                    "content_type": resp.headers.get('content-type'),
                    "http_code": history.status
                }
            new_url = str(resp.url)
            # get response url
        except Exception as exc:
            # on any exception mark url as BAD
            print('...', url, 'has error', repr(str(exc)))
            # if url not in self.errors:
            #     self.errors.add(url)
            #     self.busy.remove(url)
            #     self.addurls(('', url))
            # else:
            self.done[url] = {
                "url": url,
                "url_decode": unquote(url),
                "indexability": False,
                "indexability_status": repr(str(exc))
            }
        else:
            # only url with status == 200 and content type == 'text/html' parsed
            if (resp.status == 200 and
                    ('text/html' in resp.headers.get('content-type'))):
                data = (await resp.read()).decode('utf-8', 'replace')
                parser = Parser(data)
                urls = parser.parse()
                asyncio.Task(self.addurls([(u, url) for u in urls]))

            # even if we have no exception, we can mark url as good
            resp.close()
            self.done[new_url] = {
                "url": new_url,
                "url_decode": unquote(new_url),
                "indexability": True,
                "indexability_status": '',
                "content_type": resp.headers.get('content-type'),
                "http_code": resp.status
            }
        self.busy.remove(url)
        # self.done[new_url]["time"] = stop - start
        logging.info(len(self.done), 'completed tasks,', len(self.tasks),
                     'still pending, todo_queue', len(self.todo_queue))

    def set_exclude_url(self, urls_list):
        self.exclude_urls = urls_list

    def runsync(self):
        loop = asyncio.get_event_loop()
        loop.run_until_complete(self.run())


def check_duplicate(text1, text2):
    html1 = requests.get(text1).text
    html2 = requests.get(text2).text
    vectorizer = TfidfVectorizer().fit_transform([html1, html2])
    similarity = cosine_similarity(vectorizer[0], vectorizer[1])
    return similarity[0][0]


tracemalloc.start()
c = Crawler('https://www.thailandpostmart.com/',
            limit=1, http_request_options={"timeout": 60}, maxtasks=10)
# cProfile.run("c.runsync()")
c.runsync()
print(tracemalloc.get_traced_memory())

# stopping the library
tracemalloc.stop()
# start = time.time()
# a = check_duplicate('https://www.thailandpostmart.com/product/1013460000224/%E0%B8%8A%E0%B8%B5%E0%B8%97%E0%B8%A7%E0%B8%B1%E0%B8%99%E0%B8%AD%E0%B8%99%E0%B8%B8%E0%B8%A3%E0%B8%B1%E0%B8%81%E0%B8%A9%E0%B9%8C%E0%B8%A1%E0%B8%A3%E0%B8%94%E0%B8%81%E0%B9%84%E0%B8%97%E0%B8%A2%202562%20(1165)',
#                     'https://www.thailandpostmart.com/product/1013460000501/%E0%B9%81%E0%B8%AA%E0%B8%95%E0%B8%A1%E0%B8%9B%E0%B9%8C%E0%B8%87%E0%B8%B2%E0%B8%99%E0%B9%80%E0%B8%9B%E0%B8%B4%E0%B8%94%E0%B8%AD%E0%B8%B2%E0%B8%84%E0%B8%B2%E0%B8%A3%E0%B8%99%E0%B8%A7%E0%B8%A1%E0%B8%B4%E0%B8%99%E0%B8%97%E0%B8%A3%E0%B8%9A%E0%B8%9E%E0%B8%B4%E0%B8%95%E0%B8%A3%2084%20%E0%B8%9E%E0%B8%A3%E0%B8%A3%E0%B8%A9%E0%B8%B2%20%E0%B8%A3%E0%B8%9E.%E0%B8%A8%E0%B8%B4%E0%B8%A3%E0%B8%B4%E0%B8%A3%E0%B8%B2%E0%B8%8A%20%E0%B9%81%E0%B8%9C%E0%B9%88%E0%B8%99%20(1211)')
# stop = time.time()
# print(stop - start)
# # print typeof a
# print(a)
