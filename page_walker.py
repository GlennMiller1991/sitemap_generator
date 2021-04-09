import requests, re, os, time, sys

from urllib.parse import urlsplit, urlunsplit, urljoin, urldefrag
from multiprocessing import Process, Value, Queue
from concurrent.futures import ThreadPoolExecutor

class Page_walker:

    def __init__(self, xml_file='sitemap.xml', verbose=True, restrict=9999):
        self.count = -1
        self.found_urls = Value('i', 0)
        self.xml_file_base = xml_file
        self.xml_file = self.xml_file_base
        self.taken = Value('i', 0)
        self.given = Value('i', 0)
        self.sent = Value('i', 0)
        self.processed = Value('i', 0)
        self.output_queue = Queue()
        self.input_queue = Queue()
        self.to_write = Queue()
        self.verbose = verbose
        self.restrict = restrict

    @staticmethod
    def write_url(output_file, input_file):
        output_file = open(output_file, 'wt')
        output_file.write('<?xml version="1.0" encoding="UTF-8"?>\n\t<urlset xmlns="http://www.sitemaps.org/schemas/sitemap/0.9">')
        while True:
            link = input_file.get()
            if link == 'bye':
                break
            output_file.write("\n\t\t<url>\n\t\t\t<loc>\n\t\t\t\t{0}\n\t\t\t</loc>\n\t\t</url>".format(link))
            output_file.flush()
        output_file.write('\n</urlset>')
        output_file.flush()
        output_file.close()
        sys.exit()

    @staticmethod
    def url_test(url, given, processed, input_queue, output_queue):
        need_to_visit = {}
        while True:
            link = input_queue.get()
            if not link:
                time.sleep(0.1)
                continue
            elif link == 'bye':
                sys.exit()
            link = urldefrag(urlunsplit(urlsplit(urljoin(url, link))))[0]
            if link.startswith(url):
                if link not in need_to_visit:
                    scheme, netloc, path, query, frag = urlsplit(link)
                    path = path.rstrip('/').rsplit('/', 1)[0] + '/'
                    query = ''
                    frag = ''
                    test_link = urlunsplit((scheme, netloc, path, query, frag))
                    need_to_visit[test_link] = (need_to_visit[test_link] + 1) if (test_link in need_to_visit) else 1
                    if need_to_visit[test_link] < 300:
                        need_to_visit[link] = 0
                        output_queue.put(link)
                        given.value += 1
            processed.value += 1

    def start(self, url):
        self.url = url
        self.count += 1
        self.xml_file = str(self.count) + '_' + self.xml_file_base
        Process(target=self.write_url, args=(self.xml_file, self.to_write)).start()
        Process(target=self.url_test, args=(self.url, self.given, self.processed, self.output_queue, self.input_queue)).start()
        self.output_queue.put(self.url)
        self.sent.value += 1
        self.walk()
        urls = self.found_urls.value
        self.found_urls.value = 0
        self.taken.value = 0
        self.given.value = 0
        self.sent.value = 0
        self.processed.value = 0
        return self.url, urls, self.xml_file
    
    def gen_array(self, n):
        for i in range(n):
            yield self.input_queue.get()

    def req_and_res(self, link):
        if self.verbose:
            print('get', link)
        self.taken.value += 1
        try:
            response = requests.get(link, timeout=5)
        except:
            return
        else:
            if response and response.status_code == 200 and 'text/html' in response.headers['Content-Type']:
                if self.found_urls.value > self.restrict:
                    return
                else:
                    self.found_urls.value += 1
                    self.to_write.put(response.url)
                    page = response.text
                    pattern = '(?i)href=["\']?([^\s"\'<>]+)'
                    found_links = (match.groups()[0] for match in re.finditer(pattern, page))
                    return found_links
        

    def walk(self):
        exit_flag = False
        while True:
            while self.taken.value == self.given.value:
                if self.sent.value == self.processed.value:
                    if exit_flag:
                        self.output_queue.put('bye')
                        self.to_write.put('bye')
                        return
                    exit_flag = True
            else:
                exit_flag = False
                with ThreadPoolExecutor(16) as executor:
                    results = executor.map(self.req_and_res, self.gen_array(self.given.value - self.taken.value))
                
                for response in results:
                    if response:
                        for link in response:
                            if link:
                                self.output_queue.put(link)
                                self.sent.value += 1

if __name__ == '__main__':
    urls = ['https://glennmiller.pythonanywhere.com',
            'http://crawler-test.com/',
            'http://google.com/',
            'https://vk.com',
            'https://yandex.ru',
            'https://stackoverflow.com',
            ]

    results_file = open('results.txt', 'wt')
    
    walker = Page_walker(verbose=True)
    
    results_file.write(20 * '-' + '\n')
    results_file.write('URL\n')
    results_file.write('Time sec\n')
    results_file.write('Quantity\n')
    results_file.write('Result file\n')
    results_file.write(20 * '-' + '\n')
        
    for url in urls:
        start = time.time()
        results = walker.start(url)
        end = time.time()
        results_file.write(20 * '-' + '\n')
        results_file.write(results[0] + '\n')
        results_file.write(str(end - start) + '\n')
        results_file.write(str(results[1]) + '\n')
        results_file.write(results[2] + '\n')
        results_file.write(20 * '-' + '\n')

    results_file.close()
        
