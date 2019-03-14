#!/usr/bin/python
# -*- coding: utf-8 -*-

import codecs
import io
import os
import re
import requests
import sqlite3
import MySQLdb
import sys
import time
import threading
import traceback
import urlparse
import logging 
import logging.handlers
from datetime import datetime
from pyquery import PyQuery as pq
from PIL import Image
from warnings import filterwarnings

# status
#   url [from thread]
#   start date [from thread]
#   scaning time (current scan) [from thread]
#   number of downloaded images (current scan) [from thread]
#   number of scanned pages (current scan) [from thread]
#   number of checked images (current scan) [from thread]
#   download speed [from thread and calculate]
#   scan speed [from thread and calculate]

#sys.stdout = codecs.getwriter("utf-8")(sys.stdout)
# encoding=utf8
reload(sys)
sys.setdefaultencoding('utf8')
filterwarnings('ignore', category = MySQLdb.Warning)

def url_to_filename(url):
    return re.sub('_*$', '', url.replace('://', '_').replace('/', '_').replace(':', '_').replace('%', '_').replace('.', '_').replace('&', '_').replace('?', '_'))

def text_to_filename(text):
    text = re.sub(u'[｜\|\-].*$', '', text)
    text = text.replace('/', '_').replace(':', '_').replace('%', '_').replace('.', '_').replace('&', '_').replace('?', '_').replace(' ', '_')
    return re.sub('[ _]*$', '', text)

class ImageDownloader(threading.Thread):
    def __init__(self, url, download_wait_sec = 0, scan_wait_sec = 0, min_width = 600, min_height = 600):
        super(ImageDownloader, self).__init__()
        self.only_the_page = False
        self.site_number = 1
        self.image_exts = ['.jpg', '.jpeg', '.png']
        self.all_image_exts = ['.jpg', '.jpeg', '.png', '.gif', '.tiff']
        self.download_dir = '/xtorage/picture/sites'

        # setting parameters
        self.download_wait_sec = download_wait_sec
        self.scan_wait_sec = scan_wait_sec
        self.min_width = min_width
        self.min_height = min_height

        # status parameters
        self.start_date = None
        self.num_of_downloaded_images = 0
        self.num_of_checked_images = 0
        self.num_of_scanned_pages = 0

        self.con = self.connect_database()

        self.baseurl = re.sub('[\.\? _/]*$', '', url)
        self.site_id = self.get_site_id(self.baseurl)
        if self.site_id is None:
            self.site_id = self.create_site_id(self.baseurl)
    
            c = self.con.cursor();
            c.execute("DELETE FROM pg_stats WHERE site_id = %s AND status <> 2", (self.site_id,))

        self.basename = url_to_filename(self.baseurl)
        self.basedir = self.download_dir + '/' + self.basename

        logfile = self.basename + ".log"
        handler = logging.handlers.RotatingFileHandler(logfile, maxBytes=10*1024*1024, backupCount=5)
        handler.setLevel(logging.INFO)
        handler.setFormatter(logging.Formatter('%(asctime)s %(levelname)s %(message)s'))
        self.logger = logging.getLogger(__name__ + "." + self.basename)
        self.logger.setLevel(logging.INFO)
        self.logger.handlers = []
        self.logger.addHandler(handler)

        self.stop_event = threading.Event()

    def __del__(self):
        try:
            self.con.close()
        except:
            pass

    def stop(self):
        self.stop_event.set()

    def is_same_site(self, url):
        baseurl_tmp = re.sub('^https?://', '', self.baseurl)
        url_tmp = re.sub('^https?://', '', url)
        return url_tmp.startswith(baseurl_tmp)

    def connect_database(self):
        con = MySQLdb.connect(user='imagedownloader', passwd='imagedownloader', host='localhost', db='imagedownloader', use_unicode=True, charset="utf8")
        con.autocommit(True)
        return con

    def get_site_id(self, url):
        c = self.con.cursor()
        c.execute("SELECT id FROM sites WHERE url = %s", (url,))
        r = c.fetchone()
        return int(r[0]) if r is not None else None

    def create_site_id(self, url):
        c = self.con.cursor()
        c.execute("INSERT INTO sites (url) VALUES (%s)", (url,))
        return self.get_site_id(url)
        
    def push_url_queue(self, url):
        c = self.con.cursor()
        try:
            c.execute("INSERT IGNORE INTO url_queue (site_id, url) VALUES (%s, %s)", (self.site_id, url))
        except:
            pass

    def pop_url_queue(self):
        c = self.con.cursor()
        try:
            c.execute("SELECT id, url FROM url_queue WHERE site_id = %s LIMIT 1", (self.site_id,))
        except OperationalError:
            self.logger.warning('re-try once after 5 sec')
            time.sleep(5)
            c.execute("SELECT id, url FROM url_queue WHERE site_id = %s LIMIT 1", (self.site_id,))

        r = c.fetchone()
        if r is not None:
            id = int(r[0])
            url = r[1]
        else:
            self.logger.info("URL queue is empty")
            return None

        try:
            c.execute("DELETE FROM url_queue WHERE site_id = %s AND id = %s", (self.site_id, id))
        except OperationalError:
            self.logger.warning('re-try once after 5 sec')
            time.sleep(5)
            c.execute("DELETE FROM url_queue WHERE site_id = %s AND id = %s", (self.site_id, id))

        return url

    def delete_url_queue(self, url):
        c = self.con.cursor()
        try:
            c.execute("DELETE FROM url_queue WHERE site_id = %s AND url = %s", (self.site_id, url))
        except OperationalError:
            self.logger.warning('re-try once after 5 sec')
            time.sleep(5)
            c.execute("DELETE FROM url_queue WHERE site_id = %s AND url = %s", (self.site_id, url))

    def is_empty_url_queue(self):
        c = self.con.cursor()
        try:
            c.execute("SELECT count(id) FROM url_queue WHERE site_id = %s LIMIT 1", (self.site_id,))
        except OperationalError:
            self.logger.warning('re-try once after 5 sec')
            time.sleep(5)
            c.execute("SELECT count(id) FROM url_queue WHERE site_id = %s LIMIT 1", (self.site_id,))

        r = c.fetchone()
        count = int(r[0])
        return (count == 0)


    # page status
    # 0 ... unscanned
    # 1 ... scanning
    # 2 ... done

    # image status
    # 0 ... undownload
    # 1 ... downloaded
    # 2 ... checked (not downloaded)

    # True  ... downloaded
    # False ... not downloaded yet
    def is_downloaded(self, url):
        c = self.con.cursor()
        try:
            c.execute("SELECT count(id) FROM img_stats WHERE site_id = %s AND url = %s AND status = 1 LIMIT 1", (self.site_id, url))
        except OperationalError:
            self.logger.warning('re-try once after 5 sec')
            time.sleep(5)
            c.execute("SELECT count(id) FROM img_stats WHERE site_id = %s AND url = %s AND status = 1 LIMIT 1", (self.site_id, url))

        r = c.fetchone()
        count = int(r[0])
        return (count > 0)

    def is_checked(self, url):
        c = self.con.cursor()
        try:
            c.execute("SELECT count(id) FROM img_stats WHERE site_id = %s AND url = %s AND status = 2 LIMIT 1", (self.site_id, url))
        except OperationalError:
            self.logger.warning('re-try once after 5 sec')
            time.sleep(5)
            c.execute("SELECT count(id) FROM img_stats WHERE site_id = %s AND url = %s AND status = 2 LIMIT 1", (self.site_id, url))

        r = c.fetchone()
        count = int(r[0])
        return (count > 0)

    # True  ... scanned (or scanning now)
    # False ... not sacnned yet
    def is_scanned(self, url):
        c = self.con.cursor()
        try:
            c.execute("SELECT count(id) FROM pg_stats WHERE site_id = %s AND url = %s AND status >= 1 LIMIT 1", (self.site_id, url))
        except OperationalError:
            self.logger.warning('re-try once after 5 sec')
            time.sleep(5)
            c.execute("SELECT count(id) FROM pg_stats WHERE site_id = %s AND url = %s AND status >= 1 LIMIT 1", (self.site_id, url))

        r = c.fetchone()
        count = int(r[0])
        return (count > 0)

    def set_downloaded(self, url):
        c = self.con.cursor()
        try:
            c.execute("INSERT INTO img_stats (site_id, url, status) VALUES (%s, %s, 1)", (self.site_id, url))
        except OperationalError:
            self.logger.warning('re-try once after 5 sec')
            time.sleep(5)
            c.execute("INSERT INTO img_stats (site_id, url, status) VALUES (%s, %s, 1)", (self.site_id, url))

    def set_checked(self, url):
        c = self.con.cursor()
        try:
            c.execute("INSERT INTO img_stats (site_id, url, status) VALUES (%s, %s, 2)", (self.site_id, url))
        except OperationalError:
            self.logger.warning('re-try once after 5 sec')
            time.sleep(5)
            c.execute("INSERT INTO img_stats (site_id, url, status) VALUES (%s, %s, 2)", (self.site_id, url))

    def set_scanning(self, url):
        c = self.con.cursor()
        try:
            c.execute("INSERT INTO pg_stats (site_id, url, status) VALUES (%s, %s, 1)", (self.site_id, url))
        except OperationalError:
            self.logger.warning('re-try once after 5 sec')
            time.sleep(5)
            c.execute("INSERT INTO pg_stats (site_id, url, status) VALUES (%s, %s, 1)", (self.site_id, url))

    def set_scanned(self, url):
        c = self.con.cursor()
        try:
            c.execute("INSERT INTO pg_stats (site_id, url, status) VALUES (%s, %s, 2)", (self.site_id, url))
        except OperationalError:
            self.logger.warning('re-try once after 5 sec')
            time.sleep(5)
            c.execute("INSERT INTO pg_stats (site_id, url, status) VALUES (%s, %s, 2)", (self.site_id, url))


    def download_image(self, img_url, referer, dirname):
        filename = os.path.basename(img_url)
        if os.path.isfile(dirname + "/" + filename):
            self.logger.info("SKIP [downloaded] " + img_url)
            return

        img_res = requests.get(img_url, headers={'referer': referer})
        img_res.raise_for_status()
                  
        try:
            img_pil = Image.open(io.BytesIO(img_res.content))
            w, h = img_pil.size
        except IOError:
            self.set_checked(img_url)
            self.num_of_checked_images += 1
            self.logger.warning("might not be image - {0}".format(img_url))
            return

        if(w < self.min_width or h < self.min_height):
            self.set_checked(img_url)
            self.num_of_checked_images += 1
            self.logger.info("SKIP [too small] " + img_url)
            return

        if not os.path.isdir(dirname):
            os.makedirs(dirname)

        try:
            fout = open(dirname + "/" + filename, "wb")
            fout.write(img_res.content)
            self.logger.info("DOWNLOAD " + img_url)
        finally:
            if fout is not None:
                fout.close()

        self.set_downloaded(img_url)
        self.num_of_downloaded_images += 1
        self.num_of_checked_images += 1

        if self.download_wait_sec > 0:
            try:
                time.sleep(self.download_wait_sec)
            except:
                pass

    def _downlaod_images(self, url, dom, img_url):
        root, ext = os.path.splitext(img_url)
        if ext.lower() not in self.image_exts:
            self.logger.info("SKIP [extention] %s [%s]" % (img_url, ','.join(self.image_exts)))
            return

        if self.is_downloaded(img_url):
            self.logger.info("SKIP [downloaded] " + img_url)
            return

        if self.is_checked(img_url):
            self.logger.info("SKIP [checked] " + img_url)
            return

        title = dom('title').text()
        dirname = self.basedir + "/" + text_to_filename(title)
        #TODO dirname uniq

        try:
            self.download_image(img_url, url, dirname)
        except Exception as e:
            self.logger.warning("{0}".format(e))
            self.logger.warning(traceback.format_exc())

    def download_images(self, url, dom):
        for img in dom('img').items():
            if self.stop_event.is_set():
                return

            if img.attr['src'] is None:
                continue

            img_url = re.sub("[\r\n ]", '', img.attr['src'])
            img_url = urlparse.urljoin(url, img_url)
            self._downlaod_images(url, dom, img_url)

        for a in dom('a').items():
            if self.stop_event.is_set():
                return

            if a.attr['href'] is None:
                continue

            img_url = re.sub("[\r\n ]", '', a.attr['href'])
            img_url = urlparse.urljoin(url, img_url)
            self._downlaod_images(url, dom, img_url)


    def scan_links(self, url, dom):
        for a in dom('a').items():
            if self.stop_event.is_set():
                return

            if a.attr['href'] is None:
                continue
            link_url = re.sub("[\r\n ]", '', a.attr['href'])
            link_url = urlparse.urljoin(url, link_url)
            link_url = re.sub('#.*$', '', link_url)

            if not self.is_same_site(link_url):
                self.logger.info("SKIP [external site] " + link_url)
                continue

            root, ext = os.path.splitext(link_url)
            if ext.lower() in self.all_image_exts:
                self.logger.info("SKIP [not html] " + link_url)
                continue

            if self.is_scanned(link_url):
                self.logger.info("SKIP [scanned] " + link_url)
                continue

            self.push_url_queue(link_url)

    # 最初に渡したURLはスキャン済みでとりあえずスキャンする
    def scan(self):
        is_first = True
        self.start_date = datetime.now()

        while (not self.stop_event.is_set()) and (is_first or not self.is_empty_url_queue()):
            if is_first:
                url = self.baseurl
                is_first = False
            else:
                url = self.pop_url_queue()
                if url is None:
                    break
                if self.is_scanned(url):
                    self.logger.info("SKIP [scanned] " + url)
                    continue

            self.logger.info("SCAN " + url)
            self.set_scanning(url)
            try:
                res = requests.get(url)
                res.raise_for_status()

                self.logger.debug(res.text)
                dom = pq(b"<meta charset='" + res.apparent_encoding + "'/>" + res.content)
                self.download_images(url, dom)
                if not self.only_the_page:
                    self.scan_links(url, dom)
            except Exception as e:
                self.logger.warning("{0}".format(e))
                self.logger.warning(traceback.format_exc())

            self.set_scanned(url)
            self.num_of_scanned_pages += 1

            if self.scan_wait_sec > 0:
                try:
                    time.sleep(self.scan_wait_sec)
                except:
                    pass
    
    def run(self):
        self.scan()

        if self.stop_event.is_set():
            self.logger.info("STOPPED")
        else:
            self.logger.info("DONE")
        
if __name__ == '__main__':
    try:
        url = sys.argv[1]
    except:
        print("usage: imgdownloader.py URL")
        sys.exit(1)

    logger = None
    try:
        imgdl = ImageDownloader(url)
        logger = imgdl.logger
        imgdl.scan()
    except:
        logger.error(traceback.format_exc())
        sys.exit(1)

    logger.info("DONE")
    sys.exit(0)

