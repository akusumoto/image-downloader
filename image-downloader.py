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
import traceback
import urlparse
import logging 
import logging.handlers
from pyquery import PyQuery as pq
from PIL import Image
from warnings import filterwarnings

#sys.stdout = codecs.getwriter("utf-8")(sys.stdout)
# encoding=utf8
reload(sys)
sys.setdefaultencoding('utf8')
filterwarnings('ignore', category = MySQLdb.Warning)

only_the_page = False
site_number = 1
min_width = 600
min_height = 600
image_exts = ['.jpg', '.jpeg', '.png']
all_image_exts = ['.jpg', '.jpeg', '.png', '.gif', '.tiff']
download_dir = '/xtorage/picture/sites'

logger = None


def connect_database():
    con = MySQLdb.connect(user='imagedownloader', passwd='imagedownloader', host='localhost', db='imagedownloader', use_unicode=True, charset="utf8")
    con.autocommit(True)
    return con

def get_site_id(con, url):
    c = con.cursor()
    c.execute("SELECT id FROM sites WHERE url = %s", (url,))
    r = c.fetchone()
    return int(r[0]) if r is not None else None

def create_site_id(con, url):
    c = con.cursor()
    c.execute("INSERT INTO sites (url) VALUES (%s)", (url,))
    return get_site_id(con, url)
    
def url_to_filename(url):
    return re.sub('_*$', '', url.replace('://', '_').replace('/', '_').replace(':', '_').replace('%', '_').replace('.', '_').replace('&', '_').replace('?', '_'))

def text_to_filename(text):
    text = re.sub(u'[｜\|\-].*$', '', text)
    text = text.replace('/', '_').replace(':', '_').replace('%', '_').replace('.', '_').replace('&', '_').replace('?', '_').replace(' ', '_')
    return re.sub('[ _]*$', '', text)


def init(url):
    con = connect_database()
    site_id = get_site_id(con, url)
    if site_id is None:
        site_id = create_site_id(con, url)
    
    c = con.cursor();
    c.execute("DELETE FROM pg_stats WHERE site_id = %s AND status <> 2", (site_id,))

    global logger
    basename = url_to_filename(url)
    logfile = basename + ".log"
    handler = logging.handlers.RotatingFileHandler(logfile, maxBytes=10*1024*1024, backupCount=5)
    handler.setLevel(logging.INFO)
    handler.setFormatter(logging.Formatter('%(asctime)s %(levelname)s %(message)s'))
    logger = logging.getLogger(__name__)
    logger.setLevel(logging.INFO)
    logger.handlers = []
    logger.addHandler(handler)

    return (con, site_id, download_dir + '/' + basename)

def push_url_queue(con, site_id, url):
    c = con.cursor()
    try:
        c.execute("INSERT IGNORE INTO url_queue (site_id, url) VALUES (%s, %s)", (site_id, url))
    except:
        pass

def pop_url_queue(con, site_id):
    c = con.cursor()
    try:
        c.execute("SELECT id, url FROM url_queue WHERE site_id = %s LIMIT 1", (site_id,))
    except OperationalError:
        logger.warning('re-try once after 5 sec')
        time.sleep(5)
        c.execute("SELECT id, url FROM url_queue WHERE site_id = %s LIMIT 1", (site_id,))

    r = c.fetchone()
    if r is not None:
        id = int(r[0])
        url = r[1]
    else:
        logger.info("URL queue is empty")
        return None

    try:
        c.execute("DELETE FROM url_queue WHERE site_id = %s AND id = %s", (site_id, id))
    except OperationalError:
        logger.warning('re-try once after 5 sec')
        time.sleep(5)
        c.execute("DELETE FROM url_queue WHERE site_id = %s AND id = %s", (site_id, id))

    return url

def delete_url_queue(con, site_id, url):
    c = con.cursor()
    try:
        c.execute("DELETE FROM url_queue WHERE site_id = %s AND url = %s", (site_id, url))
    except OperationalError:
        logger.warning('re-try once after 5 sec')
        time.sleep(5)
        c.execute("DELETE FROM url_queue WHERE site_id = %s AND url = %s", (site_id, url))

def is_empty_url_queue(con, site_id):
    c = con.cursor()
    try:
        c.execute("SELECT count(id) FROM url_queue WHERE site_id = %s LIMIT 1", (site_id,))
    except OperationalError:
        logger.warning('re-try once after 5 sec')
        time.sleep(5)
        c.execute("SELECT count(id) FROM url_queue WHERE site_id = %s LIMIT 1", (site_id,))

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
def is_downloaded(con, site_id, url):
    c = con.cursor()
    try:
        c.execute("SELECT count(id) FROM img_stats WHERE site_id = %s AND url = %s AND status = 1 LIMIT 1", (site_id, url))
    except OperationalError:
        logger.warning('re-try once after 5 sec')
        time.sleep(5)
        c.execute("SELECT count(id) FROM img_stats WHERE site_id = %s AND url = %s AND status = 1 LIMIT 1", (site_id, url))

    r = c.fetchone()
    count = int(r[0])
    return (count > 0)

def is_checked(con, site_id, url):
    c = con.cursor()
    try:
        c.execute("SELECT count(id) FROM img_stats WHERE site_id = %s AND url = %s AND status = 2 LIMIT 1", (site_id, url))
    except OperationalError:
        logger.warning('re-try once after 5 sec')
        time.sleep(5)
        c.execute("SELECT count(id) FROM img_stats WHERE site_id = %s AND url = %s AND status = 2 LIMIT 1", (site_id, url))

    r = c.fetchone()
    count = int(r[0])
    return (count > 0)

# True  ... scanned (or scanning now)
# False ... not sacnned yet
def is_scanned(con, site_id, url):
    c = con.cursor()
    try:
        c.execute("SELECT count(id) FROM pg_stats WHERE site_id = %s AND url = %s AND status >= 1 LIMIT 1", (site_id, url))
    except OperationalError:
        logger.warning('re-try once after 5 sec')
        time.sleep(5)
        c.execute("SELECT count(id) FROM pg_stats WHERE site_id = %s AND url = %s AND status >= 1 LIMIT 1", (site_id, url))

    r = c.fetchone()
    count = int(r[0])
    return (count > 0)

def set_downloaded(con, site_id, url):
    c = con.cursor()
    try:
        c.execute("INSERT INTO img_stats (site_id, url, status) VALUES (%s, %s, 1)", (site_id, url))
    except OperationalError:
        logger.warning('re-try once after 5 sec')
        time.sleep(5)
        c.execute("INSERT INTO img_stats (site_id, url, status) VALUES (%s, %s, 1)", (site_id, url))

def set_checked(con, site_id, url):
    c = con.cursor()
    try:
        c.execute("INSERT INTO img_stats (site_id, url, status) VALUES (%s, %s, 2)", (site_id, url))
    except OperationalError:
        logger.warning('re-try once after 5 sec')
        time.sleep(5)
        c.execute("INSERT INTO img_stats (site_id, url, status) VALUES (%s, %s, 2)", (site_id, url))

def set_scanning(con, site_id, url):
    c = con.cursor()
    try:
        c.execute("INSERT INTO pg_stats (site_id, url, status) VALUES (%s, %s, 1)", (site_id, url))
    except OperationalError:
        logger.warning('re-try once after 5 sec')
        time.sleep(5)
        c.execute("INSERT INTO pg_stats (site_id, url, status) VALUES (%s, %s, 1)", (site_id, url))

def set_scanned(con, site_id, url):
    c = con.cursor()
    try:
        c.execute("INSERT INTO pg_stats (site_id, url, status) VALUES (%s, %s, 2)", (site_id, url))
    except OperationalError:
        logger.warning('re-try once after 5 sec')
        time.sleep(5)
        c.execute("INSERT INTO pg_stats (site_id, url, status) VALUES (%s, %s, 2)", (site_id, url))


def download_image(con, site_id, img_url, referer, dirname):
    filename = os.path.basename(img_url)
    if os.path.isfile(dirname + "/" + filename):
        logger.info("SKIP [downloaded] " + img_url)
        return

    img_res = requests.get(img_url, headers={'referer': referer})
    img_res.raise_for_status()
              
    try:
        img_pil = Image.open(io.BytesIO(img_res.content))
        w, h = img_pil.size
    except IOError:
        set_checked(con, site_id, img_url)
        logger.warning("might not be image - {0}".format(img_url))
        return

    if(w < min_width or h < min_height):
        set_checked(con, site_id, img_url)
        logger.info("SKIP [too small] " + img_url)
        return

    if not os.path.isdir(dirname):
        os.makedirs(dirname)

    try:
        fout = open(dirname + "/" + filename, "wb")
        fout.write(img_res.content)
        logger.info("DOWNLOAD " + img_url)
    finally:
        if fout is not None:
            fout.close()
    set_downloaded(con, site_id, img_url)

def _downlaod_images(con, site_id, url, dom, basedir, img_url):
    root, ext = os.path.splitext(img_url)
    if ext.lower() not in image_exts:
        logger.info("SKIP [extention] %s [%s]" % (img_url, ','.join(image_exts)))
        return

    if is_downloaded(con, site_id, img_url):
        logger.info("SKIP [downloaded] " + img_url)
        return

    if is_checked(con, site_id, img_url):
        logger.info("SKIP [checked] " + img_url)
        return

    title = dom('title').text()
    dirname = basedir + "/" + text_to_filename(title)
    #TODO dirname uniq

    try:
        download_image(con, site_id, img_url, url, dirname)
    except Exception as e:
        logger.warning("{0}".format(e))
        logger.warning(traceback.format_exc())

def download_images(con, site_id, url, dom, basedir):
    for img in dom('img').items():
        if img.attr['src'] is None:
            continue
        img_url = re.sub("[\r\n ]", '', img.attr['src'])
        img_url = urlparse.urljoin(url, img_url)
        _downlaod_images(con, site_id, url, dom, basedir, img_url)

    for a in dom('a').items():
        if a.attr['href'] is None:
            continue
        img_url = re.sub("[\r\n ]", '', a.attr['href'])
        img_url = urlparse.urljoin(url, img_url)
        _downlaod_images(con, site_id, url, dom, basedir, img_url)

def is_same_site(baseurl, url):
    baseurl_tmp = re.sub('^https?://', '', baseurl)
    url_tmp = re.sub('^https?://', '', url)
    return url_tmp.startswith(baseurl_tmp)

def scan_links(con, site_id, url, dom, baseurl):
    for a in dom('a').items():
        if a.attr['href'] is None:
            continue
        link_url = re.sub("[\r\n ]", '', a.attr['href'])
        link_url = urlparse.urljoin(url, link_url)
        link_url = re.sub('#.*$', '', link_url)

        if not is_same_site(baseurl, link_url):
            logger.info("SKIP [external site] " + link_url)
            continue

        root, ext = os.path.splitext(link_url)
        if ext.lower() in all_image_exts:
            logger.info("SKIP [not html] " + link_url)
            continue

        if is_scanned(con, site_id, link_url):
            logger.info("SKIP [scanned] " + link_url)
            continue

        push_url_queue(con, site_id, link_url)

# 最初に渡したURLはスキャン済みでとりあえずスキャンする
def scan(con, site_id, baseurl, basedir):
    is_first = True

    while (is_first or not is_empty_url_queue(con, site_id)):
        if is_first:
            url = baseurl
            is_first = False
        else:
            url = pop_url_queue(con, site_id)
            if url is None:
                break
            if is_scanned(con, site_id, url):
                logger.info("SKIP [scanned] " + url)
                continue

        logger.info("SCAN " + url)
        set_scanning(con, site_id, url)
        try:
            res = requests.get(url)
            res.raise_for_status()

            logger.debug(res.text)
            dom = pq(b"<meta charset='" + res.apparent_encoding + "'/>" + res.content)
            download_images(con, site_id, url, dom, basedir)
            if not only_the_page:
                scan_links(con, site_id, url, dom, baseurl)
        except Exception as e:
            logger.warning("{0}".format(e))
            logger.warning(traceback.format_exc())

        set_scanned(con, site_id, url)
        

def run(url):
    url = re.sub('[\.\? _/]*$', '', url)
    (con, site_id, basedir) = init(url)
    scan(con, site_id, url, basedir)

if __name__ == '__main__':
    try:
        url = sys.argv[1]
    except:
        print("usage: image_downloader.py URL")
        sys.exit(1)

    try:
        run(url)
    except:
        logger.error(traceback.format_exc())
        sys.exit(1)
    finally:
        try:
            con.close()
        except:
            pass
    logger.info("DONE")
    sys.exit(0)

