#!/usr/bin/python
# -*- coding: utf-8 -*-
import time
import codecs
import io
import os
import socket
import sys
import traceback
import logging 
import logging.handlers
import imgdownloader
import threading
from datetime import datetime

reload(sys)
sys.setdefaultencoding('utf8')

logger = None

class ServerInfo:
    ST_RUNNING = 1
    ST_SHUTDOWN = 2

    def __init__(self):
        self.status = ServerInfo.ST_RUNNING

class CommandInfo:
    ST_RUNNING = 1
    ST_EXIT = 2
    def __init__(self):
        self.status = CommandInfo.ST_RUNNING


# status
#   url [from thread]
#   start date [from thread]
#   scaning time (current scan) [from thread]
#   number of downloaded images (current scan) [from thread]
#   number of scanned pages (current scan) [from thread]
#   number of checked images (current scan) [from thread]
#   download speed [from thread and calculate]
#   scan speed [from thread and calculate]

# status (detail)
#   url [from thread]
#   start date [from thread]
#   scaning time (current scan) [from thread]
#   number of downloaded images (current scan) [from thread]
#   number of scanned pages (current scan) [from thread]
#   number of checked images (current scan) [from thread]
#   download speed [from thread and calculate]
#   scan speed [from thread and calculate]
#   total scaning time [from db]
#   total number of downloaded images [from db]
#   total number of scanned pages [from db]
#   total number of checked images [from db]

# setting 
#   download wait sec
#   scan wait sec
#   min of image width
#   min of image height

imgdownloader_threads = []
def run_command(socket, server_info, command_info, command):
    global imgdownloader_threads
    logger.info("command: {}".format(command))
    
    cmd_lower = command.lower()
    if cmd_lower == 'shutdown':
        for t in imgdownloader_threads:
            t.stop()
        for t in imgdownloader_threads:
            t.join()
            send(socket, 'stopped scanning {}'.format(t.baseurl))
        server_info.status = ServerInfo.ST_SHUTDOWN
        send(socket, 'shutdown daemon')

    elif cmd_lower == "quit":
        command_info.status = CommandInfo.ST_EXIT

    elif cmd_lower.startswith("scan "):
        cmd, url = command.split()
        if url is None or len(url) == 0:
            send(socket, 'error: scan (url)')
            return

        imgdl = imgdownloader.ImageDownloader(url)
        imgdownloader_threads.append(imgdl)
        imgdl.start()

        send(socket, 'started scanning {}'.format(url))

    elif cmd_lower == 'status':
        n_threads = 0
        for t in imgdownloader_threads:
            if t.is_alive():
                now = datetime.now()
                scanning_time = now - t.start_date
                scanning_time_days = int(scanning_time.seconds / (60*60*24))
                scanning_time_hours = int(scanning_time.seconds % (60*60*24) / (60*60))
                scanning_time_minutes = int(scanning_time.seconds % (60*60) / 60)
                scanning_time_seconds = scanning_time.seconds % 60
                if scanning_time_days > 0:
                    scanning_time_str = ("%2dd " % scanning_time_days)
                else:
                    scanning_time_str = "    "
                scanning_time_str = ("%02d:%02d:%02d" % ( \
                            scanning_time_hours, \
                            scanning_time_minutes, \
                            scanning_time_seconds))

                download_speed = int(scanning_time.seconds / t.num_of_downloaded_images) if t.num_of_downloaded_images > 0 else 0
                check_speed = int(scanning_time.seconds / t.num_of_checked_images) if t.num_of_checked_images > 0 else 0
                scan_speed = int(scanning_time.seconds / t.num_of_scanned_pages) if  t.num_of_scanned_pages > 0 else 0

                downloaded_image_str = ("%6d download (%4d sec/image)" % (t.num_of_downloaded_images, download_speed))
                checked_image_str = ("%6d check (%4d sec/image)" % (t.num_of_checked_images, check_speed))
                scanned_page_str = ("%6d scan (%4d sec/page)" % (t.num_of_scanned_pages, scan_speed))

                send(socket, '{site_id}: {start_date} {scanning_time} {scanned_page} {downloaded_image} {checked_image} - {url}'.format( \
                        site_id=t.site_id, \
                        start_date= t.start_date.strftime("%Y-%m-%d %H:%M:%S"), \
                        scanning_time=scanning_time_str, \
                        scanned_page=scanned_page_str, \
                        downloaded_image=downloaded_image_str, \
                        checked_image=checked_image_str, \
                        url=t.baseurl))

                n_threads += 1

        if n_threads > 0:
            send(socket, '{} threads running'.format(n_threads))
        else:
            send(socket, 'no threads running')

    elif cmd_lower.startswith('set '):
        params = command.split()
        if params[1] == 'download_wait_sec':
            try:
                sec = int(params[2])
                for t in imgdownloader_threads:
                    t.download_wait_sec = sec
            except:
                send(socket, 'set download_wait_sec (sec)')
        elif params[1] == 'scan_wait_sec':
            try:
                sec = int(params[2])
                for t in imgdownloader_threads:
                    t.scan_wait_sec = sec
            except:
                send(socket, 'set scan_wait_sec (sec)')
        elif params[1] == 'min_width':
            try:
                pixel = int(params[2])
                for t in imgdownloader_threads:
                    t.min_width = pixel
            except:
                send(socket, 'set min_width (pixel)')
        elif params[1] == 'min_height':
            try:
                pixel = int(params[2])
                for t in imgdownloader_threads:
                    t.min_hight = pixel
            except:
                send(socket, 'set min_height (pixel)')

    else:
        send(socket, 'unknown command: {}'.format(command))


def recv(socket):
    try:
        command = socket.recv(1024)
        command = command.strip()
    except OSError:
        return ''

    return command

def send(socket, msg):
    sent_message = msg + "\r\n"
    while True:
        sent_len = socket.send(sent_message)
        # 全て送れたら完了
        if sent_len == len(sent_message):
            break
        # 送れなかった分をもう一度送る
        sent_message = sent_message[sent_len:]

    logger.info(msg)

def main_loop():
    logger.info("image-downloaderd started")

    serversocket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    serversocket.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, True)
    serversocket.bind(('localhost', 8686))
    serversocket.listen(1)
    
    server_info = ServerInfo()
    while server_info.status == ServerInfo.ST_RUNNING:
        clientsocket, (client_address, client_port) = serversocket.accept()
        logger.info('new connection: {0}:{1}'.format(client_address, client_port))

        command_info = CommandInfo()
        while command_info.status == CommandInfo.ST_RUNNING:
            try:
                command = recv(clientsocket)
                # finish connection when length of command is 0
                if len(command) == 0:
                    break
                # execute command
                run_command(clientsocket, server_info, command_info, command)

            except:
                logger.error(traceback.format_exc())


        clientsocket.close()
        logger.info('disconnected: {0}:{1}'.format(client_address, client_port))

    logger.info("shutdown")
    sys.exit(0)

def daemonize():
    global logger 

    logfile = "imgdld.log"
    handler = logging.handlers.RotatingFileHandler(logfile, maxBytes=10*1024*1024, backupCount=5)
    handler.setLevel(logging.INFO)
    handler.setFormatter(logging.Formatter('%(asctime)s %(levelname)s %(message)s'))
    logger = logging.getLogger(__name__)
    logger.setLevel(logging.INFO)
    logger.handlers = []
    logger.addHandler(handler)

    logger.info("start image-downloaderd daemon")

    # fork child process and separate process from here
    pid = os.fork()
    if pid > 0: # for parent process. pid = pid of chid process
        pid_file = open('imgdld.pid','w')
        pid_file.write(str(pid)+"\n")
        pid_file.close()
        sys.exit()
    if pid == 0: # for chid process
        main_loop()


if __name__ == '__main__':
    daemonize()
