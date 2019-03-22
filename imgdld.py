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
def check_threads_alive():
    global imgdownloader_threads

    del_threads = []
    for t in imgdownloader_threads:
        if not t.is_alive():
            del_threads.append(t)

    for t in del_threads:
        imgdownloader_threads.remove(t)

download_wait_sec = 0
scan_wait_sec = 0
min_width = 600
min_height = 600
def run_command(socket, server_info, command_info, command):
    global imgdownloader_threads
    global download_wait_sec
    global scan_wait_sec
    global min_width
    global min_height
    logger.info("command: {}".format(command))

    check_threads_alive()
    
    cmd_lower = command.lower()
    if cmd_lower == 'shutdown':
        for t in imgdownloader_threads:
            t.stop()
        for t in imgdownloader_threads:
            t.join()
            send(socket, 'stopped {}'.format(t.baseurl))
        server_info.status = ServerInfo.ST_SHUTDOWN
        send(socket, '200 shutdown daemon')

    elif cmd_lower == "quit":
        command_info.status = CommandInfo.ST_EXIT

    elif cmd_lower.startswith("scan "):
        cmd, url = command.split()
        if url is None or len(url) == 0:
            send(socket, '500 error: scan (url)')
            return

        imgdl = imgdownloader.ImageDownloader(url, \
                    download_wait_sec = download_wait_sec, \
                    scan_wait_sec = scan_wait_sec, \
                    min_width = min_width, \
                    min_height = min_height)
        imgdownloader_threads.append(imgdl)
        imgdl.start()

        send(socket, '200 started scanning {}'.format(url))

    elif cmd_lower == 'status':
        n_threads = 0
        for t in imgdownloader_threads:
            if t.is_alive():
                now = datetime.now()
                scanning_time = now - t.start_date
                scanning_time_hours = int(scanning_time.seconds / (60*60))
                scanning_time_minutes = int(scanning_time.seconds % (60*60) / 60)
                scanning_time_seconds = scanning_time.seconds % 60
                scanning_time_str = ("%02d-%02d:%02d:%02d" % ( 
                            scanning_time.days, \
                            scanning_time_hours, \
                            scanning_time_minutes, \
                            scanning_time_seconds))

                download_speed = int(scanning_time.seconds / t.num_of_downloaded_images) if t.num_of_downloaded_images > 0 else 0
                check_speed = int(scanning_time.seconds / t.num_of_checked_images) if t.num_of_checked_images > 0 else 0
                scan_speed = int(scanning_time.seconds / t.num_of_scanned_pages) if  t.num_of_scanned_pages > 0 else 0

                send(socket, 'STATUS {site_id} {start_date} {scanning_time} {scanned_page} {scan_page_speed} {downloaded_image} {download_image_speed} {checked_image} {check_image_speed} {url}'.format( \
                        site_id=t.site_id, \
                        start_date= t.start_date.strftime("%Y-%m-%d-%H:%M:%S"), \
                        scanning_time=scanning_time_str, \
                        scanned_page=t.num_of_scanned_pages, \
                        scan_page_speed=scan_speed, \
                        downloaded_image=t.num_of_downloaded_images, \
                        download_image_speed=download_speed, \
                        checked_image=t.num_of_checked_images, \
                        check_image_speed=check_speed, \
                        url=t.baseurl))

                n_threads += 1

        if n_threads > 0:
            send(socket, '200 {} scans running'.format(n_threads))
        else:
            send(socket, '200 no scan running')

    elif cmd_lower.startswith('set '):
        params = command.split()
        if params[1] == 'download_wait_sec':
            try:
                download_wait_sec = int(params[2])
                for t in imgdownloader_threads:
                    t.download_wait_sec = download_wait_sec
                send(socket, '200 set download_wait_sec {}'.format(download_wait_sec))
            except:
                send(socket, '500 set download_wait_sec (sec)')
        elif params[1] == 'scan_wait_sec':
            try:
                scan_wait_sec = int(params[2])
                for t in imgdownloader_threads:
                    t.scan_wait_sec = scan_wait_sec
                send(socket, '200 set scan_wait_sec {}'.format(scan_wait_sec))
            except:
                send(socket, '500 set scan_wait_sec (sec)')
        elif params[1] == 'min_width':
            try:
                min_width = int(params[2])
                for t in imgdownloader_threads:
                    t.min_width = min_width
                send(socket, '200 set min_width {}'.format(min_width))
            except:
                send(socket, '500 set min_width (pixel)')
        elif params[1] == 'min_height':
            try:
                min_height = int(params[2])
                for t in imgdownloader_threads:
                    t.min_hight = min_height
                send(socket, '200 set min_height {}'.format(min_heigh))
            except:
                send(socket, '500 set min_height (pixel)')

    elif cmd_lower == 'config':
        send(socket, 'download_wait_sec {}'.format(download_wait_sec))
        send(socket, 'scan_wait_sec {}'.format(scan_wait_sec))
        send(socket, 'min_width {}'.format(min_width))
        send(socket, 'min_height {}'.format(min_height))
        send(socket, '200 done')

    else:
        send(socket, '500 unknown command {}'.format(command))


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
                try:
                    send(clientsocket, '500 ERROR')
                except:
                    pass


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
