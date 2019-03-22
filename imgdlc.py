#!/usr/bin/python

import sys
import socket

# ic config 
# ic config (param) (value)

# ic scan (url)

# ic status 
# ic status (site_id)
# ic status (url)

# ic shutdown

def usage():
    print "usage: ic (command) [params]"

def run_status(args):
    scan_statuses = []
    try:
        s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        s.connect(('localhost', 8686))
        s.send('status')
        while True:
            msg = s.recv(1024)
            if msg == None or msg.startswith('2') or msg.startswith('5'):
                # 2xx or 5xx
                break

            if not msg.startswith('STATUS'):
                break

            data = msg.split()
            scan_statuses.append(data)

    finally:
        s.close()

    if len(scan_statuses) == 0:
        print "no scans are running"
        return
    
    # check values
    #   0         1     2                      3              4    5      6     7    8     9    10
    # ['STATUS', '21', '2019-03-22-19:54:00', '00-00:03:34', '1', '214', '29', '7', '70', '3', 'https://example.com']
    labels = ['FLAG', 'ID', 'Start', 'Time', 'Scan', 'Scan Speed', 'Download', 'Download Speed', 'Check', 'Check Speed', 'URL']
    num_values = len(labels)
    max_lens = [0] * num_values
    is_within_24hour = True

    for i in range(num_values):
        max_lens[i]= len(labels[i])

    for status in scan_statuses:
        for i in range(num_values):
            if len(status[i]) > max_lens[i]:
                max_lens[i]= len(status[i])

        days, time = status[3].split('-')
        if int(days) > 0:
            is_within_24hour = False

    # ID URL start time scan download check
    print ('%{idlen}s %-{urllen}s  %-{startlen}s %-{timelen}s  %{scanlen}s %{downloadlen}s %{checklen}s'.format( \
                idlen = max_lens[1], \
                urllen = max_lens[10], \
                startlen = max_lens[2], \
                timelen = max_lens[3], \
                scanlen = max_lens[4], \
                downloadlen = max_lens[6], \
                checklen = max_lens[8] \
            )) % (labels[1], labels[10], labels[2], labels[3], labels[4], labels[6], labels[8])
    for status in scan_statuses:
        print ('%{idlen}s %-{urllen}s  %{startlen}s %{timelen}s  %{scanlen}s %{downloadlen}s %{checklen}s'.format( \
                idlen = max_lens[1], \
                urllen = max_lens[10], \
                startlen = max_lens[2], \
                timelen = max_lens[3], \
                scanlen = max_lens[4], \
                downloadlen = max_lens[6], \
                checklen = max_lens[8] \
            )) % (status[1], status[10], status[2], status[3], status[4], status[6], status[8])


if len(sys.argv) <= 1:
    usage()
    sys.exit(1)

command = sys.argv[1].lower()

if command == 'config':
    pass

elif command == 'scan':
    pass

elif command == 'status':
    run_status(sys.argv[2:])

elif command == 'shutdown':
    pass

else:
    print 'unknown command: {}'.format(command)
    usage()
    sys.exit(1)
