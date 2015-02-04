#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created by longqi on 8/18/14
"""
__author__ = 'longqi'

sys.path.append('/root/SPMS_Quantum/file_process')
sys.path.append('/root/SPMS_Quantum/model')
sys.path.append('/root/SPMS_Quantum/db')
import os
import re
import threading

import pyinotify
from pyinotify import WatchManager, Notifier

from file_process.file_process import target_distribute, file_distribute_thread, FileParser
from file_worker.file_worker import file_worker


class Options:
    # __slots__ = ["directory", "regex", "script"]

    def __init__(self):
        # self.directory = '/root/data/'
        self.directory = '/home/longqi/Dropbox/python/SPMS_Quantum/'
        self.file_filter = '.*dat$'


def restart_program():
    """Restarts the current program.
    Note: this function does not return. Any cleanup action (like
    saving data) must be done before calling this function."""
    python = sys.executable
    os.execl(python, python, *sys.argv)


class EventHandler(pyinotify.ProcessEvent):
    def __init__(self, options):
        self.regex = re.compile(options.file_filter, re.IGNORECASE)
        # self.script = options.script

    def process_IN_CREATE(self, event):
        target = os.path.join(event.path, event.name)

        # if os.path.isdir(target):
        # print('new direction: ' + target)

        # if self.regex.match(target):
        # print('create: ' + target)

    def process_IN_DELETE(self, event):
        target = os.path.join(event.path, event.name)
        # print('delete: ' + target)

    def process_IN_ATTRIB(self, event):
        target = os.path.join(event.path, event.name)
        print('attrib: ' + target)
        if self.regex.match(target):
            self.call_file_parser(target)

    def process_IN_CLOSE_WRITE(self, event):
        target = os.path.join(event.path, event.name)
        print('IN_CLOSE_WRITE: ' + target)
        if self.regex.match(target):
            self.call_file_parser(target)

    def call_file_parser(self, target):
        print('processing ' + target)
       # target_distribute(target)


if __name__ == "__main__":
    import sys

    file_parser = FileParser(target=None)
    fp_thread = threading.Thread(target=file_distribute_thread, args=(file_parser,))
    fp_thread.start()

    monitor_options = Options()

    fw_thread = threading.Thread(target=file_worker, args=(monitor_options.directory,))  # find the newest file
    fw_thread.start()

    while True:
        wm = WatchManager()
        handler = EventHandler(monitor_options)
        notifier = Notifier(wm, handler)
        event_mask = pyinotify.IN_DELETE | pyinotify.IN_CLOSE_WRITE | pyinotify.IN_CREATE | pyinotify.IN_ATTRIB | pyinotify.IN_ACCESS
        wdd = wm.add_watch(monitor_options.directory, event_mask, auto_add=True, rec=True)

        try:
            notifier.loop(pid_file='/tmp/pyinotify.pid')
        except (KeyboardInterrupt, pyinotify.PyinotifyError):
            notifier.stop()
