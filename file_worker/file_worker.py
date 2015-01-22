#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""
Created by longqi on 10/16/14
"""
__author__ = 'longqi'
import os
import time

from file_process.file_process import target_distribute


loop_interval = 5  # one complete walk per 20 minutes
changed_in_time = 20  # if the file is modified in 20 minutes, mark it


def file_worker_1(init_dir):
    for root, dirs, files in os.walk(init_dir):
        for file in files:

            file_path = os.path.join(root, file)
            try:
                file_c_time = os.path.getctime(file_path)
            except:
                pass
                continue

            if file_c_time < time.time() - 60 * changed_in_time:  # 20 minutes
                print(str(time.ctime(file_c_time)) + '\t' + file_path)
                target_distribute(file_path)

        for directory in dirs:
            file_worker_1(directory)


def file_worker(init_dir):
    while True:
        file_worker_1(init_dir)
        time.sleep(60 * loop_interval)  # do not walk too fast


file_worker('/tmp')