#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""
Created by longqi on 8/28/14
"""
__author__ = 'longqi'
import sys

sys.path.append('/root/SPMS_Quantum/file_process')
sys.path.append('/root/SPMS_Quantum/model')
sys.path.append('/root/SPMS_Quantum/db')
import re
import time
from model.model import HRU, LAB
from db.db import LQ_DB_Cassandra

tar_doing = {'target': '', 'time': 0}
tar_new = {'target': '', 'time': 0}
targets = {'doing': tar_doing, 'new': tar_new}
targets_list = []


def target_distribute(target):
    # print('file_distribute for ' + target)
    global targets
    targets['new']['target'] = target
    targets['new']['time'] = time.time()
    if (not targets['new']['target'] == targets['doing']['target']) or \
                            targets['new']['time'] - targets['doing']['time'] > 20:  # set the interval to 10 seconds
        # sometimes the filesystem will trigger the pyinotify twice at one time
        # and also the reading will also trigger it one time
        targets['doing']['target'] = targets['new']['target']
        targets['doing']['time'] = targets['new']['time']
        targets_list.append(targets['doing']['target'])
        print('adding:  ' + targets['doing']['target'])


def file_distribute_thread(file_parser):
    while True:
        print((time.ctime(), 'file in the queue: ', len(targets_list)), end='\r')
        if len(targets_list) > 0:
            target = targets_list.pop(0)
            file_parser.target = target
            file_parser.recognise()
        else:
            time.sleep(2)


class FileParserBase():
    def __init__(self, target=None):

        self.target = target
        self.data_model = None
        self.db = LQ_DB_Cassandra()
        self.new_last_pos = 0

    def process(self):
        self.recognise()

    def recognise(self):
        pass

    def check_last_pos(self):
        # last_pos = LQ_DB_Cassandra().get_pos(self.target)
        last_pos = self.db.get_pos(self.target)
        print(self.target + ' \t' + str(last_pos))
        return last_pos

    def read_data_from_last_pos(self):
        last_pos = self.check_last_pos()

        newlines = []
        try:
            file = open(self.target, 'r')
        except IOError:
            return None
        # read the data from the last position
        print(self.target + '\t' + str(last_pos))
        file.seek(last_pos)
        for newline in file:
            newlines.append(newline)

        self.new_last_pos = file.tell()
        file.close()

        print(newlines)
        return newlines
        # return all the new data from the last position to the end of the file.

    def str_to_datetime(self, source):
        # check the availability of the source string
        # 30/09/14,02:15:00
        date_re = re.compile('^\d\d/\d\d/\d\d$')
        time_re = re.compile('^\d\d:\d\d:\d\d$')
        if not (date_re.match(source[0]) and time_re.match(source[1])):
            return 0

        # for the date and time
        # convert string to seconds from epoch
        datetime = source[0].split('/')

        # TimeTuple

        for temp in source[1].split(':'):
            datetime.append(temp)
        datetime[2] = '20' + datetime[2]
        datetime.append('0')
        datetime.append('0')
        datetime.append('0')

        datetime_int = []
        for temp in datetime:
            datetime_int.append(int(temp))

        year = datetime_int[2]
        month = datetime_int[1]
        day = datetime_int[0]

        datetime_int[0] = year
        datetime_int[1] = month
        datetime_int[2] = day

        # print(datetime_int)
        seconds = int(time.mktime(tuple(datetime_int)))
        return seconds


class FileParser(FileParserBase):
    def __init__(self, target):

        FileParserBase.__init__(self, target=target)
        print('prepare for parsing file')

        self.hru_dew_re = re.compile('.*HRUCR4_1DEW.*dat$')
        self.hru_temp_re = re.compile('.*HRUCR4_1TEMP.*dat$')
        self.hru_vsd_re = re.compile('.*HRUCR4_1VSD.*dat$')

        self.lab1_a_re = re.compile('.*Lab1A.*dat$')
        self.lab1_b_re = re.compile('.*Lab1B.*dat$')
        self.lab1_temp_re = re.compile('.*Lab1TEMP.*dat$')
        self.lab1_so_re = re.compile('.*Lab1SO.*dat$')

        self.lab2_a_re = re.compile('.*Lab2A.*dat$')
        self.lab2_b_re = re.compile('.*Lab2B.*dat$')
        self.lab2_temp_re = re.compile('.*Lab2TEMP.*dat$')
        self.lab2_so_re = re.compile('.*Lab2SO.*dat$')

        self.lab3_a_re = re.compile('.*Lab3A.*dat$')
        self.lab3_b_re = re.compile('.*Lab3B.*dat$')
        self.lab3_temp_re = re.compile('.*Lab3TEMP.*dat$')
        self.lab3_so_re = re.compile('.*Lab3SO.*dat$')

        self.lab4_a_re = re.compile('.*Lab4A.*dat$')
        self.lab4_b_re = re.compile('.*Lab4B.*dat$')
        self.lab4_temp_re = re.compile('.*Lab4TEMP.*dat$')
        self.lab4_so_re = re.compile('.*Lab4SO.*dat$')

        self.lab5_a_re = re.compile('.*Lab5A.*dat$')
        self.lab5_b_re = re.compile('.*Lab5B.*dat$')
        self.lab5_temp_re = re.compile('.*Lab5TEMP.*dat$')
        self.lab5_so_re = re.compile('.*Lab5SO.*dat$')

        self.lab6_a_re = re.compile('.*Lab6A.*dat$')
        self.lab6_b_re = re.compile('.*Lab6B.*dat$')
        self.lab6_temp_re = re.compile('.*Lab6TEMP.*dat$')
        self.lab6_so_re = re.compile('.*Lab6SO.*dat$')

        self.lab7_a_re = re.compile('.*Lab7A.*dat$')
        self.lab7_b_re = re.compile('.*Lab7B.*dat$')
        self.lab7_temp_re = re.compile('.*Lab7TEMP.*dat$')
        self.lab7_so_re = re.compile('.*Lab7SO.*dat$')

        self.lab8_a_re = re.compile('.*Lab8A.*dat$')
        self.lab8_b_re = re.compile('.*Lab8B.*dat$')
        self.lab8_temp_re = re.compile('.*Lab8TEMP.*dat$')
        self.lab8_so_re = re.compile('.*Lab8SO.*dat$')

        self.lab8_so2_re = re.compile('.*Lab8SO2.*dat$')

    def recognise(self):
        if self.hru_dew_re.match(self.target):
            self.deal_hru_dew()

        elif self.hru_temp_re.match(self.target):
            self.deal_hru_temp()

        elif self.hru_vsd_re.match(self.target):
            self.deal_hru_vsd()

        elif self.lab1_a_re.match(self.target):
            self.deal_lab_a(lab_name='lab1')

        elif self.lab1_b_re.match(self.target):
            self.deal_lab_b(lab_name='lab1')

        elif self.lab1_temp_re.match(self.target):
            self.deal_lab_temp(lab_name='lab1')

        elif self.lab1_so_re.match(self.target):
            self.deal_lab_so(lab_name='lab1')

        elif self.lab2_a_re.match(self.target):
            self.deal_lab_a(lab_name='lab2')

        elif self.lab2_b_re.match(self.target):
            self.deal_lab_b(lab_name='lab2')

        elif self.lab2_temp_re.match(self.target):
            self.deal_lab_temp(lab_name='lab2')

        elif self.lab2_so_re.match(self.target):
            self.deal_lab_so(lab_name='lab2')

        elif self.lab3_a_re.match(self.target):
            self.deal_lab_a(lab_name='lab3')

        elif self.lab3_b_re.match(self.target):
            self.deal_lab_b(lab_name='lab3')

        elif self.lab3_temp_re.match(self.target):
            self.deal_lab_temp(lab_name='lab3')

        elif self.lab3_so_re.match(self.target):
            self.deal_lab_so(lab_name='lab3')

        elif self.lab4_a_re.match(self.target):
            self.deal_lab_a(lab_name='lab4')

        elif self.lab4_b_re.match(self.target):
            self.deal_lab_b(lab_name='lab4')

        elif self.lab4_temp_re.match(self.target):
            self.deal_lab_temp(lab_name='lab4')

        elif self.lab4_so_re.match(self.target):
            self.deal_lab_so(lab_name='lab4')

        elif self.lab5_a_re.match(self.target):
            self.deal_lab_a(lab_name='lab5')

        elif self.lab5_b_re.match(self.target):
            self.deal_lab_b(lab_name='lab5')

        elif self.lab5_temp_re.match(self.target):
            self.deal_lab_temp(lab_name='lab5')

        elif self.lab5_so_re.match(self.target):
            self.deal_lab_so(lab_name='lab5')

        elif self.lab6_a_re.match(self.target):
            self.deal_lab_a(lab_name='lab6')

        elif self.lab6_b_re.match(self.target):
            self.deal_lab_b(lab_name='lab6')

        elif self.lab6_temp_re.match(self.target):
            self.deal_lab_temp(lab_name='lab6')

        elif self.lab6_so_re.match(self.target):
            self.deal_lab_so(lab_name='lab6')

        elif self.lab7_a_re.match(self.target):
            self.deal_lab_a(lab_name='lab7')

        elif self.lab7_b_re.match(self.target):
            self.deal_lab_b(lab_name='lab7')

        elif self.lab7_temp_re.match(self.target):
            self.deal_lab_temp(lab_name='lab7')

        elif self.lab7_so_re.match(self.target):
            self.deal_lab_so(lab_name='lab7')

        elif self.lab8_a_re.match(self.target):
            self.deal_lab_a(lab_name='lab8')

        elif self.lab8_b_re.match(self.target):
            self.deal_lab_b(lab_name='lab8')

        elif self.lab8_temp_re.match(self.target):
            self.deal_lab_temp(lab_name='lab8')

        elif self.lab8_so_re.match(self.target):
            self.deal_lab_so(lab_name='lab8')

        elif self.lab8_so2_re.match(self.target):
            self.deal_lab8_so2(lab_name='lab8_so2')
        else:
            pass
        print(self.target + '\t' + 'deal finished***************')

    def deal_hru_dew(self):
        print('deal_hru_dew')

        lines = self.read_data_from_last_pos()
        self.data_model = HRU()

        for line in lines:
            if len(line) <= 10 or line[0] == 'D':  # especially for the fist line which contains nothing or just title
                continue

            else:
                if line.count('Date') > 0:  # especially for the second line which contain some rubbish
                    line = line[:-45]

                print('line: ' + line)
                data_list = line.split(',')

                # for the date and time
                # convert string to seconds from epoch
                self.data_model.date_time.value = self.str_to_datetime(data_list)
                print('time: ' + str(time.ctime(self.data_model.date_time.value)))
                print('seconds: ' + str(self.data_model.date_time.value))

                self.data_model.dp_temp.value = float(data_list[2])
                self.data_model.valve_position.value = float(data_list[3])

                # insert values to database insert_values(self, table, mid, parameters, values):
                try:
                    self.db.insert_values(table='spms_hru', mid='hrucr4_1',
                                          date_time=self.data_model.date_time.value * 1000,
                                          # I don't know why, I get the the solution from the code of Cassandra driver
                                          parameters=
                                          [self.data_model.dp_temp.name,
                                           self.data_model.valve_position.name],
                                          values=
                                          [self.data_model.dp_temp.value,
                                           self.data_model.valve_position.value])
                except:
                    return
                self.db.update_pos(self.target,
                                   self.new_last_pos)  # write new position to db only after the insert success

    def deal_hru_temp(self):
        print('deal_hru_temp')

        lines = self.read_data_from_last_pos()
        self.data_model = HRU()

        for line in lines:
            if len(line) <= 10 or line[0] == 'D':  # especially for the fist line which contains nothing or just title
                continue

            else:
                if line.count('Date') > 0:  # especially for the second line which contain some rubbish
                    line = line[:-73]

                print('line: ' + line)
                data_list = line.split(',')

                # for the date and time
                # convert string to seconds from epoch
                self.data_model.date_time.value = self.str_to_datetime(data_list)
                print(time.ctime(self.data_model.date_time.value))

                self.data_model.hru_temp.value = float(data_list[2])
                self.data_model.hru_temp_setpt.value = float(data_list[3])
                self.data_model.hru_rh.value = float(data_list[4])
                self.data_model.hru_rh_setpt.value = float(data_list[5])
                self.data_model.energy_consumption.value = float(data_list[6])

                try:
                    self.db.insert_values(table='spms_hru', mid='hrucr4_1',
                                          date_time=self.data_model.date_time.value * 1000,
                                          # I don't know why, I get the the solution from the code of Cassandra driver
                                          parameters=
                                          [self.data_model.hru_temp.name,
                                           self.data_model.hru_temp_setpt.name,
                                           self.data_model.hru_rh.name,
                                           self.data_model.hru_rh_setpt.name,
                                           self.data_model.energy_consumption.name],
                                          values=
                                          [self.data_model.hru_temp.value,
                                           self.data_model.hru_temp_setpt.value,
                                           self.data_model.hru_rh.value,
                                           self.data_model.hru_rh_setpt.value,
                                           self.data_model.energy_consumption.value])
                except:
                    return
                self.db.update_pos(self.target, self.new_last_pos)

    def deal_hru_vsd(self):
        print('deal_hru_vsd')

        lines = self.read_data_from_last_pos()
        self.data_model = HRU()

        for line in lines:
            if len(line) <= 10 or line[0] == 'D':  # especially for the fist line which contains nothing or just title
                continue

            else:
                if line.count('Date') > 0:  # especially for the second line which contain some rubbish
                    line = line[:-38]

                print('line: ' + line)
                data_list = line.split(',')

                # for the date and time
                # convert string to seconds from epoch
                self.data_model.date_time.value = self.str_to_datetime(data_list)
                print(time.ctime(self.data_model.date_time.value))

                self.data_model.pressure.value = float(data_list[2])
                self.data_model.vsd_control.value = float(data_list[3])
                self.data_model.vsd_speed.value = float(data_list[4])

                try:
                    self.db.insert_values(table='spms_hru', mid='hrucr4_1',
                                          date_time=self.data_model.date_time.value * 1000,
                                          # I don't know why, I get the the solution from the code of Cassandra driver
                                          parameters=
                                          [self.data_model.pressure.name,
                                           self.data_model.vsd_control.name,
                                           self.data_model.vsd_speed.name],
                                          values=
                                          [self.data_model.pressure.value,
                                           self.data_model.vsd_control.value,
                                           self.data_model.vsd_speed.value])
                except:
                    return
                self.db.update_pos(self.target, self.new_last_pos)

    def deal_lab_a(self, lab_name):
        print('deal_lab_a: ' + lab_name)

        lines = self.read_data_from_last_pos()
        self.data_model = LAB()

        for line in lines:
            if len(line) <= 10 or line[0] == 'D':  # especially for the fist line which contains nothing or just title
                continue
            else:
                if line.count('Date') > 0:  # especially for the second line which contain some rubbish
                    line = line[:-49]

                print('line: ' + line)
                data_list = line.split(',')

                # for the date and time
                # convert string to seconds from epoch
                self.data_model.date_time.value = self.str_to_datetime(data_list)
                print(time.ctime(self.data_model.date_time.value))

                self.data_model.fcv1.value = float(data_list[2])
                self.data_model.fcv2.value = float(data_list[3])
                self.data_model.fcv3.value = float(data_list[4])
                self.data_model.fcv4.value = float(data_list[5])
                self.data_model.double_gang_supply.value = float(data_list[6])

                try:
                    self.db.insert_values(table='spms_lab', mid=lab_name,
                                          date_time=self.data_model.date_time.value * 1000,
                                          # I don't know why, I get the the solution from the code of Cassandra driver
                                          parameters=
                                          [self.data_model.fcv1.name,
                                           self.data_model.fcv2.name,
                                           self.data_model.fcv3.name,
                                           self.data_model.fcv4.name,
                                           self.data_model.double_gang_supply.name],
                                          values=
                                          [self.data_model.fcv1.value,
                                           self.data_model.fcv2.value,
                                           self.data_model.fcv3.value,
                                           self.data_model.fcv4.value,
                                           self.data_model.double_gang_supply.value])
                except:
                    return
                self.db.update_pos(self.target, self.new_last_pos)

    def deal_lab_b(self, lab_name):
        print('deal_lab_b: ' + lab_name)

        lines = self.read_data_from_last_pos()
        self.data_model = LAB()

        for line in lines:
            if len(line) <= 10 or line[0] == 'D':  # especially for the fist line which contains nothing or just title
                continue
            else:
                if line.count('Date') > 0:  # especially for the second line which contain some rubbish
                    line = line[:-49]

                print('line: ' + line)
                data_list = line.split(',')

                # for the date and time
                # convert string to seconds from epoch
                self.data_model.date_time.value = self.str_to_datetime(data_list)
                print(time.ctime(self.data_model.date_time.value))

                self.data_model.fcv5.value = float(data_list[2])
                self.data_model.fcv6.value = float(data_list[3])
                self.data_model.fcv7.value = float(data_list[4])
                self.data_model.fcv8.value = float(data_list[5])
                self.data_model.single_gang_supply.value = float(data_list[6])

                try:
                    self.db.insert_values(table='spms_lab', mid=lab_name,
                                          date_time=self.data_model.date_time.value * 1000,
                                          # I don't know why, I get the the solution from the code of Cassandra driver
                                          parameters=
                                          [self.data_model.fcv5.name,
                                           self.data_model.fcv6.name,
                                           self.data_model.fcv7.name,
                                           self.data_model.fcv8.name,
                                           self.data_model.single_gang_supply.name],
                                          values=
                                          [self.data_model.fcv5.value,
                                           self.data_model.fcv6.value,
                                           self.data_model.fcv7.value,
                                           self.data_model.fcv8.value,
                                           self.data_model.single_gang_supply.value])
                except:
                    return
                self.db.update_pos(self.target, self.new_last_pos)

    def deal_lab_temp(self, lab_name):
        print('deal_lab_temp: ' + lab_name)

        lines = self.read_data_from_last_pos()
        self.data_model = LAB()

        for line in lines:
            if len(line) <= 10 or line[0] == 'D':  # especially for the fist line which contains nothing or just title
                continue
            else:
                if line.count('Date') > 0:  # especially for the second line which contain some rubbish
                    line = line[:-22]

                print('line: ' + line)
                data_list = line.split(',')

                # for the date and time
                # convert string to seconds from epoch
                self.data_model.date_time.value = self.str_to_datetime(data_list)
                print(time.ctime(self.data_model.date_time.value))

                self.data_model.temperature.value = float(data_list[2])

                try:
                    self.db.insert_values(table='spms_lab', mid=lab_name,
                                          date_time=self.data_model.date_time.value * 1000,
                                          # I don't know why, I get the the solution from the code of Cassandra driver
                                          parameters=
                                          [self.data_model.temperature.name],
                                          values=
                                          [self.data_model.temperature.value])
                except:
                    return
                self.db.update_pos(self.target, self.new_last_pos)

    def deal_lab_so(self, lab_name):
        print('deal_lab_so: ' + lab_name)

        lines = self.read_data_from_last_pos()
        self.data_model = LAB()

        for line in lines:
            if len(line) <= 10 or line[0] == 'D':  # especially for the fist line which contains nothing or just title
                continue
            else:
                if line.count('Date') > 0:  # especially for the second line which contain some rubbish
                    line = line[:-35]

                print('line: ' + line)
                data_list = line.split(',')

                # for the date and time
                # convert string to seconds from epoch
                self.data_model.date_time.value = self.str_to_datetime(data_list)
                print(time.ctime(self.data_model.date_time.value))

                self.data_model.so_temperature.value = float(data_list[2])
                self.data_model.so_boxflow.value = float(data_list[3])

                try:
                    self.db.insert_values(table='spms_lab', mid=lab_name,
                                          date_time=self.data_model.date_time.value * 1000,
                                          # I don't know why, I get the the solution from the code of Cassandra driver
                                          parameters=
                                          [self.data_model.so_temperature.name,
                                           self.data_model.so_boxflow.name],
                                          values=
                                          [self.data_model.so_temperature.value,
                                           self.data_model.so_boxflow.value])
                except:
                    return
                self.db.update_pos(self.target, self.new_last_pos)


def test_file_process():
    file1 = '/home/longqi/Dropbox/python/SPMS_Quantum/test/Lab1A 10-2013.dat'
    file_parser1 = FileParser(target=file1)
    file_parser1.recognise()


# test_file_process()



