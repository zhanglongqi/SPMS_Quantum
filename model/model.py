#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""
Created by longqi on 8/29/14
"""
__author__ = 'longqi'


class Model_Base():
    def __init__(self, name, value=None):
        self.name = name
        self.value = value


class HRU(object):
    def __init__(self):
        self.date_time = Model_Base('date_time')
        self.dp_temp = Model_Base('dp_temp')
        self.energy_consumption = Model_Base('energy_consumption')
        self.hru_rh = Model_Base('hru_rh')
        self.hru_rh_setpt = Model_Base('hru_rh_setpt')
        self.hru_temp = Model_Base('hru_temp')
        self.hru_temp_setpt = Model_Base('hru_temp_setpt')
        self.pressure = Model_Base('pressure')
        self.valve_position = Model_Base('valve_position')
        self.vsd_control = Model_Base('vsd_control')
        self.vsd_speed = Model_Base('vsd_speed')


class LAB(object):
    def __init__(self):
        self.date_time = Model_Base('date_time')
        self.fcv1 = Model_Base('fcv1')
        self.fcv2 = Model_Base('fcv2')
        self.fcv3 = Model_Base('fcv3')
        self.fcv4 = Model_Base('fcv4')
        self.fcv5 = Model_Base('fcv5')
        self.fcv6 = Model_Base('fcv6')
        self.fcv7 = Model_Base('fcv7')
        self.fcv8 = Model_Base('fcv8')
        self.single_gang_supply = Model_Base('single_gang_supply')
        self.double_gang_supply = Model_Base('double_gang_supply')
        self.temperature = Model_Base('temperature')

        self.so_temperature = Model_Base('so_temperature')
        self.so_boxflow = Model_Base('so_boxflow')
