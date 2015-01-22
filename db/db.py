# -*- coding: utf-8 -*-
# !/usr/bin/env python
"""
Created by longqi on 8/19/14
"""
__author__ = 'longqi'

"""
DESCRIBE KEYSPACE data_collection ;
"""

"""
CREATE KEYSPACE data_collection WITH replication = {
  'class': 'SimpleStrategy',
  'replication_factor': '2'
};

USE data_collection;

CREATE TABLE last_read_pos (
  file text,
  pos float,
  PRIMARY KEY ((file))
) WITH
  bloom_filter_fp_chance=0.010000 AND
  caching='KEYS_ONLY' AND
  comment='' AND
  dclocal_read_repair_chance=0.100000 AND
  gc_grace_seconds=864000 AND
  index_interval=128 AND
  read_repair_chance=0.000000 AND
  replicate_on_write='true' AND
  populate_io_cache_on_flush='false' AND
  default_time_to_live=0 AND
  speculative_retry='99.0PERCENTILE' AND
  memtable_flush_period_in_ms=0 AND
  compaction={'class': 'SizeTieredCompactionStrategy'} AND
  compression={'sstable_compression': 'LZ4Compressor'};

CREATE TABLE meters_info (
  id text,
  location text,
  remark text,
  target text,
  type text,
  vender text,
  PRIMARY KEY ((id))
) WITH
  bloom_filter_fp_chance=0.010000 AND
  caching='KEYS_ONLY' AND
  comment='' AND
  dclocal_read_repair_chance=0.100000 AND
  gc_grace_seconds=864000 AND
  index_interval=128 AND
  read_repair_chance=0.000000 AND
  replicate_on_write='true' AND
  populate_io_cache_on_flush='false' AND
  default_time_to_live=0 AND
  speculative_retry='99.0PERCENTILE' AND
  memtable_flush_period_in_ms=0 AND
  compaction={'class': 'SizeTieredCompactionStrategy'} AND
  compression={'sstable_compression': 'LZ4Compressor'};

CREATE TABLE spms_hru (
  id text,
  date_time timestamp,
  dp_temp float,
  energy_consumption float,
  hru_rh float,
  hru_rh_setpt float,
  hru_temp float,
  hru_temp_setpt float,
  pressure float,
  valve_position float,
  vsd_control float,
  vsd_speed float,
  PRIMARY KEY ((id), date_time)
) WITH
  bloom_filter_fp_chance=0.010000 AND
  caching='KEYS_ONLY' AND
  comment='' AND
  dclocal_read_repair_chance=0.100000 AND
  gc_grace_seconds=864000 AND
  index_interval=128 AND
  read_repair_chance=0.000000 AND
  replicate_on_write='true' AND
  populate_io_cache_on_flush='false' AND
  default_time_to_live=0 AND
  speculative_retry='99.0PERCENTILE' AND
  memtable_flush_period_in_ms=0 AND
  compaction={'class': 'SizeTieredCompactionStrategy'} AND
  compression={'sstable_compression': 'LZ4Compressor'};

CREATE TABLE spms_lab (
  id text,
  date_time timestamp,
  double_gang_supply float,
  fcv1 float,
  fcv2 float,
  fcv3 float,
  fcv4 float,
  fcv5 float,
  fcv6 float,
  fcv7 float,
  fcv8 float,
  single_gang_supply float,
  so_boxflow float,
  so_temperature float,
  temperature float,
  PRIMARY KEY ((id), date_time)
) WITH
  bloom_filter_fp_chance=0.010000 AND
  caching='KEYS_ONLY' AND
  comment='' AND
  dclocal_read_repair_chance=0.100000 AND
  gc_grace_seconds=864000 AND
  index_interval=128 AND
  read_repair_chance=0.000000 AND
  replicate_on_write='true' AND
  populate_io_cache_on_flush='false' AND
  default_time_to_live=0 AND
  speculative_retry='99.0PERCENTILE' AND
  memtable_flush_period_in_ms=0 AND
  compaction={'class': 'SizeTieredCompactionStrategy'} AND
  compression={'sstable_compression': 'LZ4Compressor'};

CREATE TABLE table_sample (
  id text,
  datatime timestamp,
  energy float,
  power float,
  volume float,
  PRIMARY KEY ((id), datatime)
) WITH
  bloom_filter_fp_chance=0.010000 AND
  caching='KEYS_ONLY' AND
  comment='' AND
  dclocal_read_repair_chance=0.100000 AND
  gc_grace_seconds=864000 AND
  index_interval=128 AND
  read_repair_chance=0.000000 AND
  replicate_on_write='true' AND
  populate_io_cache_on_flush='false' AND
  default_time_to_live=0 AND
  speculative_retry='99.0PERCENTILE' AND
  memtable_flush_period_in_ms=0 AND
  compaction={'class': 'SizeTieredCompactionStrategy'} AND
  compression={'sstable_compression': 'LZ4Compressor'};

"""
from cassandra.cluster import Cluster
from cassandra.auth import PlainTextAuthProvider
import cassandra

"""
class for database Cassandra
"""


class LQ_DB_Cassandra(object):
    def __init__(self):  # initialize the database and basic setting, change it base on your own situation

        # cassandra connection setting
        ap = PlainTextAuthProvider(username='spms', password='PASSWORD')
        self.cluster = Cluster(['155.69.214.102', '172.21.77.197'], auth_provider=ap)

        self.session = self.cluster.connect()
        self.session.set_keyspace('data_collection')
        self.session.default_timeout = 30  # default is 10, always fail for BBB, 30 is better,

    def check_schema(self):  # check and create your schema

        rows = self.session.execute(
            "SELECT * FROM system.schema_keyspaces WHERE keyspace_name='data_collection'")
        print('rows', rows)

        if rows:
            msg = ' It looks like you already have a mybbbs keyspace.\nDo you '
            msg += 'want to delete it and recreate it? All current data will '
            msg += 'be deleted! (y/n): '
            resp = input(msg)
            if not resp or resp[0] != 'y':
                print("Ok, then we're done here.")
                return
            self.session.execute("DROP KEYSPACE data_collection")

        self.session.execute("""
            CREATE KEYSPACE data_collection
            WITH replication = {'class': 'SimpleStrategy', 'replication_factor': '2'}
            """)

        # info using which keyspace
        self.session.set_keyspace("data_collection")
        # create tables
        self.session.execute("""
            CREATE TABLE last_read_pos (
                file_name text,
                pos float,
                PRIMARY KEY ((file_name))
                )
            """)

        print('All done!')

    def update_pos(self, file, pos):
        # pos = float(pos)
        update_file_pos_query = self.session.prepare("""
            UPDATE data_collection.last_read_pos
            SET pos = ?
            WHERE file = ?
            """)

        result = self.session.execute(update_file_pos_query, (pos, file))

    def add_pos(self, file, pos):
        update_file_pos_query = self.session.prepare("""
                INSERT INTO data_collection.last_read_pos
                (file, pos )
                VALUES
                ( ?, ?) ;
                """)

        result = self.session.execute(update_file_pos_query, (file, pos))

    def get_pos(self, file):  # get the reading position of last time
        get_last_pos_query = self.session.prepare("""
                SELECT *
                FROM data_collection.last_read_pos
                WHERE file=?
                """)

        try:
            rows = self.session.execute(get_last_pos_query, (file,))
        except (cassandra.WriteTimeout, cassandra.Timeout):
            print('get_last_pos database query filed')

        # if record exist,return the value, if not, add the one
        if rows:
            for (self.file, last_read_pos) in rows:
                return last_read_pos
        else:
            self.add_pos(file, 0)  # new file found
            return 0

    def insert_values(self, table, mid, date_time, parameters, values):  # make the insert CQL statement and execute it
        print('insert_values' + str(table) + str(mid) + str(date_time) + str(parameters) + str(values))

        # making the CQL statement
        insert_values_query = 'INSERT INTO ' + table + ' ( id , date_time, '

        for parameter in parameters:  # adding columns
            insert_values_query = insert_values_query + parameter + ' , '

        insert_values_query = insert_values_query[:-2]  # remove last comma

        insert_values_query += ') VALUES ( \'' + mid + '\' , ' + str(date_time) + ' ,'

        for value in values:
            insert_values_query = insert_values_query + str(value) + ' , '

        insert_values_query = insert_values_query[:-2]  # remove last comma
        insert_values_query += ')'
        print(insert_values_query)
        try:
            self.session.execute(insert_values_query)
        except cassandra.WriteTimeout:
            print('writing filed')

    def close_db_connection(self):
        pass


def test_db():
    test = LQ_DB_Cassandra()
    table = 'spms'
    mid = 'testid'
    date_time = 1410158125.668695 * 1000
    date_time = int(date_time)
    parameters = ('dp_temp', 'energy_consumption')
    values = (3.0, 1.6)

    test.insert_values(table=table, mid=mid, date_time=date_time,
                       parameters=parameters,
                       values=values)


    # test_db()