#!/usr/bin/env python3

# Copy UIUC resources information from a source (database) to the destination (warehouse)
import os
import pwd
import re
import sys
import argparse
import logging
import logging.handlers
import signal
import datetime
from datetime import datetime, timezone, tzinfo, timedelta
from time import sleep
import pytz
Central = pytz.timezone("US/Central")
UTC = pytz.timezone("UTC")

import http.client as httplib
import psycopg2
import json
import ssl
import shutil

import django
django.setup()
from django.db import DataError, IntegrityError
from django.utils.dateparse import parse_datetime
from resource_v3.models import *
from processing_status.process import ProcessingActivity

from elasticsearch_dsl.connections import connections
from elasticsearch import Elasticsearch

import pdb

def datetime_localparse(indate):
    try:
        return(parse_datetime(indate))
#        if isinstance(indate, datetime) or indate is None or indate == '':
#            return(indate)
#        else:
#            return(parse_datetime(indate))
    except:
        return(indate)
    
def datetime_standardize(indate):
    # Localize as Central and convert to UTC
    if isinstance(indate, datetime):
        return(Central.localize(indate).astimezone(tz = UTC))
    else:
        return(indate)

class HandleLoad():
    def __init__(self):
        self.args = None
        self.config = {}
        self.src = {}
        self.dest = {}
        for var in ['uri', 'scheme', 'path']: # Where <full> contains <type>:<obj>
            self.src[var] = None
            self.dest[var] = None
        
        self.Affiliation = 'uiuc.edu'
        self.CatalogID = self.urn_one_id('ResourceV3Catalogs', 'uiuc.edu')
        self.CatalogMetaURL = 'https://info.xsede.org/wh1/resource-api/v3/catalog/id/{}/'.format(self.CatalogID)
        self.DefaultValidity = timedelta(days = 14)
        # Field Maps "fm" local to global
        self.record_status_map = {
                '4': 'Planned',
                '3': 'pre-production',
                '2': 'decommissioned',
                '1': 'production',
            }
        # https://docs.google.com/spreadsheets/d/1UbOy3FTEBQFFTCfaXNnh-6PASPCyxsOu2vrRledAkOg
        self.type_map = {
                'resource_group_events:Event': 'Live Events:Event',
                'resource_group_online_training:StreamingResource': 'Streamed Events:Training',
                'resource_group_tools_and_services:BackupAndStorage': 'Streamed Events:Backup and Storage',
                'resource_group_tools_and_services:ConsultingAndSupport': 'Organizations:Consulting and Support',
                'resource_group_tools_and_services:Data': 'Data Resources:Data Resources',
                'resource_group_tools_and_services:Instrument': 'Computing Tools and Services:Backup and Storage',
                'resource_group_tools_and_services:NetworkingAndSecurity': 'Computing Tools and Services:Networking and Security',
                'resource_group_tools_and_services:Programming': 'Computing Tools and Services:Programming',
                'resource_group_tools_and_services:ResearchComputing': 'Computing Tools and Services:Research Computing',
                'resource_group_tools_and_services:Software': 'Software:Software',
                'resource_group_tools_and_services:WebPublishingAndCommunication': 'Computing Tools and Services:Web Publishing and Communication',
            }
        self.title_map = {
                'Research Computing': 'Research Computing',
                'Web Hosting and Publishing': 'Web Publishing and Communications',
                'Data Resources': 'Data Resources',
            }

        default_source = 'postgresql://localhost:5432/warehouse'

        parser = argparse.ArgumentParser(epilog='File SRC|DEST syntax: file:<file path and name')
        parser.add_argument('-s', '--source', action='store', dest='src', \
                            help='Messages source {postgresql} (default=postgresql)')
        parser.add_argument('-d', '--destination', action='store', dest='dest', \
                            help='Message destination {analyze, or warehouse} (default=analyze)')
        parser.add_argument('--ignore_dates', action='store_true', \
                            help='Ignore dates and force full resource refresh')
        parser.add_argument('-l', '--log', action='store', \
                            help='Logging level (default=warning)')
        parser.add_argument('-c', '--config', action='store', default='./route_uiuc.conf', \
                            help='Configuration file default=./route_uiuc.conf')
        parser.add_argument('--verbose', action='store_true', \
                            help='Verbose output')
        parser.add_argument('--pdb', action='store_true', \
                            help='Run with Python debugger')
        self.args = parser.parse_args()

        if self.args.pdb:
            pdb.set_trace()

        # Load configuration file
        config_path = os.path.abspath(self.args.config)
        try:
            with open(config_path, 'r') as file:
                conf = file.read()
                file.close()
        except IOError as e:
            raise
        try:
            self.config = json.loads(conf)
        except ValueError as e:
            print('Error "{}" parsing config={}'.format(e, config_path))
            sys.exit(1)

        # Initialize logging from arguments, or config file, or default to WARNING as last resort
        numeric_log = None
        if self.args.log is not None:
            numeric_log = getattr(logging, self.args.log.upper(), None)
        if numeric_log is None and 'LOG_LEVEL' in self.config:
            numeric_log = getattr(logging, self.config['LOG_LEVEL'].upper(), None)
        if numeric_log is None:
            numeric_log = getattr(logging, 'WARNING', None)
        if not isinstance(numeric_log, int):
            raise ValueError('Invalid log level: {}'.format(numeric_log))
        self.logger = logging.getLogger('DaemonLog')
        self.logger.setLevel(numeric_log)
        self.formatter = logging.Formatter(fmt='%(asctime)s.%(msecs)03d %(levelname)s %(message)s', \
                                           datefmt='%Y/%m/%d %H:%M:%S')
        self.handler = logging.handlers.TimedRotatingFileHandler(self.config['LOG_FILE'], when='W6', \
                                                                 backupCount = 999, utc = True)
        self.handler.setFormatter(self.formatter)
        self.logger.addHandler(self.handler)

        # Verify arguments and parse compound arguments
        if not getattr(self.args, 'src', None): # Tests for None and empty ''
            if 'SOURCE_URL' in self.config:
                self.args.src = self.config['SOURCE_URL']
        if not getattr(self.args, 'src', None): # Tests for None and empty ''
            self.args.src = default_source
        idx = self.args.src.find(':')
        if idx > 0:
            (self.src['scheme'], self.src['path']) = (self.args.src[0:idx], self.args.src[idx+1:])
        else:
            (self.src['scheme'], self.src['path']) = (self.args.src, None)
        if self.src['scheme'] not in ['file', 'http', 'https', 'postgresql']:
            self.logger.error('Source not {file, http, https}')
            sys.exit(1)
        if self.src['scheme'] in ['http', 'https', 'postgresql']:
            if self.src['path'][0:2] != '//':
                self.logger.error('Source URL not followed by "//"')
                sys.exit(1)
            self.src['path'] = self.src['path'][2:]
        if len(self.src['path']) < 1:
            self.logger.error('Source is missing a database name')
            sys.exit(1)
        self.src['uri'] = self.args.src

        if not getattr(self.args, 'dest', None): # Tests for None and empty ''
            if 'DESTINATION' in self.config:
                self.args.dest = self.config['DESTINATION']
        if not getattr(self.args, 'dest', None): # Tests for None and empty ''
            self.args.dest = 'analyze'
        idx = self.args.dest.find(':')
        if idx > 0:
            (self.dest['scheme'], self.dest['path']) = (self.args.dest[0:idx], self.args.dest[idx+1:])
        else:
            self.dest['scheme'] = self.args.dest
        if self.dest['scheme'] not in ['file', 'analyze', 'warehouse']:
            self.logger.error('Destination not {file, analyze, warehouse}')
            sys.exit(1)
        self.dest['uri'] = self.args.dest

        if self.src['scheme'] in ['file'] and self.dest['scheme'] in ['file']:
            self.logger.error('Source and Destination can not both be a {file}')
            sys.exit(1)

    def urn_one_id(self, type, id1, affiliation=None):
        if affiliation == None:
            affiliation = self.Affiliation
        return('urn:glue2:{}:{}.{}'.format(type, id1, affiliation))

    def urn_two_id(self, type, id1, id2, affiliation=None):
        if affiliation == None:
            affiliation = self.Affiliation
        return('urn:glue2:{}:{}.{}:{}.{}'.format(type, id1, affiliation, id2, affiliation))

    def urn_one2two(self, base, id2, affiliation=None):
        if affiliation == None:
            affiliation = self.Affiliation
        return('{}:{}.{}'.format(base, id2, affiliation))

    def Connect_Source(self, url):
        idx = url.find(':')
        if idx <= 0:
            self.logger.error('Source URL is not valid')
            sys.exit(1)
            
        (type, obj) = (url[0:idx], url[idx+1:])
        if type not in ['postgresql']:
            self.logger.error('Source URL is not valid')
            sys.exit(1)

        if obj[0:2] != '//':
            self.logger.error('Source URL is not valid')
            sys.exit(1)
            
        obj = obj[2:]
        idx = obj.find('/')
        if idx <= 0:
            self.logger.error('Source URL is not valid')
            sys.exit(1)
        (host, path) = (obj[0:idx], obj[idx+1:])
        idx = host.find(':')
        if idx > 0:
            port = host[idx+1:]
            host = host[:idx]
        elif type == 'postgresql':
            port = '5432'
        else:
            port = '5432'
        
        #Define our connection string
        conn_string = "host='{}' port='{}' dbname='{}' user='{}' password='{}'".format(host, port, path, self.config['SOURCE_DBUSER'], self.config['SOURCE_DBPASS'] )

        # get a connection, if a connect cannot be made an exception will be raised here
        conn = psycopg2.connect(conn_string)

        # conn.cursor will return a cursor object, you can use this cursor to perform queries
        cursor = conn.cursor()
        self.logger.info('Connected to PostgreSQL database {} as {}'.format(path, self.config['SOURCE_DBUSER']))
        return(cursor)
 
    def Connect_Elastic(self):
        if 'ELASTIC_DESTINATION' in self.config:
            connections.create_connection(hosts=[self.config['ELASTIC_DESTINATION']], timeout = 10)
            self.ESEARCH = Elasticsearch()
            ResourceV3Index.init()
        else:
            self.ESEARCH = None
    
    def Disconnect_Source(self, cursor):
        cursor.close()

    def Retrieve_Resources(self, cursor):
        try:
            sql = 'SELECT * from resource'
            cursor.execute(sql)
        except psycopg2.Error as e:
            self.logger.error("Failed '{}' with {}: {}".format(sql, e.pgcode, e.pgerror))
            sys.exit(1)

        COLS = [desc.name for desc in cursor.description]
        DATA = {}
        for row in cursor.fetchall():
            rowdict = dict(zip(COLS, row))
            if rowdict.get('record_status', None) not in [1, 2]:
                continue
            for field in ['last_updated', 'start_date_time', 'end_date_time']:
                if field in rowdict:
                    rowdict[field] = datetime_localparse(rowdict[field])
            GLOBALID = self.urn_one_id('GlobalResource', rowdict.get('id', ''))
            DATA[GLOBALID] = rowdict
        return(DATA)

    def Retrieve_Providers(self, cursor):
        try:
            sql = 'SELECT * from provider'
            cursor.execute(sql)
        except psycopg2.Error as e:
            self.logger.error("Failed '{}' with {}: {}".format(sql, e.pgcode, e.pgerror))
            sys.exit(1)

        COLS = [desc.name for desc in cursor.description]
        DATA = {}
        for row in cursor.fetchall():
            rowdict = dict(zip(COLS, row))
            GLOBALID = self.urn_one_id('GlobalResourceProvider', rowdict.get('id', ''))
            DATA[GLOBALID] = rowdict
        return(DATA)

    def Retrieve_Resource_Tags(self, cursor):
        try:
            sql = 'SELECT * from tag'
            cursor.execute(sql)
        except psycopg2.Error as e:
            self.logger.error("Failed '{}' with {}: {}".format(sql, e.pgcode, e.pgerror))
            sys.exit(1)

        COLS = [desc.name for desc in cursor.description]
        tags = {}
        for row in cursor.fetchall():
            rowdict = dict(zip(COLS, row))
            tags[rowdict['id']] = rowdict['label']
        
        try:
            sql = 'SELECT * from resources_tags'
            cursor.execute(sql)
        except psycopg2.Error as e:
            self.logger.error("Failed '{}' with {}: {}".format(sql, e.pgcode, e.pgerror))
            sys.exit(1)

        COLS = [desc.name for desc in cursor.description]
        resource_tags = {}
        for row in cursor.fetchall():
            rowdict = dict(zip(COLS, row))
            GLOBALID = self.urn_one_id('GlobalResource', rowdict.get('id', ''))
            if GLOBALID not in resource_tags:
                resource_tags[GLOBALID] = []
            try:
                resource_tags[GLOBALID].append(tags[rowdict['tag_id']])
            except:
                pass
        return(resource_tags)

    def Retrieve_Resource_Associations(self, cursor):
        try:
            sql = 'SELECT * from associated_resources'
            cursor.execute(sql)
        except psycopg2.Error as e:
            self.logger.error("Failed '{}' with {}: {}".format(sql, e.pgcode, e.pgerror))
            sys.exit(1)

        COLS = [desc.name for desc in cursor.description]
        DATA = {}
        for row in cursor.fetchall():
            rowdict = dict(zip(COLS, row))
            GLOBALID = self.urn_one_id('GlobalResource', rowdict.get('resource_id', ''))
            if GLOBALID not in DATA:
                DATA[GLOBALID] = []
            DATA[GLOBALID].append(str(rowdict['associated_resource_id']))
        return(DATA)

    def Retrieve_Guides(self, cursor):
        try:
            sql = 'SELECT * from curated_guide'
            cursor.execute(sql)
        except psycopg2.Error as e:
            self.logger.error("Failed '{}' with {}: {}".format(sql, e.pgcode, e.pgerror))
            sys.exit(1)

        COLS = [desc.name for desc in cursor.description]
        DATA = {}
        for row in cursor.fetchall():
            rowdict = dict(zip(COLS, row))
            for field in ['created_at', 'updated_at']:
                if field in rowdict:
                    rowdict[field] = datetime_localparse(rowdict[field])
            GLOBALID = self.urn_one_id('GlobalGuide', rowdict.get('id', ''))
            DATA[GLOBALID] = rowdict
        return(DATA)

    def Retrieve_Guide_Resources(self, cursor):
        try:
            sql = 'SELECT * from curated_guide_resource'
            cursor.execute(sql)
        except psycopg2.Error as e:
            self.logger.error("Failed '{}' with {}: {}".format(sql, e.pgcode, e.pgerror))
            sys.exit(1)

        COLS = [desc.name for desc in cursor.description]
        DATA = {}
        for row in cursor.fetchall():
            rowdict = dict(zip(COLS, row))
            GLOBALID = self.urn_two_id('GlobalGuideResource', rowdict.get('curated_guide_id', ''), rowdict.get('resource_id', ''))
            DATA[GLOBALID] = rowdict
        return(DATA)
    
    def Warehouse_Providers(self, new_items):
        self.cur = {}   # Items currently in database
        self.new = {}   # New resources in document
        local_type = 'provider'
        
        for item in ResourceV3Local.objects.filter(Affiliation__exact = self.Affiliation).filter(LocalType__exact=local_type):
            self.cur[item.ID] = item
        for GLOBALID in new_items:
            new_item = new_items[GLOBALID]
            new_id = str(new_item['id'])
            try:
                local = ResourceV3Local(
                            ID = GLOBALID,
                            CreationTime = datetime.now(timezone.utc),
                            Validity = self.DefaultValidity,
                            Affiliation = self.Affiliation,
                            LocalID = new_id,
                            LocalType = local_type,
                            LocalURL = None,
                            CatalogMetaURL = self.CatalogMetaURL,
                            EntityJSON = new_item,
                    )
                local.save()
                resource = ResourceV3(
                            ID = GLOBALID,
                            Affiliation = self.Affiliation,
                            LocalID = new_id,
                            QualityLevel='production',
                            Name = new_item['name'],
                            ResourceGroup='Organizations',
                            Type='Resource Provider',
                            Audience = self.Affiliation,
                     )
                resource.save()
                resource.indexing()
                self.new[GLOBALID]=resource
                self.logger.debug('ResourceProvider save ID={}'.format(GLOBALID))
                self.stats['ResourceProvider.Update'] += 1
            except (DataError, IntegrityError, TypeError) as e:
                msg = '{} saving ID={}: {}'.format(type(e).__name__, GLOBALID, e)
                self.logger.error(msg)
                return(False, msg)
                
            if new_item.get('parent_provider', None) is not None:
                RELATIONID = self.urn_one2two(GLOBALID, new_item['parent_provider'])
                PARENTID = self.urn_one_id('GlobalResourceProvider', new_item['parent_provider'])
                parent = ResourceV3Relation(
                            ID = RELATIONID,
                            FirstResourceID = GLOBALID,
                            SecondResourceID = PARENTID,
                            RelationType = 'ResourceParent',
                    )
                parent.save()
                     
        for GLOBALID in self.cur:
            if GLOBALID not in new_items:
                try:
                    ResourceV3Index.get(id = GLOBALID).delete()
                    ResourceV3Relation.filter(FirstResourceID__exact=GLOBALID).delete()
                    ResourceV3.objects.get(pk = GLOBALID).delete()
                    ResourceV3Local.objects.get(pk = GLOBALID).delete()
                    self.stats['ResourceProvider.Delete'] += 1
                    self.logger.info('ResourceProvider delete ID={}'.format(GLOBALID))
                except (DataError, IntegrityError, TypeError) as e:
                    self.logger.error('{} deleting ID={}: {}'.format(type(e).__name__, GLOBALID, e))
                    
        return(True, '')

    def Warehouse_Resources(self, new_items, item_tags, item_associations):
        self.cur = {}   # Items currently in database
        self.new = {}   # New resources in document
        local_type = 'resource'

        for item in ResourceV3Local.objects.filter(Affiliation__exact = self.Affiliation).filter(LocalType__exact = local_type):
            self.cur[item.ID] = item
        for GLOBALID in new_items:
            new_item = new_items[GLOBALID]
            new_id = str(new_item['id'])
            # Convert warehouse last_update JSON string to datetime with timezone
            # Incoming last_update is a datetime with timezone
            # Once they are both datetimes with timezone, compare their strings
            # Can't compare directly because tzinfo have different represenations in Python and Django
            if not self.args.ignore_dates:
                try:
                    cur_dtm = parse_datetime(self.cur[GLOBALID].EntityJSON['last_updated'].replace(' ',''))
                except:
                    cur_dtm = datetime.now(timezone.utc)
                try:
                    new_dtm = new_item['last_updated']
                except:
                    new_dtm = None
                if str(cur_dtm) == str(new_dtm):
                    self.stats['Resource.Skip'] += 1
                    continue

            for field in ['last_updated', 'start_date_time', 'end_date_time']:
                if field in new_item and isinstance(new_item[field], datetime):
                    new_item[field] = new_item[field].strftime('%Y-%m-%dT%H:%M:%S%z')

            try:
                ProviderID = self.urn_one_id('GlobalResourceProvider', new_item['provider'])
            except:
                ProviderID = None

            try:
                ResourceGroup = new_item['resource_group']
            except:
                ResourceGroup = None

            try:
                Type = new_item['resource_type']
            except:
                Type = None

            try:
                QualityLevel = self.record_status_map[str(new_item['record_status'])]
            except:
                QualityLevel = None

            try:
                Keywords = ','.join(item_tags[new_id])
            except:
                Keywords = None
                
            if Type == 'Event':
                try:
                    StartDateTime = datetime_standardize(parse_datetime(new_item['start_date_time']))
                except:
                    StartDateTime = None
                try:
                    EndDateTime = datetime_standardize(arse_datetime(new_item['end_date_time']))
                except:
                    EndDateTime = None
            else:
                StartDateTime = None
                EndDateTime = None
                
            try:
                # V2 to V3 type mapping
                TMKEY = '{}:{}'.format(ResourceGroup, Type)
                (new_group, new_type) = self.type_map.get(TMKEY, 'Error:Error').split(':')[:2]
                local = ResourceV3Local(
                            ID = GLOBALID,
                            CreationTime = datetime.now(timezone.utc),
                            Validity = self.DefaultValidity,
                            Affiliation = self.Affiliation,
                            LocalID = new_id,
                            LocalType = local_type,
                            LocalURL = None,
                            CatalogMetaURL = self.CatalogMetaURL,
                            EntityJSON = new_item,
                    )
                local.save()
                resource = ResourceV3(
                            ID = GLOBALID,
                            Affiliation = self.Affiliation,
                            LocalID = new_id,
                            QualityLevel = QualityLevel,
                            Name = new_item['resource_name'],
                            ResourceGroup = new_group,
                            Type = new_type,
                            ShortDescription = new_item['short_description'],
                            ProviderID = ProviderID,
                            Description = new_item['resource_description'],
                            Topics = new_item['topics'],
                            Keywords = Keywords,
                            Audience = self.Affiliation,
                            StartDateTime = StartDateTime,
                            EndDateTime = EndDateTime,
                    )
                resource.save()
                resource.indexing()
                self.new[GLOBALID]=resource
                self.logger.debug('Resource save ID={}'.format(GLOBALID))
                self.stats['Resource.Update'] += 1
            except (DataError, IntegrityError, TypeError) as e:
                msg = '{} saving ID={}: {}'.format(type(e).__name__, GLOBALID, e)
                self.logger.error(msg)
                return(False, msg)
            
            # Add/update associations (tracked in new_seconds), and delete those that weren't new_seconds
            new_seconds = set()
            if GLOBALID in item_associations:
                for new_assoc in item_associations[GLOBALID]:
                    RELATIONID = self.urn_one2two(GLOBALID, new_assoc)
                    SECONDID = self.urn_one_id('GlobalResource', new_assoc)
                    assoc = ResourceV3Relation(
                                ID = RELATIONID,
                                FirstResourceID = GLOBALID,
                                SecondResourceID = SECONDID,
                                RelationType = 'ResourceAssociation',
                        )
                    assoc.save()
                    new_seconds.add(SECONDID)
                    self.logger.debug('ResourceRelation save ID={}'.format(RELATIONID))
                    self.stats['ResourceRelation.Update'] += 1
            # Delete GLOBALID relations that weren't just added/updated
            delrc = ResourceV3Relation.objects.filter(FirstResourceID__exact = GLOBALID).exclude(SecondResourceID__in = new_seconds).delete()
            if delrc[0] > 0:
                self.stats['ResourceRelation.Delete'] += delrc[0]
                self.logger.info('ResourceRelation delete FIRSTID={} cnt={}'.format(GLOBALID, delrc[0]))

        # Delete Resource, Index, Local, and Relation
        for GLOBALID in self.cur:
            if GLOBALID not in new_items:
                try:
                    ResourceV3Index.get(id = GLOBALID).delete()
                    delrc1 = ResourceV3Relation.objects.filter(FirstResourceID__exact=GLOBALID).delete()
                    delrc2 = ResourceV3Relation.objects.filter(SecondResourceID__exact=GLOBALID).delete()
                    ResourceV3.objects.get(pk = GLOBALID).delete()
                    ResourceV3Local.objects.get(pk = GLOBALID).delete()
                    self.stats['Resource.Delete'] += 1
                    self.logger.info('Resource delete ID={}'.format(GLOBALID))
                    if delrc1[0] > 0:
                        self.stats['ResourceRelation.Delete'] += delrc1[0]
                        self.logger.info('ResourceRelation delete FIRSTID={} cnt={}'.format(GLOBALID, delrc1[0]))
                    if delrc2[0] > 0:
                        self.stats['ResourceRelation.Delete'] += delrc2[0]
                        self.logger.info('ResourceRelation delete SECONDID={} cnt={}'.format(GLOBALID, delrc2[0]))
                except (DataError, IntegrityError, TypeError) as e:
                    self.logger.error('{} deleting ID={}: {}'.format(type(e).__name__, GLOBALID, e))

        return(True, '')

    def Warehouse_Guides(self, new_items):
        self.cur = {}   # Items currently in database
        self.new = {}   # New resources in document
        local_type = 'guide'
        self.NEW_GUIDES = {}
        self.OLD_GUIDES = {}

        for item in ResourceV3Local.objects.filter(Affiliation__exact = self.Affiliation).filter(LocalType__exact = local_type):
            self.cur[item.ID] = item
        for GLOBALID in new_items:
            new_item = new_items[GLOBALID]
            new_id = str(new_item['id'])
            if 'created_at' in new_item and isinstance(new_item['created_at'], datetime):
                new_item['created_at'] = new_item['created_at'].strftime('%Y-%m-%dT%H:%M:%S%z')
            if 'updated_at' in new_item and isinstance(new_item['updated_at'], datetime):
                new_item['updated_at'] = new_item['updated_at'].strftime('%Y-%m-%dT%H:%M:%S%z')
            new_title = self.title_map.get(new_item['title'], '')
            try:
                local = ResourceV3Local(
                            ID = GLOBALID,
                            CreationTime = datetime.now(timezone.utc),
                            Validity = self.DefaultValidity,
                            Affiliation = self.Affiliation,
                            LocalID = new_id,
                            LocalType = local_type,
                            LocalURL = None,
                            CatalogMetaURL = self.CatalogMetaURL,
                            EntityJSON = new_item,
                    )
                local.save()
                resource = ResourceV3(
                            ID = GLOBALID,
                            Affiliation = self.Affiliation,
                            LocalID = new_id,
                            QualityLevel = self.record_status_map.get(new_item.get('publish_status', '1'), 'production'),
                            Name = new_title,
                            ResourceGroup = 'Guides',
                            Type = new_title,
                            ShortDescription = new_item['lede'],
                            Description = new_item['component_data'],
                            Audience = self.Affiliation,
                    )
                resource.save()
                resource.indexing()
                self.NEW_GUIDES[GLOBALID] = True
                self.new[GLOBALID] = resource
                self.logger.debug('Guide save ID={}'.format(GLOBALID))
                self.stats['Guide.Update'] += 1
            except (DataError, IntegrityError, TypeError) as e:
                msg = '{} saving ID={}: {}'.format(type(e).__name__, GLOBALID, e)
                self.logger.error(msg)
                return(False, msg)

        for GLOBALID in self.cur:
            if GLOBALID not in new_items:
                try:
                    ResourceV3Index.get(id = GLOBALID).delete()
                    ResourceV3.objects.get(pk = GLOBALID).delete()
                    ResourceV3Local.objects.get(pk = GLOBALID).delete()
                    self.stats['Guide.Delete'] += 1
                    self.logger.info('Guide delete ID={}'.format(GLOBALID))
                except (DataError, IntegrityError, TypeError) as e:
                    self.logger.error('{} deleting ID={}: {}'.format(type(e).__name__, GLOBALID, e))
                self.OLD_GUIDES.update(GLOBALID)
        return(True, '')

    def Warehouse_Guide_Resources(self, new_items):
        self.cur = {}   # Items currently in database
        self.new = {}   # New resources in document
        self.NEW_GUIDE_RESOURCES = {}
        
        for GLOBALID in new_items:
            # All the new/updated Guide+Resource relations
            new_item = new_items[GLOBALID]
            GUIDEID = self.urn_one_id('GlobalGuide', new_item['curated_guide_id'])
            RESOURCEID = self.urn_one_id('GlobalResource', new_item['resource_id'])
            try:
                model = ResourceV3Relation(
                            ID = GLOBALID,
                            FirstResourceID = GUIDEID,
                            SecondResourceID = RESOURCEID,
                            RelationType = 'GuideResource',
                    )
                model.save()
                self.new[GLOBALID] = model
                self.logger.debug('GuideResource save ID={}'.format(GLOBALID))
                self.stats['GuideResource.Update'] += 1
            except (DataError, IntegrityError, TypeError) as e:
                msg = '{} saving ID={}: {}'.format(type(e).__name__, GLOBALID, e)
                self.logger.error(msg)
                return(False, msg)

            if GUIDEID not in self.NEW_GUIDE_RESOURCES:
                self.NEW_GUIDE_RESOURCES[GUIDEID] = set()
            self.NEW_GUIDE_RESOURCES[GUIDEID].add(RESOURCEID)

        for GUIDEID in self.NEW_GUIDES:
            if GUIDEID not in self.NEW_GUIDE_RESOURCES:
                # All the new/updated guides that had no Guide+Resource relations: delete all old relations
                try:
                    delrc = ResourceV3Relation.objects.filter(FirstResourceID__exact = GUIDEID).delete()
                    if delrc[0] > 0:
                        self.stats['GuideResource.Delete'] += delrc[0]
                        self.logger.info('GuideResource delete ID={} cnt={}'.format(GUIDEID, delrc[0]))
                except (DataError, IntegrityError, TypeError) as e:
                    self.logger.error('{} deleting ID={}: {}'.format(type(e).__name__, GUIDEID, e))
            else:
                # All the new/updated guides that had some Guide+resource relations: delete select old relations
                try:
                    delrc = ResourceV3Relation.objects.filter(FirstResourceID__exact = GUIDEID).exclude(SecondResourceID__in = self.NEW_GUIDE_RESOURCES[GUIDEID]).delete()
                    if delrc[0] > 0:
                        self.stats['GuideResource.Delete'] += delrc[0]
                        self.logger.info('GuideResource delete ID={} cnt={}'.format(GUIDEID, delrc[0]))
                except (DataError, IntegrityError, TypeError) as e:
                    self.logger.error('{} deleting ID={}: {}'.format(type(e).__name__, GUIDEID, e))

        for GUIDEID in self.OLD_GUIDES:
            # All the resources that were deleted: delete all related Guide+Resource relations
            try:
                delrc = ResourceV3Relation.objects.filter(FirstResourceID__exact = GUIDEID).delete()
                if delrc[0] > 0:
                    self.stats['GuideResource.Delete'] += delrc[0]
                    self.logger.info('GuideResource delete ID={} cnt={}'.format(GUIDEID, delrc[0]))
            except (DataError, IntegrityError, TypeError) as e:
                self.logger.error('{} deleting ID={}: {}'.format(type(e).__name__, GUIDEID, e))
            
        return(True, '')
                     
    def SaveDaemonLog(self, path):
        # Save daemon log file using timestamp only if it has anything unexpected in it
        try:
            with open(path, 'r') as file:
                lines = file.read()
                if not re.match("^started with pid \d+$", lines) and not re.match("^$", lines):
                    ts = datetime.strftime(datetime.now(), '%Y-%m-%d_%H:%M:%S')
                    newpath = '{}.{}'.format(path, ts)
                    shutil.move(path, newpath)
                    print('SaveDaemonLog as {}'.format(newpath))
        except Exception as e:
            print('Exception in SaveDaemonLog({})'.format(path))
        return

    def exit_signal(self, signal, frame):
        self.logger.critical('Caught signal={}, exiting...'.format(signal))
        sys.exit(0)

    def run(self):
        signal.signal(signal.SIGINT, self.exit_signal)
        signal.signal(signal.SIGTERM, self.exit_signal)
        self.logger.info('Starting program={} pid={}, uid={}({})'.format(os.path.basename(__file__), os.getpid(), os.geteuid(), pwd.getpwuid(os.geteuid()).pw_name))

        while True:
            pa_application = os.path.basename(__file__)
            pa_function = 'Warehouse_UIUC'
            pa_id = 'resources'
            pa_topic = 'resources'
            pa_about = 'uiuc.edu'
            pa = ProcessingActivity(pa_application, pa_function, pa_id , pa_topic, pa_about)

            if self.src['scheme'] == 'postgresql':
                CURSOR = self.Connect_Source(self.src['uri'])
            self.Connect_Elastic()

            self.stats = {}
            for stat_type in ['ResourceProvider', 'Resource', 'ResourceRelation', 'Guide', 'GuideResource']:
                self.stats[stat_type + '.Update'] = 0
                self.stats[stat_type + '.Delete'] = 0
                self.stats[stat_type + '.Skip'] = 0

            self.start = datetime.now(timezone.utc)
            INPUT = self.Retrieve_Providers(CURSOR)
            (rc, warehouse_msg) = self.Warehouse_Providers(INPUT)
            self.end = datetime.now(timezone.utc)
            summary_msg = 'Processed ResourceProvider in {:.3f}/seconds: {}/updates, {}/deletes, {}/skipped'.format((self.end - self.start).total_seconds(), self.stats['ResourceProvider.Update'], self.stats['ResourceProvider.Delete'], self.stats['ResourceProvider.Skip'])
            self.logger.info(summary_msg)

            RESTAGS = self.Retrieve_Resource_Tags(CURSOR)
            RESASSC = self.Retrieve_Resource_Associations(CURSOR)
            
            self.start = datetime.now(timezone.utc)
            INPUT = self.Retrieve_Resources(CURSOR)
            (rc, warehouse_msg) = self.Warehouse_Resources(INPUT, RESTAGS, RESASSC)
            self.end = datetime.now(timezone.utc)
            summary_msg = 'Processed Resource in {:.3f}/seconds: {}/updates, {}/deletes, {}/skipped'.format((self.end - self.start).total_seconds(), self.stats['Resource.Update'], self.stats['Resource.Delete'], self.stats['Resource.Skip'])
            self.logger.info(summary_msg)

            self.start = datetime.now(timezone.utc)
            INPUT = self.Retrieve_Guides(CURSOR)
            (rc, warehouse_msg) = self.Warehouse_Guides(INPUT)
            self.end = datetime.now(timezone.utc)
            summary_msg = 'Processed Guide in {:.3f}/seconds: {}/updates, {}/deletes, {}/skipped'.format((self.end - self.start).total_seconds(), self.stats['Guide.Update'], self.stats['Guide.Delete'], self.stats['Guide.Skip'])
            self.logger.info(summary_msg)

            self.start = datetime.now(timezone.utc)
            INPUT = self.Retrieve_Guide_Resources(CURSOR)
            (rc, warehouse_msg) = self.Warehouse_Guide_Resources(INPUT)
            self.end = datetime.now(timezone.utc)
            summary_msg = 'Processed Guide Resource in {:.3f}/seconds: {}/updates, {}/deletes, {}/skipped'.format((self.end - self.start).total_seconds(), self.stats['GuideResource.Update'], self.stats['GuideResource.Delete'], self.stats['GuideResource.Skip'])
            self.logger.info(summary_msg)

            # Not disconnectiong from Elasticsearch
            self.Disconnect_Source(CURSOR)
            
            pa.FinishActivity(rc, summary_msg)
            break

if __name__ == '__main__':
    router = HandleLoad()
    myrouter = router.run()
    sys.exit(0)
