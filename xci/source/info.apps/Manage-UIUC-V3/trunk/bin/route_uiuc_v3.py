#!/usr/bin/env python3

# Copy UIUC resources information from a source (database) to the destination (warehouse)
import argparse
from collections import Counter
import datetime
from datetime import datetime, timezone, tzinfo, timedelta
from hashlib import md5
import http.client as httplib
import json
import logging
import logging.handlers
import os
import psycopg2
import pwd
import re
import shutil
import signal
import ssl
import sys
from time import sleep
from urllib.parse import urlparse
import pytz
Central = pytz.timezone('US/Central')
UTC = pytz.timezone('UTC')

import django
django.setup()
from django.forms.models import model_to_dict
from django.utils.dateparse import parse_datetime
from resource_v3.models import *
from processing_status.process import ProcessingActivity

import elasticsearch_dsl.connections
from elasticsearch import Elasticsearch, RequestsHttpConnection

import pdb

def datetime_localparse(indate):
    try:
        return(parse_datetime(indate))
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
        parser = argparse.ArgumentParser(epilog='File SRC|DEST syntax: file:<file path and name')
        parser.add_argument('-s', '--source', action='store', dest='src', \
                            help='Content source {postgresql} (default=postgresql)')
        parser.add_argument('-d', '--destination', action='store', dest='dest', \
                            help='Content destination {analyze or warehouse} (default=analyze)')
        parser.add_argument('--ignore_dates', action='store_true', \
                            help='Ignore dates and force full resource refresh')
        parser.add_argument('-l', '--log', action='store', \
                            help='Logging level override to config (default=warning)')
        parser.add_argument('-c', '--config', action='store', default='./route_uiuc.conf', \
                            help='Configuration file default=./route_uiuc.conf')
        parser.add_argument('--verbose', action='store_true', \
                            help='Verbose output')
        parser.add_argument('--dev', action='store_true', \
                            help='Running in development environment')
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
        loglevel_str = (self.args.log or self.config.get('LOG_LEVEL', 'WARNING')).upper()
        loglevel_num = getattr(logging, loglevel_str, None)
        if not isinstance(loglevel_num, int):
            raise ValueError('Invalid log level: {}'.format(loglevel_num))
        self.logger = logging.getLogger('DaemonLog')
        self.logger.setLevel(loglevel_num)
        self.formatter = logging.Formatter(fmt='%(asctime)s.%(msecs)03d %(levelname)s %(message)s', \
                                           datefmt='%Y/%m/%d %H:%M:%S')
        self.handler = logging.handlers.TimedRotatingFileHandler(self.config['LOG_FILE'], when='W6', \
                                                                 backupCount = 999, utc = True)
        self.handler.setFormatter(self.formatter)
        self.logger.addHandler(self.handler)

        # Verify arguments and parse compound arguments
        SOURCE_URL = getattr(self.args, 'src') or self.config.get('SOURCE_URL', None)
        if not SOURCE_URL:
            self.logger.error('Source was not specified')
            sys.exit(1)
        try:
            self.SOURCE_PARSE = urlparse(SOURCE_URL)
        except:
            self.logger.error('Source is missing or invalid')
            sys.exit(1)

        if self.SOURCE_PARSE.scheme not in ['file', 'http', 'https', 'postgresql']:
            self.logger.error('Source not {file, http, https, postgresql}')
            sys.exit(1)
        if not len(self.SOURCE_PARSE.path or '') >= 1:
            self.logger.error('Source is missing a database name')
            sys.exit(1)

        DEST_URL = getattr(self.args, 'dest') or self.config.get('DESTINATION', 'analyze')
        if not DEST_URL:
            self.logger.error('Destination was not specified')
            sys.exit(1)
        try:
            self.DEST_PARSE = urlparse(DEST_URL)
        except:
            self.logger.error('Destination is missing or invalid')
            sys.exit(1)

        if self.DEST_PARSE.scheme not in ['file', 'analyze', 'warehouse']:
            self.logger.error('Destination not {file, analyze, warehouse}')
            sys.exit(1)

        if self.SOURCE_PARSE.scheme in ['file'] and self.DEST_PARSE.scheme in ['file']:
            self.logger.error('Source and Destination can not both be a {file}')
            sys.exit(1)

        # Initialize appliation variables
        self.memory = {}
        self.PROVIDER_URNMAP = self.memory['provider_urnmap'] = {}
        self.Affiliation = 'uiuc.edu'
        self.URNPrefix = 'urn:ogf:glue2:'
        self.WAREHOUSE_API_PREFIX = 'http://localhost:8000' if self.args.dev else 'https://info.xsede.org/wh1'
        self.WAREHOUSE_API_VERSION = 'v3'
        self.WAREHOUSE_CATALOG = 'ResourceV3'

        # Loading all the Catalog entries for our affiliation
        self.CATALOGS = {}
        for cat in ResourceV3Catalog.objects.filter(Affiliation__exact=self.Affiliation):
            self.CATALOGS[cat.ID] = model_to_dict(cat)

        self.DefaultValidity = timedelta(days = 14)
        self.STATUSMAP = {
                '4': 'Planned',
                '3': 'Pre-production',
                '2': 'Decommissioned',
                '1': 'Production',
            }
        # https://docs.google.com/spreadsheets/d/1UbOy3FTEBQFFTCfaXNnh-6PASPCyxsOu2vrRledAkOg
        # V2 to V3 type mapping
        self.TYPEMAP = {
                'resource_group_events:Event': 'Live Events:Event',
                'resource_group_online_training:StreamingResource': 'Streamed Events:Training',
                'resource_group_tools_and_services:BackupAndStorage': 'Streamed Events:Backup and Storage',
                'resource_group_tools_and_services:ConsultingAndSupport': 'Organizations:Consulting and Support',
                'resource_group_tools_and_services:Data': 'Data Resources:Data',
                'resource_group_tools_and_services:Instrument': 'Computing Tools and Services:Backup and Storage',
                'resource_group_tools_and_services:NetworkingAndSecurity': 'Computing Tools and Services:Networking and Security',
                'resource_group_tools_and_services:Programming': 'Computing Tools and Services:Programming',
                'resource_group_tools_and_services:ResearchComputing': 'Computing Tools and Services:Research Computing',
                'resource_group_tools_and_services:Software': 'Software:Software',
                'resource_group_tools_and_services:WebPublishingAndCommunication': 'Computing Tools and Services:Web Publishing and Communications',
            }
        self.TITLEMAP = {
                'Research Computing': 'Research Computing',
                'Web Hosting and Publishing': 'Web Publishing and Communications',
                'Data Resources': 'Data',
            }

        self.STEPS = []
        for stepconf in self.config['STEPS']:
            if not stepconf.get('LOCALTYPE'):
                self.logger.error('Step LOCALTYPE is missing or invalid')
                sys.exit(1)
            if not stepconf.get('CATALOGURN'):
                self.logger.error('Step "{}" CATALOGURN is missing or invalid'.format(stepconf.get('LOCALTYPE')))
                sys.exit(1)
            if stepconf['CATALOGURN'] not in self.CATALOGS:
                self.logger.error('Step "{}" CATALOGURN is not define in Resource Catalogs'.format(stepconf.get('LOCALTYPE')))
                sys.exit(1)
            myCAT = self.CATALOGS[stepconf['CATALOGURN']]
            stepconf['SOURCEURL'] = myCAT['CatalogAPIURL']
            
            try:
                SRCURL = urlparse(stepconf['SOURCEURL'])
            except:
                self.logger.error('Step SOURCE is missing or invalid')
                sys.exit(1)
            if SRCURL.scheme not in ['sql']:
                self.logger.error('Source must be one of {sql}')
                sys.exit(1)
            stepconf['SRCURL'] = SRCURL

            try:
                DSTURL = urlparse(stepconf['DESTINATION'])
            except:
                self.logger.error('Step DESTINATION is missing or invalid')
                sys.exit(1)
            if DSTURL.scheme not in ['function']:
                self.logger.error('Destination must be one of {function}')
                sys.exit(1)
            stepconf['DSTURL'] = DSTURL
            # Merge CATALOG config and STEP config, with latter taking precendence
            self.STEPS.append({**self.CATALOGS[stepconf['CATALOGURN']], **stepconf})
            
        signal.signal(signal.SIGINT, self.exit_signal)
        signal.signal(signal.SIGTERM, self.exit_signal)
        self.logger.info('Starting program={} pid={}, uid={}({})'.format(os.path.basename(__file__), os.getpid(), os.geteuid(), pwd.getpwuid(os.geteuid()).pw_name))

    def Connect_Source(self, urlparse): # TODO
        [host, port] = urlparse.netloc.split(':')
        port = port or '5432'
        database = urlparse.path.strip('/')
        conn_string = "host='{}' port='{}' dbname='{}' user='{}' password='{}'".format(host, port, database, self.config['SOURCE_DBUSER'], self.config['SOURCE_DBPASS'] )
        # get a connection, if a connect cannot be made an exception will be raised here
        conn = psycopg2.connect(conn_string)
        # conn.cursor will return a cursor object, you can use this cursor to perform queries
        cursor = conn.cursor()
        self.logger.info('Connected to PostgreSQL database {} as {}'.format(database, self.config['SOURCE_DBUSER']))
        return(cursor)
 
    def Connect_Elastic(self):
        if 'ELASTIC_HOSTS' in self.config:
            self.ESEARCH = elasticsearch_dsl.connections.create_connection( \
                hosts = self.config['ELASTIC_HOSTS'], \
                connection_class = RequestsHttpConnection, \
                timeout = 10)
            ResourceV3Index.init()
        else:
            self.ESEARCH = None
    
    def Disconnect_Source(self, cursor):
        cursor.close()
  
    def CATALOGURN_to_URL(self, id):
        return('{}/resource-api/{}/catalog/id/{}/'.format(self.WAREHOUSE_API_PREFIX, self.WAREHOUSE_API_VERSION, id))

    def format_GLOBALURN(self, *args):
        newargs = list(args)
        newargs[0] = newargs[0].rstrip(':')
        return(':'.join(newargs))

    def Read_SQL(self, cursor, sql, localtype):
        try:
            cursor.execute(sql)
        except psycopg2.Error as e:
            self.logger.error('Failed "{}" with {}: {}'.format(sql, e.pgcode, e.pgerror))
            sys.exit(1)

        COLS = [desc.name for desc in cursor.description]
        DATA = []
        for row in cursor.fetchall():
            DATA.append(dict(zip(COLS, row)))
        return({localtype: DATA})

    def Memory_Tags(self, content, localtype, config):
        TAGS = self.memory['tags'] = {}
        for rowdict in content[localtype]:
            TAGS[str(rowdict['id'])] = rowdict['label']
        return(True, '')
        
    def Memory_Resource_Tags(self, content, localtype, config):
        TAGS = self.memory['tags']
        RTAGS = self.memory['resource_tags'] = {}
        for rowdict in content[localtype]:
            id = str(rowdict['id'])
            if id not in RTAGS:
                RTAGS[id] = []
            try:
                RTAGS[id].append(TAGS[str(rowdict['tag_id'])])
            except:
                pass
        return(True, '')

    def Memory_Resource_Associations(self, content, localtype, config):
        RA = self.memory['resource_associations'] = {}
        for rowdict in content[localtype]:
            try:
                resource_id = str(rowdict['resource_id'])
                if resource_id not in RA:
                    RA[resource_id] = []
                RA[resource_id].append(str(rowdict['associated_resource_id']))
            except:
                pass
        return(True, '')

    def Memory_Guide_Resources(self, content, localtype, config):
        GR = self.memory['guide_resources'] = {}
        for rowdict in content[localtype]:
            try:
                guide_id = str(rowdict['curated_guide_id'])
                if guide_id not in GR:
                    GR[guide_id] = []
                GR[guide_id].append(str(rowdict['resource_id']))
            except:
                pass
        return(True, '')

    #
    # Delete old items (those in 'cur') that weren't updated (those in 'new')
    #
    def Delete_OLD(self, me, cur, new):
        for URN in [id for id in cur if id not in new]:
            try:
                ResourceV3Index.get(id = URN).delete()
            except Exception as e:
                self.logger.error('{} deleting Elastic id={}: {}'.format(type(e).__name__, URN, e))
            try:
                ResourceV3Relation.objects.filter(FirstResourceID__exact = URN).delete()
                ResourceV3.objects.get(pk = URN).delete()
                ResourceV3Local.objects.get(pk = URN).delete()
            except Exception as e:
                self.logger.error('{} deleting ID={}: {}'.format(type(e).__name__, URN, e))
            else:
                self.logger.info('{} deleted ID={}'.format(me, URN))
                self.STATS.update({me + '.Delete'})
        return()
    #
    # Update relations and delete relations for myURN that weren't just updated (newURNS)
    #
    def Update_REL(self, myURN, newRELATIONS):
        newURNS = []
        for relatedID in newRELATIONS:
            try:
                relationURN = ':'.join([myURN, md5(relatedID.encode('UTF-8')).hexdigest()])
                relation = ResourceV3Relation(
                            ID = relationURN,
                            FirstResourceID = myURN,
                            SecondResourceID = relatedID,
                            RelationType = newRELATIONS[relatedID],
                     )
                relation.save()
            except Exception as e:
                msg = '{} saving Relation ID={}: {}'.format(type(e).__name__, relationURN, e)
                self.logger.error(msg)
                return(False, msg)
            newURNS.append(relationURN)
        try: # Delete myURN relations that weren't just added/updated (newURNS)
            ResourceV3Relation.objects.filter(FirstResourceID__exact = myURN).exclude(ID__in = newURNS).delete()
        except Exception as e:
            self.logger.error('{} deleting Relations for Resource ID={}: {}'.format(type(e).__name__, myURN, e))

    def Warehouse_Providers(self, content, contype, config):
        start_utc = datetime.now(timezone.utc)
        myRESGROUP = 'Organizations'
        myRESTYPE = 'Provider'
        me = '{} to {}({}:{})'.format(sys._getframe().f_code.co_name, self.WAREHOUSE_CATALOG, myRESGROUP, myRESTYPE)
        self.PROCESSING_SECONDS[me] = getattr(self.PROCESSING_SECONDS, me, 0)
        
        cur = {}   # Current items in database
        new = {}   # New/updated items
        for item in ResourceV3Local.objects.filter(Affiliation__exact = self.Affiliation).filter(LocalType__exact = contype):
            cur[item.ID] = item
            
        for item in content[contype]:
            id_str = str(item['id'])       # From number
            myGLOBALURN = self.format_GLOBALURN(self.URNPrefix, 'uiuc.edu', contype, id_str)
            try:
                local = ResourceV3Local(
                            ID = myGLOBALURN,
                            CreationTime = datetime.now(timezone.utc),
                            Validity = self.DefaultValidity,
                            Affiliation = self.Affiliation,
                            LocalID = id_str,
                            LocalType = contype,
                            LocalURL = config.get('SOURCEDEFAULTURL', None),
                            CatalogMetaURL = self.CATALOGURN_to_URL(config['CATALOGURN']),
                            EntityJSON = item,
                    )
                local.save()
            except Exception as e:
                msg = '{} saving local ID={}: {}'.format(type(e).__name__, myGLOBALURN, e)
                self.logger.error(msg)
                return(False, msg)
            new[myGLOBALURN] = local

            try:
                resource = ResourceV3(
                            ID = myGLOBALURN,
                            Affiliation = self.Affiliation,
                            LocalID = id_str,
                            QualityLevel = 'Production',
                            Name = item['name'],
                            ResourceGroup = myRESGROUP,
                            Type = myRESTYPE,
                            ShortDescription = None,
                            ProviderID = None,
                            Description = None,
                            Topics = None,
                            Keywords = None,
                            Audience = self.Affiliation,
                     )
                resource.save()
                resource.indexing()
            except Exception as e:
                msg = '{} saving ID={}: {}'.format(type(e).__name__, myGLOBALURN, e)
                self.logger.error(msg)
                return(False, msg)
                
            myNEWRELATIONS = {} # The new relations for this item, key=related ID, value=relation type
            if item.get('parent_provider'):
                parentURN = self.format_GLOBALURN(self.URNPrefix, 'uiuc.edu', contype, str(item['parent_provider']))
                myNEWRELATIONS[parentURN] = 'Provided By'
            self.Update_REL(myGLOBALURN, myNEWRELATIONS)
            
            self.STATS.update({me + '.Update'})
            self.logger.debug('Provider save ID={}'.format(myGLOBALURN))

        self.Delete_OLD(me, cur, new)

        self.PROCESSING_SECONDS[me] += (datetime.now(timezone.utc) - start_utc).total_seconds()
        self.log_target(me)
        return(True, '')

    def Warehouse_Resources(self, content, contype, config):
        start_utc = datetime.now(timezone.utc)
#       Each item has its own GROUP and TYPE set inside the loop below
#        myRESGROUP = 'Organizations'
#        myRESTYPE = 'Provider'
        me = '{} to {}({}:{})'.format(sys._getframe().f_code.co_name, self.WAREHOUSE_CATALOG, '*', '*')
        self.PROCESSING_SECONDS[me] = getattr(self.PROCESSING_SECONDS, me, 0)

        cur = {}   # Current items in database
        new = {}   # New/updated items
        for item in ResourceV3Local.objects.filter(Affiliation__exact=self.Affiliation).filter(LocalType__exact=contype):
            cur[item.ID] = item
            
        RTAGS = self.memory['resource_tags']
        RA = self.memory['resource_associations']
        self.RESOURCE_CONTYPE = contype
        for item in content[contype]:
            id_str = str(item['id'])
            myGLOBALURN = self.format_GLOBALURN(self.URNPrefix, 'uiuc.edu', contype, id_str)
            # Convert warehouse last_update JSON string to datetime with timezone
            # Incoming last_update is a datetime with timezone
            # Once they are both datetimes with timezone, compare their strings
            # Can't compare directly because tzinfo have different represenations in Python and Django
            if not self.args.ignore_dates:
                try:
                    cur_dtm = parse_datetime(cur[myGLOBALURN].EntityJSON['last_updated'].replace(' ',''))
                except:
                    cur_dtm = datetime.now(timezone.utc)
                try:
                    new_dtm = item['last_updated']
                except:
                    new_dtm = None
                if str(cur_dtm) == str(new_dtm):
                    self.STATS.update({me + '.Skip'})
                    new[myGLOBALURN] = 'Skipped'        # So that we don't Delete_OLD below
                    continue

            for field in ['last_updated', 'start_date_time', 'end_date_time']:
                if field in item and isinstance(item[field], datetime):
                    item[field] = item[field].strftime('%Y-%m-%dT%H:%M:%S%z')

            myNEWRELATIONS = {} # The new relations for this item, key=related ID, value=relation type
            try:
                myProviderID = self.format_GLOBALURN(self.URNPrefix, 'uiuc.edu', 'provider', str(item['provider']))
            except:
                myProviderID = None
            else:
                myNEWRELATIONS[myProviderID] = 'Provided By'

            # V2 to V3 type mapping
            MAPKEY = '{}:{}'.format(item.get('resource_group', ''), item.get('resource_type', ''))
            (myRESGROUP, myRESTYPE) = self.TYPEMAP.get(MAPKEY, 'Error:Error').split(':')[:2]
            try:
                QualityLevel = self.STATUSMAP[str(item['record_status'])]
            except:
                QualityLevel = None
            try:
                Keywords = ','.join(RTAGS[id_str])
            except:
                Keywords = None
                
            if myRESTYPE or '' == 'Event': # In case it is None
                try:
                    StartDateTime = datetime_standardize(parse_datetime(item['start_date_time']))
                except:
                    StartDateTime = None
                try:
                    EndDateTime = datetime_standardize(parse_datetime(item['end_date_time']))
                except:
                    EndDateTime = None
            else:
                StartDateTime = None
                EndDateTime = None
                
            try:
                local = ResourceV3Local(
                            ID = myGLOBALURN,
                            CreationTime = datetime.now(timezone.utc),
                            Validity = self.DefaultValidity,
                            Affiliation = self.Affiliation,
                            LocalID = id_str,
                            LocalType = contype,
                            LocalURL = config.get('SOURCEDEFAULTURL', None),
                            CatalogMetaURL = self.CATALOGURN_to_URL(config['CATALOGURN']),
                            EntityJSON = item,
                    )
                local.save()
            except Exception as e:
                msg = '{} saving local ID={}: {}'.format(type(e).__name__, myGLOBALURN, e)
                self.logger.error(msg)
                return(False, msg)
            new[myGLOBALURN] = local
                
            try:
                resource = ResourceV3(
                            ID = myGLOBALURN,
                            Affiliation = self.Affiliation,
                            LocalID = id_str,
                            QualityLevel = QualityLevel,
                            Name = item.get('resource_name', None),
                            ResourceGroup = myRESGROUP,
                            Type = myRESTYPE,
                            ShortDescription = item.get('short_description', None),
                            ProviderID = myProviderID,
                            Description = item.get('resource_description', None),
                            Topics = item.get('topics', None),
                            Keywords = Keywords,
                            Audience = self.Affiliation,
                            StartDateTime = StartDateTime,
                            EndDateTime = EndDateTime,
                    )
                resource.save()
                resource.indexing()
            except Exception as e:
                msg = '{} saving ID={}: {}'.format(type(e).__name__, myGLOBALURN, e)
                self.logger.error(msg)
                return(False, msg)

            if id_str in RA:
                for assoc_id in RA[id_str]:
                    relatedID = self.format_GLOBALURN(self.URNPrefix, 'uiuc.edu', contype, assoc_id)
                    myNEWRELATIONS[relatedID] = 'Resource Association'
            self.Update_REL(myGLOBALURN, myNEWRELATIONS)

            self.STATS.update({me + '.Update'})
            self.logger.debug('{} updated ID={}'.format(contype, myGLOBALURN))

        self.Delete_OLD(me, cur, new)

        self.PROCESSING_SECONDS[me] += (datetime.now(timezone.utc) - start_utc).total_seconds()
        self.log_target(me)
        return(True, '')

    def Warehouse_Guides(self, content, contype, config):
        start_utc = datetime.now(timezone.utc)
        myRESGROUP = 'Guides'
#       Each item has its own TYPE set inside the loop below
#        myRESTYPE = 'Provider'
        me = '{} to {}({}:{})'.format(sys._getframe().f_code.co_name, self.WAREHOUSE_CATALOG, myRESGROUP, '*')
        self.PROCESSING_SECONDS[me] = getattr(self.PROCESSING_SECONDS, me, 0)

        GR = self.memory['guide_resources']
        cur = {}   # Current items in database
        new = {}   # New/updated items
        for item in ResourceV3Local.objects.filter(Affiliation__exact = self.Affiliation).filter(LocalType__exact = contype):
            cur[item.ID] = item
        
        for item in content[contype]:
            id_str = str(item['id'])       # From number
            myGLOBALURN = self.format_GLOBALURN(self.URNPrefix, 'uiuc.edu', contype, id_str)
            if 'created_at' in item and isinstance(item['created_at'], datetime):
                item['created_at'] = item['created_at'].strftime('%Y-%m-%dT%H:%M:%S%z')
            if 'updated_at' in item and isinstance(item['updated_at'], datetime):
                item['updated_at'] = item['updated_at'].strftime('%Y-%m-%dT%H:%M:%S%z')
            myRESTYPE = self.TITLEMAP.get(item['title'], '')
            try:
                local = ResourceV3Local(
                            ID = myGLOBALURN,
                            CreationTime = datetime.now(timezone.utc),
                            Validity = self.DefaultValidity,
                            Affiliation = self.Affiliation,
                            LocalID = id_str,
                            LocalType = contype,
                            LocalURL = config.get('SOURCEDEFAULTURL', None),
                            CatalogMetaURL = self.CATALOGURN_to_URL(config['CATALOGURN']),
                            EntityJSON = item,
                    )
                local.save()
            except Exception as e:
                msg = '{} saving local ID={}: {}'.format(type(e).__name__, myGLOBALURN, e)
                self.logger.error(msg)
                return(False, msg)
            new[myGLOBALURN] = local

            try:
                resource = ResourceV3(
                            ID = myGLOBALURN,
                            Affiliation = self.Affiliation,
                            LocalID = id_str,
                            QualityLevel = self.STATUSMAP.get(item.get('publish_status', '1'), 'Production'),
                            Name = myRESTYPE,
                            ResourceGroup = myRESGROUP,
                            Type = myRESTYPE,
                            ShortDescription = item['lede'],
                            ProviderID = None,
                            Description = item.get('component_data',''),
                            Topics = None,
                            Keywords = None,
                            Audience = self.Affiliation,
                    )
                resource.save()
                resource.indexing()
            except Exception as e:
                msg = '{} saving ID={}: {}'.format(type(e).__name__, myGLOBALURN, e)
                self.logger.error(msg)
                return(False, msg)

            myNEWRELATIONS = {} # The new relations for this item, key=related ID, value=relation type
            if id_str in GR:
                for assoc_id in GR[id_str]:
                    myRESOURCEURN = self.format_GLOBALURN(self.URNPrefix, 'uiuc.edu', self.RESOURCE_CONTYPE, assoc_id)
                    myNEWRELATIONS[myRESOURCEURN] = 'Guide Resource'
            self.Update_REL(myGLOBALURN, myNEWRELATIONS)

            self.STATS.update({me + '.Update'})
            self.logger.debug('Guide save ID={}'.format(myGLOBALURN))

        self.Delete_OLD(me, cur, new)

        self.PROCESSING_SECONDS[me] += (datetime.now(timezone.utc) - start_utc).total_seconds()
        self.log_target(me)
        return(True, '')

    def SaveDaemonLog(self, path):
        # Save daemon log file using timestamp only if it has anything unexpected in it
        try:
            with open(path, 'r') as file:
                lines = file.read()
                if not re.match('^started with pid \d+$', lines) and not re.match('^$', lines):
                    nowstr = datetime.strftime(datetime.now(), '%Y-%m-%d_%H:%M:%S')
                    newpath = '{}.{}'.format(path, nowstr)
                    shutil.move(path, newpath)
                    print('SaveDaemonLog as {}'.format(newpath))
        except Exception as e:
            print('Exception in SaveDaemonLog({})'.format(path))
        return

    def exit_signal(self, signal, frame):
        self.logger.critical('Caught signal={}, exiting...'.format(signal))
        sys.exit(0)

    def run(self):
        while True:
            if self.SOURCE_PARSE.scheme == 'postgresql':
                CURSOR = self.Connect_Source(self.SOURCE_PARSE)
            self.Connect_Elastic()
            self.STATS = Counter()
            self.PROCESSING_SECONDS = {}

            for stepconf in self.STEPS:
                start_utc = datetime.now(timezone.utc)
                pa_application = os.path.basename(__file__)
                pa_function = stepconf['DSTURL'].path
                pa_topic = stepconf['LOCALTYPE']
                pa_about = self.Affiliation
                pa_id = '{}:{}:{}:{}->{}'.format(pa_application, pa_function, pa_topic,
                    stepconf['SRCURL'].scheme, stepconf['DSTURL'].scheme)
                pa = ProcessingActivity(pa_application, pa_function, pa_id , pa_topic, pa_about)

                if stepconf['SRCURL'].scheme != 'sql':   # This is already checked in __inir__
                    self.logger.error('Source scheme must be "sql"')
                    sys.exit(1)
                if stepconf['DSTURL'].scheme != 'function':     # This is already checked in __inir__
                    self.logger.error('Destination scheme must be "function"')
                    sys.exit(1)

                # Retrieve from SOURCE
                content = self.Read_SQL(CURSOR, stepconf['SRCURL'].path, stepconf['LOCALTYPE'])
                # Content does not have the expected results
                if stepconf['LOCALTYPE'] not in content:
                    (rc, message) = (False, 'JSON results is missing the \'{}\' element'.format(stepconf['LOCALTYPE']))
                    self.logger.error(message)
                    pa.FinishActivity(rc, message)
                    continue

                (rc, message) = getattr(self, pa_function)(content, stepconf['LOCALTYPE'], stepconf)
                if not rc and message == '':  # No errors
                    message = 'Executed {} in {:.3f}/seconds'.format(pa_function,
                            (datetime.now(timezone.utc) - start_utc).total_seconds())
                pa.FinishActivity(rc, message)

            # Not disconnecting from Elasticsearch
            self.Disconnect_Source(CURSOR)
            break

    def log_target(self, me):
        summary_msg = 'Processed {} in {:.3f}/seconds: {}/updates, {}/deletes, {}/skipped'.format(me,
            self.PROCESSING_SECONDS[me],
            self.STATS[me + '.Update'], self.STATS[me + '.Delete'], self.STATS[me + '.Skip'])
        self.logger.info(summary_msg)

if __name__ == '__main__':
    try:
        router = HandleLoad()
        myrouter = router.run()
    except Exception as e:
        msg = '{} Exception: {}'.format(type(e).__name__, e)
        router.logger.error(msg)
        sys.exit(1)
    else:
        sys.exit(0)
