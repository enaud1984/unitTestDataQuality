#!/usr/bin/env /opt/cloudera/parcels/CDH-7.1.7-1.cdh7.1.7.p0.15945976/lib/hue/build/env/bin/python

import sys
import os
import inspect
from enum import Enum
import logging
import traceback
import psycopg2
from psycopg2.pool import ThreadedConnectionPool
from logging.handlers import RotatingFileHandler
import json
from datetime import datetime

class ScriptConfiguration:
    def __init__(self):
        CONFIG_FILE = 'config.json'
        CONFIG_COMMON_FILE = "common.json"
        try:
            with open(CONFIG_FILE, 'r') as config_json:
                custom_cfg = json.loads(config_json.read())
                
            with open(CONFIG_COMMON_FILE, 'r') as c_json:
                common_cfg = json.loads(c_json.read())
        except Exception as e:
            print("initVar - ERROR")
            #raise self.manageException(e, info="ERROR JSON decode: unable to decode json file... check {}".format(CONFIG_FILE))
            
        config = self.merge_dicts(common_cfg, custom_cfg)
        
        self.app_name = self.getJsonValue(config, "app_name", "app", "name")
        self.app_code = str(self.getJsonValue(config, -1, "app", "code"))
        self.exit_code = self.getJsonValue(config, 0, "app", "exit_code")
        self.app_validity_minutes = self.getJsonValue(config, -1, "app", "validity_minutes")
        self.app_description = self.getJsonValue(config, "", "app", "description")
        self.schema_file = self.getJsonValue(config, "", "app", "schema_file")
        self.schema_path = self.getJsonValue(config, "", "app", "schema_path")
        self.file_path_dest = self.getJsonValue(config, "", "app", "file_path_dest")
        
        # LOG PARAMS
        self.level_debug = self.getJsonValue(config, False, "log", "level_debug")
        self.level_verbose = self.getJsonValue(config, False, "log", "level_verbose")
        self.log_filename = self.getStringTodayFormat(self.getJsonValue(config, None, "log", "filename"))
        self.log_local_folder = self.getStringTodayFormat(self.getJsonValue(config, None, "log", "local_folder"))
        self.log_remote_folder = self.getStringTodayFormat(self.getJsonValue(config, None, "log", "remote_folder"))
        
        
        # DB PARAMS
        self.db_log_table = self.getJsonValue(config, "LOG", "db", "log_table")
        self.db_log_table_hdfs = self.getJsonValue(config, "LOG", "db", "log_table_hdfs")

        self.db_host = self.getJsonValue(config, None, "db", "host")
        self.db_port = self.getJsonValue(config, None, "db", "port")
        self.db_database = self.getJsonValue(config, None, "db", "database")
        self.db_schema = self.getJsonValue(config, None, "db", "schema")
        self.db_user = self.getJsonValue(config, None, "db", "user")
        self.db_password = self.getJsonValue(config, None, "db", "password")
        
        self.special_char_table = self.getJsonValue(config, None, "db", "special_char_table")
        self.string_poss_table = self.getJsonValue(config, None, "db", "string_poss_table")
        self.get_id_function = self.getJsonValue(config, None, "db", "get_id_function")
        
        # DEFAULT VALUES
        self.defaultValue_string = self.getJsonValue(config, None, "defaultValues", "string")
        self.defaultValue_date = self.getJsonValue(config, None, "defaultValues", "date")
        self.defaultValue_number = self.getJsonValue(config, None, "defaultValues", "number")
        
        self.cfg = config

    def merge_dicts(self, dict1, dict2):
        """ merge ricorsivo di due dict, sovrascrive i valori in 'conflitto' """
        if not isinstance(dict1, dict) or not isinstance(dict2, dict):
            return dict2

        for k in dict2:
            if k in dict1:
                dict1[k] = self.merge_dicts(dict1[k], dict2[k])
            else:
                dict1[k] = dict2[k]

        return dict1
    
    
    def getJsonValue(self, data, defval=None, *args):
        try:
            val = data
            for x in args :
               val = val[x]
            return val
        except:
            return defval
   
   
    def getStringTodayFormat(self, stringTime):
        today = datetime.today()
        return stringTime \
            .replace( '%Y', str( today.year ) ) \
            .replace( '%m', "{:02d}".format( today.month ) ) \
            .replace( '%d', "{:02d}".format( today.day ) ) \
            .replace( '%H', "{:02d}".format( today.hour ) ) \
            .replace( '%M', "{:02d}".format( today.minute ) ) \
            .replace( '%S', "{:02d}".format( today.second ) ) \
            .replace( '%f', str( today.microsecond ) )
            
        
    def getDbClient(self, host=None, database=None, user=None, password=None, schema=None, port=None):
        if not hasattr(self, "dbClient") or self.dbClient is None:
            self.dbClient = None
            try:
                host = host if host else self.db_host
                database = database if database else self.db_database
                user = user if user else self.db_user
                password = password if password else self.db_password
                schema = schema if schema else self.db_schema
                port = port if port else self.db_port
                assert host is not None, "DB client params - 'host' not set in configuration file"
                assert database is not None, "DB client params - 'database' not set in configuration file"
                assert user is not None, "DB client params - 'user' not set in configuration file"
                assert password is not None, "DB client params - 'password' not set in configuration file"
                assert schema is not None, "DB client params - 'schema' not set in configuration file"
                assert port is not None, "DB client params - 'port' not set in configuration file"
                
                self.dbClient = Db(host, database, user, password, schema, port)
            except Exception as e:
                print("getDbClient - ERROR")
                raise Exception(str(e))

        return self.dbClient


class Logger:
    local_log_filename = None
    mylogger = None
    mylogger_handler = None
    enable_print = False
    enable_verbose = False
    level_num = -1

    class LogLevel(str, Enum):
        VERBOSE = 0
        DEBUG = 1
        INFO = 2
        WARNING = 3
        ERROR = 4
        CRITICAL = 5


    def __init__(self, class_name, app_name, local_log_file, time_precision="second", log_level="INFO"):
        mode = {
            "day": '%Y%m%d',
            "hour": '%Y%m%d%H',
            "minute": '%Y%m%d%H%M',
            "second": '%Y%m%d%H%M%S%f'
        }

        level_ = ""
        if log_level == "DEBUG" :
            level_ = logging.DEBUG
            self.level_num = self.LogLevel.DEBUG
        elif log_level == "VERBOSE" :
            level_ = logging.DEBUG
            self.level_num = self.LogLevel.DEBUG
            self.enable_verbose = True
        elif log_level == "ERROR" :
            level_ = logging.ERROR
            self.level_num = self.LogLevel.ERROR
        else :
            level_ = logging.INFO
            self.level_num = self.LogLevel.INFO

        if time_precision not in mode.keys():
            time_precision = "second"

        # logging.basicConfig(filename=LOCAL_LOG_FILE, level=logging.INFO)
        self.local_log_filename = local_log_file
        self.hdfs_date_format = datetime.today().strftime( mode[time_precision] )

        logging.getLogger("py4j").setLevel(logging.ERROR)
        logging.getLogger('pyspark').setLevel(logging.ERROR)
        class_name = "ingestion"
        Logger.mylogger = logging.getLogger( class_name )
        Logger.mylogger.setLevel( level_ )

        formatter = logging.Formatter( '%(name)s\t- %(levelname)s\t- (%(threadName)-10s)\t- %(message)s' )

        self.logger_handler = logging.handlers.RotatingFileHandler( self.local_log_filename, maxBytes = 18874368, backupCount = 1)
        self.logger_handler.setLevel( level_)
        self.logger_handler.setFormatter( formatter )
        Logger.mylogger.addHandler( self.logger_handler )

        self.mylogger_handler = self.logger_handler

        ENABLE_PRINT = os.environ.get( 'ENABLE_PRINT' )
        if ENABLE_PRINT is not None:
            print( 'ENABLE_PRINT = ' + ENABLE_PRINT )
            #self.enable_print = True
            consoleHandler = logging.StreamHandler()
            consoleHandler.setFormatter(formatter)
            Logger.mylogger.addHandler(consoleHandler)

    def closeLogger(self, logger_handler):
        self.logger_handler.flush()
        self.logger_handler.close()

    def __del__(self):
        self.mylogger_handler.flush()
        self.mylogger_handler.close()

    def getLoggerFileName(self):
        return self.local_log_filename

    def getLogger(self):
        return Logger.mylogger

    def removeLogger(self):
        proc = subprocess.Popen( "rm -f {}".format( self.getLoggerFileName() ), shell=True )
        proc.communicate()

    def error(self, msg, flag_date=True, exc_info=None):
        if self.level_num <= self.LogLevel.ERROR :
            m = msg if not flag_date else "{} - {}".format( datetime.now().strftime( "%Y/%m/%d %H:%M:%S" ), msg )
    
            if self.enable_print:
                print( "ERROR - {}".format( m ) )
    
            if exc_info is None:
                Logger.mylogger.error( m )
            else:
                Logger.mylogger.error( m, exc_info )

    def warning(self, msg, flag_date=True):
        if self.level_num <= self.LogLevel.WARNING :
            m = msg if not flag_date else "{} - {}".format( datetime.now().strftime( "%Y/%m/%d %H:%M:%S" ), msg )
    
            if self.enable_print:
                print( "WARNING - {}".format( m ) )
            Logger.mylogger.warning( m )

    def info(self, msg, flag_date=True):
        if self.level_num <= self.LogLevel.INFO :
            m = msg if not flag_date else "{} - {}".format( datetime.now().strftime( "%Y/%m/%d %H:%M:%S" ), msg )
    
            if self.enable_print:
                print( "INFO - {}".format( m ) )
            Logger.mylogger.info( m )

    def debug(self, msg, flag_date=True):
        if self.level_num <= self.LogLevel.DEBUG :
            caller = inspect.stack()[1][3]
            method = inspect.stack()[2][3] if caller == "wrapper" else caller
            m = msg if not flag_date else "{} - {}".format( datetime.now().strftime( "%Y/%m/%d %H:%M:%S" ), msg )
    
            if self.enable_print:
                print( "DEBUG - {} - {}".format(method, m))
            Logger.mylogger.debug( m )

    def verbose(self, msg, flag_date=True):
        if self.level_num <= self.LogLevel.VERBOSE :
            caller = inspect.stack()[1][3]
            method = inspect.stack()[2][3] if caller == "wrapper" else caller
            m = msg if not flag_date else "{} - {}".format( datetime.now().strftime( "%Y/%m/%d %H:%M:%S" ), msg )
    
            if self.enable_print:
                print( "VERBOSE - {} - {}".format(method, m))
            Logger.mylogger.debug( " (v) {}".format(m) )

    def debug_old(self, msg, flag_date=True):
        m = msg if not flag_date else "{} - {}".format( datetime.now().strftime( "%Y/%m/%d %H:%M:%S" ), msg )

        if self.enable_print:
            print( "DEBUG - {}".format( m ) )
        Logger.mylogger.debug( m )
        
    
    def copyLogHdfs(self, hdfs_log_path, local_path_filename):
        print( "hdfs dfs -copyFromLocal  -f  local_path_filename:{} hdfs_log_path:{}".format( local_path_filename,
                                                                                              hdfs_log_path ) )
        proc = subprocess.Popen( "hdfs dfs -copyFromLocal  -f  {} {}".format( local_path_filename, hdfs_log_path ),
                                 shell=True )
        proc.communicate()


class Db:
    class DatabaseError(Exception):
        def __init__(self, exc):
            super(Db.DatabaseError, self).__init__(exc)

    DATA_TYPE_TIMESTAMP = "timestamp"
    DATA_TYPE_INTEGER = "int"
    DATA_TYPE_STRING = "string"
    DATA_TYPE_FLOAT = "float"
    DATA_TYPE_LIST = "list"
    LIST_SEPARATOR = "||"
    DATETIME_FORMAT = "%Y-%m-%d %H:%M:%S"

    DICT_SCRIPT = "SCRIPT"
    DICT_KEY = "KEY"
    DICT_VALUE = "VALUE"
    DICT_TYPE = "TYPE_VALUE"

    def __init__(self, db_host, db_database, user, password, schema=None, port=None):
        self.db_host = db_host
        self.db_database = db_database
        self.user = user
        self.password = password
        self.connection = None
        self.connected = False
        self.db_schema = schema
        self.db_port = port
        self.openConnection()

    def openConnection(self):
        try:
                
            self.connection = psycopg2.connect(host=self.db_host, \
                database=self.db_database, \
                user=self.user, \
                password=self.password, \
                options="-c standard_conforming_strings=on".format(self.db_schema))
            
            self.connected = True
        except Exception as e:
            raise self.DatabaseError(e)

    def getConnection(self):
        return self.connection

    def close(self):
        if self.connected:
            self.connection.commit()
            self.connection.close()
            self.connected = False

    def resetConnection(self):
        self.connection.close()
        self.openConnection()

    def checkTableExists(self, tablename):
        stmt="SELECT to_regclass('{}')".format(tablename)
        stmt="SELECT '{}'::regclass".format(tablename)
        
        dbcur = self.connection.cursor()
        dbcur.execute( stmt )
        print("RETURN QUERY CHECKTABLES EXIST:", dbcur.fetchall())
        print("dbcur.fetchone() is not None: ", dbcur.fetchall() is not None)
        if dbcur.fetchall() is not None:
            print("TABELLA ESISTE")
            dbcur.close()
            return True
        print( "TABELLA non ESISTE" )
        dbcur.close()
        return False

    def executeMany(self, query, tuples, commit=False):

        print("executeMany - query == {}".format(query))
        print("executeMany - tuples == {}".format(tuples))

        if self.connected:
            cursor = self.connection.cursor()
            try:                            
                cursor.executemany(query, tuples)
                if commit:
                    self.connection.commit()
            except Exception as e:
                raise
            finally:
                cursor.close()
        else:
            pass

    def executeQuery(self, query, parameter=None, commit=False, isSelect=False):
        if self.connected:
            cursor = self.connection.cursor()
            try:                
                cursor.execute(query, parameter) if parameter else cursor.execute(query)
                if commit:
                    self.connection.commit()
                if isSelect:
                    res = cursor.fetchall()
                    return res
            except Exception as e:
                raise
            finally:
                cursor.close()
        else:
            pass

    def truncateTable(self, table_name):
        query = "truncate table {}".format(table_name)
        self.executeQuery(query)

    def truncateWithDelete(self, table_name, where_clause="", commit=False):
        query = "DELETE FROM {}".format(table_name)
        if where_clause:
            query += " WHERE {}".format(where_clause)
        self.executeQuery(query, commit=commit)  # ????

    def renameTable(self, old_table_name, new_table_name):
        query = "rename table {} to {}".format(old_table_name, new_table_name)
        self.executeQuery(query)

    def dropTable(self, table_name):
        query = "drop table {}".format(table_name)
        self.executeQuery(query)

    # value_dict = dictionary contenente i valori da modificare e le chiavi sono i nomi delle colonne
    def updateTableWhere(self, table_name, value_dict, where_clause="", commit=False):
        query = "UPDATE {} SET ".format(table_name)
        query += ", ".join("{}='{}'".format(k, v) for k, v in value_dict.items())
        if where_clause:
            query += " where {}".format(where_clause)

        self.executeQuery(query, commit)

    # Da capire
    def insertListOfDictsIntoTable(self, table_name, l, dt_cols=[], commit=False):
        cols = sorted(list(l[0].keys()))

        if dt_cols:
            non_dt_cols = list(set(cols) - set(dt_cols))
        else:
            non_dt_cols = cols

        cols = non_dt_cols + dt_cols

        l = [[dic[col] for col in cols] for dic in l]

        print("insertListOfDictsIntoTable l = {}".format(l))
        self.insertListIntoTable(table_name, l, non_dt_cols, dt_cols)

        if commit:
            self.connection.commit()

    def insertListIntoTable(self, table_name, l, non_dt_cols, dt_cols=[]):
        '''
        The first values of each row should represent non-datetime fields.
        '''
        print("insertListIntoTable l = {}".format(l))
        print("insertListIntoTable dt_cols = {}".format(dt_cols))
        brackets_str = ", ".join(["{}"] * len(non_dt_cols))
        if dt_cols:
            brackets_str += ", "
            brackets_str += ", ".join(["to_timestamp({}, 'yyyy-MM-dd HH24:mi:ss')"] * len(dt_cols))
        n_cols = len(dt_cols) + len(non_dt_cols)
        # brackets_str = brackets_str.format( *[":" + str( i + 1 ) for i in range( 0, n_cols )] )
        brackets_str = brackets_str.format(*["?" for i in range(0, n_cols)])

        cols_str = ", ".join(non_dt_cols + dt_cols)

        query = "INSERT INTO {} ({}) VALUES ({})".format(table_name, cols_str, brackets_str)
        print("insertListIntoTable query = {}".format(query))
        print("insertListIntoTable cols_str = {}".format(cols_str))
        print("insertListIntoTable brackets_str = {}".format(brackets_str))

        self.executeMany(query, l)

    def executeProcedure(self, procname, commit=False, parameters=None):
        if self.connected:
            procname = procname if ("." not in procname) else procname.split(".")[-1]
            final_procname = "{}.{}".format(self.db_schema, procname)

            proc_str = final_procname + "({})".format(",".join(parameters) if parameters else "")
            procedure_query = "CALL {}".format(proc_str)

            cursor = self.connection.cursor()
            try: 
                cursor.execute(procedure_query)
            except Exception as e:
                self.connection.rollback()
                raise
            else:
                self.connection.commit() if commit else None
            finally:
                cursor.close()
        else:
            pass