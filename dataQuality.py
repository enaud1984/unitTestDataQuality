#!/usr/bin/env /opt/cloudera/parcels/CDH-7.1.7-1.cdh7.1.7.p0.15945976/lib/hue/build/env/bin/python

import sys
from utility import ScriptConfiguration, Logger
import logging
from logging.handlers import RotatingFileHandler
import json
from jsonschema import validate, ValidationError
from datetime import datetime
from dateutil.parser import parse
from dateutil import parser
import pytz
from copy import deepcopy
import traceback
import unicodedata

class DataQuality(object):

    def __init__(self):
        self.config = ScriptConfiguration()
        self.file_path = self.config.file_path_dest
        local_log = self.config.log_local_folder + "/" + self.config.log_filename
        #remote_log = self.config.log_remote_folder + "/" + self.config.log_filename
        log_lev = "DEBUG" if self.config.level_debug else "VERBOSE" if self.config.level_verbose else "INFO"
        self.logger = Logger(class_name='', app_name=self.config.app_name, local_log_file=local_log, time_precision='second', log_level=log_lev) if local_log else None
        self.logger.debug("init - local_log: '{}'".format(local_log))
        #self.logger.debug("init - remote_log: '{}'".format(remote_log))
        self.dbClient = self.config.getDbClient()
        self.logger.info("Connection to db opened! Db url: jdbc:postgresql://{}:{}/{}".format(self.config.db_host, self.config.db_port, self.config.db_database))
    
    ''' ##########################################################################
    ###                   DATA REFINEMENT                                      ###
    ########################################################################## '''
    
    
    def jsonValidation(self, json_file, schema=None):
        ''' Validazione del json_file secondo lo schema in input. Di default se non passato, lo schema viene individuato
            dal file di configurazione: self.config.schema_path + "/" + self.config.schema_file '''
        try:
            if schema is None:
                schema = self.config.schema_path + "/" + self.config.schema_file
            self.logger.info("init - jsonValidation for '{}' with schemafile '{}' in path '{}'".format(self.config.app_name, self.config.schema_file, self.config.schema_path))
            with open(schema) as schema:
                self.json_schema = json.load(schema)
            validate(json_file, self.json_schema)
            self.logger.info("END OK - jsonValidation")
        except ValidationError as err:
            self.logger.error("ValidationError - Error in validation json schema. Details: {}".format(str(err)))
            raise ValidationError("ValidationError - Error in validation json schema. Details: {}".format(str(err)))
        except Exception as exc:
            self.logger.error("Generic error in validation json schema. Details: {}".format(str(exc)))
            raise Exception("Generic error in validation json schema. Details: {} and traceback: ".format(str(exc)))
            
    
    def capitalizeFirstLetter(self, value):
        ''' Se la prima lettera non e' maiuscola, restituisce il valore originario con la lettera maisucola. '''
        if (not value[0].isupper()):
            return value.capitalize()
        else:
            return value
          
    def extractYearFromEveryDate(self, value, fuzzy = False):
        ''' Estrae l'anno da una stringa rappresentante una data (qualsiasi formato) e lo restituisce come intero.
        '''
        try: 
            return parse(value, fuzzy=fuzzy).year
        except ValueError as ve:
            self.logger.error("Error in extractYearFromEveryDate for field date value '{}'. Details: {}".format(value, str(ve)))
            
    def is_date(self, string, fuzzy = False):
        ''' Analizza una stringa e ritorna True se rappresenta una data (qualsiasi sia il formato), False altrimenti '''
        try:
            parse(string, fuzzy=fuzzy)
            return True
        except ValueError as ve:
            return False
            
    def byAThousand(self, value=1):
        ''' Moltiplica il valore in input per 1000. Di default il valore in input e' 1. '''
        return int(value) * 1000
                
            
    def standardizeDate(self, string):
        ''' Ritorna una data standardizzata in formato utc'''
        try:
            try:
                date = parser.parse(string)
            except:
                return None
            local = pytz.timezone("Europe/Vienna")
            naive = datetime.strptime(date.strftime("%Y-%m-%dT%H:%M:%SZ"), "%Y-%m-%dT%H:%M:%SZ")
            local_dt = local.localize(naive, is_dst=None)
            utc_dt = local_dt.astimezone(pytz.utc)
            return utc_dt.strftime("%Y-%m-%dT%H:%M:%SZ")
        except ValueError as ve:
            self.logger.error("Error in extractYearFromEveryDate for field date value '{}'. Details: {}".format(ve, str(ve)))
            
            
            
    def is_number(self, string):
        ''' Verifica se una stringa contiene un numero (sia esso float, int, ecc..) e ritorna un booleano'''
        try:
            float(string)
            if isinstance(string, bool):
                return False
            else:
                return True
        except ValueError:
            return False
            
    def beforeEnd(self, className=None):
        ''' Metodo utilizzato per chiudere la connessione al db se aperta prima di terminare l'esecuzione dello script
            e per copiare il log in hdfs. '''
        try:
            self.dbClient.close()
            self.logger.info("Connection to db closed!")
            
            className = className if className else self.config.app_name
            self.logger.info("{} END".format(className))
            #log_local_path = self.config.log_local_folder + "/" + self.config.log_filename
            #self.config.copyLogHdfs(self.config.log_remote_folder, log_local_path)
        except Exception as exc:
            self.logger.error("Error in closing db connection! Details: {}".format(str(exc)))
            
    def structuralStringErrorRepair(self, key, string):
        ''' Effettua gli step richiesti per riparare agli errori strutturali delle stringhe in questo contesto e ritorna
            la stringa modificata.
            In particolare:
            - Rimozione dei caratteri speciali - metodo: specialChar(self, string)
            - Standardizzazione del testo - metodo: standardizeText(self, string)
            - Standardizzazione della capitalizzazione - title()
            - Rimozione di eventuali spazi iniziali e finali all interno della stringa
            Per maggiori informazioni, verificare la sezione 6.2.2 dell'Allegato D - Componenti di Sistema 
            (punto 'Errori strutturali').
        '''
        try:
            self.logger.info("Inizio esamina stringa. Stringa ricevuta: '{}'".format(string))
            if string != '':
                strng = self.specialChar(string) if self.specialChar(string) else string
                strng = self.standardizeText(strng) if self.standardizeText(strng) else strng
                string_to_return = ' '.join(strng.split()).title()
                return string_to_return
            else:
                return self.defaultValues(key) if self.defaultValues(key) else "Null"
        except Exception as exc:
            self.logger.error("Error while repairing structural error in string value '{}'. Details: {}".format(string, str(exc)))
 
    def standardizeText(self, string):
        ''' Effettua le operazioni di standardizzazione del testo richieste e ritorna la stringa modificata. 
            In particolare, per farlo viene utilizzata una tabella presente sul db postgresql.
        '''
        try:
            self.logger.info("Inizio standardizzazione stringa.")
            res = self.dbClient.executeQuery("SELECT * from {}.{}".format(self.config.db_schema, self.config.string_poss_table), isSelect=True)
            for record in res:
                for el in record[0]:
                    #self.logger.debug("el {}".format(el))
                    if string.upper() == el:
                        self.logger.debug("String value found in standard table. Replacing it with: '{}'".format(record[1]))
                        return record[1]
        except Exception as exc:
            self.logger.error("Error while trying to standardize text '{}'. Details: {}".format(string, str(exc)))
            raise Exception("Error while trying to standardize text '{}'. Details: {}".format(string, str(exc)))
            
    def specialChar(self, string):
        ''' Individua e sostituisce/rimuove i caratteri speciali tramite tabella parametrica e ritorna la stringa modificata. 
            In particolare, per farlo viene utilizzata una tabella presente sul db postgresql.
        '''
        try:
            self.logger.info("Inizio ricerca caratteri speciali")
            res = self.dbClient.executeQuery("SELECT * from {}.{}".format(self.config.db_schema, self.config.special_char_table), isSelect=True)
            #self.logger.debug("res: {}".format(res))
            for record in res:
                if record[0] in string:
                    self.logger.debug("Trovato carattere speciale: {}".format(record[0]))
                    string = string.replace(record[0], record[1]) if record[1] else string.replace(record[0], "")
        except UnicodeDecodeError as ude:
            self.logger.warning("UnicodeDecodeError: {}".format(str(ude)))
            for record in res:
                if record[0] in string.decode('utf-8'):
                    self.logger.debug("Trovato carattere speciale: {}".format(record[0]))
                    string = string.decode('utf-8').replace(record[0], record[1]) if record[1] else string.decode('utf-8').replace(record[0], "")
        finally:
            self.logger.debug("return_value: {}".format(string))
            return string
            
    def defaultValues(self, key, schema=None):
        ''' Imposta i valori di default per i campi mandatori che vengono ricevuti nulli. In particolare:
            - campo stringa - valore di default '--'
            - campo data - valore di default '01-01-0001'
            - campo numerico - valore di default -1
            Per maggiori informazioni, verificare la sezione 6.2.2 dell'Allegato D - Componenti di Sistema 
            (punto 'Errori strutturali').
        '''
        try:
            self.logger.info("defaultValues - INIT")
            requiredFields = self.config.getJsonValue(self.json_schema, None, "required") if self.config.getJsonValue(self.json_schema, None, "required") \
                else self.config.getJsonValue(self.json_schema, None, "items", "required")
            for i in range(len(requiredFields)):
                if requiredFields[i] == key:
                    type = self.config.getJsonValue(self.json_schema, None,"items","properties", key, "type")
                    if type == "string":
                        return self.config.defaultValue_string
                    elif type == "date":
                        return self.config.defaultValue_date
                    else:
                        return self.config.defaultValue_number
                else:
                    continue
        except Exception as e:
            self.logger.error("defaultValues - ERROR: {}".format(str(e)))
    
    def removeDuplicateJson(self, json_list):
        ''' Rimuove json duplicati all'interno di una lista di json e ritorna la lista pulita
        '''
        try:
            self.logger.info("removeDuplicateJson - INIT")
            seen = []
            for json_el in json_list:
                if json_el not in seen:
                    seen.append(json_el)
            return seen if seen else json_list
        except Exception as exc:
            self.logger.error("removeDuplicateJson - ERROR {}".format(str(exc)))
    
    def ordered(self, obj):
        ''' Ordina qualsiasi tipo di lista e converte i dizionari in liste di chiave,valore cosi da ordinarli.
        '''
        if isinstance(obj, dict):
            return sorted((k, self.ordered(v)) for k,v in obj.items())
        if isinstance(obj, list):
            return sorted(self.ordered(x) for x in obj)
        else:
            return obj
            
    def defYear(self, millenium, year):
        ''' Definisce e aggiunge il millennio ad un anno trocato. Per esempio, 20 ---> 2020
        '''
        return millenium*100+year
    
    def verifyTimeSlot(self, timeSlotString, char):
        ''' Verifica se una fascia oraria stringa e' coerente. Per es 06-08 --> True, 06-25 --> False
            Prende in input anche il carattere di divisione della fascia e ritorna:
            - il valore originale se la fascia e' coerente
            - la stringa 'Null' se la fascia non e' corente.'''
        try:
            timeSlots = timeSlotString.split(char)
            self.logger.info("timeSlots - {}".format(timeSlots))
            flag = False
            for time in timeSlots:
                if int(time) in range (1, 24):
                    flag = True
                else:
                    flag = False
            if flag:
                return timeSlotString
            else:
                return 'Null'
        except Exception as exc:
            self.logger.error("verifyTimeSlot - ERROR {}".format(str(exc)))
            
    def standardizeNumericFormat(self, numeric, comma=True):
        ''' Il metodo prende in input un numero (formato numerico/stringa) e lo standardizza arrotondando con due cifre decimali.
            Restituisce una stringa e se il parametro comma e' True (default) viene sostituito il punto con la virgola.'''
        try:
            self.logger.info("standardizeNumericFormat - INIT")
            if not isinstance(numeric, str) and not isinstance(numeric, unicode):
                self.logger.debug("type not string - {} is type {}".format(numeric, type(numeric)))
                rounded_value = format(round(numeric), '.2f')
                self.logger.debug("rounded_value pre comma- {}".format(rounded_value))
                if comma:
                    self.logger.debug("comma")
                    rounded_value = rounded_value.replace('.',',')
                self.logger.debug("rounded_value - {}".format(rounded_value))
                return rounded_value
            else:
                self.logger.debug("type string")
                rounded_value = format(round(float(numeric)), '.2f')
                if comma:
                    rounded_value = rounded_value.replace('.',',')
                self.logger.debug("rounded_value - {}".format(rounded_value))
                return rounded_value
        except Exception as exc:
            self.logger.error("standardizeNumericFormat - ERROR {}".format(str(exc)))
    
    def standardizeBoolValue(self, boolValue):
        ''' Standardizza un valore booleano true/false trasformandolo in si/no'''
        try:
            true_values = self.config.getJsonValue(self.config.cfg, None, "booleanValues", "true_values")
            false_values = self.config.getJsonValue(self.config.cfg, None, "booleanValues", "false_values")
            if boolValue in true_values:
                return 'Si'
            elif boolValue in false_values:
                return 'No'
            else:
                return 'Null'
        except Exception as exc:
            self.logger.error("standardizeBoolValue - ERROR {}".format(str(exc)))
        
    def decodingUnicodeJson(self, unicodeJsonObject):
        self.logger.info("decodingUnicodeJson - INIT")
        json_dict = dict()
        for k in unicodeJsonObject.keys():
            self.logger.debug("input key {}".format(k))
            try:
                new_key = unicodedata.normalize('NFKD', k).encode('ascii','ignore')
                json_dict[new_key] = unicodeJsonObject[k]
            except TypeError as te:
                self.logger.warning("TypeError: {}. Real type = {}".format(str(te), type(k)))
                continue
            except Exception as exc:
                self.logger.error("decodingUnicodeJson - ERROR {}".format(str(exc)))
        for k, v in json_dict.items():
            #self.logger.debug("input value {}".format(v))
            try:
                if not isinstance(v, bool):
                    new_value = unicodedata.normalize('NFKD', v).encode('ascii','ignore')
                    json_dict[k] = new_value
            except TypeError as te:
                self.logger.warning("TypeError: {}. Real type = {}. Value error key = {}".format(str(te), type(v), k))
                continue
            except Exception as exc:
                self.logger.error("decodingUnicodeJson - ERROR {}".format(str(exc)))
        self.logger.debug("output json {}".format(json.dumps(json_dict, indent=4)))
        self.logger.info("decodingUnicodeJson - END")
        return json_dict if json_dict else unicodeJsonObject
        
    
    ##########################################################################
    def splitMultipleValues(self, json_in):
        ''' Individua la presenza di valori multipli in un unico campo testuale e li splitta in campi distinti.
            Per farlo, cambia la struttura dell'oggetto json originale, per questo motivo restituisce un nuovo json.
        '''
        try:
            self.logger.info("splitMultipleValues - INIT")
            list_of_key = []
            list_of_value = []
            list_of_k_v = []
            for key, value in json_in.items():
                if (isinstance(value, basestring) and value[0].isalpha()) and ',' in value:
                    self.logger.debug("Sono presenti valori multipli in '{}'".format(value))
                    values = value.split(',')
                    list_of_k_v.append((key, values))
                    list_of_key.append(key)
                    self.logger.debug("list_of_k_v = {}".format(list_of_k_v))
                    self.logger.debug("list_of_key = {}".format(list_of_key))
                else:
                    continue
            
            num_key = 0
            num_values = 0
            for i, tuple in enumerate(list_of_k_v):
                v = tuple[1]
                k = tuple[0]
                self.logger.debug("key == {}, value == {}, len(v) == {}".format(k, v, len(v)))
                num_key += i
                num_values += len(v)
            self.logger.debug("num_key == {}; num_values == {}".format(num_key, num_values))
            
            json_list = []
            for k in range(len(list_of_key)):
                for n in range(len(list_of_k_v)):
                    if list_of_key[k] == list_of_k_v[n][0]:
                        self.logger.debug("list_of_key[k] == {}; list_of_k_v[n][0] == {}".format(list_of_key[k], list_of_k_v[n][0]))
                        json_copy = deepcopy(json_in)
                        self.logger.debug("json_copy in == {}".format(json_copy))
                        for x in range(len(list_of_k_v[n][1])):
                            self.logger.debug("list_of_k_v[n][1][x] == {}".format(list_of_k_v[n][1][x]))
                            json_copy[list_of_key[k]] = list_of_k_v[n][1][x]
                            json_list.append(json_copy)
            self.logger.debug("json_list == {}".format(json_list))
        except Exception as e:
            self.logger.error("splitMultipleValues - ERROR: {}".format(str(e)))
            raise
            
    
    ''' ##########################################################################
    ###                   DATA ENRICHMENT                                      ###
    ########################################################################## '''
    
    def addingIngestionDate(self, json_in):
        ''' Metodo utilizzato per introdurre campi nuovi nel json. In particolare viene introdotto il campo
            - Data di ingestion
            Per maggiori informazioni, verificare la sezione 6.2.3 dell'Allegato D - Componenti di Sistema.
        '''
        try:
            self.logger.info("addingFields - INIT")
            #json_in['DataDiIngestion'] = self.standardizeDate(str(datetime.now()))
            json_in['DataDiIngestion'] = datetime.now().strftime("%Y-%m-%dT%H:%M:%SZ")
            self.logger.info("addingFields - END")
            return json_in
        except Exception as e:
            self.logger.error("addingFields - ERROR: {}".format(str(e)))
    
    
    def addModifyDate(self, json_in, isList=False):
        ''' Metodo utilizzato per introdurre campi nuovi nel json. Prende in input sia una lista di json che un singolo
            json, differenziando il caso grazie al booleano isList di default settato a False.        
            In particolare viene introdotto il campo
            - Data di modifica
            Per maggiori informazioni, verificare la sezione 6.2.3 dell'Allegato D - Componenti di Sistema.
        '''
        try:
            self.logger.info("addModifyDate - INIT")
            if isList:
                self.logger.debug("Lista di json in input")
                for json_obj in json_in:
                    #json_obj['DataDiModifica'] = self.standardizeDate(str(datetime.now()))
                    json_obj['DataDiModifica'] = datetime.now().strftime("%Y-%m-%dT%H:%M:%SZ")
                self.logger.info("addModifyDate - END")
                return json_in
            else:
                #json_in['DataDiModifica'] = self.standardizeDate(str(datetime.now()))
                json_in['DataDiModifica'] = datetime.now().strftime("%Y-%m-%dT%H:%M:%SZ")
                self.logger.info("addModifyDate - END")
                return json_in
        except Exception as e:
            self.logger.info("addModifyDate - ERROR: {}".format(str(e)))
    
    def addRecordId(self, json_in, isList=False):
        ''' Metodo utilizzato per introdurre campi nuovi nel json. Prende in input sia una lista di json che un singolo
            json, differenziando il caso grazie al booleano isList di default settato a False.      
            In particolare, utilizzando una funzione presente sul db postgresql, viene introdotto il campo
            - Identificativo del record
            Per maggiori informazioni, verificare la sezione 6.2.3 dell'Allegato D - Componenti di Sistema.
        '''
        try:
            self.logger.info("addRecordId - INIT")
            if isList:
                self.logger.debug("Lista di json in input")
                for json_obj in json_in:
                    res = self.dbClient.executeQuery("SELECT * from {}.{}(%s);".format(self.config.db_schema, self.config.get_id_function), 
                        (self.config.app_name,), isSelect=True)
                    json_obj['Identificativo'] = res[0][0]
                self.logger.info("addRecordId - END")
                return json_in
            else:
                res = self.dbClient.executeQuery("SELECT * from {}.{}(%s);".format(self.config.db_schema, self.config.get_id_function), 
                    (self.config.app_name,), isSelect=True)
                json_in['Identificativo'] = res[0][0]
                return json_in
        except TypeError as te:
            self.logger.error("addRecordId - TypeError - {}".format(str(te)))
            raise
        except Exception as e:
            self.logger.error("addRecordId - ERROR: {}".format(str(e)))