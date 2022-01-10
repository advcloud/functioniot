#from typing import List
import logging

import azure.functions as func
import json
import os
import pyodbc
import time
db_connection_str = os.environ["SQLCONNSTR_SQLConnectionString"]

def update_table(payload):
    ## Get connection string
    # from Azure Functions' configuration
	#logging.info('start insert')
    temperature = payload['temperature']
    humidity    = payload['humidity']
    with pyodbc.connect(db_connection_str, autocommit=False) as db_connection:
        with db_connection.cursor() as db_cursor:	
            start_time = time.time()
            logging.info("insert start")
            try:
                sql ="INSERT INTO dbo.home_stat_temp VALUES ("+str(temperature)+","+ str(humidity)+");"
                #sql ="INSERT INTO dbo.home_stat_temp VALUES (34, 35);"
                #sql = ' insert into '+schema_name+'.'+table_name+' values '+ '(' +  str(temperature) + ',' + str(humidity) + ');'
                logging.info('execute the sql: %s',sql)
                db_cursor.execute(sql)
                #end_time = time.time()
                #logging.info(str(end_time-start_time))

                #db_cursor.executemany("insert into " + TABLE_NAME + " (DeviceId, MeasureTime, GeneratedPower, WindSpeed, TurbineSpeed) values (?,?,?,?,?)", data_table)
                #print("insert done")
                logging.info("insert done")
            except Exception as ex:
                #print(ex)
                logging.info("insert fail")
                logging.error(ex)
            finally:
                db_connection.commit()
    return True

def main(event: func.EventHubEvent):
    #schema_name = 'dbo',
    #table_name  = 'home_stat_temp'
    logging.info('Python EventHub trigger processed an event: %s', event.get_body().decode('utf-8'))

    # convert string to a list of dicts
    payload = json.loads(event.get_body().decode('utf-8'))

    # update payload to table
    update_result = update_table(payload)

    # log success/fail status
    if update_result: logging.info(f'Payload update successful!')
    else: logging.info(f'Payload update failed!')
