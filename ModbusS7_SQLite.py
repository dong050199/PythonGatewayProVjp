import sqlite3
from sqlite3 import Error
from pymodbus.client.sync import ModbusTcpClient
import threading
import snap7.client as c
from snap7.util import *
from snap7.types import *
import time
import json
import paho.mqtt.client as paho
from bson.json_util import dumps
import datetime

######################[READ ALL VALUE JUST INPUT STRING]########################
def Read_All_FromString(plc, add: str):
    if (add.find("DB") != -1):
        return DB_Value_FromAdd(plc,add)
    else:
        return Memory_Value_FromAdd(plc,add)

############################[READ MEMORY S7 SIEMENS]############################
def Memory_Value_FromAdd(plc, add: str):
    if(add.find("M") != -1 and add.find("D") == -1 and add.find("W") == -1):
        datatype  = S7WLBit
        Value = add.replace("M","")
        ValueByte = int(Value.split(".")[0])
        ValueBit = int(Value.split(".")[1])
        result = plc.read_area(areas['MK'], 0, ValueByte, datatype)
        return get_bool(result, 0, ValueBit)
    if(add.find("MD") != -1):
        ValueByte = int(add.replace("MD", ""))
        result = plc.read_area(areas['MK'], 0, ValueByte, S7WLReal)
        return get_real(result, 0)
    if(add.find("MW")!= -1):
        datatype  = S7WLWord
        ValueByte = int(add.replace("MW", ""))
        result = plc.read_area(areas['MK'], 0, ValueByte, datatype)
        return get_int(result, 0)

#############################[READ DATABLOCK S7 SIEMENS]#############################
def DB_Value_FromAdd(plc,add: str):
    data = add.split('.')
    dbnum = int(data[0].replace("DB",""))
    if(add.find("DBD") != -1):
        offset = int(data[1].replace("DBD",""))
        value = get_real(plc.db_read(dbnum,offset,4),0)
    if(add.find("DBX") != -1):
        offsetbf = data[1].replace("DBX", "")
        offset = offsetbf.split(".")
        offset0 = int(offset[0])
        offset1 = int(data[2])
        value = get_bool(plc.db_read(dbnum,offset0,1),0,offset1)
    if(add.find("DBW") != -1):
        offset = int(data[1].replace("DBW",""))
        value = get_int(plc.db_read(dbnum,offset,2),0)
    return value

database = r"F:\SQLite_data\Modbus.db"

def create_connection(db_file):
    conn = None
    try:
        conn = sqlite3.connect(db_file)
        return conn
    except Error as e:
        print(e)
    return conn

def create_table(conn, create_table_sql):
    try:
        c = conn.cursor()
        c.execute(create_table_sql)
    except Error as e:
        print(e)

def delete_all(table,conn):
    sql = f'DELETE FROM {table}'
    cur = conn.cursor()
    cur.execute(sql)
    conn.commit()

def count_row(table,conn):
    sql = f'SELECT COUNT(*) FROM {table}'
    cur = conn.execute(sql)
    row = cur.fetchone()
    row, = row
    return row

def insert_s7com(id, ip, rack, slot, db):
    db.execute(f"INSERT INTO s7com VALUES ({id},'{ip}',{rack},{slot})")

def insert_s7comtag(id, id_dev, name, add, db):
    db.execute(f"INSERT INTO s7comtag VALUES ({id},{id_dev},'{name}','{add}')")

def insert_s7commqtt(id, ip, port, user, pwd, clid, id_dev, topic, db):
    db.execute(f"INSERT INTO s7commqtt VALUES ({id},'{ip}',{port},'{user}','{pwd}','{clid}','{id_dev}','{topic}')")

def insert_s7comstore(id,message, db):
    db.execute(f"INSERT INTO s7comstore VALUES ({id},'{message}')")

def insert_modbus(id, ip, port, db):
    db.execute(f"INSERT INTO modbus VALUES ({id},'{ip}',{port})")

def insert_modbustag(id, id_dev, r_start, r_num, db):
    db.execute(f"INSERT INTO modbustag VALUES ({id},{id_dev},{r_start},{r_num})")

def insert_modbusmqtt(id, ip, port, user, pwd, clid, id_dev, topic, db):
    db.execute(f"INSERT INTO modbusmqtt VALUES ({id},'{ip}',{port},'{user}','{pwd}','{clid}','{id_dev}','{topic}')")

def insert_modbusstore(id,message, db):
    db.execute(f"INSERT INTO modbusstore VALUES ({id},'{message}')")

def on_publish(client, userdata, result):
    pass

#################################[GET DATABASE SERVER]##############################
def get_database_server():
    from pymongo import MongoClient
    import pymongo
    CONNECTION_STRING = "mongodb://mongoAdmin:ttsxtm@128.199.124.231:27017/Becanatomy?authSource=admin&readPreference=primary&appname=MongoDB%20Compass&directConnection=true&ssl=false"
    from pymongo import MongoClient
    client = MongoClient(CONNECTION_STRING)
    return client['DONG_TEST']

##############################[COUNT DOCUMENT FROM DB]##############################
def count_document(intem_d):
    itemcount = 0
    for item in intem_d:
        # This does not give a very readable output
        itemcount = itemcount + 1
    return itemcount

def start_up_s7com():
    #############################[CREATE table s7com]###########################
    sql_create_s7com_table = """  CREATE TABLE IF NOT EXISTS s7com (
                                        id integer,
                                        ip text,
                                        rack integer,
                                        slot integer
                                    ); """

    sql_create_s7comtag_table = """  CREATE TABLE IF NOT EXISTS s7comtag (
                                        id integer,
                                        id_dev integer,
                                        name text,
                                        addr text
                                    ); """

    ############################[CREATE table s7commqtt]#########################
    sql_create_s7commqtt_table = """  CREATE TABLE IF NOT EXISTS s7commqtt (
                                        id integer,
                                        ip text,
                                        port integer,
                                        user text,
                                        pwd text,
                                        clid text,
                                        id_dev text,
                                        topic text
                                    ); """

    ###########################[CREATE table s7comstore]##########################
    sql_create_s7comstore_table = """  CREATE TABLE IF NOT EXISTS s7comstore (
                                        id integer,
                                        message text
                                    ); """
    ###################################[CREATE DB]##################################
    conn = create_connection(database)
    if conn is not None:
        create_table(conn, sql_create_s7com_table)
        create_table(conn, sql_create_s7comtag_table)
        create_table(conn, sql_create_s7commqtt_table)
        create_table(conn, sql_create_s7comstore_table)
    else:
        print("Error! cannot create the database connection.")

def start_up_modbus():
    #############################[CREATE table modbus]###########################
    sql_create_modbus_table = """  CREATE TABLE IF NOT EXISTS modbus (
                                        id integer,
                                        ip text,
                                        port integer
                                    ); """

    ############################[CREATE table modbustag]##########################
    sql_create_modbustag_table = """  CREATE TABLE IF NOT EXISTS modbustag (
                                        id integer,
                                        id_dev integer,
                                        r_start integer,
                                        r_num integer
                                    ); """

    ############################[CREATE table modbusmqtt]#########################
    sql_create_modbusmqtt_table = """  CREATE TABLE IF NOT EXISTS modbusmqtt (
                                        id integer,
                                        ip text,
                                        port integer,
                                        user text,
                                        pwd text,
                                        clid text,
                                        id_dev text,
                                        topic text
                                    ); """

    ###########################[CREATE table modbusstore]##########################
    sql_create_modbusstore_table = """  CREATE TABLE IF NOT EXISTS modbusstore (
                                        id integer,
                                        message text
                                    ); """
    ###################################[CREATE DB]##################################
    conn = create_connection(database)
    if conn is not None:
        create_table(conn, sql_create_modbus_table)
        create_table(conn, sql_create_modbustag_table)
        create_table(conn, sql_create_modbusmqtt_table)
        create_table(conn, sql_create_modbusstore_table)
    else:
        print("Error! cannot create the database connection.")

def Check_S7update():
    while True:
        try:
            S7comDB = sqlite3.connect(database)
            cur = S7comDB.cursor()

            #################################################[   READ DEVICE IN4   ]####################################
            CHECKDBname = get_database_server()
            CHECKDBcollection_name = CHECKDBname["CHECK_S7COM"]
            CHECKDBItem_detail = CHECKDBcollection_name.find()
            MB = 0
            for item in CHECKDBItem_detail:
                MB += 1
            print("*******************************[ Waiting Sync S7 Command ]********************************************")
            if (MB == 1):
                print("****************************[ UPDATE DB FROM SERVER TO LOCAL ]*************************")
                ###########################################[   SYNC MODBUS   ]##########################################
                s7comDBname = get_database_server()
                s7comDBcollection_name = s7comDBname["S7COM"]
                s7comDBItem_detail = s7comDBcollection_name.find()
                print("***************************[ SYNC S7C0M DB SERVER TO LOCAL ]**************************")
                delete_all("s7com", S7comDB)
                for item in s7comDBItem_detail:
                    print(item)
                    data = json.loads(dumps(item))
                    insert_s7com(data["id"], str(data["ip"]), data["rack"],data["slot"], cur)
                    S7comDB.commit()
                    time.sleep(1)
                time.sleep(1)

                ###########################################[   SYNC MODBUS   ]##########################################
                s7comDBname = get_database_server()
                s7comDBcollection_name = s7comDBname["S7COM_TAG"]
                s7comDBItem_detail = s7comDBcollection_name.find()
                print("***************************[ SYNC S7COM_TAG DB SERVER TO LOCAL ]**************************")
                delete_all("s7comtag", S7comDB)
                for item in s7comDBItem_detail:
                    print(item)
                    data = json.loads(dumps(item))
                    insert_s7comtag(int(data["id"]), int(data["id_dev"]), str(data["name"]),str(data["add"]), cur)
                    S7comDB.commit()
                    time.sleep(1)
                time.sleep(1)

                ###########################################[   SYNC MODBUS   ]##########################################
                s7comDBname = get_database_server()
                s7comDBcollection_name = s7comDBname["MODBUS_MQTT"]
                s7comDBItem_detail = s7comDBcollection_name.find()
                print("***************************[ SYNC S7COM_MQTT DB SERVER TO LOCAL ]**************************")
                delete_all("s7commqtt", S7comDB)
                for item in s7comDBItem_detail:
                    print(item)
                    data = json.loads(dumps(item))
                    insert_s7commqtt(int(data["id"]), str(data["ip"]), int(data["port"]), str(data["user"]),str(data["pwd"]), str(data["clid"]), str(data["id_dev"]), str(data["topic"]),cur)
                    S7comDB.commit()
                    time.sleep(1)
                time.sleep(1)

                print("*****************************************[ SYNC DONE! ]***********************************")
                CHECKDBcollection_name.delete_many({})
            time.sleep(15)

        except Exception as e:
            print("Error connect command" + e)
            continue

def Check_MODBUSupdate():
    while True:
        try:
            ModbusDB = sqlite3.connect(database)
            cur = ModbusDB.cursor()

            #################################################[   READ DEVICE IN4   ]####################################
            CHECKDBname = get_database_server()
            CHECKDBcollection_name = CHECKDBname["CHECK_MODBUS"]
            CHECKDBItem_detail = CHECKDBcollection_name.find()
            CHECKDB_numdoc = count_document(CHECKDBItem_detail)
            CHECKDBItem_detail = CHECKDBcollection_name.find()
            MB = 0
            for item in CHECKDBItem_detail:
                MB += 1
            print("*******************************[ Waiting Sync Modbus Command ]********************************************")
            if (MB == 1):
                print("****************************[ UPDATE DB FROM SERVER TO LOCAL ]*************************")
                ###########################################[   SYNC MODBUS   ]##########################################
                modbusDBname = get_database_server()
                modbusDBcollection_name = modbusDBname["MODBUS"]
                modbusDBItem_detail = modbusDBcollection_name.find()
                print("***************************[ SYNC MODBUS DB SERVER TO LOCAL ]**************************")
                delete_all("modbus", ModbusDB)
                for item in modbusDBItem_detail:
                    print(item)
                    data = json.loads(dumps(item))
                    print(data["id"])

                    insert_modbus(data["id"], str(data["ip"]), data["port"], cur)
                    ModbusDB.commit()
                    time.sleep(1)
                time.sleep(1)

                ###########################################[   SYNC MODBUS TAG  ]#######################################
                modbusDBname = get_database_server()
                modbusDBcollection_name = modbusDBname["MODBUS_TAG"]
                modbusDBItem_detail = modbusDBcollection_name.find()
                print("***************************[ SYNC MODBUS TAG DB SERVER TO LOCAL ]**************************")
                delete_all("modbustag", ModbusDB)
                for item in modbusDBItem_detail:
                    print(item)
                    data = json.loads(dumps(item))
                    insert_modbustag(data["id"], data["id_dev"], data["r_start"], data["r_num"], cur)
                    ModbusDB.commit()
                    time.sleep(1)
                time.sleep(1)

                #########################################[   SYNC MODBUS MQTT   ]#######################################
                modbusDBname = get_database_server()
                modbusDBcollection_name = modbusDBname["MODBUS_MQTT"]
                modbusDBItem_detail = modbusDBcollection_name.find()
                print("***************************[ SYNC MODBUS MQTT SERVER TO LOCAL ]**************************")
                delete_all("modbusmqtt", ModbusDB)
                for item in modbusDBItem_detail:
                    print(item)
                    data = json.loads(dumps(item))
                    insert_modbusmqtt(data["id"], str(data["ip"]), data["port"], str(data["user"]),
                                      str(data["pwd"]), str(data["clid"]), str(data["id_dev"]), str(data["topic"]),
                                      cur)
                    ModbusDB.commit()
                    time.sleep(1)
                time.sleep(1)

                print("*****************************************[ SYNC DONE! ]***********************************")
                CHECKDBcollection_name.delete_many({})
            time.sleep(15)

        except Exception as e:
            print("Error connect command")
            continue

def mainModbus():
    start_up_modbus()
    ModbusDB = sqlite3.connect(database)
    cur = ModbusDB.cursor()
    ModbusDB.commit()
    messid = 0
    while True:
        try:
            ####################################[   MODBUS READ DB FOR CONFIGURATION   ]################################
            MQTT_payload = "{"
            numdev = 0
            for modbus_row in cur.execute('SELECT * FROM modbus ORDER BY id'):
                id, ip, port = modbus_row
                numdev += 1
                cur = ModbusDB.cursor()
                for modbustag_row in cur.execute(f'SELECT * FROM modbustag WHERE (id_dev = {numdev} ) ORDER BY id'):
                    idx, id_dev, r_strat, r_num = modbustag_row

                    clientmb = ModbusTcpClient(str(ip))
                    connection = clientmb.connect()
                    #print("Connected" + str(connection))
                    UNIT = 0X1
                    requets = clientmb.read_holding_registers(r_strat, r_num, unit=UNIT)
                    numr = r_num
                    clientmb.close()
                    while (numr > 0):
                        MQTT_payload += f"\"ID_{id_dev}_R_{numr}\":{requets.registers[numr - 1]},"
                        numr = numr - 1
            ################################[   MODBUS READ DB FOR CONFIGURATION MQTT  ]################################
            try:
                cur = ModbusDB.cursor()
                for modbusmqtt_row in cur.execute('SELECT * FROM modbusmqtt ORDER BY id'):
                    idmqtt, ipmqtt, portmqtt, user, pwd, clid, devid, topic = modbusmqtt_row
                    client = paho.Client(str(clid))
                    client.on_publish = on_publish
                    client.username_pw_set(str(user), str(pwd))

                    MQTT_payload += f"Devide_id:\"{devid}\""
                    MQTT_payload += "}"
                    payload = "{\"ID\":1,\"TimeStamp\":" + "\"" + str(datetime.datetime.now().strftime(
                        '%Y-%m-%dT%H:%M:%SZ')) + "\"" + "," + "\"Data\":[" + MQTT_payload + "]}"
                    client.connect(str(ipmqtt), portmqtt, keepalive=10)
                    cur = ModbusDB.cursor()
                    ###################################[   FORWARD FROM DATABASE  ]#####################################
                    if (count_row("modbusstore", cur) > 0):
                        cur = ModbusDB.cursor()
                        for modbusstore_row in cur.execute('SELECT * FROM modbusstore ORDER BY id'):
                            idstore, payloadstore = modbusstore_row
                            print(payloadstore)
                            client.publish(str(topic), payloadstore)
                            print("message forwarding")
                        delete_all("modbusstore", ModbusDB)
                        ModbusDB.commit()
                    client.publish(str(topic), payload)
                    print("message published")
                    print(payload)
                    messid = 0
                time.sleep(2)
                #######################################[   STORE FROM DATABASE  ]#######################################
            except Exception as e:
                messid += 1
                dataa = f"{messid}, '{str(payload)}'"
                insert_modbusstore(messid, f'{str(payload)}', cur)
                ModbusDB.commit()
                print("storing message")
                print(payload)
                print(messid)
                ########################[   DELETE ALL MESSAGE IF LARGE 100000 MESSSGA STORE  ]#########################
                if(messid > 100000):
                    delete_all("modbusstore",ModbusDB)
            time.sleep(2)
        except Exception as e:
            print(e)
            continue


def mainS7():
    start_up_s7com()
    S7comDB = sqlite3.connect(database)
    cur = S7comDB.cursor()
    S7comDB.commit()
    messid = 0
    while True:
        try:
            MQTT_payload = "{"
            numdev = 0
            ####################################[   MODBUS READ DB FOR CONFIGURATION   ]################################
            cur = S7comDB.cursor()
            for s7comrow in cur.execute('SELECT * FROM s7com ORDER BY id'):
                s7comid, s7comip, s7comrack, s7comslot = s7comrow
                numdev += 1
                plc = c.Client()
                plc.connect(str(s7comip),int(s7comrack),int(s7comslot))
                cur = S7comDB.cursor()
                for s7tagrow in cur.execute(f'SELECT * FROM s7comtag WHERE (id_dev = {numdev} ) ORDER BY id'):
                    s7tag_id, s7tag_iddev, s7tag_name, s7tag_add = s7tagrow
                    MQTT_payload += f"\"ID_{s7tag_iddev}_{s7tag_name}\" : {Read_All_FromString(plc,str(s7tag_add))},"
                ################################[   MODBUS READ DB FOR CONFIGURATION MQTT  ]################################
            try:
                cur = S7comDB.cursor()
                for modbusmqtt_row in cur.execute('SELECT * FROM s7commqtt ORDER BY id'):
                    idmqtt, ipmqtt, portmqtt, user, pwd, clid, devid, topic = modbusmqtt_row
                    client = paho.Client(str(clid))
                    client.on_publish = on_publish
                    client.username_pw_set(str(user), str(pwd))

                    MQTT_payload += f"Devide_id:\"{devid}\""
                    MQTT_payload += "}"
                    payload = "{\"ID\":1,\"TimeStamp\":" + "\"" + str(datetime.datetime.now().strftime(
                        '%Y-%m-%dT%H:%M:%SZ')) + "\"" + "," + "\"Data\":[" + MQTT_payload + "]}"
                    client.connect(str(ipmqtt), portmqtt, keepalive=10)
                    cur = S7comDB.cursor()
                    ###################################[   FORWARD FROM DATABASE  ]#####################################
                    if (count_row("s7comstore", cur) > 0):
                        cur = S7comDB.cursor()
                        for modbusstore_row in cur.execute('SELECT * FROM s7comstore ORDER BY id'):
                            idstore, payloadstore = modbusstore_row
                            print(payloadstore)
                            client.publish(str(topic), payloadstore)
                            print("message forwarding")
                        delete_all("s7comstore", S7comDB)
                        S7comDB.commit()
                    client.publish(str(topic), payload)
                    print("message published")
                    print(payload)
                    messid = 0
                time.sleep(2)
                #######################################[   STORE FROM DATABASE  ]#######################################
            except Exception as e:
                messid += 1
                insert_s7comstore(messid, f'{str(payload)}', cur)
                S7comDB.commit()
                print("storing message")
                print(payload)
                ########################[   DELETE ALL MESSAGE IF LARGE 100000 MESSSGA STORE  ]#########################
                if (messid > 100000):
                    delete_all("s7comstore", S7comDB)
            time.sleep(2)

        except Exception as e:
            print(e)

if __name__ == '__main__':
    ####################################################[   MAIN FUNCTION  ]############################################
    thread1 = threading.Thread(target=mainS7)
    thread1.start()
    thread2 = threading.Thread(target=mainModbus)
    thread2.start()
    thread3 = threading.Thread(target=Check_MODBUSupdate)
    thread3.start()
    thread4 = threading.Thread(target=Check_S7update)
    thread4.start()
