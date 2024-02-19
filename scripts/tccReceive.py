# -*- coding: utf-8 -*-
import paho.mqtt.client as mqtt    # Módulo MQTT
import time            # Módulo para trabalhar com tempo e conversões
import threading
import json            # Módulo Json
import psycopg
from sys import exit
from math import log
from psycopg.rows import dict_row

from azure.iot.device.aio import IoTHubDeviceClient, ProvisioningDeviceClient
from azure.iot.device import Message, MethodResponse

# Configurações do Broker local
BROKER_ADDRESS = "20.120.0.64"  # Endereço do broker Mosquitto
BROKER_PORT = 1883  # Porta padrão para MQTT
BROKER_TIMEOUT = 60  # Timeout em segundos

################################ FUNÇÕES MYSQL ###############################
# Inicia conexão com o Banco de Dados Mysql
def connect_mysql():
    try:
        return psycopg.connect('postgresql://cleber:wkhOH0Bi£07;+ZA2@c-campusdbcluster.m6a2vvrofppvhg.postgres.cosmos.azure.com:5432/citus', row_factory=dict_row)
        #return postgresql.open('pq://cleber:wkhOH0Bi£07;+ZA2@c-campusdbcluster.m6a2vvrofppvhg.postgres.cosmos.azure.com:5432/citus')
    except Exception as error:
        print("Não foi possível conectar-se ao banco de dados.", error)
        sys.stdout.flush()
        return 0

def send_commands(connection, roomId, actualPoints):
    try:
        if actualPoints == -999:
            return

        address = ''
        command = ''
        up = ''
        down = ''
        stmt = connection.execute("""SELECT DISTINCT
            r.roomId,
            arc.deviceid,
            arc.address,
            arcb.name,
            arcb.command
        FROM campusinteligente.Rooms as r
            INNER JOIN campusinteligente.ControlsxRooms as cr ON cr.roomid = r.roomid AND cr.status = 1
            INNER JOIN campusinteligente.ac_remote_control as arc
                ON arc.remote_control_id = cr.remotecontrolid AND arc.status = 1
            INNER JOIN campusinteligente.ac_remote_control_buttons as arcb
                ON arcb.status = 1 AND arcb.remote_control_id = arc.remote_control_id
        WHERE r.roomid = %s AND arcb.name IN ('UP', 'DOWN');""", (roomId, ))
        for row in stmt:
            address = row['address'].split('|')

            if row['name'] == 'UP':
                up = row['command'].split('|')
            elif row['name'] == 'DOWN':
                down = row['command'].split('|')

        if actualPoints > 500:
            command = down
            address = 'down'
        elif actualPoints < 500:
            command = up
        else:
            command = ''

        client.publish("monitor/receive", json.dumps({ "address": command, "command": address }))
        save_commands_sended(connection, roomId, "monitor/receive", command, address)
    except Exception as error:
        print("Falhar ao enviar comandos para o ar-condicionado selecionado", error)

def get_actual_data_from_room(connection, roomId):
    try:
        return connection.execute("""select
            ac.dispositiveid as deviceId, hour, COALESCE(temperature, -99) AS temperature, COALESCE(humidity, 0) AS humidity,
            COALESCE(luminosity, 0) AS luminosity, COALESCE(noise, 0) AS noise, points
        from campusinteligente.air_conditioner as ac
            inner join campusinteligente.devicexrooms as d on d.deviceid = ac.dispositiveid
            inner join (
                    select
                        max(id) as id, dispositiveid
                    from campusinteligente.air_conditioner as ac
                    group by dispositiveid
            ) as ds on ds.id = ac.id
        where d.roomid = %s;""", (roomId, ))
    except Exception as error:
        print("Falhar ao obter dados atual", error)
        return []

def get_commands(connection, roomId):
    try:
        commands = {}
        result = connection.execute("""SELECT DISTINCT
            arc.deviceid,
            arc.address,
            arcb.name,
            arcb.command
        FROM campusinteligente.Rooms as r
            INNER JOIN campusinteligente.ControlsxRooms as cr ON cr.roomid = r.roomid AND cr.status = 1
            INNER JOIN campusinteligente.ac_remote_control as arc
                ON arc.remote_control_id = cr.remotecontrolid AND arc.status = 1
            INNER JOIN campusinteligente.ac_remote_control_buttons as arcb
                ON arcb.status = 1 AND arcb.remote_control_id = arc.remote_control_id
        WHERE r.roomid = %s;""", (roomId, ))
        for command in result:
            commands[command["name"]] = command["command"].split('|')

        return commands
    except Exception as error:
        print("Falha ao obter commandos", error)
        return []

def get_rooms(connection):
    try:
        return connection.execute("""SELECT DISTINCT
            r.roomid, r.name, r.temperature_ideal
        FROM campusinteligente.rooms as r
        WHERE r.status = 1;""")
    except Exception as error:
        print("Falha ao obter locais ativos", error)
        return []

def save_commands_sended(connection, roomId, mqttUrl, address, command):
    try:
        connection.execute("""INSERT INTO campusinteligente.ac_log (roomId, date_create, mqtturl, address, command)
                VALUES (%s, NOW(), %s, %s, %s)""",
                (roomId, mqttUrl, address, command))
        connection.commit()
    except Exception as error:
        print("Falha ao salvar comandos enviados!", error)

################################ FUNÇÕES MQTT ################################
# Define função de retorno de chamada ao conectar-se com o Broker.
def on_connect(client, userdata, flags, rc):
    print("Conectado ao broker.")
    connection = connect_mysql()
    if connection == 0:
        return 0

    while(True):
        for room in get_rooms(connection):
            total = 0
            totalHasLight = 0
            totalHasNoise = 0
            commands = get_commands(connection, room["roomid"])
            for data in get_actual_data_from_room(connection, room["roomid"]):
                total+=1
                if data["luminosity"] > 25:
                    totalHasLight+=1
                if data["noise"] > 40:
                    totalHasNoise+=1
                send_commands(connection, room["roomid"], data["points"])
            if total > 0:
                percHasLight = totalHasLight/total
                percHasNoise = totalHasNoise/total
                if percHasLight > 0.5 and percHasNoise > 0.5:
                    status = client.publish("monior/receive", json.dumps({ "command": 'on', "address": commands["ON"] }))
                    if status[0] == 0:
                        save_commands_sended(connection, room['roomid'], "monitor/receive", commands["ON"], 'on')
                elif percHasLight < 0.5 and percHasNoise < 0.5:
                    status = client.publish("monior/receive", json.dumps({ "command": 'off', "address": commands["OFF"] }))
                    if status[0] == 0:
                        save_commands_sended(connection, room['roomid'], "monitor/receive", commands["OFF"], 'off')
        time.sleep(600)

# Define função de retorno de chamada após uma desconexão.
def on_disconnect(client, userdata, rc):
    if rc != 0:
        print("Desconexão MQTT Inesperada.")
    print("Reconectando-se ao Broker em 3 segundos...")
    time.sleep(3)
    client.connect(BROKER_ADDRESS, 1883, 60)

# Instancia cliente MQTT.
client = mqtt.Client()
client.on_connect = on_connect        # Define como callback a função on_connect
client.on_disconnect = on_disconnect    # Define como callback a função on_disconnect

# Inicia conexão MQTT com o Broker Mosquitto.
# client.connect("mqtt.eclipseprojects.io", 1883, 60)
client.connect(BROKER_ADDRESS, 1883, 60)
print("Connecting to broker...")

client.loop_forever()
