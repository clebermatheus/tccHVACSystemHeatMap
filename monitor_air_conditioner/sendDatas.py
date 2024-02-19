# -*- coding: utf-8 -*-
import paho.mqtt.client as mqtt    # Módulo MQTT
import time            # Módulo para trabalhar com tempo e conversões
import psycopg
import json            # Módulo Json
import sys
from math import log
from psycopg.rows import dict_row

from azure.iot.device.aio import IoTHubDeviceClient, ProvisioningDeviceClient
from azure.iot.device import Message, MethodResponse

# Configurações do Broker local
BROKER_ADDRESS = "localhost"  # Endereço do broker Mosquitto
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

def send_commands(connection, deviceId, actualPoints):
    try:
        address = ''
        command = ''
        up = ''
        down = ''
        stmt = connection.prepare("""SELECT DISTINCT
            arc.deviceid,
            arc.address,
            arcb.name,
            arcb.command
        FROM Rooms as r
            INNER JOIN ControlsxRooms as cr ON cr.roomid = r.roomid AND cr.status = 1
            INNER JOIN ac_remote_control as arc
                ON arc.remote_control_id = cr.remotecontrolid AND arc.status = 1
            INNER JOIN ac_remote_control_buttons as arcb
                ON arcb.status = 1 AND arcb.remote_control_id = arc.remote_control_id
        WHERE r.roomid = (SELECT roomId FROM DevicexRooms WHERE deviceId = $1) AND arcb.name IN ('UP', 'DOWN');""")
        with connection.xact():
            """
            for row in stmt(deviceId):
                address = row['address'].split('|')

                if row['name'] == 'UP':
                    up = row['command']
                elif row['name'] == 'DOWN':
                    down = row['command']

            if actualPoints > 500:
                command = down
            elif actualPoint < 500:
                command = up
            else
                command = ''

            client.publish("monitor/receive", { "address": address, "command": command })
            """
    except:
        print("Falhar ao enviar comandos para o ar-condicionado selecionado")

def send_mysql(connection, json):
    try:
        # Prepare data
        dispositiveId = json["d"]["Name"]
        hum = json["d"]["humidity"]
        lum = json["d"]["luminosity"]
        temp = json["d"]["temperature"]
        noise = json["d"]["noise"]
        diff = int(json["d"]["Diff_hour"])
        hour = time.strftime('%Y-%m-%d %H:%M:%S', time.gmtime(int(json["d"]["Hour"])))
        points =  calculate_pontuation(connection, dispositiveId, temp, hum)
        #points = -1

        connection.execute("INSERT INTO campusinteligente.air_conditioner (dispositiveid, temperature, humidity, luminosity, noise, diff_hour, hour, points) VALUES(%s, %s, %s, %s, %s, %s, %s, %s)", 
                (dispositiveId, temp, hum, lum, noise, diff, hour, points))
        print("Dados inseridos: dispId = "+str(dispositiveId)+", temp = "+str(temp)+", hum = "+str(hum)+", diff = "+str(diff)+", hour = "+str(hour) )
        connection.commit()
    except Exception as error:
        print("Falha ao inserir dados no banco de dados.", error)
    finally:
        # Encerra conexão com Mysql
        # connection.close()
        return 1

def calculate_pontuation(connection, deviceId, temperature, humidity):
    try:
        result = connection.execute("""SELECT
            r.temperature_ideal
        FROM campusinteligente.Rooms r
            INNER JOIN campusinteligente.DevicexRooms as dr ON dr.roomid = r.roomid
        WHERE dr.deviceid = %s;""", (deviceId, )).fetchone()['temperature_ideal']

        idealTemperature = result
        difference = idealTemperature - float(temperature)

        return (((temperature/idealTemperature)+(humidity/50))/2)*500
    except Exception as error:
        print("Falha ao obter temperatura ideal para o local.", error)
        return -999

def get_actual_points(connection, deviceId):
    try:
        sumPoints = 0
        result = connection.execute("""SELECT DISTINCT
            dr.deviceid,
            (SELECT ac.temperature FROM campusinteligente.air_conditioner as ac WHERE ac.dispositiveid = dr.deviceid ORDER BY ac.hour DESC LIMIT 1) AS value,
            (SELECT ac.points FROM campusinteligente.air_conditioner as ac WHERE ac.dispositiveid = dr.deviceid ORDER BY ac.hour DESC LIMIT 1) AS point
        FROM campusinteligente.Rooms as r
            INNER JOIN campusinteligente.DevicexRooms as dr ON dr.roomid = r.roomid AND dr.status = 1
        WHERE r.roomid = (SELECT roomId FROM campusinteligente.DevicexRooms WHERE deviceId = %s);""", (deviceId, ))
        for x in result:
            sumPoints += x["point"]

        return sumPoints/result.rowcount
    except Exception as error:
        print("Falhar ao obter pontuação atual", error)
        return 500

################################ FUNÇÕES MQTT ################################
# Define função de retorno de chamada ao conectar-se com o Broker.
def on_connect(client, userdata, flags, rc):
    print("Conectado ao broker.")
    # Inscreve-se no tópico para receber mensagens.
    client.subscribe("monitor/send")

# Define função de retorno de chamada ao receber mensagem.
def on_message(client, userdata, msg):
    connection = connect_mysql()

    if connection == 0:
        return 0
    # Converte mensagem em bytes para string
    msg_string=str(msg.payload.decode("utf-8","ignore"))
    print("Message received: "+str(msg_string))
    # Desserializa string Json para dicionário Python
    dict_json=json.loads(msg_string)
    send_mysql(connection, dict_json)
    connection.close()

# Define função de retorno de chamada após uma desconexão.
def on_disconnect(client, userdata, rc):
    if rc != 0:
        print("Desconexão MQTT Inesperada.")
    print("Reconectando-se ao Broker em 3 segundos...")
    time.sleep(3)
    client.connect("20.120.0.64", 1883, 60)

# Instancia cliente MQTT.
client = mqtt.Client()
client.on_connect = on_connect        # Define como callback a função on_connect
client.on_message = on_message        # Define como callback a função on_message
client.on_disconnect = on_disconnect    # Define como callback a função on_disconnect

# Inicia conexão MQTT com o Broker Mosquitto.
# client.connect("mqtt.eclipseprojects.io", 1883, 60)
client.connect("20.120.0.64", 1883, 60)
print("Connecting to broker...")

client.loop_forever()
