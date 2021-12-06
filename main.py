import base64
import json
import requests
import paho.mqtt.client as mqtt
import mysql.connector
from mysql.connector import Error

# API call information
# 1 hour -> 20 API calls
# 1 day -> 480 API calls
# 1 month -> 14,400 API calls

# BROKER INFORMATION
broker = 'eu1.cloud.thethings.network'
port = 8883

# BROKER CREDENTIALS
username = 'project-software-engineering@ttn'
password = 'NNSXS.DTT4HTNBXEQDZ4QYU6SG73Q2OXCERCZ6574RVXI.CQE6IG6FYNJOO2MOFMXZVWZE4GXTCC2YXNQNFDLQL4APZMWU6ZGA'

class LhtSensor:
    def __init__(self, device_id, data):
        self.device_id = device_id
        self.data = data

    # Parse lht-data using raw payload
    def parse_lht_raw_payload(self, data):
        raw_payload = data['uplink_message']['frm_payload']

        b_payload = raw_payload.encode('ascii')
        decoded_b64payload = base64.b64decode(b_payload)

        # Battery status
        value = (decoded_b64payload[0] << 8 | decoded_b64payload[1]) & 0x3FFF
        bat_v = value / 1000

        # Temperature in degrees
        value = decoded_b64payload[2] << 8 | decoded_b64payload[3]
        if decoded_b64payload[2] & 0x80:
            value |= 0xFFFF0000

        temp_sht = (value / 100)

        # Humidity in percentage
        value = decoded_b64payload[4] << 8 | decoded_b64payload[5]
        hum_sht = (value / 10)

        return temp_sht, hum_sht, bat_v

    # Parse lht-data using decoded payload
    def parse_lht_decoded_payload(self, data):
        temperature = data['uplink_message']['decoded_payload']['TempC_SHT']
        humidity = data['uplink_message']['decoded_payload']['Hum_SHT']
        light = data['uplink_message']['decoded_payload']['ILL_lx']
        battery_p = data['uplink_message']['decoded_payload']['BatV']
        time_str = data['received_at']

        longitude = data['uplink_message']['rx_metadata'][0]['location']['longitude']
        latitude = data['uplink_message']['rx_metadata'][0]['location']['latitude']
        gateway_id = data['uplink_message']['rx_metadata'][0]['gateway_ids']['gateway_id']

        time_str = parse_timestamp(time_str)

        return temperature, humidity, light, battery_p, time_str, latitude, longitude, gateway_id

    # Parsing for the lht-modules
    def parse_lht(self):
        tbp_lht = self.data

        end_device_id = tbp_lht['end_device_ids']
        device_id = end_device_id['device_id']

        # Parse with raw payload
        raw_temp, raw_hum, raw_bat_v = self.parse_lht_raw_payload(self.data)

        # Parse with decoded payload
        temperature, humidity, light, battery_p, time_str, latitude, longitude, gateway_id \
            = self.parse_lht_decoded_payload(self.data)

        print("\nRaw payload:")

        print("\nDevice ID: ", device_id)
        print("Temperature: ", raw_temp)
        print("Humidity: ", raw_hum)
        print("Timestamp:", time_str)
        print("Gateway ID: ", gateway_id)
        print("Battery percentage: ", raw_bat_v, "%")

        print("\nDecoded payload:")

        print("\nDevice ID: ", device_id)
        print("Temperature: ", temperature)
        print("Humidity: ", humidity)
        print("Light: ", light)
        print("Timestamp:", time_str)
        print("Latitude: ", latitude)
        print("Longitude: ", longitude)
        print("Gateway ID: ", gateway_id)
        print("Battery percentage: ", battery_p, "%")

        # Store to local database
        send_to_local_database(local_db_client, device_id, temperature, "", light, humidity, time_str)

        # Store to the web-database
        push_database("weather-data-lht", device_id, temperature, "", light, humidity, time_str)

class PySensor:
    def __init__(self, device_id, data):
        self.device_id = device_id
        self.data = data

    # Parsing for the py-modules
    def parse_py(self):
        end_device_id = self.data['end_device_ids']
        device_id = end_device_id['device_id']

        temperature = self.data['uplink_message']['decoded_payload']['temperature']
        pressure = self.data['uplink_message']['decoded_payload']['pressure']
        light = self.data['uplink_message']['decoded_payload']['light']
        time_str = self.data['received_at']

        time_str = parse_timestamp(time_str)

        print("Device ID: ", device_id)
        print("Temperature: ", temperature)
        print("Light: ", light)
        print("Pressure: ", pressure)
        print("Time: ", time_str)

        # Store to local database
        send_to_local_database(local_db_client, device_id, temperature, pressure, light, "", time_str)

        # Store to web-database
        push_database("weather-data-py", device_id, temperature, pressure, light, "", time_str)

# HELPER FUNCTIONS:
def parse_timestamp(time_str):
    # Temporary variable to store time string
    temp = ""

    # Parsing: remove 'T' character
    for character in time_str:
        if character != 'T':
            temp += character
        else:
            temp += ' '

    # Bookkeeping
    time_str = temp
    temp = ""

    # Parsing: remove '.' character
    for character in time_str:
        if character == '.':
            break
        else:
            temp += character

    return temp

# The callback when client receives a response from the server
def on_connect(client, userdata, flags, rc):
    if rc == 0:
        print("Successfully connected to MQTT broker")

        # reconnect then subscriptions will be renewed.
        client.subscribe("#")

# Function to initialise connection to the local database
def initialise_local_connection():
    mydb = mysql.connector.connect(
        host='192.168.68.108',
        user='arief',
        password='SaxionTest',
        database='PSET_test_db',
        port='8455'
    )

    return mydb

# Function to send data to the local database
def send_to_local_database(db_connector, name, temp, press, light, humidity, time):
    db_cursor = db_connector.cursor()

    query = "DEFAULT"
    args = ()

    if 'py' in name:
        query = "INSERT INTO PyData(device_name,temperature,pressure,light,timestamp) values (%s,%s,%s,%s,%s)"
        args = (name, temp, press, light, time)

    if 'lht' in name:
        query = "INSERT INTO LhtData(device_name,temperature,humidity,light,timestamp) values (%s,%s,%s,%s,%s)"
        args = (name, temp, humidity, light, time)

    try:

        db_cursor.execute(query, args)
        db_connector.commit()

        print("Successfully updated the database")

    except Error as error:
        print(error)

    finally:
        db_cursor.close()

# Storing to the web database using an API call
def push_database(url_id, device_id, temperature, pressure, light, humidity, time_str):

    url = "https://weatherinfopset-6db9.restdb.io/rest/" + url_id

    if "py" in url_id:
        payload = json.dumps(
            {"device_name": device_id, "temperature": temperature, "pressure": pressure, "light": light,
             "timestamp": time_str})
        headers = {
            'content-type': "application/json",
            'x-apikey': "93af08f6121ad55e76feef3fc1ae242eb2673",
            'cache-control': "no-cache"
        }

        response = requests.request("POST", url, data=payload, headers=headers)

        print(response.text)

    if "lht" in url_id:
        payload = json.dumps(
            {"device_name": device_id, "temperature": temperature, "humidity": humidity, "light": light,
             "timestamp": time_str})
        headers = {
            'content-type': "application/json",
            'x-apikey': "93af08f6121ad55e76feef3fc1ae242eb2673",
            'cache-control': "no-cache"
        }

        response = requests.request("POST", url, data=payload, headers=headers)

        print(response.text)

# The callback for when a message is received from the server.
def on_message(client, userdata, msg):

    data = json.loads(msg.payload)

    end_device_id = data['end_device_ids']
    device_id = end_device_id['device_id']

    if "py" in device_id:
        py_sensor = PySensor(device_id,data)
        py_sensor.parse_py()

    if "lht" in device_id:

        lht_sensor = LhtSensor(device_id, data)
        lht_sensor.parse_lht()

local_db_client = initialise_local_connection()
client = mqtt.Client()
client.tls_set()
client.on_connect = on_connect
client.on_message = on_message
client.username_pw_set(username, password)

client.connect(broker, int(port), 60)
client.loop_forever()
