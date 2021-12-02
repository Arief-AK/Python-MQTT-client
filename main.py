import json
import requests
import paho.mqtt.client as mqtt

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

# The callback when client receives a response from the server
def on_connect(client, userdata, flags, rc):
    if rc == 0:
        print("Successfully connected to MQTT broker")

        # reconnect then subscriptions will be renewed.
        client.subscribe("#")

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

# Parsing for the lht-modules
def parse_lht(data):
    tbp_lht = data

    end_device_id = tbp_lht['end_device_ids']
    device_id = end_device_id['device_id']

    temperature = data['uplink_message']['decoded_payload']['TempC_SHT']
    humidity = data['uplink_message']['decoded_payload']['Hum_SHT']
    light = data['uplink_message']['decoded_payload']['ILL_lx']
    time_str = data['received_at']

    time_str = parse_timestamp(time_str)

    print("\nDevice ID: ", device_id)
    print("Temperature: ", temperature)
    print("Humidity: ", humidity)
    print("Light: ", light)
    print("Timestamp:", time_str)

    push_database("weather-data-lht", device_id, temperature, "", light, humidity, time_str)

# Parsing for the py-modules
def parse_py(data):

    end_device_id = data['end_device_ids']
    device_id = end_device_id['device_id']

    temperature = data['uplink_message']['decoded_payload']['temperature']
    pressure = data['uplink_message']['decoded_payload']['pressure']
    light = data['uplink_message']['decoded_payload']['light']
    time_str = data['received_at']

    time_str = parse_timestamp(time_str)

    print("Device ID: ", device_id)
    print("Temperature: ", temperature)
    print("Light: ", light)
    print("Pressure: ", pressure)
    print("Time: ", time_str)

    push_database("weather-data-py", device_id, temperature, pressure, light, "", time_str)

# The callback for when a message is received from the server.
def on_message(client, userdata, msg):

    data = json.loads(msg.payload)

    end_device_id = data['end_device_ids']
    device_id = end_device_id['device_id']

    if "py" in device_id:
        parse_py(data)

    if "lht" in device_id:
        parse_lht(data)

client = mqtt.Client()
client.tls_set()
client.on_connect = on_connect
client.on_message = on_message
client.username_pw_set(username, password)

client.connect(broker, int(port), 60)
client.loop_forever()
