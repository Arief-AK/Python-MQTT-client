import json
import requests
import paho.mqtt.client as mqtt

broker = 'eu1.cloud.thethings.network'
port = 8883

username = 'project-software-engineering@ttn'
password = 'NNSXS.DTT4HTNBXEQDZ4QYU6SG73Q2OXCERCZ6574RVXI.CQE6IG6FYNJOO2MOFMXZVWZE4GXTCC2YXNQNFDLQL4APZMWU6ZGA'


# The callback for when the client receives a CONNACK response from the server.
def on_connect(client, userdata, flags, rc):
    print("Connected with result code " + str(rc))

    # Subscribing in on_connect() means that if we lose the connection and
    # reconnect then subscriptions will be renewed.
    client.subscribe("#")


# The callback for when a PUBLISH message is received from the server.
def on_message(client, userdata, msg):
    # print(msg.topic + " " + str(msg.payload))

    data = json.loads(msg.payload)

    end_device_id = data['end_device_ids']
    device_id = end_device_id['device_id']

    if "py" in device_id:
        print("This is a py device")

        temperature = data['uplink_message']['decoded_payload']['temperature']
        pressure = data['uplink_message']['decoded_payload']['pressure']
        light = data['uplink_message']['decoded_payload']['light']
        time_str = data['received_at']

        # Temporary variable to store time string
        temp = ""

        # Parse time string
        for character in time_str:
            if character == '.':
                break
            else:
                temp += character

        time_str = temp

        print("Device ID: ", device_id)
        print("Temperature: ", temperature)
        print("Light: ", light)
        print("Pressure: ", pressure)
        print("Time: ", time_str)

        url = "https://weatherinfopset-6db9.restdb.io/rest/weather-data-py"

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

    if "lht" in device_id:
        print("This is a lht device")


client = mqtt.Client()
client.tls_set()
client.on_connect = on_connect
client.on_message = on_message
client.username_pw_set(username, password)

client.connect(broker, int(port), 60)
client.loop_forever()
