import paho.mqtt.client as mqtt
import datetime
import sqlite3

setting = open("settings.txt", "r")
settingtext=(setting.read())

f1,f2,f3,f4 = settingtext.split(",")
f2 = int (f2)
setting.close()

server_ip =f1
server_port = f2
topic1 = f3
client = f4

def on_connect(client, userdata, flags, rc):  # The callback for when the client connects to the broker
    print("Connected to Server!".format(str(rc)))
    client.subscribe(topic1)
    now = datetime.datetime.now()

def on_message(client, userdata, msg: object):
    msg.payload = msg.payload.decode("utf-8")
    now = datetime.datetime.now()
    print(now.strftime("%H:%M")+ ";" + msg.topic + ";" + str(msg.payload))
    filename: str = datetime.datetime.now().strftime("%Y-%m-%d")+".CSV"
    file = open(filename, "a") #DOSYA AÇMA APPEND
    #file.write("Tarih" + ";" + "Topic" + ";" + "Temp" + ";" +"Pressure" + ";" + "Humidity  \n")
    file.write(now.strftime("%H:%M") + ";" + msg.topic + ";" + str(msg.payload)+ " \n")
    file.close() #DOSYA KAPAMA


client = mqtt.Client(f4)  # Create instance of client with client ID “digi_mqtt_test”
client.on_connect = on_connect  # Define callback function for successful connection
client.on_message = on_message  # Define callback function for receipt of a message
# client.connect("m2m.eclipse.org", 1883, 60)  # Connect to (broker, port, keepalive-time)
client.connect(server_ip, server_port)

client.loop_forever()  # Start networking daemon
