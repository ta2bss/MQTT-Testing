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
    print("Connected with result code {0}".format(str(rc)))  # Print result of connection attempt
    client.subscribe(topic1)  # Subscribe to the topic “digitest/test1”, receive any messages published on it
    now = datetime.datetime.now()
    filename: str = datetime.datetime.now().strftime("%Y-%m-%d") + ".db"
    con = sqlite3.connect(filename)  # Tabloya bağlanıyoruz.
    cursor = con.cursor()  # cursor isimli değişken veritabanı üzerinde işlem yapmak için kullanacağımız imleç olacak.
    cursor.execute(
        "CREATE TABLE IF NOT EXISTS MAKINEADI (TIME TEXT, TOPIC TEXT , TYPE TEXT, SENSOR01  TEXT , DATA01 TEXT , SENSOR02 TEXT ,"
        "DATA02 TEXT, SENSOR03 TEXT , DATA03 TEXT , SENSOR04 TEXT ,DATA04 TEXT, SENSOR05 TEXT , DATA05 TEXT, SENSOR06 TEXT , DATA06 TEXT , "
        "SENSOR07 TEXT ,DATA07 TEXT, SENSOR08 TEXT , DATA08 TEXT , SENSOR09 TEXT , DATA09 TEXT, SENSOR10 TEXT , DATA10 TEXT, SENSOR11 TEXT , "
        "DATA11 TEXT, SENSOR12 TEXT , DATA12 TEXT)")  # Sorguyu çalıştırıyoruz.
    con.commit()  # Sorgunun veritabanı üzerinde geçerli olması için commit işlemi gerekli.
    con.close()

def on_message(client, userdata, msg: object):  # The callback for when a PUBLISH message is received from the server.
    msg.payload = msg.payload.decode("utf-8")
    now = datetime.datetime.now()

    print(now.strftime("%H:%M")+ "," + msg.topic + "," + str(msg.payload))  # EKRANA YAZMA
    filename: str = datetime.datetime.now().strftime("%Y-%m-%d")+".CSV" # TXT DOSYASY ADI OLUŞTURMA
    file = open(filename, "a") #DOSYA AÇMA APPEND
    file.write(now.strftime("%H:%M") + ";" + msg.topic + ";" + str(msg.payload)+ " \n") #DOSYAYA YAZMA
    file.close() #DOSYA KAPAMA
    #print (str(msg.payload))

    filename: str = datetime.datetime.now().strftime("%Y-%m-%d") + ".db"

    con = sqlite3.connect(filename)  # Tabloya bağlanıyoruz.
    cursor = con.cursor()  # cursor isimli değişken veritabanı üzerinde işlem yapmak için kullanacağımız imleç olacak.
    cursor.execute(
        "CREATE TABLE IF NOT EXISTS MAKINEADI (TIME TEXT, TOPIC TEXT , TYPE TEXT, SENSOR01  TEXT , DATA01 TEXT , SENSOR02 TEXT ,"
        "DATA02 TEXT, SENSOR03 TEXT , DATA03 TEXT , SENSOR04 TEXT ,DATA04 TEXT, SENSOR05 TEXT , DATA05 TEXT, SENSOR06 TEXT , DATA06 TEXT , "
        "SENSOR07 TEXT ,DATA07 TEXT, SENSOR08 TEXT , DATA08 TEXT , SENSOR09 TEXT , DATA09 TEXT, SENSOR10 TEXT , DATA10 TEXT, SENSOR11 TEXT , "
        "DATA11 TEXT, SENSOR12 TEXT , DATA12 TEXT)")  # Sorguyu çalıştırıyoruz.
    con.commit()  # Sorgunun veritabanı üzerinde geçerli olması için commit işlemi gerekli.
    con.close()

    con = sqlite3.connect(filename)  # Tabloya bağlanıyoruz.
    cursor = con.cursor()  # cursor isimli değişken veritabanı üzerinde işlem yapmak için kullanacağımız imleç olacak.
    mesaj=(now.strftime("%H:%M") + "," + msg.topic + "," + str(msg.payload))
    a1,a2,a3,a4,a5,a6,a7,a8,a9,a10,a11,a12,a13,a14,a15,a16,a17,a18,a19,a20,a21,a22,a23,a24,a25,a26,a27 = mesaj.split(",")
    cursor.execute("INSERT INTO MAKINEADI VALUES  (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)" ,
                   (a1,a2,a3,a4,a5,a6,a7,a8,a9,a10,a11,a12,a13,a14,a15,a16,a17,a18,a19,a20,a21,a22,a23,a24,a25,a26,a27))
    con.commit()
    con.close()

client = mqtt.Client(f4)  # Create instance of client with client ID “digi_mqtt_test”
client.on_connect = on_connect  # Define callback function for successful connection
client.on_message = on_message  # Define callback function for receipt of a message
# client.connect("m2m.eclipse.org", 1883, 60)  # Connect to (broker, port, keepalive-time)
client.connect(server_ip, server_port)

client.loop_forever()  # Start networking daemon
