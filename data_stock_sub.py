import paho.mqtt.client as mqtt
import json
from influxdb_client import InfluxDBClient, Point, WriteOptions
from influxdb_client.client.write_api import SYNCHRONOUS
import time 

#InfluxDB 設定
INFLUXDB_URL = "http://XXX.XXX.XXX.XXX:8086"
INFLUXDB_TOKEN = "###" # あなたのAPIトークンをここに貼り付け
INFLUXDB_ORG = "rin" # 組織名
INFLUXDB_BUCKET = "sensor_data" #設定したバケット名

#アラート設定

CO2_ALERT_THRESHOLD = 2000 # この値 (ppm) を超えたら警告を表示



#MQTT 設定
MQTT_BROKER_ADDRESS = "test.mosquitto.org"
MQTT_PORT = 1883
MQTT_TOPIC = "sensors/co2"

#InfluxDBクライアントの初期化
influx_client = InfluxDBClient(url=INFLUXDB_URL, token=INFLUXDB_TOKEN, org=INFLUXDB_ORG)
write_api = influx_client.write_api(write_options=SYNCHRONOUS)



def on_connect(client, userdata, flags, rc):
    """MQTTブローカーに接続したときに呼ばれる関数"""
    if rc == 0:
        print("Connected to MQTT Broker.")
        client.subscribe(MQTT_TOPIC)
        print(f"Subscribed to topic: {MQTT_TOPIC}")
    else:
        print(f"Failed to connect, return code {rc}\n")

def on_message(client, userdata, msg):
    """MQTTメッセージを受信したときに呼ばれる関数"""
    print(f"Received message: {msg.payload.decode()} from topic {msg.topic}")
    
    try:
        # 1. データの解析
        data = json.loads(msg.payload.decode())
        device_id = data.get("deviceId")
        co2_value = data.get("co2")

        if device_id is None or co2_value is None:
            print("Message is missing 'deviceId' or 'co2' key.")
            return

        # 2. InfluxDBへのデータ書き込み
        point = Point("co2_measurement") \
            .tag("deviceId", device_id) \
            .field("ppm", float(co2_value))
            
        write_api.write(bucket=INFLUXDB_BUCKET, org=INFLUXDB_ORG, record=point)
        print(f"Successfully wrote data to InfluxDB: CO2={co2_value}")

        # 3. アラート判定 (コンソールに表示)
        if co2_value > CO2_ALERT_THRESHOLD:
            
            print(f"ALERT: CO2 value {co2_value} exceeds threshold {CO2_ALERT_THRESHOLD}!")

    except json.JSONDecodeError:
        print("Could not decode message payload as JSON.")
    except Exception as e:
        print(f"An error occurred in on_message: {e}")

def main():
    """メイン処理"""
    client = mqtt.Client()
    client.on_connect = on_connect
    client.on_message = on_message
    
    try:
        client.connect(MQTT_BROKER_ADDRESS, MQTT_PORT, 60)
    except Exception as e:
        print(f"Could not connect to MQTT Broker: {e}")
        return

    #無限ループでメッセージを待ち受ける
    client.loop_forever()

if __name__ == '__main__':
    main()
