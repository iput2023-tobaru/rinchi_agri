import time
import json
import serial
import paho.mqtt.client as mqtt

#センサー設定
#CO2センサーのUARTポートとボーレート
CO2_SERIAL_PORT = '/dev/ttyS0'
CO2_BAUD_RATE = 9600

#MQTT 設定
MQTT_BROKER_ADDRESS = "test.mosquitto.org"
MQTT_PORT = 1883
MQTT_TOPIC = "sensors/co2" # データを送信するトピック名(一応)

#デバイス情報
DEVICE_ID = "rpi-sensor-01" # センサーの識別子

def read_co2_data(ser_co2):
    """
    CO2センサーからデータを読み取る関数 (UART通信)。
    MH-Z19Bのような一般的なCO2センサーのプロトコルを仮定しています。
    """
    if ser_co2 is None or not ser_co2.is_open:
        return None

    try:
        # CO2濃度測定コマンド
        command = bytearray([0xFF, 0x01, 0x86, 0x00, 0x00, 0x00, 0x00, 0x00, 0x79])
        ser_co2.write(command)
        time.sleep(0.2) #応答を待つ時間を少し長く

        if ser_co2.in_waiting >= 9:
            response = ser_co2.read(9)
            if response[0] == 0xFF and response[1] == 0x86:
                co2_ppm = response[2] * 256 + response[3]
                return co2_ppm
            else:
                print(f"Unexpected CO2 response: {response.hex()}")
                return None
        else:
            return None
    except Exception as e:
        print(f"Error reading CO2 data: {e}")
        return None

def main():
    """メイン処理"""
    # MQTTクライアントのセットアップ
    client = mqtt.Client()
    try:
        client.connect(MQTT_BROKER_ADDRESS, MQTT_PORT, 60)
        print("Connected to MQTT Broker.")
    except Exception as e:
        print(f"Could not connect to MQTT Broker: {e}")
        return

    # シリアルポートのセットアップ
    ser_co2 = None
    try:
        ser_co2 = serial.Serial(CO2_SERIAL_PORT, CO2_BAUD_RATE, timeout=1)
        print(f"CO2 sensor serial port {CO2_SERIAL_PORT} opened.")
    except serial.SerialException as e:
        print(f"Could not open CO2 serial port {CO2_SERIAL_PORT}: {e}")
        
    client.loop_start() # MQTTのバックグラウンド処理を開始

    try:
        while True:
            co2_value = read_co2_data(ser_co2)

            if co2_value is not None:
                # 送信するデータ（ペイロード）をJSON形式で作成
                payload = {
                    "deviceId": DEVICE_ID,
                    "co2": co2_value
                }
                payload_json = json.dumps(payload)

                # MQTTトピックにデータを公開（Publish）
                result = client.publish(MQTT_TOPIC, payload_json)
                
                if result[0] == 0:
                    print(f"Sent: {payload_json} to topic {MQTT_TOPIC}")
                else:
                    print(f"Failed to send message to topic {MQTT_TOPIC}")
            else:
                print("Failed to read CO2 data. Retrying...")

            time.sleep(10) # 10秒ごとにデータを送信

    except KeyboardInterrupt:
        print("\nProgram terminated by user.")
    finally:
        client.loop_stop()
        client.disconnect()
        print("Disconnected from MQTT Broker.")
        if ser_co2 and ser_co2.is_open:
            ser_co2.close()
            print("CO2 serial port closed.")

if __name__ == '__main__':
    main()

