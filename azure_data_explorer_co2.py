import os
import time
import json
import serial
from azure.iot.device import IoTHubDeviceClient, Message
# --- Azure IoT Hub Settings ---

CONNECTION_STRING = "#####"
# --- Device ID ---

DEVICE_ID = "ras-tk230383"
# --- CO2 Sensor (UART) Settings ---
CO2_SERIAL_PORT = '/dev/ttyS0'
CO2_BAUD_RATE = 9600
ser_co2 = None
try:
    ser_co2 = serial.Serial(CO2_SERIAL_PORT, CO2_BAUD_RATE, timeout=1)
    print(f"CO2 sensor serial port {CO2_SERIAL_PORT} opened.")
except serial.SerialException as e:
    print(f"Could not open CO2 serial port {CO2_SERIAL_PORT}: {e}")
    ser_co2 = None
def read_co2_data():
    """
    Reads data from the CO2 sensor via UART.
    Assumes a protocol like MH-Z19B.
    """
    if ser_co2 is None:
        return None
    try:
        command = bytearray([0xFF, 0x01, 0x86, 0x00, 0x00, 0x00, 0x00, 0x00, 0x79])
        ser_co2.write(command)
        time.sleep(0.1)
        if ser_co2.in_waiting >= 9:
            response = ser_co2.read(9)
            if response[0] == 0xFF and response[1] == 0x86:
                co2_ppm = response[2] * 256 + response[3]
                return co2_ppm
    except Exception as e:
        print(f"Error reading CO2 data: {e}")
    return None
def send_telemetry_data(client, co2_value):
    """
    Constructs and sends telemetry data to IoT Hub.
    """
    try:
        if co2_value is None:
            print("No valid CO2 data to send.")
            return
        # --- MODIFICATION ---: Added timestamp and deviceId to the payload.
        # Get the current time in ISO 8601 UTC format, which is ideal for Azure.
        utc_timestamp = time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime())
        
        telemetry_data = {
            "timestamp": utc_timestamp,
            "deviceId": DEVICE_ID,
            "co2": co2_value
        }
        msg_txt_formatted = json.dumps(telemetry_data)
        message = Message(msg_txt_formatted)
        message.content_type = "application/json"
        message.content_encoding = "utf-8"
        print("-" * 30)
        print(f"Sending message: {msg_txt_formatted}")
        client.send_message(message)
        print("Message successfully sent")
    except Exception as e:
        print(f"Error sending message to IoT Hub: {e}")
def main():
    """
    Main function to connect to IoT Hub and send data periodically.
    """
    client = None
    try:
        client = IoTHubDeviceClient.create_from_connection_string(CONNECTION_STRING)
        print("Connecting to IoT Hub...")
        client.connect()
        print("IoT Hub client connected.")
        while True:
            co2_value = read_co2_data()
            print(f"Read CO2 (ppm): {co2_value if co2_value is not None else 'N/A'}")
            
            send_telemetry_data(client, co2_value)
            
            time.sleep(10)
    except KeyboardInterrupt:
        print("\nProgram terminated by user.")
    except Exception as e:
        print(f"An error occurred in the main loop: {e}")
    finally:
        if client:
            print("Shutting down IoT Hub client.")
            client.shutdown()
        if ser_co2 and ser_co2.is_open:
            ser_co2.close()
            print("CO2 serial port closed.")
        print("Program termination complete.")
if __name__ == '__main__':
    main()









