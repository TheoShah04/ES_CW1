import time
import smbus2
import json
import paho.mqtt.client as mqtt
import ssl

# I2C setup from datasheet: https://cdn-shop.adafruit.com/datasheets/ads1115.pdf
ADS1115_ADDR = 0x48
REG_CONVERSION = 0x00
REG_CONFIG = 0x01
CONFIG_MSB = 0x8F
CONF_LSB = 0x83

bus = smbus2.SMBus(1)


# MQTT config
BROKER = "test.mosquitto.org"
PORT = 8884
TOPIC = "IC.embedded/deeplyembedded/test"
CLIENT_ID = "pi_loadcell"
# Certificate paths
CA_CERT = "mosquitto.org.crt"    
CLIENT_CERT = "client.crt"       
CLIENT_KEY = "client.key"

def i2c_comm():
    bus.write_i2c_block_data(ADS1115_ADDR, REG_CONFIG, [CONFIG_MSB, CONF_LSB])
    time.sleep(0.01)
    data = bus.read_i2c_block_data(ADS1115_ADDR, REG_CONVERSION, 2)
    # convert to 16-bit signed int
    conversion_raw = (data[0] << 8) | data[1]
    if conversion_raw > 32767:
            conversion_raw -= 65536
    print(f"Raw data: {conversion_raw}")
    return {
             "sensor": "TAL220",
             "value": conversion_raw
        }

def on_connect(client, userdata, flags, rc):
    if rc == 0:
          print(f"Connected to {BROKER}:{PORT}")
          client.subscribe(TOPIC)
          print(f"Subscribed to topic: {TOPIC}")
    else:
        print(f"Connection failed with code {rc}")

def on_message(client, userdata, msg):
    try:
        data = json.loads(msg.payload.decode())
        #print(f"Parsed: {json.dumps(data, indent=2)}")
    except:
        pass

client = mqtt.Client(client_id=CLIENT_ID)
client.tls_set(
     ca_certs=CA_CERT,
     certfile=CLIENT_CERT,
     keyfile=CLIENT_KEY,
     tls_version=ssl.PROTOCOL_TLSv1_2
)

client.on_connect = on_connect
client.on_message = on_message
client.connect(BROKER, PORT, keepalive=60)
client.loop_start()

try:
    while True:
        raw_data = i2c_comm()
        json_message=json.dumps(raw_data)

        result = client.publish(TOPIC,json_message, qos=1)

        if result.rc != mqtt.MQTT_ERR_SUCCESS:
            print(f"Publish failed: {result.rc}")

        time.sleep(0.5)

except KeyboardInterrupt:
     print("\nStop")
     client.loop_stop()
     client.disconnect()
     bus.close()
