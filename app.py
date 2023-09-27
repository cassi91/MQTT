# 分割后的http中转指令
import base64
import time

from flask import Flask, request
import redis
# from Crypto.Cipher import AES
from flask_mqtt import Mqtt

app = Flask(__name__)
app.config['MQTT_BROKER_URL'] = '123.206.94.11'
app.config['MQTT_BROKER_PORT'] = 1883

mqtt = Mqtt(app)

"""
class MyAESCipher:
    IV = b'\0\0\0\0\0\0\0\0\0\0\0\0\0\0\0\0'
    KEY = b"qc5644abd868x36f"
    C = AES.new(KEY, AES.MODE_CBC, IV)

    @classmethod
    def __pad(cls, text):
        text_len = len(text)
        amount_to_pad = AES.block_size - (text_len % AES.block_size)
        if amount_to_pad == 0:
            amount_to_pad = AES.block_size

        pad = chr(amount_to_pad)
        return text + pad*amount_to_pad

    @classmethod
    def __un_pad(cls, text):
        pad = ord(text[-1])
        return text[:-pad]

    @classmethod
    def encrypt(cls, raw):
        raw = cls.__pad(raw)
        return base64.b64encode(cls.C.encrypt(raw.encode("utf-8")))

    @classmethod
    def decrypt(cls, enc):
        enc = base64.b64decode(enc)
        return cls.__un_pad(cls.C.decrypt(enc).decode('utf-8'))
"""

@app.route("/SmartPlugSetting", methods=["POST", "GET"])
def smart_plug_setting():
    if request.method == "GET":
        data = request.args.get('data')
        json_data = str.encode(data)
        d = eval(json_data[:-2])
        message_id = d.get("MESSAGEID")
        if message_id:
            print(message_id)
            mqtt.publish("/smartpower/command", json_data, qos=2)
            for i in range(10):
                time.sleep(0.5)
                r = redis_conn.hget(message_id, "message")
                if r:
                    return r
        # 设置执行失败
        return {"Result": "2", "MessageType": "3"}


if __name__ == "__main__":
    redis_conn = redis.Redis()

    app.run('0.0.0.0', 8862)
