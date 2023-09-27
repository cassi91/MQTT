# 用于处理device发送给MQTT的信息
import datetime
import json
from json import JSONDecodeError
import paho.mqtt.client as mqtt
import cx_Oracle
import redis


# 定义回调函数来处理连接成功的事件
def on_connect(c, userdata, flags, rc):
    print("Connected with result code " + str(rc))

    for topic in topic_list:
        client.subscribe(topic)


# 定义回调函数来处理收到消息的事件
def on_message(c, userdata, msg):
    try:
        payload_dict = json.loads(msg.payload)
    except JSONDecodeError as e:
        print(e)
        return

    if msg.topic == "/smartpower/status":
        handle_status_message(payload_dict)
    if msg.topic == "/smartpower/systeminfo":
        handle_systeminfo_message(payload_dict)
    if msg.topic == "/smartpower/settingres":
        handle_setting_response(payload_dict)
    print("Received message '" + str(msg.payload) + "' on topic '" + msg.topic + "'")


def handle_status_message(d: dict):
    k = d.keys()
    bind_str = [":" + str(x) for x in range(len(k) + 2)]
    v = list(d.values()) + [datetime.datetime.now(), datetime.datetime.utcnow().strftime("%Y-%m-%d %H:%M:%S")]
    # 写入 status
    status_sql = "INSERT INTO SMARTPLUG.DEVICE_STATUS(" + (",".join(k)).upper() + ", INSERT_TIME, ITIME) VALUES (" + \
                 ','.join(bind_str) + ")"
    # 写入status_first
    delete_device_first = "DELETE FROM SMARTPLUG.DEVICE_STATUS_FIRST WHERE DEVICEID=:1"
    status_first_sql = "INSERT INTO SMARTPLUG.DEVICE_STATUS_FIRST(" + (",".join(k)).upper() + ", INSERT_TIME, ITIME) \
                    VALUES (" + ','.join(bind_str) + ")"
    conn = pool_oracle.acquire()
    cursor = conn.cursor()
    try:
        cursor.execute(status_sql, v)
        cursor.execute(delete_device_first, (d.get("DeviceID"),))
        cursor.execute(status_first_sql, v)
        conn.commit()
    except Exception as e:
        print("status ex: ", e)
    finally:
        pool_oracle.release(conn)


def handle_systeminfo_message(d: dict):
    device_info_sql = "INSERT INTO SMARTPLUG.DEVICE_INFO(DEVICEID, PROJECT, TYPE, OTP_VALUE, TIMEZONE, FW_VERSION, " + \
                      "MAX_CURRENT_STATUS, MAX_CURRENT_VALUE, ACOUTAUTORECOVER, CO_VALUE, OVER_TEMP, INSERT_TIME)" + \
                      "VALUES (:1, :2, :3, :4, :5, :6, :7, :8, :9, :10, :11, :12)"
    device_value = (d.get("DeviceID"), d.get("Project"), d.get("Type"), d.get("OTP_Value"), d.get("TimeZone"),
                    d.get("FwVersion"), d.get("MaxCurrentStatus"), d.get("MaxCurrentValue"), d.get("AcOutAutoRecover"),
                    d.get("CO_Value"), d.get("OVER_TEMP"), datetime.datetime.now())
    delete_device_info = "DELETE FROM SMARTPLUG.DEVICE_INFO WHERE DEVICEID=:1"
    conn = pool_oracle.acquire()
    cursor = conn.cursor()
    try:
        cursor.execute(device_info_sql, device_value)
        cursor.execute(delete_device_info, (d.get("DeviceID"),))
        conn.commit()
    except Exception as e:
        print("system info ex: ", e)
    finally:
        pool_oracle.release(conn)


def handle_setting_response(d: dict):
    message_id = d.get("MESSAGEID")
    result = d.get("Result")
    if message_id and result:
        redis_conn.hset(message_id, "result", result)
        redis_conn.hset(message_id, "message", json.dumps(d))
        redis_conn.expire(message_id, 60)


if __name__ == "__main__":
    topic_list = [
        "/smartpower/status",
        "/smartpower/systeminfo",
        "/smartpower/settingres"
    ]
    # cx_oracle 连接池
    pool_oracle = cx_Oracle.SessionPool(
        user="SMARTPLUG", password="ABB", dsn="localhost:1521/orclpdb", min=10, max=10, increment=0, threaded=True,
        encoding="UTF-8")

    # redis连接
    redis_conn = redis.Redis()
    # 创建MQTT客户端实例
    client = mqtt.Client()

    # 设置连接回调函数
    client.on_connect = on_connect

    # 设置消息接收回调函数
    client.on_message = on_message

    try:
        # 连接到MQTT服务器
        client.connect("mlbaeabbserver.cn", 1883, 60)

        # 开始循环处理网络流量和调用回调函数
        client.loop_forever()
    except KeyboardInterrupt:
        pool_oracle.close()
