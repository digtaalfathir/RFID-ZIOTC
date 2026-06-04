import pyziotc
import json
import os
import time
import signal
import http.client
from Logger import Logger
from RestAPI import RestAPI

DEBUG_SERVER = "192.168.0.234"
DEBUG_PORT = 514
LOG_ONLY_TO_CONSOLE = False
REST_API_RETRY_COUNT = 3

BATCH_TIMEOUT = 3      # flush kalau tidak ada tag baru selama 3 detik
KEYBOARD_COOLDOWN = 3  # proteksi duplikat untuk keyboard

Stop = False
ziotcObject = pyziotc.Ziotc()
logger = Logger(DEBUG_SERVER, DEBUG_PORT, LOG_ONLY_TO_CONSOLE)
restAPI = RestAPI(logger, REST_API_RETRY_COUNT, ziotcObject)

# Buffer batch tag
tag_batch = []
tag_set = set()
last_batch_time = 0
batch_metadata = {"reader_id": "", "antenna": "", "timestamp": ""}
keyboard_cache = {}

def sigHandler(signum, frame):
    global Stop
    Stop = True

def post_to_flask(payload):
    try:
        json_data = json.dumps(payload)
        conn = http.client.HTTPConnection(DEBUG_SERVER, 5000, timeout=2)
        headers = {"Content-type": "application/json"}
        conn.request("POST", "/rfid", json_data, headers)
        res = conn.getresponse()
        conn.close()
        logger.debug(f"POST to Flask OK: {res.status}")
    except Exception as e:
        logger.err(f"HTTP POST to Flask failed: {str(e)}")


def post_to_api(payload):
    try:
        json_data = json.dumps(payload)
        conn = http.client.HTTPConnection("wms.suite.stechoq-j.com", timeout=5)
        headers = {"Content-type": "application/json"}
        conn.request("POST", "/api/v1/warehouse-management/counting-log-rfid", json_data, headers)
        res = conn.getresponse()
        response_body = res.read().decode()
        conn.close()

        logger.debug(f"POST to API OK: {res.status} - {response_body}")

        # Kirim hasil response API ke server lokal untuk monitoring
        debug_payload = {
            "status": res.status,
            "response": response_body,
            "original_payload": payload,
        }
        post_to_flask({"api_result": debug_payload})

    except Exception as e:
        logger.err(f"HTTP POST to API failed: {str(e)}")
        post_to_flask({
            "api_result": {
                "error": str(e),
                "original_payload": payload
            }
        })


def flush_batch():
    """Kirim semua tag dalam batch ke Flask + API lalu reset buffer"""
    global tag_batch, tag_set, batch_metadata
    if tag_batch:
        payload = {
            "reader_id": batch_metadata["reader_id"],
            "antenna": batch_metadata["antenna"],
            "idHex": tag_batch,
            "timestamp": batch_metadata["timestamp"],
        }
        post_to_flask(payload)
        post_to_api(payload)
        logger.info(f"Flushed {len(tag_batch)} tags: {payload}")
        tag_batch = []
        tag_set = set()  # reset juga



def process_tag(msg_in):
    global tag_batch, tag_set, last_batch_time, batch_metadata, keyboard_cache

    try:
        msg_in_json = json.loads(msg_in)
        data = msg_in_json.get("data", {})
        local_time = time.localtime(time.time() + 7 * 3600)
        timestamp = time.strftime("%Y-%m-%dT%H:%M:%S", local_time) + ".000+0700"
        id_hex = data.get("idHex", "")
        antenna = int(data.get("antenna", 0))

        if not id_hex:
            logger.warn("No idHex found in data.")
            return

        now = time.time()

        # --- Jika antenna 1–8 → masukkan ke batch
        if 1 <= antenna <= 8:
            fake_antenna_id = "1"

            if not tag_batch:
                batch_metadata = {
                    "reader_id": data.get("hostName", ""),
                    "antenna": fake_antenna_id,
                    "timestamp": timestamp,
                }

            if id_hex not in tag_set:
                tag_set.add(id_hex)
                tag_batch.append(id_hex)
                last_batch_time = now
                logger.debug(f"New tag added to batch: {id_hex}")
            else:
                logger.debug(f"Duplicate tag ignored (no timer extension): {id_hex}")


    except Exception as e:
        logger.err(f"Failed to process tag: {str(e)}")


# === Main Loop ===
signal.signal(signal.SIGINT, sigHandler)

logger.debug("System Started: PID " + str(os.getpid()))
logger.debug("Reader Version: " + restAPI.getReaderVersion())
logger.debug("Reader Serial Number: " + restAPI.getReaderSerial())
logger.debug("Script Version: " + str(os.getenv("VERSION")))

ziotcObject.reg_new_msg_callback(lambda t, m: process_tag(m) if t == pyziotc.MSG_IN_JSON else None)
restAPI.startInventory()

try:
    while not Stop:
        time.sleep(1)
        # Flush kalau ada batch & sudah idle lebih lama dari BATCH_TIMEOUT
        if tag_batch and (time.time() - last_batch_time > BATCH_TIMEOUT):
            flush_batch()
finally:
    restAPI.stopInventory()
    flush_batch()
    logger.info("Stopped")
