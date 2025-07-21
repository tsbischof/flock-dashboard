import gzip
import json
import logging
import os
import queue
import re
import threading
import time
from pathlib import Path

import bs4
import dateutil
import requests
from websocket_server import WebsocketServer

logger = logging.getLogger(__name__)

broadcast_queue = queue.Queue()
path_queue = queue.Queue()

clients = dict()
clients_lock = threading.Lock()

broadcastable = dict()


def fetch_sequence(sequence):
    url = url_from_sequence(sequence)
    dst = dst_from_url(url)
    do_sleep = not dst.exists()

    if not dst.exists():
        fetch(url, dst)

    return do_sleep, dst


def fetch(url, dst, retries=5, retry_delay=5):
    dst.parent.mkdir(exist_ok=True, parents=True)

    for n in range(retries):
        try:
            r = requests.get(url)
            r.raise_for_status()
            dst.write_bytes(r.content)

            last_modified = r.headers.get("Last-Modified")
            if last_modified:
                dt = dateutil.parser.parse(last_modified)
                ts = dt.timestamp()

                os.utime(dst, (ts, ts))
            return
        except Exception as e:
            logger.error(f"failed to get {url}, retry {n + 1} of {retries}: {e}")
            time.sleep(retry_delay)


def url_from_sequence(sequence):
    s = f"{sequence:09d}"
    return f"https://planet.openstreetmap.org/replication/minute/{s[0:3]}/{s[3:6]}/{s[6:9]}.osc.gz"


def dst_from_url(url):
    return Path(re.sub("^.*://", "", url))


def get_current_sequence(retries=5, retry_delay=5):
    for n in range(retries):
        try:
            r = requests.get(
                "https://planet.openstreetmap.org/replication/minute/state.txt",
                stream=True,
            )
            r.raise_for_status()
            state = next(r.iter_content(1024)).decode()
            current_sequence = int(re.search("sequenceNumber=(\d+)", state).groups()[0])
            return current_sequence
        except Exception:
            logger.error(f"failed to get sequence, retry {n + 1} of {retries}")
            time.sleep(retry_delay)


def osm_fetcher(path_queue, retain_minutes=60 * 24, fetch_delay=2):
    current_sequence = get_current_sequence()
    start = current_sequence - retain_minutes

    for sequence in range(start, current_sequence + 1):
        do_sleep, dst = fetch_sequence(sequence)
        path_queue.put(dst)
        if do_sleep:
            time.sleep(fetch_delay)

    last_sequence = current_sequence
    while True:
        current_sequence = get_current_sequence()
        if current_sequence > last_sequence:
            for sequence in range(last_sequence + 1, current_sequence + 1):
                do_sleep, dst = fetch_sequence(sequence)
                path_queue.put(dst)
                if do_sleep:
                    time.sleep(fetch_delay)

        time.sleep(15)

    # todo remove older files


def find_flock(path):
    if isinstance(path, str):
        path = Path(path)

    with gzip.open(path, "rt") as f:
        raw = f.read()

    soup = bs4.BeautifulSoup(raw, "xml")
    for tag in soup.find_all("tag", {"k": "manufacturer", "v": "Flock Safety"}):
        method = tag.parent.parent.name
        content = tag.parent
        yield method, content


def node_to_msg(node):
    return {
        "lat": float(node["lat"]),
        "lon": float(node["lon"]),
        "label": "flock watch",
        "createdAt": node["timestamp"],
    }


def osm_finder(path_queue, broadcast_queue):
    path = None
    while True:
        try:
            path = path_queue.get()
        except queue.Empty:
            continue

        if path is not None:
            logger.info(f"process {path}")

            for method, node in find_flock(path):
                logger.info(node)
                msg = node_to_msg(node)
                broadcastable[(node["id"], node["changeset"])] = msg
                broadcast_queue.put(json.dumps(msg))


def connect(client, server):
    # send historical data
    for msg in broadcastable.values():
        try:
            server.send_message(client, json.dumps(msg))
        except Exception:
            logger.error("error sending message to client {client['id']}: {e}")

    with clients_lock:
        clients[client["id"]] = client


def disconnect(client, server):
    with clients_lock:
        del clients[client["id"]]


def broadcast_loop(server, broadcast_queue):
    while True:
        msg = broadcast_queue.get()
        with clients_lock:
            for client in clients.values():
                try:
                    server.send_message(client, msg)
                except Exception as e:
                    logger.error(f"could not send to client {client['id']}: {e}")


def main():
    logging.basicConfig(level=logging.DEBUG, force=True)

    server = WebsocketServer(port=3000, host="localhost")
    server.set_fn_new_client(connect)
    server.set_fn_client_left(disconnect)

    threading.Thread(
        target=broadcast_loop, args=(server, broadcast_queue), daemon=True
    ).start()
    threading.Thread(
        target=osm_finder, args=(path_queue, broadcast_queue), daemon=True
    ).start()
    threading.Thread(target=osm_fetcher, args=(path_queue,), daemon=True).start()
    server.run_forever()


if __name__ == "__main__":
    main()
