import argparse
import datetime
import gzip
import json
import logging
import os
import queue
import re
import sqlite3
import threading
import time
from pathlib import Path
from typing import Dict, Tuple

import bs4
import dateutil
import requests
from websocket_server import WebsocketServer


class Node:
    def __init__(self, soup, action):
        self.soup = soup
        self.action = action

    @classmethod
    def from_text(cls, text, action):
        soup = bs4.BeautifulSoup(text, "xml")
        node = soup.find("node")
        if node is None:
            raise ValueError("no node found")

        return cls(node, action)

    def __lt__(self, other):
        return self.key() < other.key()

    def __eq__(self, other):
        return self.key() == other.key()

    def __repr__(self):
        return str(self.soup)

    @property
    def timestamp(self):
        return dateutil.parser.parse(self.soup["timestamp"])

    @property
    def id(self):
        return int(self.soup["id"])

    @property
    def changeset(self):
        return int(self.soup["changeset"])

    @property
    def lat(self):
        return float(self.soup["lat"])

    @property
    def lon(self):
        return float(self.soup["lon"])

    def key(self):
        return (self.id, self.changeset, self.action)

    def age(self):
        now = datetime.datetime.now(datetime.timezone.utc)
        age = now - self.timestamp
        return age

    def tags(self):
        return {tag["k"]: tag["v"] for tag in self.soup.find_all("tag")}

    def to_dict(self):
        return {
            "id": self.id,
            "lat": self.lat,
            "lon": self.lon,
            "tags": self.tags(),
            "createdAt": self.soup["timestamp"],
            "action": self.action,
        }


class Nodes:
    def __init__(
        self,
        elems: Dict[Tuple[int, int, str], Node] = dict(),
        max_age: datetime.timedelta = None,
    ):
        self.max_age = max_age
        self.elems = elems

    @classmethod
    def from_db(cls, conn, max_age: datetime.timedelta = None):
        self = cls(max_age=max_age)

        cur = conn.cursor()
        cur.execute("SELECT node, action FROM node")
        for node, action in cur.fetchall():
            try:
                n = Node.from_text(node, action)
                self.add(n)
                logger.info("loaded")
            except ValueError:
                logger.error(f"could not load {node} as node")

        return self

    def set_max_age(self, max_age: datetime.timedelta):
        self.max_age = max_age

    def add(self, node):
        if self.max_age and node.age() < self.max_age:
            self.elems[node.key()] = node
        else:
            logger.debug(f"dropped {node} due to age")

    def extend(self, nodes):
        for node in nodes:
            self.add(node)

    def __iter__(self):
        for key, node in list(self.elems.items()):
            if self.max_age and node.age() > self.max_age:
                del self.elems[key]
            else:
                yield node

    def __len__(self):
        return len([e for e in self])


class Changeset:
    def __init__(self, sequence, root=Path(".")):
        s = f"{sequence:09d}"
        self.url = f"https://planet.openstreetmap.org/replication/minute/{s[0:3]}/{s[3:6]}/{s[6:9]}.osc.gz"
        self.root = root
        self.rel_path = Path("minute") / s[0:3] / s[3:6] / f"{s[6:9]}.osc.gz"

    @property
    def path(self):
        return self.root / self.rel_path

    def __eq__(self, other):
        return self.rel_path == other.rel_path

    def fetch(self):
        logger.debug(f"exists? {self.path}")
        if self.path.exists():
            do_sleep = False
        else:
            do_sleep = True
            fetch(self.url, self.path)  # silently fail for now if we cannot get it

        return do_sleep

    def find_flock(self):
        db = get_db()
        osc_path = self.rel_path
        logger.info(f"nodes in {osc_path}?")

        # first check sql, then do work if needed
        cur = db.cursor()
        cur.execute("SELECT processed FROM path WHERE path = ?", (str(osc_path),))
        r = cur.fetchone()
        if r is not None and r[0]:
            cur.execute(
                "SELECT node, action FROM node WHERE path = ?", (str(osc_path),)
            )
            r = cur.fetchall()

            if r is not None:
                for node, action in r:
                    try:
                        yield Node.from_text(node, action)
                    except Exception as _:
                        logger.error(f"could not load {node} as node")
                        continue
        else:
            with gzip.open(self.path, "rt") as f:
                raw = f.read()

            soup = bs4.BeautifulSoup(raw, "xml")
            for tag in soup.find_all("tag", {"k": "manufacturer", "v": "Flock Safety"}):
                action = tag.parent.parent.name
                try:
                    node = Node(tag.parent, action)
                except ValueError:
                    logger.error(f"could not load {tag.parent} as node")
                    continue

                cur.execute(
                    "INSERT INTO node (path, action, node) VALUES (?, ?, ?)",
                    (str(osc_path), action, str(node)),
                )
                yield node

            cur.execute(
                """
INSERT INTO path (path, processed) 
VALUES (?, true) 
ON CONFLICT(path) DO UPDATE 
SET processed = true;""",
                (str(osc_path),),
            )

        db.commit()


def init_db(conn):
    cur = conn.cursor()
    cur.execute("""
CREATE TABLE IF NOT EXISTS path (
  path text,
  processed bool
);""")
    cur.execute("""
CREATE TABLE IF NOT EXISTS node (
  path text,
  action text,
  node text
);""")
    cur.execute("""
CREATE UNIQUE INDEX IF NOT EXISTS path_path_idx
ON path (path);""")
    conn.commit()


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
            return True
        except Exception as e:
            logger.error(f"failed to get {url}, retry {n + 1} of {retries}: {e}")
            time.sleep(retry_delay)

    return False


def get_current_sequence(retries=5, retry_delay=5):
    """Poll osm for the latest sequence number"""
    for n in range(retries):
        try:
            r = requests.get(
                "https://planet.openstreetmap.org/replication/minute/state.txt",
                stream=True,
            )
            r.raise_for_status()
            state = next(
                r.iter_content(1024)
            ).decode()  # no content-length so we terminate ourselves
            current_sequence = int(re.search("sequenceNumber=(\d+)", state).groups()[0])
            return current_sequence
        except Exception:
            logger.error(f"failed to get sequence, retry {n + 1} of {retries}")
            time.sleep(retry_delay)


def osm_fetcher(
    changeset_queue,
    max_age=datetime.timedelta(days=1),
    fetch_delay=2,
    replication_dir=Path("."),
):
    """Catch up on history, then listen for changes"""
    current_sequence = get_current_sequence()
    start = current_sequence - int(max_age.total_seconds() / 60)

    logger.info(f"find changesets starting at sequence={start}")

    for sequence in range(start, current_sequence + 1):
        cs = Changeset(sequence, root=replication_dir)
        do_sleep = cs.fetch()
        changeset_queue.put(cs)
        if do_sleep:
            time.sleep(fetch_delay)

    last_sequence = current_sequence
    while True:
        current_sequence = get_current_sequence()
        if current_sequence > last_sequence:
            for sequence in range(last_sequence + 1, current_sequence + 1):
                cs = Changeset(sequence, root=replication_dir)
                do_sleep = cs.fetch()
                changeset_queue.put(cs)
                if do_sleep:
                    time.sleep(fetch_delay)

        last_sequence = current_sequence
        time.sleep(60)

    # todo remove older files


def flock_finder(changeset_queue, broadcast_queue):
    """Find nodes in changesets, then send them for broadcast"""
    global nodes

    cs = None
    while True:
        try:
            cs = changeset_queue.get()
        except queue.Empty:
            continue

        if cs is not None:
            for node in cs.find_flock():
                logger.info(f"found node: {node.key()}")
                with nodes_lock:
                    nodes.add(node)

                broadcast_queue.put(node)


def connect(client, server):
    global nodes
    global nodes_lock

    logger.info(f"connect: {client}")
    with clients_lock:
        clients[client["id"]] = client

    # send historical data
    with nodes_lock:
        ns = list(sorted(nodes))
        for node in ns:
            try:
                server.send_message(client, json.dumps(node.to_dict()))
            except Exception as e:
                logger.error(f"error sending message to client {client['id']}: {e}")


def heartbeat(server):
    while True:
        with clients_lock:
            logger.debug("heartbeat")
            for client in clients.values():
                try:
                    server.send_message(client, "ping")
                except Exception as e:
                    logger.error(f"failed ping to {client}: {e}")

        time.sleep(10)


def disconnect(client, server):
    logger.info(f"disconnect: {client}")
    with clients_lock:
        del clients[client["id"]]


def broadcast_loop(server, broadcast_queue):
    while True:
        node = broadcast_queue.get()
        logger.info("do broadcast")
        with clients_lock:
            msg = json.dumps(node.to_dict())

            for client in clients.values():
                try:
                    server.send_message(client, msg)
                except Exception as e:
                    logger.error(f"could not send to client {client['id']}: {e}")


def main():
    global nodes

    parser = argparse.ArgumentParser(
        "provide a live websocket feed of flock cameras found in openstreetmaps"
    )
    parser.add_argument("-v", "--verbose", action="store_true")
    parser.add_argument(
        "-a",
        "--max-age",
        type=int,
        default=24 * 60,
        help="Oldest cameras to send to the client, in units of minutes",
    )
    parser.add_argument(
        "-d",
        "--replication-dir",
        type=Path,
        default=Path("."),
        help="directory to store OSM changes in",
    )

    args = parser.parse_args()

    logging.basicConfig(
        level=logging.DEBUG if args.verbose else logging.INFO,
        force=True,
        format="[%(asctime)s] [%(process)d] [%(levelname)s] [%(name)s:%(lineno)s] %(message)s",
        datefmt="%Y-%m-%d %H:%M:%S %z",
    )

    db = get_db()
    init_db(db)

    broadcast_queue = queue.Queue()
    changeset_queue = queue.Queue()

    max_age = datetime.timedelta(minutes=args.max_age)

    with nodes_lock:
        nodes = Nodes.from_db(db, max_age=max_age)

    logger.info(f"loaded {len(nodes)} nodes")

    server = WebsocketServer(port=3000, host="localhost")
    server.set_fn_new_client(connect)
    server.set_fn_client_left(disconnect)

    threading.Thread(
        target=osm_fetcher,
        args=(changeset_queue,),
        kwargs={"max_age": max_age, "replication_dir": args.replication_dir},
        daemon=True,
    ).start()
    threading.Thread(
        target=flock_finder, args=(changeset_queue, broadcast_queue), daemon=True
    ).start()
    threading.Thread(
        target=broadcast_loop, args=(server, broadcast_queue), daemon=True
    ).start()
    threading.Thread(target=heartbeat, args=(server,), daemon=True).start()
    server.run_forever()


logger = logging.getLogger(__name__)
clients = dict()
clients_lock = threading.Lock()
thread_local = threading.local()


def get_db():
    if not hasattr(thread_local, "conn"):
        thread_local.conn = sqlite3.connect("camera.sqlite3")
    return thread_local.conn


nodes = None
nodes_lock = threading.Lock()

if __name__ == "__main__":
    main()
