"""
Producer Wikimedia EventStreams (recentchange) -> Kafka
Soal 1b/2 - Studi Kasus Pemantauan Penyuntingan Wikipedia

Cara pakai (Windows, dari folder yang sama dengan file ini):
    set WIKI_HASH_KEY=isi-dengan-kunci-rahasia-kelompok
    python wiki_producer.py --duration 12

Opsi:
    --bootstrap   alamat Kafka (default 127.0.0.1:9092)
    --topic       nama topic (default wiki.recentchange.raw)
    --duration    lama berjalan dalam menit; 0 = sampai Ctrl+C
    --stats-every interval ringkasan dalam detik (default 30)
"""

import argparse
import hashlib
import hmac
import ipaddress
import json
import os
import signal
import sys
import time
from collections import Counter

import requests
from confluent_kafka import Producer

STREAM_URL = "https://stream.wikimedia.org/v2/stream/recentchange"
USER_AGENT = (
    "WikiMonitor-BigData/1.1 "
    "(tugas kuliah Big Data; kontak: abyanhf@students.amikom.ac.id)"
)
ALLOWED_TYPES = {"edit", "new"}
MAX_COMMENT = 500          # comment dipotong agar satu ringkasan panjang tidak membengkakkan pesan


class RateLimited(Exception):
    """HTTP 429 dari Wikimedia; membawa lama tunggu dalam detik."""

    def __init__(self, wait):
        super().__init__(f"HTTP 429, diminta menunggu {wait} detik")
        self.wait = wait


# ---------------------------------------------------------------------------
# Penyamaran user anonim
# ---------------------------------------------------------------------------
def classify_user(user):
    """Kembalikan 'ip', 'temp' (akun sementara, diawali '~'), atau None."""
    if not user:
        return None
    try:
        ipaddress.ip_address(user)
        return "ip"
    except ValueError:
        pass
    if user.startswith("~"):
        return "temp"
    return None


def anonymize(user, key):
    """HMAC-SHA256 dengan kunci rahasia: konsisten, tetapi tidak bisa dibalik
    dengan menebak semua alamat IP tanpa mengetahui kuncinya."""
    digest = hmac.new(key, user.encode("utf-8"), hashlib.sha256).hexdigest()
    return "anon_" + digest[:16]


def build_message(ev, key):
    """Ambil field yang diperlukan saja. None berarti event dibuang."""
    if ev.get("meta", {}).get("domain") == "canary":
        return None
    if ev.get("type") not in ALLOWED_TYPES:
        return None
    if ev.get("id") is None or not ev.get("wiki") or ev.get("title") is None:
        return None

    user = ev.get("user")
    anon_kind = classify_user(user)
    if anon_kind is not None:
        user = anonymize(user, key)

    length = ev.get("length") or {}
    return {
        "id": ev["id"],
        "type": ev["type"],
        "wiki": ev["wiki"],
        "server_name": ev.get("server_name"),
        "title": ev["title"],
        "namespace": ev.get("namespace"),
        "user": user,
        "is_anon": anon_kind is not None,
        "anon_kind": anon_kind,
        "bot": bool(ev.get("bot", False)),
        "minor": bool(ev.get("minor", False)),
        "timestamp": ev.get("timestamp"),
        "length_old": length.get("old"),
        "length_new": length.get("new"),
        "comment": (ev.get("comment") or "")[:MAX_COMMENT],
        "ingest_ts": round(time.time(), 3),
    }


# ---------------------------------------------------------------------------
# Pembaca SSE dengan reconnect
# ---------------------------------------------------------------------------
def sse_events(session, url, last_event_id, stats):
    """Satu koneksi SSE. Menghasilkan (event_id, data_json)."""
    headers = {"Accept": "text/event-stream"}
    if last_event_id:
        headers["Last-Event-ID"] = last_event_id

    with session.get(url, headers=headers, stream=True, timeout=(10, 60)) as resp:
        if resp.status_code == 429:
            try:
                wait = int(resp.headers.get("Retry-After", "30"))
            except ValueError:
                wait = 30
            raise RateLimited(wait)
        resp.raise_for_status()
        stats["connected_at"] = time.time()

        event_id, data_lines = None, []
        for raw in resp.iter_lines(decode_unicode=True):
            if raw is None:
                continue
            if raw == "":
                if data_lines:
                    yield event_id, "\n".join(data_lines)
                event_id, data_lines = None, []
                continue
            if raw.startswith(":"):
                continue
            field, _, value = raw.partition(":")
            value = value[1:] if value.startswith(" ") else value
            if field == "id":
                event_id = value
            elif field == "data":
                data_lines.append(value)


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------
def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--bootstrap", default="127.0.0.1:9092")
    ap.add_argument("--topic", default="wiki.recentchange.raw")
    ap.add_argument("--duration", type=float, default=0)
    ap.add_argument("--stats-every", type=int, default=30)
    args = ap.parse_args()

    key_str = os.environ.get("WIKI_HASH_KEY")
    if not key_str:
        sys.exit("WIKI_HASH_KEY belum diset. Contoh: set WIKI_HASH_KEY=kunci-rahasia")
    hash_key = key_str.encode("utf-8")

    producer = Producer({
        "bootstrap.servers": args.bootstrap,
        "client.id": "wiki-sse-producer",
        "acks": "all",
        "enable.idempotence": True,
        "linger.ms": 50,
        "compression.type": "lz4",
    })

    stats = {
        "received": 0, "filtered": 0, "bad_json": 0,
        "sent": 0, "delivered": 0, "failed": 0,
        "reconnects": 0, "bytes": 0, "connected_at": None,
    }
    per_partition = Counter()

    def on_delivery(err, msg):
        if err is not None:
            stats["failed"] += 1
            print(f"[GAGAL] {err}", file=sys.stderr)
        else:
            stats["delivered"] += 1
            per_partition[msg.partition()] += 1

    stop = {"flag": False}

    def handle_stop(signum, frame):
        stop["flag"] = True

    signal.signal(signal.SIGINT, handle_stop)

    session = requests.Session()
    session.headers["User-Agent"] = USER_AGENT

    t_start = time.time()
    t_end = t_start + args.duration * 60 if args.duration > 0 else None
    last_stats = t_start
    last_delivered = 0
    last_event_id = None
    backoff = 1

    print(f"Mulai {time.strftime('%Y-%m-%d %H:%M:%S')} -> {args.bootstrap} / {args.topic}")

    while not stop["flag"] and (t_end is None or time.time() < t_end):
        try:
            for event_id, data in sse_events(session, STREAM_URL, last_event_id, stats):
                if event_id:
                    last_event_id = event_id
                backoff = 1
                stats["received"] += 1

                try:
                    ev = json.loads(data)
                except json.JSONDecodeError:
                    stats["bad_json"] += 1
                    continue

                msg = build_message(ev, hash_key)
                if msg is None:
                    stats["filtered"] += 1
                else:
                    value = json.dumps(msg, ensure_ascii=False).encode("utf-8")
                    key = f"{msg['wiki']}:{msg['title']}".encode("utf-8")
                    while True:
                        try:
                            producer.produce(args.topic, key=key, value=value,
                                             on_delivery=on_delivery)
                            break
                        except BufferError:
                            producer.poll(0.5)
                    stats["sent"] += 1
                    stats["bytes"] += len(value)
                producer.poll(0)

                now = time.time()
                if now - last_stats >= args.stats_every:
                    rate = (stats["delivered"] - last_delivered) / (now - last_stats)
                    avg = stats["bytes"] / stats["sent"] if stats["sent"] else 0
                    print(
                        f"[{time.strftime('%H:%M:%S')}] "
                        f"diterima={stats['received']} terkirim={stats['delivered']} "
                        f"dibuang={stats['filtered']} gagal={stats['failed']} "
                        f"laju={rate:.1f} msg/s rata2={avg:.0f} B "
                        f"reconnect={stats['reconnects']} "
                        f"partisi={dict(sorted(per_partition.items()))}"
                    )
                    last_stats, last_delivered = now, stats["delivered"]

                if stop["flag"] or (t_end is not None and now >= t_end):
                    break

        except RateLimited as e:
            stats["reconnects"] += 1
            print(f"[RECONNECT] {e}", file=sys.stderr)
            time.sleep(e.wait)
        except (requests.RequestException, OSError) as e:
            stats["reconnects"] += 1
            print(f"[RECONNECT] {type(e).__name__}: {e}; tunggu {backoff} detik",
                  file=sys.stderr)
            time.sleep(backoff)
            backoff = min(backoff * 2, 60)

    print("Menghentikan, mengirim sisa pesan...")
    remaining = producer.flush(30)
    elapsed = time.time() - t_start
    avg = stats["bytes"] / stats["sent"] if stats["sent"] else 0

    print("\n===== RINGKASAN PRODUCER =====")
    print(f"Durasi           : {elapsed / 60:.1f} menit")
    print(f"Event diterima   : {stats['received']}")
    print(f"Dibuang (filter) : {stats['filtered']}")
    print(f"JSON rusak       : {stats['bad_json']}")
    print(f"Terkirim (ack)   : {stats['delivered']}")
    print(f"Gagal            : {stats['failed']}  | belum terkirim: {remaining}")
    print(f"Laju rata-rata   : {stats['delivered'] / elapsed:.1f} pesan/detik")
    print(f"Ukuran rata-rata : {avg:.0f} byte/pesan")
    print(f"Reconnect        : {stats['reconnects']}")
    print(f"Per partisi      : {dict(sorted(per_partition.items()))}")
    print(f"Last-Event-ID    : {last_event_id}")


if __name__ == "__main__":
    main()
