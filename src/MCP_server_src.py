"""
NLHome MCP Server - Smart Home System
Hermes Agent MCP server za upravljanje pametnim domom putem MQTT-a.
Kompatibilan sa Tasmota i picoETF uredajima.
"""

from mcp.server.fastmcp import FastMCP
import paho.mqtt.client as mqtt
import paho.mqtt.publish as publish
import json
import os
import re
import time
import threading
import datetime
import requests
from contextlib import contextmanager

# ddgs (ranije duckduckgo_search) - ziva DuckDuckGo pretraga bez API kljuca.
# Uvoz je NE-fatalan: ako paket fali, server i dalje radi (tasteri, senzori,
# aktuacija), samo web-pretraga vraca jasnu gresku umjesto da srusi proces.
try:
    from ddgs import DDGS
except ImportError:
    try:
        from duckduckgo_search import DDGS  # stariji naziv paketa
    except ImportError:
        DDGS = None

# ──────────────────────────────────────────────
# Konfiguracija
# ──────────────────────────────────────────────
MQTT_HOST = "195.130.59.221"
MQTT_PORT = 1883
TIM = "tim12"

TOPIC_TELE       = f"tele/{TIM}_tasmota/SENSOR"
TOPIC_POWER1     = f"cmnd/{TIM}_tasmota/POWER1"
TOPIC_POWER2     = f"cmnd/{TIM}_tasmota/POWER2"
TOPIC_STAT1      = f"stat/{TIM}_tasmota/POWER1"
TOPIC_STAT2      = f"stat/{TIM}_tasmota/POWER2"
TOPIC_STAT_SNS   = f"stat/{TIM}_tasmota/STATUS10"
TOPIC_STAT_RESULT = f"stat/{TIM}_tasmota/RESULT"
TOPIC_PICO_CMD   = f"{TIM}/pico/cmd"
TOPIC_PICO_STATE = f"{TIM}/pico/state"

DATA_FILE = "/root/nlhome_db.json"
CIJENA_KWH_BAM = 0.18  # BAM/kWh – EPBiH tarifa (struja; mijenja se rijetko)

# Cijene goriva se NE hardkodiraju u kod - cuvaju se u bazi i postavljaju
# runtime-om preko postavi_cijenu_goriva() (vidi dolje).

# WWO weatherCode -> kratki bosanski opis (za 16x2 LCD, drzati kratko)
_WMO_BS = {
    "113": "Vedro", "116": "Promj.obl", "119": "Oblacno", "122": "Tmurno",
    "143": "Magla", "248": "Magla", "260": "Magla",
    "176": "Slaba kisa", "263": "Slaba kisa", "266": "Slaba kisa",
    "293": "Slaba kisa", "296": "Slaba kisa", "353": "Pljusak",
    "299": "Kisa", "302": "Kisa",
    "305": "Jaka kisa", "308": "Jaka kisa", "356": "Jak pljusak", "359": "Jak pljusak",
    "200": "Grmljavina", "386": "Grmljavina", "389": "Grmljavina",
    "227": "Mecava", "230": "Mecava",
    "179": "Snijeg", "323": "Snijeg", "326": "Snijeg", "368": "Snijeg",
    "329": "Jak snijeg", "332": "Jak snijeg", "335": "Jak snijeg",
    "338": "Jak snijeg", "371": "Jak snijeg",
    "182": "Susnjezica", "185": "Susnjezica", "281": "Susnjezica",
    "284": "Susnjezica", "311": "Susnjezica", "314": "Susnjezica",
    "317": "Susnjezica", "320": "Susnjezica", "362": "Susnjezica", "365": "Susnjezica",
    "350": "Led", "374": "Led", "377": "Led",
    "392": "Snij.grml", "395": "Snij.grml",
}

def _novi_mqtt_klijent() -> mqtt.Client:
    """paho-mqtt 2.0+ trazi callback_api_version; ovo radi i na 1.x i na 2.x."""
    try:
        return mqtt.Client(mqtt.CallbackAPIVersion.VERSION1)
    except (AttributeError, TypeError):
        return mqtt.Client()


_db_lock = threading.RLock()

def load_db() -> dict:
    default = {
        "podsjetnici": [],
        "pravila": [],
        "scene": {},
        "log_akcija": [],
        "log_potrosnje": [],
        "ciljna_temperatura": None,
        "pracenja": [],
        "zadnje_kretanje": {"ts": 0, "kretanje": False},
        "zadnji_klik": {"ts": 0, "klik": None},
        "cache": {},  # zadnje uspjesno ocitanje vremena (fallback na demou)
        "cijene_goriva": {"dizel": None, "benzin": None, "azurirano": None},
    }
    if not os.path.exists(DATA_FILE):
        return default
    with open(DATA_FILE, "r") as f:
        data = json.load(f)
    for key, val in default.items():
        data.setdefault(key, val)
    return data

def save_db(data: dict):
    # atomican upis: piši u .tmp pa rename, da paralelni threadovi ne pokvare fajl
    tmp = DATA_FILE + ".tmp"
    with open(tmp, "w") as f:
        json.dump(data, f, ensure_ascii=False, indent=2)
    os.replace(tmp, DATA_FILE)

@contextmanager
def db_session():
    """
    Zakljucan read-modify-write nad bazom.
    Sprjecava da pozadinski proces pregazi PIR/klik evente koje upise listener.
    """
    with _db_lock:
        db = load_db()
        yield db
        save_db(db)

def log_akciju(opis: str):
    with db_session() as db:
        db["log_akcija"].append({
            "ts": datetime.datetime.now().isoformat(),
            "akcija": opis
        })
        db["log_akcija"] = db["log_akcija"][-500:]

# ──────────────────────────────────────────────
# Cache (zadnja poznata vrijednost - fallback kad mreza padne)
# ──────────────────────────────────────────────
def _spremi_cache(kljuc: str, tekst: str):
    with db_session() as db:
        db.setdefault("cache", {})[kljuc] = {"tekst": tekst, "ts": time.time()}

def _procitaj_cache(kljuc: str) -> str | None:
    db = load_db()
    c = db.get("cache", {}).get(kljuc)
    return c.get("tekst") if c else None

# ──────────────────────────────────────────────
# Web pretraga (DuckDuckGo, bez API kljuca)
# ──────────────────────────────────────────────
def _web_search(upit: str, limit: int = 3) -> list[dict]:
    """Vraca listu {'title','href','body'} sa DuckDuckGo-a ili baca exception."""
    if DDGS is None:
        raise RuntimeError("ddgs nije instaliran (pip install ddgs)")
    with DDGS() as ddgs:
        return list(ddgs.text(upit, max_results=limit))

# ──────────────────────────────────────────────
# Vrijeme (wttr.in + bosanski opis iz weatherCode)
# ──────────────────────────────────────────────
def _dohvati_vrijeme(grad: str = "Sarajevo") -> dict:
    """Vraca current_condition dict sa wttr.in ili baca exception."""
    url = f"https://wttr.in/{grad}?format=j1"
    resp = requests.get(url, timeout=5)
    return resp.json()["current_condition"][0]

def _opis_bs(cur: dict) -> str:
    """Bosanski opis vremena iz weatherCode (fallback na engleski, skraceno)."""
    code = str(cur.get("weatherCode", ""))
    if code in _WMO_BS:
        return _WMO_BS[code]
    return cur.get("weatherDesc", [{}])[0].get("value", "")[:11]

# ──────────────────────────────────────────────
# MQTT pomocne funkcije
# ──────────────────────────────────────────────
def mqtt_subscribe_once(topic: str, timeout: float = 3.0) -> str | None:
    """Pretplati se na topic i vrati prvu poruku ili None ako timeout."""
    result = [None]
    done = threading.Event()

    def on_message(client, userdata, msg):
        result[0] = msg.payload.decode()
        done.set()

    client = _novi_mqtt_klijent()
    client.on_message = on_message
    client.connect(MQTT_HOST, MQTT_PORT, 10)
    client.subscribe(topic)
    client.loop_start()
    done.wait(timeout)
    client.loop_stop()
    client.disconnect()
    return result[0]

def mqtt_publish(topic: str, payload: str):
    publish.single(topic, payload, hostname=MQTT_HOST, port=MQTT_PORT)

def pico_cmd(payload: dict):
    mqtt_publish(TOPIC_PICO_CMD, json.dumps(payload))

def tasmota_zatrazi_sns(timeout: float = 5.0) -> dict | None:
    """
    Subscribe prvo, pa posalji STATUS 10, pa cekaj odgovor.
    Vraca StatusSNS dict ili None.
    """
    rezultat = {"msg": None}
    ev = threading.Event()

    def _on_message(client, userdata, msg):
        rezultat["msg"] = msg.payload.decode()
        ev.set()

    c = _novi_mqtt_klijent()
    c.on_message = _on_message
    c.connect(MQTT_HOST, MQTT_PORT, 60)
    c.subscribe(TOPIC_STAT_SNS)
    c.loop_start()
    time.sleep(0.2)
    c.publish(f"cmnd/{TIM}_tasmota/STATUS", "10")
    ev.wait(timeout)
    c.loop_stop()
    c.disconnect()

    if not rezultat["msg"]:
        return None
    try:
        return json.loads(rezultat["msg"]).get("StatusSNS", {})
    except Exception:
        return None

# ──────────────────────────────────────────────
# FastMCP server
# ──────────────────────────────────────────────
mcp = FastMCP("NLHomeSystem")

# ══════════════════════════════════════════════
# 7.0 SISTEMSKO VRIJEME (sat sa Raspberry Pi)
# ══════════════════════════════════════════════

@mcp.tool()
def ocitaj_vrijeme() -> str:
    """Vraca tacno sistemsko vrijeme i datum sa Raspberry Pi (lokalna zona)."""
    sad = datetime.datetime.now()
    dani = ["ponedjeljak", "utorak", "srijeda", "cetvrtak",
            "petak", "subota", "nedjelja"]
    return json.dumps({
        "vrijeme": sad.strftime("%H:%M"),
        "datum": sad.strftime("%d.%m.%Y"),
        "dan": dani[sad.weekday()],
    }, ensure_ascii=False)

# ══════════════════════════════════════════════
# 7.1 SENZORSKA OCITANJA
# ══════════════════════════════════════════════

@mcp.tool()
def ocitaj_klimu() -> str:
    """Vraca trenutnu temperaturu i vlaznost iz DHT11 senzora (Tasmota)."""
    sns = tasmota_zatrazi_sns()
    if sns is None:
        return "Greska: Nema odgovora od Tasmota uredjaja."
    try:
        dht = sns.get("DHT11", {})
        return json.dumps({
            "temperatura": dht.get("Temperature"),
            "vlaznost": dht.get("Humidity")
        }, ensure_ascii=False)
    except Exception as e:
        return f"Greska parsiranja: {e}"

@mcp.tool()
def ocitaj_snagu() -> str:
    """Vraca trenutnu snagu u vatima (potenciometar -> ANALOG A1)."""
    sns = tasmota_zatrazi_sns()
    if sns is None:
        return "Greska: Nema odgovora od Tasmota uredjaja."
    try:
        analog = sns.get("ANALOG", {})
        return json.dumps({
            "snaga_W": analog.get("A1")
        }, ensure_ascii=False)
    except Exception as e:
        return f"Greska: {e}"

@mcp.tool()
def ocitaj_osvjetljenje() -> str:
    """Vraca nivo ambijentalnog svjetla s LDR senzora (ANALOG Illuminance1)."""
    sns = tasmota_zatrazi_sns()
    if sns is None:
        return "Greska: Nema odgovora od Tasmota uredjaja."
    try:
        analog = sns.get("ANALOG", {})
        return json.dumps({
            "osvjetljenje": analog.get("Illuminance1")
        }, ensure_ascii=False)
    except Exception as e:
        return f"Greska: {e}"

@mcp.tool()
def ocitaj_kretanje() -> str:
    """
    Vraca da li je nedavno bilo kretanja (PIR senzor, Switch2 na stat/RESULT).
    Cita zadnje poznato stanje iz baze (puni ga pozadinski listener).
    'nedavno' = unutar zadnjih 30 sekundi.
    """
    db = load_db()
    zk = db.get("zadnje_kretanje", {"ts": 0, "kretanje": False})
    nedavno = (time.time() - zk.get("ts", 0)) < 30
    return json.dumps({
        "kretanje": bool(zk.get("kretanje")) and nedavno,
        "zadnji_put_prije_s": round(time.time() - zk.get("ts", 0), 1) if zk.get("ts") else None,
    }, ensure_ascii=False)

@mcp.tool()
def ocitaj_tastere() -> str:
    """
    Vraca koji je taster zadnji pritisnut na panelu (pico/state {"klik": N}).
    Cita iz baze (puni ga pozadinski listener).
        0 - temperatura   1 - vlaga      2 - gorivo
        3 - potrosnja     4 - vrijeme    5 - podsjetnik
    """
    db = load_db()
    zk = db.get("zadnji_klik", {"ts": 0, "klik": None})
    nazivi = {0: "temperatura", 1: "vlaga", 2: "gorivo",
              3: "potrosnja", 4: "vrijeme", 5: "podsjetnik"}
    return json.dumps({
        "klik": zk.get("klik"),
        "naziv": nazivi.get(zk.get("klik"), "nepoznato"),
        "prije_s": round(time.time() - zk.get("ts", 0), 1) if zk.get("ts") else None,
    }, ensure_ascii=False)

# ══════════════════════════════════════════════
# 7.2 UPRAVLJANJE I AKTUACIJA
# ══════════════════════════════════════════════

@mcp.tool()
def postavi_relej(broj: int, stanje: str) -> str:
    """
    Ukljuci/iskljuci relej.
    broj: 1 (klima) ili 2 (ventilator)
    stanje: '1' (ukljuceno) ili '0' (iskljuceno)
    """
    if broj not in (1, 2):
        return "Greska: broj mora biti 1 ili 2."
    if stanje not in ("0", "1"):
        return "Greska: stanje mora biti 0 ili 1."
    topic = TOPIC_POWER1 if broj == 1 else TOPIC_POWER2
    naziv = "klima" if broj == 1 else "ventilator"
    mqtt_publish(topic, stanje)
    log_akciju(f"Relej {broj} ({naziv}) postavljen na {stanje}")
    return f"Relej {broj} ({naziv}): {stanje}"

@mcp.tool()
def postavi_ciljnu_temperaturu(temp: float) -> str:
    """
    Postavlja ciljnu temperaturu. Pozadinski proces
    automatski upravlja relejom klime (POWER1) prema ocitanju DHT11.
    """
    with db_session() as db:
        db["ciljna_temperatura"] = temp
    log_akciju(f"Ciljna temperatura postavljena na {temp}C")
    return f"Ciljna temperatura: {temp}C. Automatska regulacija aktivna."

@mcp.tool()
def postavi_roletne(pozicija: str) -> str:
    """
    Upravlja roletama putem servo motora (pico/cmd).
    pozicija: 'otvoreno', 'poluotvoreno' ili 'zatvoreno'
    """
    mapa = {"otvoreno": 0, "poluotvoreno": 90, "zatvoreno": 180}
    p = pozicija.lower()
    if p not in mapa:
        return f"Greska: pozicija mora biti jedna od: {list(mapa.keys())}"
    pico_cmd({"servo": mapa[p]})
    log_akciju(f"Roletne postavljene na: {p}")
    return f"Roletne: {p} (servo={mapa[p]})"

@mcp.tool()
def postavi_indikator_po_osvjetljenju(osvjetljenje: float) -> str:
    """
    Postavlja RGB diodu prema nivou osvjetljenja (0.0 – 1.0).
    Pravilo: mračno → topla boja, svijetlo → hladna boja.
      0.0 – 0.25  → narandzasto  (vrlo mračno, toplo)
      0.25 – 0.50 → zuto         (polutama, blago toplo)
      0.50 – 0.75 → bijelo       (normalno, neutralno)
      0.75 – 1.0  → plavo        (jako osvjetljenje, hladno)
    """
    if not (0.0 <= osvjetljenje <= 1.0):
        return "Greška: osvjetljenje mora biti između 0.0 i 1.0"

    if osvjetljenje < 0.25:
        boja, rgb = "narandzasto", (255, 165, 0)
    elif osvjetljenje < 0.50:
        boja, rgb = "zuto",        (255, 255, 0)
    elif osvjetljenje < 0.75:
        boja, rgb = "bijelo",      (255, 255, 255)
    else:
        boja, rgb = "plavo",       (0,   0,255)

    r, g, b = rgb
    pico_cmd({"rgb": {"r": r, "g": g, "b": b}})
    log_akciju(f"Osvjetljenje {osvjetljenje:.2f} → indikator: {boja}")
    return f"Osvjetljenje {osvjetljenje:.2f} → {boja} RGB({r},{g},{b})"

# ══════════════════════════════════════════════
# 7.3 PRIKAZ
# ══════════════════════════════════════════════

@mcp.tool()
def prikazi_na_displeju(tekst: str) -> str:
    """Ispisuje poruku, podsjetnik ili status na LCD displej."""
    pico_cmd({"lcd": tekst})
    log_akciju(f"LCD prikaz: {tekst}")
    return f"LCD: '{tekst}'"

@mcp.tool()
def postavi_mod_displeja(mod: str) -> str:
    """
    Postavlja mod LCD displeja.
    mod: 'podsjetnici', 'status', 'preporuke'
    """
    modovi = ["podsjetnici", "status", "preporuke"]
    if mod.lower() not in modovi:
        return f"Greska: mod mora biti jedan od {modovi}"
    pico_cmd({"lcd": f"[MOD:{mod.upper()}]"})
    return f"Mod displeja: {mod}"

@mcp.tool()
def prikazi_rezultat(d1: str, d2: str, aktivan: bool = True) -> str:
    """
    Prikazuje numericke rezultate na dva 7-segmentna displeja.
    d1, d2: stringovi do 4 cifre, npr. '2', '1' za rezultat utakmice
    aktivan: True = upali displej, False = ugasi displej (default: True)
    """
    pico_cmd({"seg": {"d1": str(d1), "d2": str(d2), "active": 1 if aktivan else 0}})
    log_akciju(f"7-seg prikaz: d1={d1}, d2={d2}, aktivan={aktivan}")
    return f"7-segment: d1={d1}, d2={d2}, aktivan={aktivan}"

@mcp.tool()
def ocisti_displeje() -> str:
    """Brise LCD i 7-segmentne prikaze."""
    pico_cmd({"lcd": "", "seg": {"d1": "  ", "d2": "  ", "active": 0}})
    return "Displezi ocisceni."

# ══════════════════════════════════════════════
# 7.4 PERZISTENCIJA - Podsjetnici i pravila
# ══════════════════════════════════════════════

@mcp.tool()
def dodaj_podsjetnik(opis: str, vrijeme: str) -> str:
    """
    Registruje podsjetnik.
    opis: tekst podsjetnika
    vrijeme: ISO format, npr. '2026-12-25T09:00'
    Vraca ID podsjetnika.
    """
    pid = f"p{int(time.time())}"
    with db_session() as db:
        db["podsjetnici"].append({
            "id": pid,
            "opis": opis,
            "vrijeme": vrijeme,
            "aktivan": True
        })
    log_akciju(f"Dodan podsjetnik [{pid}]: {opis} u {vrijeme}")
    return f"Podsjetnik dodan. ID: {pid}"

@mcp.tool()
def daj_podsjetnike() -> str:
    """Vraca listu svih aktivnih podsjetnika."""
    db = load_db()
    aktivni = [p for p in db["podsjetnici"] if p.get("aktivan")]
    if not aktivni:
        return "Nema aktivnih podsjetnika."
    return json.dumps(aktivni, ensure_ascii=False, indent=2)

@mcp.tool()
def obrisi_podsjetnik(id: str) -> str:
    """Brise podsjetnik prema ID-u."""
    with db_session() as db:
        for p in db["podsjetnici"]:
            if p["id"] == id:
                p["aktivan"] = False
                log_akciju(f"Obrisan podsjetnik [{id}]")
                return f"Podsjetnik {id} obrisan."
    return f"Podsjetnik {id} nije pronadjen."

@mcp.tool()
def dodaj_pravilo(uslov: str, akcija: str) -> str:
    """
    Postavlja automatizacijsko pravilo.
    uslov: npr. 'temperatura > 25', 'kretanje == true'
    akcija: npr. 'postavi_relej(1, 1)', 'postavi_indikator(crveno)'
    Vraca ID pravila.
    """
    rid = f"r{int(time.time())}"
    with db_session() as db:
        db["pravila"].append({
            "id": rid,
            "uslov": uslov,
            "akcija": akcija,
            "aktivno": True
        })
    log_akciju(f"Dodano pravilo [{rid}]: IF {uslov} THEN {akcija}")
    return f"Pravilo dodano. ID: {rid}"

@mcp.tool()
def daj_pravila() -> str:
    """Vraca listu svih aktivnih pravila."""
    db = load_db()
    aktivna = [r for r in db["pravila"] if r.get("aktivno")]
    if not aktivna:
        return "Nema aktivnih pravila."
    return json.dumps(aktivna, ensure_ascii=False, indent=2)

@mcp.tool()
def obrisi_pravilo(id: str) -> str:
    """Uklanja/deaktivira pravilo prema ID-u."""
    with db_session() as db:
        for r in db["pravila"]:
            if r["id"] == id:
                r["aktivno"] = False
                log_akciju(f"Obrisano pravilo [{id}]")
                return f"Pravilo {id} deaktivirano."
    return f"Pravilo {id} nije pronadjeno."

# ══════════════════════════════════════════════
# 7.5 PERZISTENCIJA - Scene, potrosnja, dnevnik
# ══════════════════════════════════════════════

@mcp.tool()
def daj_izvjestaj_potrosnje(period: str) -> str:
    """
    Vraca izvjestaj potrosnje elektricne energije.
    period: 'danas', 'sedmica', 'mjesec'
    Cita A1 vrijednost iz log_potrosnje (akumulira pozadinski proces).
    """
    db = load_db()
    log = db.get("log_potrosnje", [])

    now = datetime.datetime.now()
    if period == "danas":
        granica = now.date().isoformat()
        filtered = [l for l in log if l.get("ts", "").startswith(granica)]
    elif period == "sedmica":
        granica = (now - datetime.timedelta(days=7)).isoformat()
        filtered = [l for l in log if l.get("ts", "") >= granica]
    elif period == "mjesec":
        granica = (now - datetime.timedelta(days=30)).isoformat()
        filtered = [l for l in log if l.get("ts", "") >= granica]
    else:
        filtered = log

    if not filtered:
        return json.dumps({
            "period": period,
            "poruka": "Nema podataka o potrosnji za ovaj period."
        }, ensure_ascii=False, indent=2)

    vrijednosti = [l.get("snaga_W", 0) for l in filtered if l.get("snaga_W") is not None]
    if not vrijednosti:
        return json.dumps({"period": period, "poruka": "Nema A1 ocitavanja u logu."}, ensure_ascii=False)

    prosjek_W = sum(vrijednosti) / len(vrijednosti)
    # Ocitavanja svakih 30s -> 30/3600 sati po ocitavanju
    kwh = (prosjek_W * len(vrijednosti) * 30) / 3600000
    trosak = kwh * CIJENA_KWH_BAM

    rezultat = {
        "period": period,
        "broj_ocitavanja": len(vrijednosti),
        "prosjek_W": round(prosjek_W, 2),
        "ukupno_kWh": round(kwh, 4),
        "trosak_KM": round(trosak, 4),
    }

    tekst = f"Potrosnja: {round(kwh,4)} kWh | {round(trosak,4)} KM"
    pico_cmd({"lcd": tekst})
    log_akciju(f"Izvjestaj potrosnje ({period}): {tekst}")
    return json.dumps(rezultat, ensure_ascii=False, indent=2)

@mcp.tool()
def daj_dnevnik_akcija(period: str) -> str:
    """
    Vraca historiju akcija sistema.
    period: 'danas', 'sat', 'sve'
    """
    db = load_db()
    log = db.get("log_akcija", [])

    if period == "sat":
        granica = (datetime.datetime.now() - datetime.timedelta(hours=1)).isoformat()
        log = [l for l in log if l["ts"] >= granica]
    elif period == "danas":
        granica = datetime.datetime.now().date().isoformat()
        log = [l for l in log if l["ts"].startswith(granica)]

    if not log:
        return f"Nema akcija za period: {period}"
    return json.dumps(log[-50:], ensure_ascii=False, indent=2)

# ══════════════════════════════════════════════
# 7.6 NOTIFIKACIJE I VANJSKI IZVORI
# ══════════════════════════════════════════════

@mcp.tool()
def posalji_notifikaciju(kanal: str, tekst: str) -> str:
    """
    Salje obavijest korisniku.
    kanal: 'lcd' ili 'telegram'
    tekst: sadrzaj poruke
    """
    kanal = kanal.lower()
    if kanal == "lcd":
        pico_cmd({"lcd": tekst})
        log_akciju(f"Notifikacija (LCD): {tekst}")
        return f"Notifikacija prikazana na LCD: '{tekst}'"
    elif kanal == "telegram":
        log_akciju(f"Notifikacija (Telegram): {tekst}")
        return f"[Telegram] Notifikacija logirana (konfigurisati BOT_TOKEN i CHAT_ID): '{tekst}'"
    else:
        return "Greska: kanal mora biti 'lcd' ili 'telegram'."

@mcp.tool()
def daj_prognozu(grad: str) -> str:
    """
    Vraca vremensku prognozu za grad (za upravljanje roletama, klimom).
    Opis je na bosanskom (iz weatherCode).
    """
    try:
        cur = _dohvati_vrijeme(grad)
        return json.dumps({
            "grad": grad,
            "temperatura_C": cur["temp_C"],
            "opis": _opis_bs(cur),
            "opis_en": cur["weatherDesc"][0]["value"],
            "vlaznost": cur["humidity"],
            "vjetar_kmh": cur["windspeedKmph"]
        }, ensure_ascii=False)
    except Exception as e:
        return f"Greska pri dohvatu prognoze: {e}"



@mcp.tool()
def postavi_cijenu_goriva(dizel: float, benzin: float) -> str:
    """
    Rucno postavlja trenutne cijene goriva (KM/litar). Spremaju se u bazu,
    nisu u kodu - mijenjaju se bez ponovnog deploya.
    """
    with db_session() as db:
        db["cijene_goriva"] = {
            "dizel": dizel,
            "benzin": benzin,
            "azurirano": datetime.datetime.now().isoformat(),
        }
    log_akciju(f"Cijene goriva postavljene: dizel {dizel}, benzin {benzin}")
    return f"Cijene goriva: dizel {dizel:.2f} KM, benzin {benzin:.2f} KM"

@mcp.tool()
def ocitaj_cijenu_goriva() -> str:
    """Vraca trenutno poznate cijene goriva iz baze."""
    db = load_db()
    cg = db.get("cijene_goriva", {})
    if cg.get("dizel") is None and cg.get("benzin") is None:
        return json.dumps({
            "poruka": "Cijene goriva nisu postavljene. Koristi postavi_cijenu_goriva() ili dohvati_cijene_goriva_web()."
        }, ensure_ascii=False)
    return json.dumps(cg, ensure_ascii=False)

@mcp.tool()
def dohvati_cijene_goriva_web() -> str:
  
    try:
        rezultati = _web_search("trenutna cijena dizela benzina Bosna KM litar", 5)
    except Exception as e:
        return f"Greska pri pretrazi: {e}"
    tekst = " ".join(r.get("body", "") for r in rezultati)
    kandidati = sorted({float(x.replace(",", ".")) for x in re.findall(r"\b[123][.,]\d{2}\b", tekst)})
    if not kandidati:
        return json.dumps({
            "poruka": "Nije pronadjena nijedna cijena. Postavi rucno sa postavi_cijenu_goriva()."
        }, ensure_ascii=False)
    return json.dumps({
        "kandidati_KM": kandidati,
        "napomena": "NEPOUZDANO - potvrdi i upisi tacne vrijednosti sa postavi_cijenu_goriva()."
    }, ensure_ascii=False)

@mcp.tool()
def web_pretraga(upit: str, mod: str = "tekst") -> str:
    """
    Jednokratna web pretraga (rezultati utakmica, vijesti itd.)
    upit: tekst pretrage
    mod: 'tekst' (default) ili 'vijesti'
    Koristi DuckDuckGo (ddgs). Napomena: vraca opise stranica, ne strukturisane
    podatke - ne tretiraj rezultat kao tacan broj.
    """
    try:
        rezultati = _web_search(upit, 3)
        log_akciju(f"Web pretraga ({mod}): {upit}")
        if not rezultati:
            return json.dumps({"upit": upit, "poruka": "Nema rezultata."}, ensure_ascii=False)
        return json.dumps(rezultati, ensure_ascii=False, indent=2)
    except Exception as e:
        return f"Greska pri pretrazi: {e}. Upit: '{upit}'"

@mcp.tool()
def prati_informaciju(upit: str, interval: int = 300) -> str:
    """
    Registruje periodicno pracenje informacije (npr. rezultat utakmice).
    upit: sta pratiti
    interval: koliko sekundi izmedju provjera (default: 300 = 5 minuta)
    Vraca ID pracenja.
    """
    pid = f"w{int(time.time())}"
    with db_session() as db:
        db["pracenja"].append({
            "id": pid,
            "upit": upit,
            "interval": interval,
            "aktivno": True,
            "zadnji_rezultat": None,
            "zadnja_provjera": None
        })
    log_akciju(f"Registrovano pracenje [{pid}]: '{upit}' svakih {interval}s")
    return f"Pracenje registrovano. ID: {pid}. Pozadinski proces osvjezava svakih {interval}s."

@mcp.tool()
def zaustavi_pracenje(id: str) -> str:
    """Zaustavlja aktivno pracenje prema ID-u."""
    with db_session() as db:
        for p in db["pracenja"]:
            if p["id"] == id:
                p["aktivno"] = False
                log_akciju(f"Zaustavljeno pracenje [{id}]")
                return f"Pracenje {id} zaustavljeno."
    return f"Pracenje {id} nije pronadjeno."


# ══════════════════════════════════════════════
# Pracenje potrosnje (alternativni, in-memory akumulator)
# ══════════════════════════════════════════════

_dnevna_potrosnja_wh: float = 0.0
_zadnje_ocitanje_ts: float  = 0.0
_dan_akumulatora: str       = datetime.date.today().isoformat()

@mcp.tool()
def azuriraj_potrosnju() -> str:
    """
    Poziva ocitaj_snagu(), akumulira Wh za danas i sprema u fajl.
    Treba se zvati periodično (npr. svakih 15 min).
    """
    global _dnevna_potrosnja_wh, _zadnje_ocitanje_ts, _dan_akumulatora

    rezultat   = json.loads(ocitaj_snagu())
    trenutni_w = rezultat.get("snaga_W")
    sad        = time.time()

    if trenutni_w is None:
        return "Greska: A1 analogni pin nije vratio vrijednost."

    # reset akumulatora kad pocne novi dan
    danas = datetime.date.today().isoformat()
    if danas != _dan_akumulatora:
        _dnevna_potrosnja_wh = 0.0
        _zadnje_ocitanje_ts = 0.0
        _dan_akumulatora = danas

    if _zadnje_ocitanje_ts > 0:
        dt_h = (sad - _zadnje_ocitanje_ts) / 3600.0
        _dnevna_potrosnja_wh += trenutni_w * dt_h

    _zadnje_ocitanje_ts = sad

    zapis = {
        "datum":        danas,
        "ukupno_wh":    round(_dnevna_potrosnja_wh, 3),
        "zadnji_watts": trenutni_w,
        "zadnji_ts":    sad,
    }
    with open("potrosnja_log.json", "w") as f:
        json.dump(zapis, f, indent=2)

    log_akciju(f"Potrosnja azurirana: {trenutni_w}W, ukupno danas: {_dnevna_potrosnja_wh:.1f}Wh")
    return f"Trenutno: {trenutni_w}W | Danas ukupno: {_dnevna_potrosnja_wh/1000:.4f} kWh"


@mcp.tool()
def izracunaj_racun() -> str:
    """
    Ucitava dnevnu potrosnju i vraca procijenjeni mjesecni racun u BAM.
    """
    try:
        with open("potrosnja_log.json") as f:
            zapis = json.load(f)
        dnevno_kwh = zapis["ukupno_wh"] / 1000.0
    except FileNotFoundError:
        return "Nema podataka o potrosnji. Prvo pokreni azuriraj_potrosnju()."

    mjesecno_kwh = dnevno_kwh * 30
    iznos_bam    = mjesecno_kwh * CIJENA_KWH_BAM

    log_akciju(f"Racun izracunat: {iznos_bam:.2f} BAM")

    return (
        f"Procjena racuna\n"
        f"  Cijena kWh     : {CIJENA_KWH_BAM:.2f} BAM\n"
        f"  Potrosnja danas: {dnevno_kwh:.4f} kWh\n"
        f"  Proj. mjesec   : {mjesecno_kwh:.2f} kWh\n"
        f"  Iznos          : {iznos_bam:.2f} BAM"
    )

# ══════════════════════════════════════════════
# POZADINSKI LISTENER (PIR + tasteri)
# ══════════════════════════════════════════════

def _on_bg_message(client, userdata, msg):
    topic = msg.topic
    try:
        data = json.loads(msg.payload.decode())
    except Exception:
        return

    if topic == TOPIC_STAT_RESULT:
        # PIR stize kao {"Switch2":{"Action":"TOGGLE"}}
        if "Switch2" in data:
            with db_session() as db:
                db["zadnje_kretanje"] = {"ts": time.time(), "kretanje": True}
        return

    if topic == TOPIC_PICO_STATE and "klik" in data:
        klik = data["klik"]
        with db_session() as db:
            db["zadnji_klik"] = {"ts": time.time(), "klik": klik}
            aktivni = [p for p in db.get("podsjetnici", []) if p.get("aktivan")]

        # ── obrada klika (mrezne operacije van locka) ──
        if klik == 0:  # temperatura
            sns = tasmota_zatrazi_sns(timeout=5.0)
            if sns:
                temp = sns.get("DHT11", {}).get("Temperature", "?")
                pico_cmd({"lcd": f"Temp: {temp}C"})

        elif klik == 1:  # vlaga
            sns = tasmota_zatrazi_sns(timeout=5.0)
            if sns:
                vlaga = sns.get("DHT11", {}).get("Humidity", "?")
                pico_cmd({"lcd": f"Vlaga: {vlaga}%"})

        elif klik == 2:  # gorivo (iz baze, ne hardkodirano)
            db2 = load_db()
            cg = db2.get("cijene_goriva", {})
            d, b = cg.get("dizel"), cg.get("benzin")
            if d is not None and b is not None:
                pico_cmd({"lcd": f"Dz{d:.2f} Bn{b:.2f}"})
            elif d is not None or b is not None:
                v = d if d is not None else b
                pico_cmd({"lcd": f"Gorivo {v:.2f}KM"})
            else:
                pico_cmd({"lcd": "Cijena N/A"})

        elif klik == 3:  # potrosnja
            try:
                with open("potrosnja_log.json") as f:
                    zapis = json.load(f)
                kwh = zapis["ukupno_wh"] / 1000.0
                bam = kwh * 30 * CIJENA_KWH_BAM
                pico_cmd({"lcd": f"{kwh:.3f}kWh ~{bam:.2f}BAM"})
            except FileNotFoundError:
                pico_cmd({"lcd": "Nema podataka"})

        elif klik == 4:  # vrijeme (opis na bosanskom iz weatherCode)
            try:
                cur = _dohvati_vrijeme("Sarajevo")
                tekst = f"{cur['temp_C']}C {_opis_bs(cur)}"
                _spremi_cache("vrijeme", tekst)
                pico_cmd({"lcd": tekst})
            except Exception:
                zadnje = _procitaj_cache("vrijeme")
                pico_cmd({"lcd": zadnje if zadnje else "Vrijeme N/A"})

        elif klik == 5:  # podsjetnik
            if aktivni:
                pico_cmd({"lcd": f"P: {aktivni[0]['opis'][:28]}"})
            else:
                pico_cmd({"lcd": "Nema podsjetnika"})

def pokreni_listener():
    """Pozadinski thread koji slusha PIR i tastere."""
    c = _novi_mqtt_klijent()
    c.on_message = _on_bg_message
    c.connect(MQTT_HOST, MQTT_PORT, 60)
    c.subscribe(TOPIC_STAT_RESULT)
    c.subscribe(TOPIC_PICO_STATE)
    c.loop_forever()


def pozadinski_proces():
    """
    Pozadinski thread koji svakih 30s:
    1. Regulise temperaturu prema ciljanoj vrijednosti (STATUS 10)
    2. Provjera i okida podsjetnike
    3. Osvjezava aktivna pracenja
    4. Loguje potrosnju (A1)

    Spore mrezne operacije se rade VAN locka; baza se zakljucava samo
    nakratko za upis, da ne pregazi PIR/klik evente listenera.
    """
    while True:
        try:
            # --- snapshot konfiguracije (kratko zakljucano) ---
            with _db_lock:
                db0 = load_db()
                ciljna = db0.get("ciljna_temperatura")
                aktivna_pracenja = [dict(p) for p in db0.get("pracenja", []) if p.get("aktivno")]

            # --- spore mrezne operacije (bez locka) ---
            sns = tasmota_zatrazi_sns(timeout=5.0)

            nova_pracenja = {}
            for pr in aktivna_pracenja:
                zadnja = pr.get("zadnja_provjera")
                interval = pr.get("interval", 300)
                if zadnja is None or (time.time() - datetime.datetime.fromisoformat(zadnja).timestamp()) >= interval:
                    try:
                        nova_pracenja[pr["id"]] = {
                            "zadnji_rezultat": _web_search(pr["upit"], 1),
                            "zadnja_provjera": datetime.datetime.now().isoformat(),
                        }
                    except Exception:
                        pass

            # --- regulacija klime (ne dira bazu) ---
            if sns and ciljna is not None:
                temp = sns.get("DHT11", {}).get("Temperature")
                if temp is not None:
                    if float(temp) > float(ciljna) + 0.5:
                        mqtt_publish(TOPIC_POWER1, "1")
                    elif float(temp) < float(ciljna) - 0.5:
                        mqtt_publish(TOPIC_POWER1, "0")

            # --- kratak zakljucani upis svjezih izmjena ---
            okinuti_podsjetnici = []
            with db_session() as db:
                # potrosnja
                if sns:
                    a1 = sns.get("ANALOG", {}).get("A1")
                    if a1 is not None:
                        db.setdefault("log_potrosnje", []).append({
                            "ts": datetime.datetime.now().isoformat(),
                            "snaga_W": a1
                        })
                        db["log_potrosnje"] = db["log_potrosnje"][-2880:]  # ~24h pri 30s

                # podsjetnici
                sada = datetime.datetime.now().isoformat()
                for p in db.get("podsjetnici", []):
                    if p.get("aktivan") and p.get("vrijeme", "") <= sada:
                        okinuti_podsjetnici.append(p["opis"])
                        p["aktivan"] = False

                # upisi svjeze rezultate pracenja u stvarne zapise
                for pr in db.get("pracenja", []):
                    if pr["id"] in nova_pracenja:
                        pr.update(nova_pracenja[pr["id"]])

            # --- akcije van locka ---
            for opis in okinuti_podsjetnici:
                pico_cmd({"lcd": f"PODSJETNIK: {opis}"})
                log_akciju(f"Okidan podsjetnik: {opis}")

        except Exception:
            pass

        time.sleep(30)

# ──────────────────────────────────────────────
# Pokretanje
# ──────────────────────────────────────────────
if __name__ == "__main__":
    threading.Thread(target=pokreni_listener, daemon=True).start()
    threading.Thread(target=pozadinski_proces, daemon=True).start()
    mcp.run()