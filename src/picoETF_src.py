import machine
import utime
import network
import ujson
from umqtt.simple import MQTTClient

# --- PODEŠAVANJA MREŽE I MQTT ---
wifi_ssid = "Lab220"
wifi_password = "lab220lozinka"
mqtt_host = "195.130.59.221"
temain = b"tim12/pico/cmd"
temaout = b"tim12/pico/state"

# Povezivanje na WiFi
wlan = network.WLAN(network.STA_IF)
wlan.active(True)
wlan.connect(wifi_ssid, wifi_password)
print('Waiting for connection...')
while not wlan.isconnected():
    utime.sleep(1)
print("Connected to WiFi!")

# --- GLOBALNE VARIJABLE ZA PODATKE ---
d1_rezultat = "0"
d2_rezultat = "0"
zadnji_lcd_tekst = ""
seg_active = 0  # 0 = ugašeno, 1 = upaljeno
stanje = 0      # Varijabla koja prati klikove

# Zastavice za prekide
akcija_taster1 = False
akcija_taster2 = False

# --- KONTROLA LCD EKRANA ---
rs = machine.Pin(2, machine.Pin.OUT)
e = machine.Pin(3, machine.Pin.OUT)
d4 = machine.Pin(4, machine.Pin.OUT)
d5 = machine.Pin(5, machine.Pin.OUT)
d6 = machine.Pin(6, machine.Pin.OUT)
d7 = machine.Pin(7, machine.Pin.OUT)

def pulseE():
    e.value(1)
    utime.sleep_us(40)
    e.value(0)
    utime.sleep_us(40)

def send2LCD4(BinNum):
    d4.value((BinNum & 0b00000001) >> 0)
    d5.value((BinNum & 0b00000010) >> 1)
    d6.value((BinNum & 0b00000100) >> 2)
    d7.value((BinNum & 0b00001000) >> 3)
    pulseE()

def send2LCD8(BinNum):
    d4.value((BinNum & 0b00010000) >> 4)
    d5.value((BinNum & 0b00100000) >> 5)
    d6.value((BinNum & 0b01000000) >> 6)
    d7.value((BinNum & 0b10000000) >> 7)
    pulseE()
    d4.value((BinNum & 0b00000001) >> 0)
    d5.value((BinNum & 0b00000010) >> 1)
    d6.value((BinNum & 0b00000100) >> 2)
    d7.value((BinNum & 0b00001000) >> 3)
    pulseE()

def setUpLCD():
    rs.value(0)
    send2LCD4(0b0011)
    send2LCD4(0b0011)
    send2LCD4(0b0011)
    send2LCD4(0b0010)
    send2LCD8(0b00101000)
    send2LCD8(0b00001100)
    send2LCD8(0b00000110)
    send2LCD8(0b00000001)
    utime.sleep_ms(2)

def lcd_obrisi():
    rs.value(0)
    send2LCD8(0b00000001)
    utime.sleep_ms(5)
    send2LCD8(0b00000010)
    utime.sleep_ms(5)

def lcd_goto_red2():
    rs.value(0)
    send2LCD8(0b11000000)  # DDRAM adresa 0x40 = početak 2. reda
    utime.sleep_ms(2)

def lcd_ispisi(tekst):
    linije = tekst.split("\\n")
    rs.value(1)
    for x in linije[0][:16]:  # Max 16 znakova u prvom redu
        send2LCD8(ord(x))
        utime.sleep_ms(2)
    rs.value(0)
    if len(linije) > 1:
        lcd_goto_red2()
        rs.value(1)
        for x in linije[1][:16]:  # Max 16 znakova u drugom redu
            send2LCD8(ord(x))
            utime.sleep_ms(2)
        rs.value(0)

# --- KONTROLA 7-SEGMENTNOG DISPLEJA ---
segments = [
    machine.Pin(10, machine.Pin.OUT), machine.Pin(11, machine.Pin.OUT),
    machine.Pin(12, machine.Pin.OUT), machine.Pin(13, machine.Pin.OUT),
    machine.Pin(14, machine.Pin.OUT), machine.Pin(15, machine.Pin.OUT),
    machine.Pin(16, machine.Pin.OUT), machine.Pin(17, machine.Pin.OUT)
]
digits = [
    machine.Pin(18, machine.Pin.OUT), machine.Pin(19, machine.Pin.OUT),
    machine.Pin(20, machine.Pin.OUT), machine.Pin(21, machine.Pin.OUT)
]

brojevi = {
    '0': [0, 0, 0, 0, 0, 0, 1, 1], '1': [1, 0, 0, 1, 1, 1, 1, 1],
    '2': [0, 0, 1, 0, 0, 1, 1, 0], '3': [0, 0, 0, 0, 1, 1, 1, 0],
    '4': [1, 0, 0, 1, 1, 0, 1, 0], '5': [0, 1, 0, 0, 1, 0, 1, 0],
    '6': [0, 1, 0, 0, 0, 0, 1, 0], '7': [0, 0, 0, 1, 1, 1, 1, 1],
    '8': [0, 0, 0, 0, 0, 0, 1, 0], '9': [0, 0, 0, 0, 1, 0, 1, 0],
    'E': [0, 1, 1, 0, 0, 0, 1, 0], 'e': [0, 1, 1, 0, 0, 0, 1, 0],
    ' ': [1, 1, 1, 1, 1, 1, 1, 1]
}

# --- KONTROLA RGB DIODE ---
pwm_26 = machine.PWM(machine.Pin(26))
pwm_27 = machine.PWM(machine.Pin(27))
pwm_28 = machine.PWM(machine.Pin(28))
pwm_26.freq(1000)
pwm_27.freq(1000)
pwm_28.freq(1000)

def postavi_rgb(r, g, b):
    pwm_26.duty_u16(r * 257)
    pwm_27.duty_u16(g * 257)
    pwm_28.duty_u16(b * 257)

# --- KONTROLA SERVO MOTORA ---
servo = machine.PWM(machine.Pin(22))
servo.freq(50)

def postavi_servo(stepeni):
    if stepeni < 0: stepeni = 0
    if stepeni > 180: stepeni = 180
    duty = int(3276 + (stepeni / 180) * (6553 - 3276))
    servo.duty_u16(duty)

# --- FUNKCIJE PREKIDA (ISR) ---
# Ove funkcije moraju biti što kraće i brže!
def taster1_isr(pin):
    global akcija_taster1
    akcija_taster1 = True

def taster2_isr(pin):
    global akcija_taster2
    akcija_taster2 = True

# --- KONTROLA TASTERA (Inicijalizacija prekida) ---
taster1 = machine.Pin(0, machine.Pin.IN, machine.Pin.PULL_DOWN)
taster1.irq(trigger=machine.Pin.IRQ_RISING, handler=taster1_isr)

taster2 = machine.Pin(1, machine.Pin.IN, machine.Pin.PULL_DOWN)
taster2.irq(trigger=machine.Pin.IRQ_RISING, handler=taster2_isr)

zadnji_klik_t1 = 0
zadnji_klik_t2 = 0

def posalji_stanje():
    poruka = ujson.dumps({"klik": stanje})
    mqtt_conn.publish(temaout, poruka.encode('utf-8'))
    print("Poslano na MQTT:", poruka)

# --- MQTT CALLBACK ---
def mqtt_prijem(topic, msg):
    global d1_rezultat, d2_rezultat, seg_active, zadnji_lcd_tekst
    print("Primljeno na", topic, ":", msg)
    try:
        podaci = ujson.loads(msg)
        
        # Osvježi RGB
        if "rgb" in podaci:
            b = podaci["rgb"].get("r", 0) #zamijenjeno ovo
            g = podaci["rgb"].get("g", 0)
            r = podaci["rgb"].get("b", 0)
            postavi_rgb(r, g, b)
            
        # Osvježi Servo
        if "servo" in podaci:
            postavi_servo(podaci["servo"])
            
        # Osvježi LCD
        if "lcd" in podaci:
            novi_tekst = podaci["lcd"]
            # Ažuriraj samo ako je tekst drugačiji od onog koji je već na ekranu
            if novi_tekst != zadnji_lcd_tekst:
                lcd_obrisi()
                lcd_ispisi(novi_tekst)
                zadnji_lcd_tekst = novi_tekst
            
        # Osvježi Segmentne rezultate i stanje "active"
        if "seg" in podaci:
            d1_rezultat = podaci["seg"].get("d1", "0")
            d2_rezultat = podaci["seg"].get("d2", "0")
            seg_active = podaci["seg"].get("active", 0)
            
    except ValueError:
        print("Greska: Poruka nije validan JSON!")

# Inicijalizacija MQTT klijenta
mqtt_conn = MQTTClient(client_id='kalin', server=mqtt_host, port=1883)
mqtt_conn.set_callback(mqtt_prijem)
mqtt_conn.connect()
mqtt_conn.subscribe(temain)
print("Spojeno na MQTT broker i pretplaćeno na temu.")

# Početna priprema periferija
setUpLCD()
lcd_ispisi("Sistem spreman!")
postavi_rgb(0, 0, 0)
postavi_servo(0)

# Tajmeri za neblokirajući rad
zadnje_vrijeme_7seg = utime.ticks_ms()
korak_7seg = 0
trenutni_tekst_7seg = "    "

# --- GLAVNA PETLJA ---
while True:
    # 1. Provjera pristiglih MQTT poruka (neblokirajuće)
    mqtt_conn.check_msg()
    
    trenutno_vrijeme = utime.ticks_ms()
    
    # 2. Obrada hardverskih prekida za tastere
    if akcija_taster1:
        akcija_taster1 = False # Odmah spusti zastavicu
        # Debouncing: Ignoriši ako je prošlo manje od 300ms od zadnjeg klika
        if utime.ticks_diff(trenutno_vrijeme, zadnji_klik_t1) > 300:
            stanje = (stanje + 5) % 6
            posalji_stanje()
            zadnji_klik_t1 = trenutno_vrijeme
            
    if akcija_taster2:
        akcija_taster2 = False # Odmah spusti zastavicu
        if utime.ticks_diff(trenutno_vrijeme, zadnji_klik_t2) > 300:
            stanje = (stanje + 1) % 6
            posalji_stanje()
            zadnji_klik_t2 = trenutno_vrijeme

    # 3. Logika ciklusa 7-segmentnog displeja (Svake 2 sekunde)
    if utime.ticks_diff(trenutno_vrijeme, zadnje_vrijeme_7seg) >= 2000:
        if korak_7seg == 0:
            trenutni_tekst_7seg = "E1  "               
        elif korak_7seg == 1:
            trenutni_tekst_7seg = "{:>4}".format(d1_rezultat)[:4] 
        elif korak_7seg == 2:
            trenutni_tekst_7seg = "E2  "               
        elif korak_7seg == 3:
            trenutni_tekst_7seg = "{:>4}".format(d2_rezultat)[:4] 
            
        korak_7seg = (korak_7seg + 1) % 4
        zadnje_vrijeme_7seg = trenutno_vrijeme

    # 4. Multipleksiranje 7-segmentnog displeja - Samo ako je active == 1
    if seg_active == 1:
        for i in range(4):
            for d in digits:
                d.value(0) # Ugasi sve cifre
                
            karakter = trenutni_tekst_7seg[i]
            vrijednosti_segmenata = brojevi.get(karakter, brojevi[' ']) 
            
            for s in range(8):
                segments[s].value(vrijednosti_segmenata[s])
                
            digits[i].value(1) # Upali trenutnu cifru
            utime.sleep_ms(1)
    else:
        # Ako je active == 0, osiguraj da su sve cifre ugašene
        for d in digits:
            d.value(0)