"""Радар воздушных целей. Запуск: pip install aiohttp beautifulsoup4 && python server.py
Открыть на телефоне: http://<IP-компьютера>:8080  (опц. NTFY_TOPIC=мой_топик — пуш на телефон по КАБ)"""
import asyncio, json, math, os, re, time, itertools
from datetime import datetime
import aiohttp
from aiohttp import web
from bs4 import BeautifulSoup

CHANNELS = ["eyes_everywhere_ua", "operatyvnohlep", "gnilayachereha", "rotodrellomod"]
HOME = ("Запоріжжя", 47.838, 35.139)
POLL = 1.5                                   # сек между опросами каждого канала
NTFY = os.getenv("NTFY_TOPIC")
KAB_H0, KAB_RANGE = 9000, 90                 # м высота пуска, км макс. дальность планирования
ALERT_ETA2 = 60                              # сек до цели -> «красный» уровень

PLACES = [(n, la, lo, rx) for n, la, lo, rx in [
 ("Запоріжжя", 47.838, 35.139, r"запоріж|запорож|запорож"), ("Балабино", 47.79, 35.27, r"балабин"),
 ("Вільнянськ", 47.95, 35.43, r"вільнянськ|вольнянск"), ("Василівка", 47.435, 35.278, r"василівк|васильевк"),
 ("Кам'янка-Дніпровська", 47.494, 34.417, r"кам.янк|каменк"), ("Енергодар", 47.499, 34.658, r"енергодар|энергодар"),
 ("Оріхів", 47.567, 35.784, r"оріхів|орехов|оріхов"), ("Гуляйполе", 47.661, 36.259, r"гуляйпол"),
 ("Пологи", 47.482, 36.254, r"полог"), ("Токмак", 47.254, 35.704, r"токмак"),
 ("Молочанськ", 47.194, 35.624, r"молочан"), ("Мелітополь", 46.849, 35.365, r"мелітопол|мелитопол"),
 ("Приазовське", 46.7, 35.67, r"приазовськ|приазовск"), ("Кирилівка", 46.667, 35.283, r"кирилівк|кирилловк"),
 ("Бердянськ", 46.755, 36.788, r"бердянськ|бердянск"), ("Маріуполь", 47.097, 37.543, r"маріупол|мариупол"),
 ("Дніпро", 48.465, 35.046, r"дніпр(?!опетр)|днепр(?!опетр)"),
 ("Дніпропетровщина (край біля Запоріжжя)", 48.12, 35.3, r"(?:з|із|зі|від|со)\s+(?:дніпр|днепр)\w*|дніпропетровщин\w*|днепропетровщин\w*"), ("Нікополь", 47.567, 34.4, r"нікопол|никопол"),
 ("Павлоград", 48.533, 35.867, r"павлоград"), ("Кривий Ріг", 47.91, 33.392, r"кривий ріг|кривой рог|кривог"),
 ("Херсон", 46.635, 32.617, r"херсон"), ("Миколаїв", 46.975, 31.995, r"миколаїв|николаев"),
 ("Одеса", 46.482, 30.723, r"одес"), ("Харків", 49.994, 36.23, r"харків|харьков"),
 ("Джанкой", 45.712, 34.391, r"джанко"), ("Сімферополь", 44.952, 34.102, r"сімферополь|симферополь"),
]]
PLACES += [("Дніпропетровщина", 48.5, 35.0, r"дніпропетровщин|днепропетровщин"), ("Донеччина", 48.0, 37.8, r"донеччин|донетчин"),
           ("Харківщина", 49.6, 36.4, r"харківщин|харьковщин"), ("Херсонщина", 46.7, 33.0, r"херсонщин"),
           ("Запорізька обл.", 47.5, 35.6, r"запоріжчин|запорожчин|запорізьк\w+ обл"), ("Полтавщина", 49.6, 34.5, r"полтавщин"),
           ("Миколаївщина", 47.2, 31.9, r"миколаївщин|николаевщин"), ("Одещина", 46.6, 30.5, r"одещин|одесчин")]
# Только Запорізька обл. и ближние районы Дніпропетровщини: всё остальное отбрасываем
DROP = {"Маріуполь", "Херсон", "Миколаїв", "Одеса", "Харків", "Джанкой", "Сімферополь", "Кривий Ріг", "Дніпропетровщина",
        "Донеччина", "Харківщина", "Херсонщина", "Полтавщина", "Миколаївщина", "Одещина"}
PLACES[:] = [p for p in PLACES if p[0] not in DROP]
VIEWBOX = "34.0,48.7,37.3,46.3"                      # lon_лев, lat_верх, lon_прав, lat_низ
# ── КАТАЛОГ ЦЕЛЕЙ ─────────────────────────────────────────────────────────────
# Каждая цель: id, kind (группа для фильтров), название, regex (по lower-case тексту),
# скорость км/ч (крейсерская/средняя по открытым источникам — можно править),
# радиус склейки с уже летящей целью (км), TTL без новых сообщений (сек), иконка, цвет.
# generic=True — «общее» слово (БпЛА/ракета/літак): игнорируется, если в сообщении есть конкретный тип.
def S(id, kind, name, rx, v, merge, ttl, ico, col=None, generic=False):
    return dict(id=id, kind=kind, name=name, rx=rx, v=v, merge=merge, ttl=ttl, ico=ico, col=col, generic=generic)

SUBS = [
 # --- ударные БпЛА / барражирующие боеприпасы ---
 S("banderol", "drone", "Бандероль (S8000)", r"бандерол\w*|\bs-?8000", 560, 80, 1200, "cruise", "#ff3d81"),        # крейс. 520–560, макс ~650
 S("geran3", "drone", "Реактивний Шахед (Герань-3)",
   r"реактивн\w*\s+(?:шахед|шахид|герань|геран|мопед|бпла|бла|дрон|безпілот)\w*|(?:шахед|шахид|герань|геран)\w*[-–]\s?(?:3|238)\b|реактив(?!н\w*\s+(?:систем|залп))",
   550, 80, 1200, "jet", "#ff6b3d"),                                                                               # ~400–600
 S("geran2", "drone", "Шахед (Герань-2)", r"шахед\w*|шахид\w*|герань\w*|геран\w*|мопед\w*", 170, 35, 1800, "delta", "#e3b341"),   # ~150–185
 S("gerbera", "drone", "Гербера (імітатор)", r"гербер\w*", 170, 35, 1800, "decoy", "#c9a227"),
 S("italmas", "drone", "Італмас", r"італмас\w*|итальмас\w*", 150, 35, 1800, "decoy", "#c9a227"),                  # оценка
 S("orion", "drone", "Оріон (ударний)", r"оріон\w*|орион\w*", 200, 60, 1200, "plane", "#ff9f1c"),
 # --- тактические / FPV ---
 S("lancet", "drone", "Ланцет", r"ланцет\w*", 110, 20, 600, "xwing", "#3dd6c6"),                                   # 80–110, в пикировании до ~300
 S("molniya", "drone", "Молния", r"молні\w*|молни\w*", 100, 20, 600, "flywing", "#7ee787"),
 S("fpv", "drone", "FPV", r"\bфпв\w*|\bfpv\b", 110, 10, 600, "quad", "#f2cc60"),
 S("orlan", "drone", "Орлан-10", r"орлан\w*", 110, 25, 900, "plane", "#9aa7b8"),
 S("recon", "drone", "Розвідувальний БпЛА", r"суперкам\w*|supercam|\bzala\b|\bзала\b|елерон\w*|\beleron|розвідувальн\w*\s+(?:бпла|дрон|бла)\w*", 95, 25, 900, "plane", "#9aa7b8"),
 S("drone", "drone", "БпЛА", r"бпла|\bбла\b|дрон\w*|безпілотн\w*|камікадзе|камикадзе|\bуав\b|\bбпак\b", 160, 35, 1800, "delta", None, True),
 # --- авиабомбы ---
 S("kab_big", "kab", "ФАБ-1500/3000", r"(?:фаб|каб)-?(?:1500|3000)", 600, 25, 600, "kab", None),
 S("kab", "kab", "КАБ", r"\bкаб(?:и|ів|ах|ам|ом|у|а)?\b|авіабомб\w*|авиабомб\w*|умпк|умпб|\bфаб\w*|\bодаб", 650, 25, 600, "kab", None),
 # --- ракеты ---
 S("kinzhal", "missile", "Х-47М2 Кинджал", r"кинджал\w*|кінжал\w*|\b[xх]-?47", 4000, 120, 300, "dart", "#ff2d55"),
 S("iskander_m", "missile", "Іскандер-М (балістика)", r"[іи]скандер-?м|9м723|[іи]скандер(?!-?к)", 5500, 120, 300, "ballistic", "#ff453a"),
 S("iskander_k", "missile", "Іскандер-К (крилата)", r"[іи]скандер-?к|9м72[89]", 800, 100, 900, "cruise", "#ff8a3d"),
 S("ballistic", "missile", "Балістична ракета", r"баліст\w*|балист\w*", 5500, 120, 300, "ballistic", "#ff453a"),
 S("s300", "missile", "С-300/С-400 (по землі)", r"\bс-?(?:300|400)\b", 3500, 60, 300, "dart", "#ff453a"),
 S("kh22", "missile", "Х-22/Х-32", r"\b[xх]-?(?:22|32)\b", 3500, 120, 300, "dart", "#ff453a"),
 S("onyx", "missile", "П-800 Онікс", r"онікс\w*|оникс\w*|\bп-?800", 2600, 100, 300, "dart", "#ff453a"),
 S("kh101", "missile", "Х-101/Х-555", r"\b[xх]-?(?:101|102|555|55)\b", 780, 100, 900, "cruise", "#ff8a3d"),
 S("kalibr", "missile", "Калібр", r"калібр\w*|калибр\w*|3м-?14|3м-?54", 850, 100, 900, "cruise", "#ff8a3d"),
 S("kh59", "missile", "Х-35/Х-59/Х-69", r"\b[xх]-?(?:35|59|69)\b", 850, 100, 900, "cruise", "#ff8a3d"),
 S("missile", "missile", "Ракета", r"ракет\w*|\bкр\b|крилат\w*", 800, 100, 900, "missile", None, True),
 # --- пилотируемая авиация ---
 S("bomber", "aircraft", "Ракетоносій (Ту-95/160/22)", r"\bту-?(?:95|160|22)\w*|ракетоносц\w*|ракетоносн\w*|стратегічн\w*\s+авіац\w*", 700, 40, 1200, "bomber", None),
 S("heli", "aircraft", "Гелікоптер", r"вертол\w*|гелікоптер\w*|\bка-?52|\bмі-?(?:8|24|28|35)\b", 220, 20, 900, "heli", "#79c0ff"),
 S("aircraft", "aircraft", "Літак", r"\bборт\w*|літак\w*|самол[её]т\w*|\bсу-?\d\d\w*|\bміг-?\d\d\w*|тактичн\w*\s+авіац\w*|авіаці\w*|авиаци\w*", 650, 20, 900, "plane2", None, True),
 # --- обстрелы (скорость 0, не двигаются) ---
 S("mlrs", "strike", "РСЗО", r"рсзо|реактивн\w*\s+систем\w*|\bград(?:и|ів|у|ом|ами)?\b|ураган\w*|смерч\w*|торнадо", 0, 10, 600, "star", None),
 S("arty", "strike", "Обстріл", r"артобстр\w*|артилер\w*|обстріл\w*|обстрел\w*|приліт\w*|прилет\w*|мінометн\w*", 0, 10, 600, "star", None, True),
]
SUBS.append(S("impact", "impact", "Приліт", r"(?!x)x", 0, 1, 900, "star", "#ff5c5c"))   # создаётся только логикой «упало/влучило», по regex не ловится
SUB = {s["id"]: s for s in SUBS}
KIND_COL = {"drone": "#e3b341", "kab": "#f85149", "missile": "#ff8a3d", "aircraft": "#58a6ff", "strike": "#d2a8ff", "impact": "#ff5c5c"}

CNT = re.compile(r"(?<![\d:.,/])(\d{1,2})(?:\s*[-–—]\s*(\d{1,2}))?\s*(?:шт\.?\s*)?(?:[xх×]\s*)?(?:ударн\w*\s+|нов\w*\s+)?$")

def count_before(tl, st):
    """«2-3 шахеди», «3х бандеролі» → '2-3' / '3'."""
    m = CNT.search(tl[max(0, st - 24):st])
    return (m.group(1) + ("-" + m.group(2) if m.group(2) else "")) if m else None

def detect(tl):
    """Находит ВСЕ типы целей в тексте. Более длинное/конкретное совпадение побеждает пересекающееся
    («реактивних шахедів» → Герань-3, а не Герань-2 + «реактив»); общие слова (БпЛА, ракета) отбрасываются,
    если рядом есть конкретный тип той же группы."""
    hits = [(m.start(), m.end(), i) for i, s in enumerate(SUBS) for m in re.finditer(s["rx"], tl)]
    hits.sort(key=lambda h: (-(h[1] - h[0]), h[2]))
    keep = []
    for h in hits:
        if all(h[1] <= c[0] or h[0] >= c[1] for c in keep):
            keep.append(h)
    keep.sort()
    specific = {SUBS[i]["kind"] for _, _, i in keep if not SUBS[i]["generic"]}
    out, done = [], set()
    for st, en, i in keep:
        s = SUBS[i]
        if (s["generic"] and s["kind"] in specific) or s["id"] in done:
            continue
        done.add(s["id"]); out.append((s, count_before(tl, st)))
    return out

TGT = re.compile(r"(?:\bна|\bпо|\bк|\bдо|напрям\w*|направ\w*|бік|сторон\w*|курс\w*)\s*$")
CLEAR = re.compile(r"відбій|отбой|збит|сбит|знищен|минус|мінус|чисто")
# «упало / влучило / вибухи» — событие УЖЕ произошло: рисуем место прильота, а не полёт
IMPACT = re.compile(r"(?:упал|впал|падал|падінн|падіння|падение|влуч|попад|приліт|прилет|прильот|вибух|взрыв|бахн|бахк|гучн|вдарил|вдарив|накрил|ударил|уражен)\w*")
FLIGHT = re.compile(r"летить|летят|летит|курс|напрям|рухаєть|движ|в бік|у бік|підлітає|заходить|заходят|прямує|прямують")
def intent_of(tl):
    if CLEAR.search(tl): return "clear"
    if IMPACT.search(tl) and not FLIGHT.search(tl): return "impact"
    return "flight"

def hav(a, b, c, d):
    p = math.pi / 180
    x = math.sin((c - a) * p / 2) ** 2 + math.cos(a * p) * math.cos(c * p) * math.sin((d - b) * p / 2) ** 2
    return 12742 * math.asin(math.sqrt(x))

def brg(a, b, c, d):
    p = math.pi / 180
    y = math.sin((d - b) * p) * math.cos(c * p)
    x = math.cos(a * p) * math.sin(c * p) - math.sin(a * p) * math.cos(c * p) * math.cos((d - b) * p)
    return (math.degrees(math.atan2(y, x)) + 360) % 360

tracks, ids, seen, clients, alerted, stats = {}, itertools.count(1), set(), set(), set(), {}

# ── ОБЛАСТЬ: показываем только Запорізьку обл. ───────────────────────────────
# Грубый контур (lon, lat). Цели вне контура (+OBL_MARGIN км) трекаются внутри, но на карту не отдаются —
# так дрон, летящий с Днепропетровщины, появится сам, как только пересечёт границу.
OBLAST = [(34.30, 47.40), (34.46, 47.56), (34.80, 47.72), (35.02, 47.98), (35.20, 48.14), (35.50, 48.10), (35.80, 47.98),
          (36.20, 48.02), (36.50, 47.95), (36.78, 47.72), (36.96, 47.40), (37.05, 47.05), (36.95, 46.80), (36.55, 46.58),
          (36.00, 46.52), (35.45, 46.42), (35.00, 46.38), (34.75, 46.55), (34.78, 46.90), (34.50, 47.15)]
OBL_MARGIN = 10.0                                   # км допуска (контур приблизительный, а координаты из сводок — ещё приблизительнее)

def _pip(x, y, poly):
    ins, j = False, len(poly) - 1
    for i in range(len(poly)):
        xi, yi = poly[i]; xj, yj = poly[j]
        if (yi > y) != (yj > y) and x < (xj - xi) * (y - yi) / (yj - yi) + xi:
            ins = not ins
        j = i
    return ins

def _segd(px, py, ax, ay, bx, by):
    dx, dy = bx - ax, by - ay
    L = dx * dx + dy * dy
    u = 0 if L == 0 else max(0, min(1, ((px - ax) * dx + (py - ay) * dy) / L))
    return math.hypot(px - (ax + u * dx), py - (ay + u * dy))

def in_oblast(la, lo, margin=OBL_MARGIN):
    if _pip(lo, la, OBLAST): return True
    if margin <= 0: return False
    k = 111.2 * math.cos(math.radians(la))
    pts = [(x * k, y * 111.2) for x, y in OBLAST]
    px, py = lo * k, la * 111.2
    return any(_segd(px, py, *pts[i - 1], *pts[i]) <= margin for i in range(len(pts)))

# ── НЕТОЧНОСТЬ МЕСТ ───────────────────────────────────────────────────────────
# «Шахед над Зеленим» ≠ ровно в центре Зеленого: это может быть окраина или стык с соседним районом.
# Поэтому каждое место = пятно с радиусом (км), а не точка. Неизвестные нас. пункты берём из bbox Nominatim.
RAD = {"Запоріжжя": 8, "Балабино": 3.5, "Вільнянськ": 4, "Василівка": 4.5, "Кам'янка-Дніпровська": 4.5, "Енергодар": 5, "Оріхів": 4.5,
       "Гуляйполе": 4.5, "Пологи": 5, "Токмак": 5, "Молочанськ": 4, "Мелітополь": 6.5, "Приазовське": 4, "Кирилівка": 4,
       "Бердянськ": 6, "Дніпро": 15, "Нікополь": 7, "Павлоград": 7, "Дніпропетровщина (край біля Запоріжжя)": 25, "Запорізька обл.": 45}
def sigma_of(name): return max(2.5, RAD.get(name, 4.0))

# ── ТРЕКЕР ────────────────────────────────────────────────────────────────────
REPORT_LAG = 20          # сек: пока увидели → написали → канал опубликовал. Цель за это время уже улетела дальше
CORR_TAU = 8.0           # сек: поправка позиции по новому сообщению «вливается» плавно, без телепорта
TURN_KIND = {"drone": 4, "kab": 6, "missile": 8, "aircraft": 3, "strike": 0, "impact": 0}          # макс. скорость поворота, °/с
TURN_SUB = {"geran2": 2.5, "gerbera": 2.5, "italmas": 2.5, "banderol": 4, "geran3": 4, "fpv": 40, "lancet": 25, "molniya": 25,
            "orlan": 6, "recon": 6, "ballistic": 30, "kinzhal": 30, "iskander_m": 30, "heli": 10}
ATTACK_END = {"kab", "missile"}                    # долетел до цели → «завершено» (+ударные БпЛА, кроме разведчиков)

def ang(a): return (a + 180) % 360 - 180
def blend(a, b, w): return (a + ang(b - a) * w) % 360
def fwd(la, lo, h, km):
    r = math.radians(h)
    return la + km * math.cos(r) / 111.2, lo + km * math.sin(r) / (111.2 * math.cos(math.radians(la)))

def step(t, now):
    """Двигаем цель до момента now: плавный разворот (с лимитом °/с) + движение по курсу + плавная поправка позиции."""
    dt = min(now - t["ls"], 1800.0)
    if dt <= 0: return
    t["ls"] = now
    if t.get("ended"): return
    c = t["corr"]
    if c[0] or c[1]:
        f = 1 - math.exp(-dt / CORR_TAU)
        t["lat"] += c[0] * f; t["lon"] += c[1] * f
        t["corr"] = (c[0] * (1 - f), c[1] * (1 - f))
    v = t["v"]
    if not v: return
    umax = 25.0 if v < 400 else 40.0
    if t["hdg"] is None:                           # курс неизвестен: стоим на месте, пятно неопределённости растёт
        t["unc"] = min(umax, t["unc"] + dt * v / 3600 * 0.25); return
    rate, g = TURN_SUB.get(t["sub"], TURN_KIND.get(t["kind"], 3)), t["goal"]
    n = int(dt / 4) + 1; h = dt / n
    for _ in range(n):
        if g: t["thdg"] = brg(t["lat"], t["lon"], g[1], g[2])
        m = rate * h
        t["hdg"] = (t["hdg"] + max(-m, min(m, ang(t["thdg"] - t["hdg"])))) % 360
        t["lat"], t["lon"] = fwd(t["lat"], t["lon"], t["hdg"], v * h / 3600)
        if g and hav(t["lat"], t["lon"], g[1], g[2]) <= max(1.5, RAD.get(g[0], 4.0) * 0.5):
            if t["kind"] in ATTACK_END or (t["kind"] == "drone" and t["sub"] not in ("recon", "orlan")):
                t["ended"] = True; break
            t["goal"] = g = None
        if t["kind"] == "kab" and hav(*t["o"], t["lat"], t["lon"]) >= KAB_RANGE:
            t["ended"] = True; break
    t["unc"] = min(umax, t["unc"] + dt * v / 3600 * (0.05 + 0.3 * (1 - t["hq"])))   # чем хуже знаем курс, тем быстрее «расплывается»

def set_goal(t, tgt, te):
    if not tgt or not t["v"] or hav(t["lat"], t["lon"], tgt[1], tgt[2]) < 1.0: return
    t["goal"] = tgt
    b = brg(t["lat"], t["lon"], tgt[1], tgt[2])
    t["thdg"] = b
    if t["hdg"] is None: t["hdg"] = b; t["hq"] = max(t["hq"], 0.4)

def new_track(sub, pos, sg, tgt, te, cnt, text):
    i = next(ids)
    t = tracks[i] = dict(id=i, kind=sub["kind"], sub=sub["id"], ttl=sub["ttl"], v=sub["v"], o=(pos[1], pos[2]), frm=pos[0],
                         lat=pos[1], lon=pos[2], hdg=None, thdg=None, hq=0.0, unc=sg, goal=None, corr=(0.0, 0.0),
                         ls=te, seen=te, n=1, fx=[(te, pos[1], pos[2], sg)], text=text, cnt=cnt)
    set_goal(t, tgt, te)
    return t

def observe(t, pos, sg, m, te):
    """Новая фиксация: позиция сливается с прогнозом по весам (Калман-подобно), курс уточняется мягко."""
    t["seen"] = max(t["seen"], te)
    v = t["v"]
    if not v:                                      # обстрел/прилёт — просто точка
        t["lat"], t["lon"], t["unc"] = pos[1], pos[2], sg
        t["fx"] = [(te, pos[1], pos[2], sg)]; t["n"] += 1; return
    gap = max(0.0, t["ls"] - te)
    sm = sg + 0.1 * v * gap / 3600                 # + неопределённость из-за возраста сообщения
    sp = t["unc"]
    K = min(0.85, max(0.12, sp * sp / (sp * sp + sm * sm)))
    c = t["corr"]
    t["corr"] = (c[0] + K * (m[0] - (t["lat"] + c[0])), c[1] + K * (m[1] - (t["lon"] + c[1])))
    t["unc"] = max(0.6 * sg, 1.5, (sp * sp * sm * sm / (sp * sp + sm * sm)) ** 0.5)
    prev = t["fx"][-1]
    d, dtf = hav(prev[1], prev[2], pos[1], pos[2]), te - prev[0]
    if d > 0.3 and dtf > 0 and d <= v * dtf / 3600 * 2.5 + sg + prev[3] + 2:      # отсекаем «телепорты»
        w = d / (d + 0.8 * (sg + prev[3]))         # доверие к курсу: чем больше сдвиг относительно «пятен», тем выше
        b = brg(prev[1], prev[2], pos[1], pos[2])
        g = t["goal"]
        if g:
            if abs(ang(b - brg(t["lat"], t["lon"], g[1], g[2]))) > 75 and w > 0.45:
                t["goal"] = None                   # фиксации ясно показывают: летит не туда, куда считали
                t["thdg"] = blend(t["hdg"], b, w)
            else:
                t["hq"] = min(1.0, t["hq"] + 0.1)
        elif t["hdg"] is None:
            t["hdg"] = t["thdg"] = b; t["hq"] = w
        else:
            dev = abs(ang(b - t["hdg"]))
            if dev > 130 and not t.get("rev"):     # разворот на 180° по одной неточной фиксации — шум/другой борт; нужно 2 подряд
                t["rev"] = True
            else:
                t["rev"] = False
                t["thdg"] = blend(t["thdg"], b, w) # целевой курс сдвигается частично; сам курс довернётся с лимитом °/с
                t["hq"] = max(0.0, min(1.0, t["hq"] + (0.2 if dev < 35 else -0.1)))
    t["fx"] = (t["fx"] + [(te, pos[1], pos[2], sg)])[-8:]
    t["n"] += 1

def view(t, now):
    la, lo, v, g = t["lat"], t["lon"], t["v"], t["goal"]
    home = hav(la, lo, HOME[1], HOME[2])
    eta, short = None, False
    if v and g and not t.get("ended"):
        dg = hav(la, lo, g[1], g[2]); lim = dg
        if t["kind"] == "kab":
            rem = max(0, KAB_RANGE - hav(*t["o"], la, lo)); lim = min(dg, rem); short = rem < dg - 0.5
        eta = int(lim / v * 3600)
    etah = None                                    # ETA до Запорожья «по курсу», даже если цель его не называли
    if v and t["hdg"] is not None and not t.get("ended"):
        tol = 20 + math.degrees(math.atan2(t["unc"], max(home, 1)))
        if abs(ang(brg(la, lo, HOME[1], HOME[2]) - t["hdg"])) <= tol: etah = int(home / v * 3600)
    sb = SUB[t["sub"]]
    r = dict(id=t["id"], kind=t["kind"], lat=la, lon=lo, unc=round(t["unc"], 1), v=v, hdg=None if t["hdg"] is None else round(t["hdg"]),
             goal=g[1:] if g else None, to=g[0] if g else None, frm=t["frm"], fx=[[x[1], x[2]] for x in t["fx"]], fs=round(t["fx"][-1][3], 1),
             n=t["n"], age=int(now - t["seen"]), text=t["text"][:140], eta=eta, etah=etah, ended=bool(t.get("ended")),
             home=round(home, 1), short=short, sub=t["sub"], name=t.get("label") or sb["name"], ico=sb["ico"], col=sb["col"] or KIND_COL[t["kind"]], cnt=t.get("cnt"))
    if t["kind"] == "kab":
        r["alt"] = round(max(0, KAB_H0 * (1 - hav(*t["o"], la, lo) / KAB_RANGE)))
    return r

def upsert(sub, pos, tgt, ts, text, kmax=None, cnt=None):
    te = min(ts + REPORT_LAG, time.time())         # момент, к которому относится фиксация
    v = sub["v"]
    live = [t for t in tracks.values() if t["sub"] == sub["id"] and not t.get("ended") and te - t["seen"] < t["ttl"]]
    for c in live: step(c, te)
    sg = sigma_of(pos[0]) if pos else 0.0
    t = tm = None
    if pos:                                        # привязка к треку: расстояние до прогноза с учётом пятен + согласованность с курсом
        best = 1.0
        for c in live:
            gap = max(0.0, c["ls"] - te)           # трек уже «уехал» вперёд относительно момента сообщения
            dirn = c["hdg"] if c["hdg"] is not None else (brg(pos[1], pos[2], tgt[1], tgt[2]) if tgt else None)
            m = fwd(pos[1], pos[2], dirn, v * gap / 3600) if (dirn is not None and v and gap) else (pos[1], pos[2])
            dist = hav(m[0], m[1], c["lat"], c["lon"])
            cost = dist / ((kmax or sub["merge"] * 0.5) + c["unc"] + sg)
            if c["hdg"] is not None and dist > 2:
                cost += 0.25 * abs(ang(brg(c["lat"], c["lon"], m[0], m[1]) - c["hdg"])) / 180
            if cost < best: best, t, tm = cost, c, m
    elif live:
        if tgt and v:
            t = min(live, key=lambda c: abs(ang(brg(c["lat"], c["lon"], tgt[1], tgt[2]) - c["hdg"])) if c["hdg"] is not None
                    else 90 + hav(c["lat"], c["lon"], tgt[1], tgt[2]) / 10)
        else:
            t = max(live, key=lambda c: c["seen"])
    if t is None:
        if not pos: return
        t = new_track(sub, pos, sg, tgt, te, cnt, text)
    else:
        if pos: observe(t, pos, sg, tm, te)
        if tgt: set_goal(t, tgt, te)
        t["seen"] = max(t["seen"], te)
    if cnt: t["cnt"] = cnt
    t["text"] = text
    return t

log, GEO_CACHE, geo_lock = [], {}, asyncio.Lock()
feed, fed = [], set()
SKIP = re.compile(r"^(курс|курсом|напрям|балістик|групи|група|рухаєть|також|увага|реактив)")

def note(m):
    log.append(m[:160]); del log[:-60]

GEO_Q = None
NOMI = "https://nominatim.openstreetmap.org/search"

def geo_words(text):
    """Незнакомые названия (с заглавной буквы), которых ещё нет в справочнике."""
    out = []
    for w in dict.fromkeys(re.findall(r"[А-ЯІЇЄҐ][а-яіїєґ'’\-]{4,}", text)):
        lw = w.lower()
        if lw in GEO_CACHE or SKIP.match(lw) or any(re.match(rx, lw) for *_, rx in PLACES) or any(re.search(sb["rx"], lw) for sb in SUBS):
            continue
        out.append(w)
    return out

async def geo_one(s, w):
    """Ищет одно название в Nominatim; из нескольких одноимённых сёл берёт ближайшее к Запорожью (внутри области)."""
    lw = w.lower()
    GEO_CACHE[lw] = None
    async with GEO_LOCK:
      for q in dict.fromkeys([w, w[:-1] + "а", w[:-1] + "я"]):
        try:
            async with s.get(NOMI, headers={"User-Agent": "radar-app/1.0"},
                             params={"q": q, "format": "json", "countrycodes": "ua", "limit": 8, "accept-language": "uk",
                                     "viewbox": VIEWBOX, "bounded": 1}, timeout=aiohttp.ClientTimeout(total=5)) as r:
                j = await r.json()
        except Exception:
            GEO_CACHE.pop(lw, None); return False        # сеть/лимит — не запоминаем промах, попробуем позже
        await asyncio.sleep(1.0)
        if not j: continue
        c = [x for x in j if in_oblast(float(x["lat"]), float(x["lon"]), 15)] or j
        x = min(c, key=lambda x: hav(float(x["lat"]), float(x["lon"]), HOME[1], HOME[2]))
        nm = x["display_name"].split(",")[0]
        PLACES.append((nm, float(x["lat"]), float(x["lon"]), re.escape(lw[:-2] if len(lw) > 6 else lw[:-1])))
        GEO_CACHE[lw] = True
        try:
            bb = [float(v) for v in x["boundingbox"]]
            RAD[nm] = max(2.5, min(25.0, max((bb[1] - bb[0]) * 111.2, (bb[3] - bb[2]) * 111.2 * math.cos(math.radians(bb[0]))) / 2))
        except Exception: pass
        return True
    return False

async def geo_worker(s):
    """Фон: сообщение уже нанесено по известным местам; если нашли новые названия — наносим его повторно, уже точнее."""
    while True:
        ts, text, ch = await GEO_Q.get()
        added = False
        for w in geo_words(text):
            added = await geo_one(s, w) or added
        if added:
            ingest(ts, text, ch); await push()

NEWS = re.compile(r"https?://|t\.me|підписуй|подпиш|читайте|джерело|реклам|донат|загинул|загинув|поранен|постраждал|внаслідок|наслідк|зведення|за добу|минулої|вчора|заявив|повідомив|міністр|командув|генштаб|збито \d|знищено \d|втрат")

def feed_add(ts, ch, text, kinds, pos, tgt):
    if (ts, text[:60]) in fed or not any(in_oblast(p[1], p[2], 8) for p in (pos, tgt) if p): return
    fed.add((ts, text[:60]))                       # в ленту — только то, что касается области
    feed.insert(0, dict(t=ts, ch=ch, text=text[:200], kinds=kinds, place=" → ".join(p[0] for p in (pos, tgt) if p)))
    del feed[30:]

def do_clear(kinds, loc, one=False):
    """Отбой/сбит: убираем цели нужных типов рядом с местом (one=True — только ближайшую каждого типа)."""
    cand = [t for t in tracks.values() if (not kinds or t["kind"] in kinds) and (not loc or hav(t["lat"], t["lon"], loc[1], loc[2]) < 40)]
    if one and loc:
        cand = [min([t for t in cand if t["kind"] == k], key=lambda t: hav(t["lat"], t["lon"], loc[1], loc[2])) for k in {t["kind"] for t in cand}]
    for t in cand: tracks.pop(t["id"], None)

def do_impact(subs, loc, ts, text):
    """Упало: ближайшая летящая цель этого типа «долетела» (завершается в точке прилёта) + маркер прилёта на карте."""
    for k in {sb["kind"] for sb, _ in subs} & {"kab", "missile", "drone", "aircraft"}:
        cand = [t for t in tracks.values() if t["kind"] == k and not t.get("ended")]
        if not cand: continue
        t = min(cand, key=lambda t: hav(t["lat"], t["lon"], loc[1], loc[2]))
        if hav(t["lat"], t["lon"], loc[1], loc[2]) < 45 or (t["goal"] and hav(t["goal"][1], t["goal"][2], loc[1], loc[2]) < 15):
            t["ended"] = True; t["lat"], t["lon"], t["corr"] = loc[1], loc[2], (0.0, 0.0); t["seen"] = max(t["seen"], time.time())
    names = list(dict.fromkeys(sb["name"] for sb, _ in subs if sb["kind"] not in ("strike", "impact")))
    m = upsert(SUB["impact"], loc, None, ts, text, None, next((c for _, c in subs if c), None))
    if m: m["label"] = ("Приліт: " + ", ".join(names)) if names else "Приліт/вибух"

def act(intent, subs, pos, tgt, ts, text, ch, how=""):
    """Единая логика для regex-разбора и для ШІ: что делать с сообщением."""
    now, loc = time.time(), pos or tgt
    kinds = list(dict.fromkeys(sb["kind"] for sb, _ in subs))
    tag = f"{ch}{how}: «{text[:50]}» → "
    if intent in ("clear", "downed"):
        do_clear(kinds, loc, one=(intent == "downed"))
        return note(tag + ("збито" if intent == "downed" else "відбій"))
    if intent == "impact":
        if not loc: return note(tag + "приліт, але місця немає")
        do_impact(subs, loc, ts, text)
        feed_add(ts, ch, text, ["impact"] + [k for k in kinds if k != "impact"], pos, tgt)
        return note(tag + f"ПРИЛІТ {[sb['id'] for sb, _ in subs]} у {loc[0]}")
    kmax = None
    if not subs:                                   # короткие посты вида «До Новомиколаївки» — только уточняют уже летящую цель
        ref = loc
        live = [t for t in tracks.values() if t["kind"] not in ("strike", "impact") and now - t["seen"] < t["ttl"]]
        if not ref or not live or len(text) > 160:
            return note(tag + "без типа цели и нет активных целей, пропуск")
        near = min(live, key=lambda t: hav(t["lat"], t["lon"], ref[1], ref[2]))
        subs, kmax, kinds = [(SUB[near["sub"]], None)], 200, [near["kind"]]
    for sb, cnt in subs:
        if sb["kind"] == "strike": upsert(sb, loc, None, ts, text, None, cnt)
        else: upsert(sb, pos, tgt, ts, text, kmax, cnt)
    feed_add(ts, ch, text, kinds, pos, tgt)
    note(tag + f"{[(sb['id'], c) for sb, c in subs]} поз={pos and pos[0]} цель={tgt and tgt[0]}")

def ingest(ts, text, ch=""):
    """Запасной разбор без ШІ: по ключевым словам."""
    tl, now = text.lower(), time.time()
    for t in tracks.values(): step(t, now)
    if NEWS.search(tl) or len(text) > 280:        # новости, итоги, реклама — не сводка
        return note(f"{ch}: «{text[:50]}» → новость/не сводка, пропуск")
    subs = detect(tl)
    best = {}                                      # одно место на позицию, берём самое длинное совпадение
    for n, la, lo, rx in PLACES:
        for m in re.finditer(r"(?<![а-яіїєґ'’])(?:" + rx + r")\w*", tl):
            if m.start() not in best or (m.end(), len(rx)) > best[m.start()][:2]:
                best[m.start()] = (m.end(), len(rx), n, la, lo)
    pl = [(st, n, la, lo, tl[max(0, st - 20):st]) for st, (_, _, n, la, lo) in sorted(best.items())]
    pos = next(((n, la, lo) for _, n, la, lo, pre in pl if not TGT.search(pre)), None)
    tgt = next(((n, la, lo) for _, n, la, lo, pre in pl if TGT.search(pre)), None)
    if not pl:
        return note(f"{ch}: «{text[:50]}» → места не найдены")
    intent = intent_of(tl)
    if intent == "impact" and not [1 for sb, _ in subs if sb["kind"] != "strike"]:
        subs = [x for x in subs if x[0]["id"] != "arty"]         # «приліт» сам по себе — не отдельный тип цели
    act(intent, subs, pos, tgt, ts, text, ch)

# ── ШІ-РАЗБОР (Claude API, по желанию) ────────────────────────────────────────
# Включается переменной окружения ANTHROPIC_API_KEY. Без ключа или при ошибке работает разбор по ключевым словам выше.
# ШІ понимает СМЫСЛ сообщения (летит / упало / сбито / отбой / не про цели), типы целей и названия в именительном падеже.
# Координаты ШІ не придумывает: названия превращаются в точки через справочник мест и Nominatim.
ANTHROPIC_KEY = os.getenv("ANTHROPIC_API_KEY")
LLM_MODEL = os.getenv("LLM_MODEL", "claude-haiku-5-5")
LLM_CACHE, LLM_SEM, GEO_LOCK = {}, None, None
INTENTS = {"flight", "impact", "downed", "clear", "other"}

def llm_system():
    types = "\n".join(f"- {k}: {v['name']}" for k, v in SUB.items() if k != "impact")
    return f"""Ти аналізуєш повідомлення з українських Telegram-каналів про повітряні загрози в Запорізькій області та поруч. Поверни ЛИШЕ один JSON-об'єкт без пояснень:
{{"intent": "flight|impact|downed|clear|other", "threats": [{{"type": "<id>", "count": <число або null>}}], "at": <назва або null>, "to": <назва або null>, "from": <назва або null>}}

intent:
- flight: ціль зараз у русі / помічена / курс (летить, курсом на, у напрямку, рухається, над X, біля X).
- impact: УЖЕ сталося: впали, упали, влучили, приліт, вибухи, гучно, накрило. Це місце події, а не політ.
- downed: збито/знищено конкретну ціль.
- clear: відбій, чисто, загроза минула.
- other: новини, підсумки, статистика, втрати, реклама, коментарі, не про конкретну ціль.
Коротке повідомлення лише з місцем без типу цілі («Вже біля Новомиколаївки») = flight з порожнім threats.

Назви: at = де ціль ЗАРАЗ або де сталося; to = куди летить («на X», «у бік X», «курсом на X»); from = звідки («з X», «від X»). Усі назви — населений пункт у називному відмінку українською, без слів «район», «околиці». Не вигадуй: якщо немає — null.
type — тільки id зі списку:
{types}
Якщо тип невідомий, але це дрон — "drone", ракета — "missile", авіабомба — "kab", літак — "aircraft".

Приклади:
«КАБи впали на Запоріжжя» → {{"intent":"impact","threats":[{{"type":"kab","count":null}}],"at":"Запоріжжя","to":null,"from":null}}
«2 шахеди з Оріхова курсом на Запоріжжя» → {{"intent":"flight","threats":[{{"type":"geran2","count":2}}],"at":"Оріхів","to":"Запоріжжя","from":null}}
«Відбій по БпЛА у Токмаку» → {{"intent":"clear","threats":[{{"type":"drone","count":null}}],"at":"Токмак","to":null,"from":null}}"""

async def llm_parse(s, text):
    if not ANTHROPIC_KEY: return None
    if text in LLM_CACHE: return LLM_CACHE[text]
    async with LLM_SEM:
        try:
            async with s.post("https://api.anthropic.com/v1/messages",
                              headers={"x-api-key": ANTHROPIC_KEY, "anthropic-version": "2023-06-01", "content-type": "application/json"},
                              json={"model": LLM_MODEL, "max_tokens": 300, "system": llm_system(), "messages": [{"role": "user", "content": text[:700]}]},
                              timeout=aiohttp.ClientTimeout(total=10)) as r:
                j = await r.json()
            raw = "".join(b.get("text", "") for b in j["content"] if b.get("type") == "text")
            info = json.loads(re.search(r"\{.*\}", raw, re.S).group(0))
            assert info.get("intent") in INTENTS
        except Exception as e:
            note(f"ШІ: помилка ({str(e)[:60]}) → розбір за ключовими словами"); return None
    LLM_CACHE[text] = info
    if len(LLM_CACHE) > 500: LLM_CACHE.pop(next(iter(LLM_CACHE)))
    return info

def find_place(name):
    if not name or not isinstance(name, str): return None
    n = name.lower().strip()
    best = None
    for pn, la, lo, rx in PLACES:
        if pn.lower() == n or re.match(r"(?:" + rx + r")", n):
            if best is None or len(rx) > best[0]: best = (len(rx), (pn, la, lo))
    return best and best[1]

async def resolve(s, name):
    p = find_place(name)
    if p or not isinstance(name, str) or len(name.strip()) < 4: return p
    w = name.strip()
    if w.lower() not in GEO_CACHE and await geo_one(s, w[0].upper() + w[1:]): return find_place(name)
    return None

async def apply_llm(s, info, ts, text, ch):
    tl, now = text.lower(), time.time()
    for t in tracks.values(): step(t, now)
    intent = info["intent"]
    if intent == "other": return note(f"{ch} ШІ: «{text[:50]}» → не про цілі")
    cur, to, org = await resolve(s, info.get("at")), await resolve(s, info.get("to")), await resolve(s, info.get("from"))
    subs = []
    for x in info.get("threats") or []:
        sb = SUB.get(str(x.get("type"))) if isinstance(x, dict) else None
        if sb and sb["id"] != "impact":
            c = x.get("count"); subs.append((sb, str(c) if c else None))
    if not subs: subs = [x for x in detect(tl) if x[0]["id"] != "arty" or intent != "impact"]
    pos, tgt = cur or org, to
    if not (pos or tgt): return note(f"{ch} ШІ: «{text[:50]}» → {intent}, але місця не знайдено ({info.get('at')}/{info.get('to')}/{info.get('from')})")
    act(intent, subs, pos, tgt, ts, text, ch, " ШІ")

async def process(s, ts, text, ch):
    try:
        tl = text.lower()
        if NEWS.search(tl) or len(text) > 280:
            return note(f"{ch}: «{text[:50]}» → новость/не сводка, пропуск")
        info = await llm_parse(s, text)
        if info: await apply_llm(s, info, ts, text, ch)
        else:
            ingest(ts, text, ch)
            if geo_words(text): GEO_Q.put_nowait((ts, text, ch))
        await push()
    except Exception as e:
        print("process", e)

def snap(now):
    vis = []
    for t in tracks.values():
        step(t, now)
        if in_oblast(t["lat"], t["lon"]): vis.append(view(t, now))
    return {"now": now, "home": HOME, "tracks": vis, "feed": feed, "oblast": [[la, lo] for lo, la in OBLAST]}

async def send(payload, event=None):
    msg = (f"event: {event}\n" if event else "") + f"data: {json.dumps(payload, ensure_ascii=False)}\n\n"
    for c in list(clients):
        try: await c.write(msg.encode())
        except Exception: clients.discard(c)

async def push(): await send(snap(time.time()))

async def tick(s):
    while True:
        now = time.time()
        for i, t in list(tracks.items()):
            if now - t["seen"] > t["ttl"] or (t.get("ended") and now - t["seen"] > 120):
                tracks.pop(i)
        for t in list(tracks.values()):                 # ТРЕВОГА только по КАБ на Запорожье
            step(t, now)
            if t["kind"] != "kab" or t.get("ended") or not t["goal"] or hav(*t["goal"][1:], HOME[1], HOME[2]) > 25:
                continue
            v = view(t, now)
            if v["short"]:
                continue
            lvl = 2 if v["eta"] is not None and v["eta"] <= ALERT_ETA2 else 1
            if (t["id"], lvl) not in alerted:
                alerted.add((t["id"], lvl))
                txt = f"КАБ на {HOME[0]}! Пуск от {t['frm']}, {v['home']} км, высота {v.get('alt')} м, ~{v['eta']} с"
                await send({"lvl": lvl, "text": txt}, "alert")
                if NTFY:
                    try: await s.post(f"https://ntfy.sh/{NTFY}", data=txt.encode(), headers={"Title": "KAB ALERT", "Priority": "urgent", "Tags": "rotating_light"})
                    except Exception as e: print("ntfy", e)
        await push()
        await asyncio.sleep(1)

async def poll(s, ch):
    first = True
    while True:
        try:
            async with s.get(f"https://t.me/s/{ch}", headers={"User-Agent": "Mozilla/5.0"}, timeout=aiohttp.ClientTimeout(total=8)) as r:
                html, code = await r.text(), r.status
            new = []
            ws = BeautifulSoup(html, "html.parser").select(".tgme_widget_message")
            lt = ws[-1].select_one(".tgme_widget_message_text") if ws else None
            stats[ch] = {"http": code, "html_len": len(html), "messages_on_page": len(ws),
                         "last_post": lt.get_text(" ", strip=True)[:200] if lt else None, "checked": time.strftime("%H:%M:%S")}
            for w in ws:
                pid, tx, tm = w.get("data-post"), w.select_one(".tgme_widget_message_text"), w.select_one("time")
                if not (pid and tx and tm) or pid in seen: continue
                seen.add(pid)
                ts = datetime.fromisoformat(tm["datetime"]).timestamp()
                if not first or time.time() - ts < 900:   # при старте берём только последние 15 минут
                    new.append((ts, tx.get_text(" ", strip=True)))
            first = False
            for ts, text in sorted(new):
                asyncio.create_task(process(s, ts, text, ch))
            if new: await push()
        except Exception as e:
            print(ch, e); stats[ch] = {"error": str(e)}
        await asyncio.sleep(POLL)

async def events(req):
    r = web.StreamResponse(headers={"Content-Type": "text/event-stream", "Cache-Control": "no-cache", "X-Accel-Buffering": "no"})
    await r.prepare(req)
    clients.add(r)
    try:
        await r.write(f"data: {json.dumps(snap(time.time()), ensure_ascii=False)}\n\n".encode())
        while True: await asyncio.sleep(3600)
    finally:
        clients.discard(r)

async def main():
    global GEO_Q, LLM_SEM, GEO_LOCK
    LLM_SEM, GEO_LOCK = asyncio.Semaphore(4), asyncio.Lock()
    s = aiohttp.ClientSession()
    GEO_Q = asyncio.Queue()
    asyncio.create_task(geo_worker(s))
    app = web.Application()
    D = os.path.dirname(os.path.abspath(__file__))
    f = lambda n: (lambda r: web.FileResponse(os.path.join(D, n)))
    app.add_routes([web.get("/", f("index.html")), web.get("/events", events),
                    web.get("/manifest.json", f("manifest.json")), web.get("/sw.js", f("sw.js")),
                    web.get("/icon-192.png", f("icon-192.png")), web.get("/icon-512.png", f("icon-512.png")),
                    web.get("/health", lambda r: web.Response(text="ok")),
                    web.get("/debug", lambda r: web.json_response({"channels": stats, "tracks": [dict(id=t["id"], sub=t["sub"], hdg=t["hdg"] and round(t["hdg"]), unc=round(t["unc"], 1), n=t["n"], pos=[round(t["lat"], 3), round(t["lon"], 3)], shown=in_oblast(t["lat"], t["lon"])) for t in tracks.values()], "log": log, "geo": [k for k, v in GEO_CACHE.items() if v]}, dumps=lambda o: json.dumps(o, ensure_ascii=False, indent=1)))])
    for ch in CHANNELS: asyncio.create_task(poll(s, ch))
    asyncio.create_task(tick(s))
    run = web.AppRunner(app); await run.setup()
    await web.TCPSite(run, "0.0.0.0", int(os.getenv("PORT", 8080))).start()
    print("http://localhost:8080"); await asyncio.Event().wait()

if __name__ == "__main__":
    asyncio.run(main())