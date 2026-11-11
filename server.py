"""Радар воздушных целей. Запуск: pip install aiohttp beautifulsoup4 && python server.py
Открыть на телефоне: http://<IP-компьютера>:8080  (опц. NTFY_TOPIC=мой_топик — пуш на телефон по КАБ)"""
import asyncio, json, math, os, re, time, itertools
from datetime import datetime
import aiohttp
from aiohttp import web
from bs4 import BeautifulSoup

CHANNELS = ["eyes_everywhere_ua", "operatyvnohlep"]
HOME = ("Запоріжжя", 47.838, 35.139)
POLL = 1.5                                   # сек между опросами каждого канала
NTFY = os.getenv("NTFY_TOPIC")
SPEED = {"drone": 160, "kab": 650, "missile": 900, "aircraft": 0, "strike": 0}   # км/ч (средние)
TTL = {"drone": 1800, "kab": 600, "missile": 900, "aircraft": 900, "strike": 600}  # сек без новых сообщений
MERGE_KM = {"drone": 35, "kab": 25, "missile": 60, "aircraft": 20, "strike": 10}
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
KINDS = [("kab", r"\bкаб|авіабомб|авиабомб|умпк|\bфаб"),
         ("strike", r"рсзо|реактивн\w* систем|\bград\b|ураган|смерч|артобстр|артилер|обстріл|обстрел|приліт|прилет"),
         ("missile", r"баліст|балист|ракет|іскандер|искандер|калібр|калибр|кинджал|кінжал"),
         ("aircraft", r"\bборт|літак|самол[её]т|\bсу-?\d\d|\bту-?\d+|авіаці|авиаци"),
         ("drone", r"бпла|шахед|шахид|герань|реактив(?!н\w* систем)|мопед|ланцет|орлан|\bфпв|камікадзе|дрон|\bбла\b")]
TGT = re.compile(r"(?:\bна|\bпо|\bк|\bдо|напрям\w*|направ\w*|бік|сторон\w*|курс\w*)\s*$")
CLEAR = re.compile(r"відбій|отбой|збит|сбит|знищен|минус|мінус|чисто")

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

def calc(t, now):
    """Позиция по «счислению пути»: от последней точки к цели со средней скоростью."""
    _, gla, glo = t["goal"]
    tot = hav(t["lat"], t["lon"], gla, glo)
    lim = tot
    if t["kind"] == "kab":
        lim = min(tot, max(0, KAB_RANGE - hav(*t["o"], t["lat"], t["lon"])))
    d = min(lim, t["v"] * (now - t["t0"]) / 3600)
    f = d / tot if tot > 0.05 else 1
    la, lo = t["lat"] + (gla - t["lat"]) * f, t["lon"] + (glo - t["lon"]) * f
    return la, lo, d, lim, tot

def view(t, now):
    la, lo, d, lim, tot = calc(t, now)
    ended = d >= lim - 0.01 and t["v"] > 0
    r = dict(id=t["id"], kind=t["kind"], lat=la, lon=lo, o=t["o"], to=t["goal"][0], goal=t["goal"][1:],
             v=t["v"], hdg=round(brg(la, lo, *t["goal"][1:])), age=int(now - t["seen"]), text=t["text"][:140],
             eta=int((lim - d) / t["v"] * 3600) if t["v"] else None, ended=ended,
             home=round(hav(la, lo, HOME[1], HOME[2]), 1), short=lim < tot - 0.5, frm=t["frm"])
    if t["kind"] == "kab":
        r["alt"] = round(max(0, KAB_H0 * (1 - hav(*t["o"], la, lo) / KAB_RANGE)))
    return r

def upsert(kind, pos, tgt, ts, text, kmax=None):
    now = time.time()
    live = [t for t in tracks.values() if t["kind"] == kind and now - t["seen"] < TTL[kind]]
    t = None
    if pos:
        if live:
            n = min(live, key=lambda x: hav(*calc(x, now)[:2], pos[1], pos[2]))
            if hav(*calc(n, now)[:2], pos[1], pos[2]) < (kmax or MERGE_KM[kind]):
                t = n
    elif live:
        t = max(live, key=lambda x: x["seen"])
    if t is None:
        if not pos: return
        i = next(ids)
        t = tracks[i] = dict(id=i, kind=kind, o=(pos[1], pos[2]), frm=pos[0], goal=tgt or (pos if kind in ("aircraft", "strike") else HOME))
    if pos:
        t["lat"], t["lon"], t["t0"] = pos[1], pos[2], ts
    if tgt:
        if "v" in t:                                    # зафиксировать текущую точку, потом сменить курс
            t["lat"], t["lon"] = calc(t, now)[:2]; t["t0"] = now
        t["goal"] = tgt
    t["v"] = (600 if tgt else 0) if kind == "aircraft" else SPEED[kind]
    t["seen"], t["text"] = now, text

log, GEO_CACHE, geo_lock = [], {}, asyncio.Lock()
SKIP = re.compile(r"^(курс|курсом|напрям|балістик|групи|група|рухаєть|також|увага|реактив)")

def note(m):
    log.append(m[:160]); del log[:-25]

async def geo(s, text):
    """Неизвестные названия (с заглавной буквы) ищем в OpenStreetMap Nominatim и запоминаем."""
    for w in set(re.findall(r"[А-ЯІЇЄҐ][а-яіїєґ'’\-]{4,}", text)):
        lw = w.lower()
        if lw in GEO_CACHE or SKIP.match(lw) or any(re.match(rx, lw) for *_, rx in PLACES) or any(re.search(rx, lw) for _, rx in KINDS):
            continue
        GEO_CACHE[lw] = None
        async with geo_lock:
            for q in dict.fromkeys([w, w[:-1] + "а", w[:-1] + "я", w[:-1] + "е"]):
                try:
                    async with s.get("https://nominatim.openstreetmap.org/search", headers={"User-Agent": "radar-app/1.0"},
                                     params={"q": q, "format": "json", "countrycodes": "ua", "limit": 1, "accept-language": "uk",
                                             "viewbox": VIEWBOX, "bounded": 1}, timeout=aiohttp.ClientTimeout(total=6)) as r:
                        j = await r.json()
                except Exception:
                    j = []
                await asyncio.sleep(1.1)
                if j:
                    PLACES.append((j[0]["display_name"].split(",")[0], float(j[0]["lat"]), float(j[0]["lon"]),
                                   re.escape(lw[:-2] if len(lw) > 6 else lw[:-1])))
                    GEO_CACHE[lw] = True
                    break

NEWS = re.compile(r"https?://|t\.me|підписуй|подпиш|читайте|джерело|реклам|донат|загинул|загинув|поранен|постраждал|внаслідок|наслідк|зведення|за добу|минулої|вчора|заявив|повідомив|міністр|командув|генштаб|збито \d|знищено \d|втрат")

def ingest(ts, text, ch=""):
    tl, now = text.lower(), time.time()
    if NEWS.search(tl) or len(text) > 280:        # новости, итоги, реклама — не сводка
        return note(f"{ch}: «{text[:50]}» → новость/не сводка, пропуск")
    kinds = [k for k, rx in KINDS if re.search(rx, tl)]
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
    if CLEAR.search(tl):
        for t in list(tracks.values()):
            if (not kinds or t["kind"] in kinds) and (not pos or hav(*calc(t, now)[:2], pos[1], pos[2]) < 40):
                tracks.pop(t["id"])
        return note(f"{ch}: «{text[:50]}» → отбой")
    kmax = None
    if not kinds:                                  # короткие посты вида «До Новомиколаївки» — только уточняют уже летящую цель
        ref = pos or tgt
        live = [t for t in tracks.values() if t["kind"] in ("drone", "missile") and now - t["seen"] < TTL[t["kind"]]]
        if not live or len(text) > 160:
            return note(f"{ch}: «{text[:50]}» → без типа цели и нет активных целей, пропуск")
        kinds, kmax = [min(live, key=lambda t: hav(*calc(t, now)[:2], ref[1], ref[2]))["kind"]], 200
    for k in kinds:
        if k == "strike": upsert(k, pos or tgt, None, ts, text)
        else: upsert(k, pos, tgt, ts, text, kmax)
    note(f"{ch}: «{text[:50]}» → {kinds} поз={pos and pos[0]} цель={tgt and tgt[0]}")

def snap(now):
    return {"now": now, "home": HOME, "tracks": [view(t, now) for t in tracks.values()]}

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
            if now - t["seen"] > TTL[t["kind"]] or (view(t, now)["ended"] and now - t["seen"] > 120):
                tracks.pop(i)
        for t in tracks.values():                       # ТРЕВОГА только по КАБ на Запорожье
            v = view(t, now)
            if t["kind"] != "kab" or v["ended"] or hav(*t["goal"][1:], HOME[1], HOME[2]) > 25 or v["short"]:
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
                await geo(s, text)
                ingest(ts, text, ch)
            if new: await push()
        except Exception as e:
            print(ch, e); stats[ch] = {"error": str(e)}
        await asyncio.sleep(POLL)

async def events(req):
    r = web.StreamResponse(headers={"Content-Type": "text/event-stream", "Cache-Control": "no-cache"})
    await r.prepare(req)
    clients.add(r)
    try:
        await r.write(f"data: {json.dumps(snap(time.time()), ensure_ascii=False)}\n\n".encode())
        while True: await asyncio.sleep(3600)
    finally:
        clients.discard(r)

async def main():
    s = aiohttp.ClientSession()
    app = web.Application()
    D = os.path.dirname(os.path.abspath(__file__))
    f = lambda n: (lambda r: web.FileResponse(os.path.join(D, n)))
    app.add_routes([web.get("/", f("index.html")), web.get("/events", events),
                    web.get("/manifest.json", f("manifest.json")), web.get("/sw.js", f("sw.js")),
                    web.get("/icon-192.png", f("icon-192.png")), web.get("/icon-512.png", f("icon-512.png")),
                    web.get("/health", lambda r: web.Response(text="ok")),
                    web.get("/debug", lambda r: web.json_response({"channels": stats, "tracks": len(tracks), "log": log, "geo": [k for k, v in GEO_CACHE.items() if v]}, dumps=lambda o: json.dumps(o, ensure_ascii=False, indent=1)))])
    for ch in CHANNELS: asyncio.create_task(poll(s, ch))
    asyncio.create_task(tick(s))
    run = web.AppRunner(app); await run.setup()
    await web.TCPSite(run, "0.0.0.0", int(os.getenv("PORT", 8080))).start()
    print("http://localhost:8080"); await asyncio.Event().wait()

asyncio.run(main())
