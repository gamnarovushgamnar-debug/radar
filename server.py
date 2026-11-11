"""Радар повітряних цілей — v4.

Принципи:
1. Нічого не вигадуємо. Точка пуску, курс і швидкість беруться лише з тексту поста.
   Немає даних — немає маркера (або маркер без стрілки).
2. ШІ (якщо є ключ) витягує події у строгій схемі. Кожне поле перевіряється:
   назва має бути в тексті, число курсу/швидкості — теж. Без ключа працюють правила.
3. Ціль не зникає мовчки: стани active → stale → lost → ended (з причиною).
4. Точки поза областю (Таганрог, Ростов, Крим) — дійсні точки пуску.
5. Курси не усереднюються: береться останній явний курс.

Запуск: pip install aiohttp beautifulsoup4 && python server.py
"""

import asyncio, json, math, os, re, time
from datetime import datetime

import aiohttp
from aiohttp import web
from bs4 import BeautifulSoup

# ─── Налаштування ────────────────────────────────────────────────────────────
CHANNELS = [c.strip() for c in os.getenv(
    "CHANNELS",
    "eyes_everywhere_ua,operatyvnohlep,gnilayachereha,rotodrellomod,kpszsu,zp_oblig,vanek_nikolaev"
).split(",") if c.strip()]
HOME = ("Запоріжжя", 47.838, 35.139)
POLL = float(os.getenv("POLL_INTERVAL", "1.5"))
LOOKBACK = int(os.getenv("LOOKBACK", "1500"))       # с: скільки старих постів брати при старті
NTFY = os.getenv("NTFY_TOPIC")
PORT = int(os.getenv("PORT", "8080"))

GROQ_KEY = os.getenv("GROQ_API_KEY")
GEMINI_KEY = os.getenv("GEMINI_API_KEY")
OPENAI_KEY = os.getenv("OPENAI_API_KEY")
ANTHROPIC_KEY = os.getenv("ANTHROPIC_API_KEY")
LLM_ON = bool(GROQ_KEY or GEMINI_KEY or OPENAI_KEY or ANTHROPIC_KEY)

ALERT_ETA_LVL2 = 60          # с: «негайно в укриття»
HOME_RADIUS_KM = 25          # ціль «на Запоріжжі», якщо в межах цього радіусу
KAB_RANGE_KM = 85            # дальність планування КАБ
KAB_H0 = 9000                # м: орієнтовна висота скидання (лише для показу)
OBLAST_BBOX = [[46.7, 34.3], [46.7, 36.9], [48.2, 36.9], [48.2, 34.3]]  # приблизна рамка
UA_RU_BOX = (43.0, 50.5, 33.0, 41.0)                                     # геокодер: UA/RU
MERGE_KM = {"drone": 25, "kab": 30, "missile": 60, "aircraft": 40, "impact": 2, "strike": 12}

# ─── Геометрія ───────────────────────────────────────────────────────────────
def hav(la1, lo1, la2, lo2):
    p = math.pi / 180
    a = (math.sin((la2 - la1) * p / 2) ** 2
         + math.cos(la1 * p) * math.cos(la2 * p) * math.sin((lo2 - lo1) * p / 2) ** 2)
    return 12742 * math.asin(math.sqrt(min(1.0, a)))

def brg(la1, lo1, la2, lo2):
    p = math.pi / 180
    y = math.sin((lo2 - lo1) * p) * math.cos(la2 * p)
    x = math.cos(la1 * p) * math.sin(la2 * p) - math.sin(la1 * p) * math.cos(la2 * p) * math.cos((lo2 - lo1) * p)
    return (math.degrees(math.atan2(y, x)) + 360) % 360

def fwd(la, lo, hdg, km):
    d = km / 6371.0
    th = math.radians(hdg)
    p1, l1 = math.radians(la), math.radians(lo)
    p2 = math.asin(math.sin(p1) * math.cos(d) + math.cos(p1) * math.sin(d) * math.cos(th))
    l2 = l1 + math.atan2(math.sin(th) * math.sin(d) * math.cos(p1), math.cos(d) - math.sin(p1) * math.sin(p2))
    return math.degrees(p2), math.degrees(l2)

def angdiff(a, b):
    return (a - b + 180) % 360 - 180

# ─── Географія: назви з тексту → координати ──────────────────────────────────
# (назва, lat, lon, regex-основа). Точки поза областю — точки пуску (LAUNCH).
RAW_PLACES = [
    # Місто Запоріжжя та райони
    ("Дніпровський р-н / Правий берег (ЗП)", 47.868, 35.062, r"дніпровськ\w+\s+р-?н\w*|дніпровськ\w+\s+район\w*|осипенківськ\w*|осипенковск\w*|бородінськ\w*|бородинск\w*|верхн\w+\s+хортиц\w*|правий\s+берег"),
    ("Хортицький р-н / Бабурка (ЗП)", 47.816, 35.025, r"бабурк\w*|хортицьк\w+\s+р|хортицк\w+\s+р"),
    ("Хортиця (Запоріжжя)", 47.822, 35.075, r"острів\s+хортиц\w*|о\.\s*хортиц\w*|\bхортиц\w*"),
    ("Шевченківський р-н (ЗП)", 47.832, 35.225, r"шевченківськ\w+\s+р|шевченковск\w+\s+р|зелен\w+\s+яр\w*|шевчик\w*"),
    ("Комунарський р-н / Космос (ЗП)", 47.785, 35.185, r"комунарськ\w*|коммунарск\w*|космос\w*|південн\w+\s+мкрн|\bпіски\b"),
    ("Олександрівський р-н (ЗП)", 47.815, 35.152, r"олександрівськ\w+\s+р|александровск\w+\s+р"),
    ("Вознесенівський р-н (ЗП)", 47.842, 35.128, r"вознесенівськ\w*|вознесеновск\w*"),
    ("Заводський р-н / Кічкас (ЗП)", 47.888, 35.158, r"заводськ\w+\s+р|кічкас\w*|кичкас\w*|павло-?кічкас\w*"),
    ("Запоріжжя", 47.838, 35.139, r"запоріжж\w+|запорожь\w+|\bзп\b"),
    # Передмістя і південь області
    ("Балабине", 47.745, 35.220, r"балабин\w*|балабін\w*"),
    ("Кушугум", 47.712, 35.208, r"кушугум\w*"),
    ("Малокатеринівка", 47.659, 35.260, r"малокатеринівк\w*|малокатериновк\w*"),
    ("Канівське", 47.721, 35.118, r"канівськ\w*|каневск\w*"),
    ("Розумівка", 47.752, 35.038, r"розумівк\w*|разумовк\w*"),
    ("Нижня Хортиця", 47.772, 35.033, r"нижн\w+\s+хортиц\w*"),
    ("Біленьке", 47.625, 35.041, r"біленьк\w*|беленьк\w*"),
    ("Мар'ївка", 47.669, 35.064, r"мар['’]?ївк\w*|марьевк\w*"),
    ("Долинське", 47.788, 34.943, r"долинськ\w*|долинск\w*"),
    ("Широке", 47.892, 34.906, r"широк\w+"),
    ("Сонячне", 47.866, 34.985, r"сонячн\w+|солнечн\w+"),
    ("Володимирівське", 47.902, 35.068, r"володимирівськ\w*|владимировск\w*"),
    ("Лежине", 47.807, 35.335, r"лежин\w*"),
    ("Наталівка", 47.838, 35.349, r"наталівк\w*|наталовк\w*"),
    ("Степне", 47.795, 35.308, r"степн(?!огір)\w+"),
    ("Матвіївка", 47.909, 35.313, r"матвіївк\w*|матвеевк\w*"),
    ("Кам'яне", 47.878, 35.398, r"кам['’]?ян(?!ськ|к)\w+"),
    ("Дружелюбівка", 47.893, 35.367, r"дружелюбівк\w*|дружелюбовк\w*"),
    # Вільнянський / Новомиколаївський напрямок
    ("Вільнянськ", 47.945, 35.433, r"вільнянськ\w*|вольнянск\w*|вільнянщ\w*"),
    ("Люцерна", 47.921, 35.231, r"люцерн\w*"),
    ("Михайлівка (Вільнянський р-н)", 47.953, 35.253, r"михайлівк\w*|михайловк\w*"),
    ("Петро-Михайлівка", 48.055, 35.235, r"петро-?михайлівк\w*|петро-?михайловк\w*"),
    ("Максимівка", 48.046, 35.405, r"максимівк\w*|максимовк\w*"),
    ("Павлівське", 47.962, 35.485, r"павлівськ\w*|павловск\w*"),
    ("Славгород", 48.105, 35.512, r"славгород\w*"),
    ("Новогупалівка", 48.023, 35.421, r"новогупалівк\w*|новогупаловк\w*"),
    ("Новомиколаївка", 47.976, 35.908, r"новомиколаївк\w*|новониколаевк\w*|новомиколаївщ\w*"),
    ("Терсянка", 47.882, 35.885, r"терсянк\w*"),
    ("Покровське", 47.982, 36.221, r"покровськ\w+|покровск\w+"),
    # Комишуваха / Оріхівський напрямок
    ("Комишуваха", 47.717, 35.529, r"комишува\w*|камышева\w*"),
    ("Григорівка", 47.680, 35.331, r"григорівк\w*|григорьевк\w*"),
    ("Веселянка", 47.684, 35.398, r"веселянк\w*"),
    ("Річне", 47.697, 35.428, r"річн\w+|речн\w+"),
    ("Юрківка", 47.651, 35.662, r"юрківк\w*|юрковк\w*"),
    ("Таврійське", 47.638, 35.696, r"таврійськ\w*|таврийск\w*"),
    ("Преображенка", 47.595, 35.816, r"преображенк\w*"),
    ("Оріхів", 47.567, 35.784, r"оріхів\w*|орехов\w*|оріхов\w*"),
    ("Мала Токмачка", 47.535, 35.893, r"мал\w+\s+токмачк\w*|токмачк\w*"),
    ("Білогір'я", 47.525, 36.002, r"білогір\w*|белогорь\w*"),
    ("Новоданилівка", 47.515, 35.808, r"новоданилівк\w*|новоданиловк\w*"),
    ("Роботине", 47.445, 35.839, r"роботин\w*|работин\w*"),
    ("Вербове", 47.428, 35.955, r"вербов\w+"),
    ("Новопокровка", 47.503, 35.986, r"новопокровк\w*"),
    ("Нестерянка", 47.495, 35.654, r"нестерянк\w*"),
    ("Копані", 47.454, 35.736, r"копан\w+"),
    # Василівський / Енергодарський напрямок
    ("Степногірськ", 47.587, 35.361, r"степногірськ\w*|степногорск\w*"),
    ("Приморське (Василівський р-н)", 47.619, 35.291, r"приморськ\w+"),
    ("Плавні", 47.561, 35.312, r"плавн\w+"),
    ("Кам'янське", 47.541, 35.349, r"кам['’]?янськ\w+"),
    ("Лобкове", 47.523, 35.437, r"лобков\w+"),
    ("П'ятихатки", 47.508, 35.456, r"п['’]?ятихатк\w*|пятихатк\w*"),
    ("Жереб'янки", 47.492, 35.426, r"жереб['’]?янк\w*"),
    ("Василівка", 47.435, 35.278, r"василівк\w*|васильевк\w*|василівськ\w*"),
    ("Зелений Гай", 47.392, 35.225, r"зелен\w+\s+га\w*"),
    ("Бурчак", 47.358, 35.261, r"бурчак\w*"),
    ("Дніпрорудне", 47.381, 34.985, r"дніпрорудн\w*|днепрорудн\w*"),
    ("Мала Білозерка", 47.246, 34.915, r"мал\w+\s+білозерк\w*|білозерк\w*"),
    ("Велика Білозерка", 47.274, 34.708, r"велик\w+\s+білозерк\w*"),
    ("Енергодар", 47.499, 34.658, r"енергодар\w*|энергодар\w*|\bзаес\b|\bзаэс\b"),
    ("Водяне (біля Енергодара)", 47.484, 34.492, r"водян\w+"),
    ("Кам'янка-Дніпровська", 47.494, 34.417, r"кам['’]?янк\w*-?дніпров\w*|кам['’]?янк\w*|каменк\w*"),
    ("Веселе", 46.992, 34.915, r"весел\w+"),
    # Гуляйпільський / Пологівський напрямок
    ("Гуляйполе", 47.661, 36.259, r"гуляйпол\w*|гуляйпіл\w*"),
    ("Залізничне", 47.649, 36.165, r"залізничн\w*|зализничн\w*|железнодорожн\w*"),
    ("Гуляйпільське", 47.612, 36.065, r"гуляйпільськ\w*"),
    ("Чарівне", 47.575, 36.052, r"чарівн\w*|чаривн\w*"),
    ("Червоне (Гуляйполе)", 47.652, 36.382, r"червон\w+"),
    ("Малинівка", 47.676, 36.471, r"малинівк\w*|малиновк\w*"),
    ("Полтавка", 47.712, 36.482, r"полтавк\w*"),
    ("Успенівка", 47.768, 36.452, r"успенівк\w*|успеновк\w*"),
    ("Воздвижівка", 47.742, 36.175, r"воздвижівк\w*|воздвижевк\w*"),
    ("Верхня Терса", 47.691, 36.071, r"верхн\w+\s+терс\w*|терс\w+"),
    ("Пологи", 47.482, 36.254, r"полог\w*"),
    ("Інженерне", 47.508, 36.131, r"інженерн\w*|инженерн\w*"),
    ("Костянтинівка (Пологи)", 47.461, 36.185, r"костянтинівк\w*|константиновк\w*"),
    ("Федорівка", 47.545, 36.435, r"федорівк\w*|федоровк\w*"),
    ("Більмак / Пологи", 47.358, 36.652, r"більмак\w*|бильмак\w*|куйбишев\w*"),
    ("Комиш-Зоря", 47.326, 36.692, r"комиш-?зор\w*|камыш-?зар\w*"),
    ("Розівка", 47.388, 37.065, r"розівк\w*|розовк\w*"),
    # Токмацький / Мелітопольський / Бердянський напрямок
    ("Токмак", 47.254, 35.704, r"токмак\w*|токмацьк\w*"),
    ("Молочанськ", 47.194, 35.624, r"молочанськ\w*|молочанск\w*"),
    ("Чернігівка", 47.192, 36.201, r"чернігівк\w*|черниговк\w*"),
    ("Мелітополь", 46.849, 35.365, r"мелітопол\w*|мелитопол\w*"),
    ("Мирне (Мелітополь)", 46.942, 35.431, r"мирн\w+"),
    ("Якимівка", 46.697, 35.164, r"якимівк\w*|акимовк\w*"),
    ("Кирилівка", 46.373, 35.367, r"кирилівк\w*|кирилловк\w*"),
    ("Приазовське", 46.736, 35.635, r"приазовськ\w*|приазовск\w*"),
    ("Приморськ", 46.735, 36.344, r"приморськ\b|приморск\b"),
    ("Бердянськ", 46.755, 36.788, r"бердянськ\w*|бердянск\w*"),
    # Сусідні райони
    ("Марганець", 47.645, 34.604, r"марганц\w*|марганец\w*"),
    ("Нікополь", 47.567, 34.400, r"нікопол\w*|никопол\w*"),
    ("Томаківка", 47.816, 34.742, r"томаківк\w*|томаковк\w*"),
    ("Мирова", 47.765, 34.645, r"миров\w+"),
    ("Солоне", 48.204, 34.872, r"солон\w+"),
    ("Синельникове", 48.318, 35.512, r"синельников\w*|синельників\w*"),
    ("Васильківка", 48.212, 36.032, r"васильківк\w*|васильковк\w*"),
    ("Павлоград", 48.533, 35.867, r"павлоград\w*"),
    # ТОЧКИ ПУСКУ ПОЗА ОБЛАСТЮ (дійсні джерела польоту)
    ("Таганрог", 47.236, 38.897, r"таганрог\w*|таганрож\w*"),
    ("Ростов-на-Дону", 47.235, 39.701, r"ростов\w*"),
    ("Міллерово", 48.923, 40.390, r"міллеров\w*|миллеров\w*"),
    ("Новочеркаськ", 47.422, 40.093, r"новочеркасск\w*|новочеркаськ\w*"),
    ("Бєлгород", 50.596, 36.588, r"бєлгород\w*|белгород\w*|білгород\w*"),
    ("Сімферополь", 44.952, 34.102, r"сімферопол\w*|симферопол\w*"),
    ("Джанкой", 45.711, 34.389, r"джанкой\w*"),
    ("Бельбек", 44.687, 33.572, r"бельбек\w*"),
    ("Саки", 45.128, 33.602, r"саки\b|сакі\b"),
]
PLACES = RAW_PLACES
BLOCK_NAMES = {"дніпро", "запорізька область", "запорізька обл", "запорізький напрямок"}

def find_place(name):
    """Назва → (назва, lat, lon) або None. Не відкидає точки за межами області."""
    if not isinstance(name, str):
        return None
    n = re.sub(r"^(?:м\.|с\.|смт|селище|місто|р-н)\s+", "", name.lower().strip())
    if len(n) < 3 or n in BLOCK_NAMES:
        return None
    best = None
    for pn, la, lo, rx in PLACES:
        if pn.lower() == n or re.match(rf"(?:{rx})", n):
            if best is None or len(rx) > best[0]:
                best = (len(rx), (pn, la, lo))
    return best[1] if best else None

def scan_places(sentence):
    """Знаходить згадки місць у реченні разом із прийменником перед ними."""
    low = sentence.lower()
    hits, seen = [], set()
    for pn, la, lo, rx in PLACES:
        for m in re.finditer(rf"(?<![а-яіїєґa-z'’])(?:{rx})", low):
            if m.start() in seen:
                continue
            seen.add(m.start())
            pre = re.search(r"([а-яіїєґa-z'’\-]+)\s+$", low[:m.start()])
            hits.append((m.start(), pn, la, lo, pre.group(1) if pre else ""))
    hits.sort()
    return hits

# ─── Класифікація тексту (правила) ───────────────────────────────────────────
KINDS = ("drone", "kab", "missile", "aircraft", "strike", "impact")
KIND_NAME = {"drone": "БпЛА", "kab": "КАБ", "missile": "Ракета", "aircraft": "Борт", "strike": "Обстріл", "impact": "Приліт"}
KIND_COL = {"drone": "#e3b341", "kab": "#f85149", "missile": "#ff8a3d", "aircraft": "#58a6ff", "strike": "#d2a8ff", "impact": "#ff5c5c"}
KIND_ICO = {"drone": "delta", "kab": "kab", "missile": "missile", "aircraft": "plane2", "strike": "star", "impact": "star"}

KIND_RX = [
    ("kab", re.compile(r"\bкаб(?:и|ів|ах|ам|ом|у|а)?\b|авіабомб\w*|авиабомб\w*|\bумпк\w*|\bумпб\w*|\bфаб\b|фаб-?\s?(?:1500|3000)")),
    ("aircraft", re.compile(r"ракетоносі\w*|ракетоносц\w*|ракетоносн\w*|\bту-?(?:95|160|22)\w*|борт\w*|\bсу-?\d{2}\w*|міг-?\d{2}|миг-?\d{2}|гелікоптер\w*|вертол\w*|\bка-?52|\bмі-?\d{1,2}\b")),
    ("missile", re.compile(r"ракет\w*|кинджал\w*|кінджал\w*|циркон\w*|іскандер\w*|искандер\w*|калібр\w*|калибр\w*|онікс\w*|оникс\w*|баліст\w*|балист\w*|\bх-?\d{2,3}\b|\bс-?(?:300|400)\b|\bп-?800\b|\bкр\b")),
    ("drone", re.compile(r"бпла|\bбла\b|шахед\w*|шахид\w*|герань|геран\w*|дрон\w*|безпілотн\w*|беспилотн\w*|\bfpv\b|\bфпв\w*|орлан\w*|ланцет\w*|молні\w*|молни\w*|мопед\w*|гербер\w*|італмас|италмас|газонокосарк\w*|суперкам\w*|елерон\w*|форпост\w*|камікадз\w*|камикадз\w*")),
    ("strike", re.compile(r"рсзо|\bград\w*|обстріл\w*|обстрел\w*|артобстр\w*|артилер\w*|приліт\w*|прилет\w*|мінометн\w*")),
]
CLEAR_RX = re.compile(r"збит\w*|сбит\w*|знищ\w*|уничтож\w*|відбій|отбой|\bмінус\b|\bминус\b|приземлил\w*")
LOST_RX = re.compile(r"\bвпали\b|\bвпала\b|зник\w*|пропал\w*|втрачен\w*|не\s+фіксу\w*|не\s+видно")
IMPACT_RX = re.compile(r"влуч\w*|\bупав\b|\bупала\b|\bупали\b|вибух\w*|взрыв\w*|попад\w*|\bвпав\b")
CNT_RX = re.compile(r"(?<![\d:.,/])(\d{1,2})\s*(?:шт\b|од\b|×|бпла|дрон|каб|ракет|борт|ціл)")
DEG_RX = re.compile(r"(?:курс\w*|азимут\w*|напрям\w*)\s*(?:на\s*)?(\d{1,3})\s*(?:°|град\w*)", re.I)
SPEED_RX = re.compile(r"(\d{2,4})\s*км\s*/\s*(?:год|г|ч)\b", re.I)
FLIGHT_HINT = re.compile(
    r"бпла|\bбла\b|каб|ракет|шахед|шахид|герань|геран|дрон|борт|збит|сбит|впал|зник|пропал|летит|летить|"
    r"курс|баліст|балист|кинджал|кінджал|циркон|іскандер|калібр|онікс|приліт|прилет|влуч|вибух|fpv|фпв|"
    r"орлан|ланцет|мопед|гербер|італмас|ту-\d|су-\d|міг|миг|вертол|гелікоптер|обстріл|обстрел|рсзо"
)
ORIGIN_PREP = {"з", "із", "зі", "від", "з-під", "із-під", "с", "из", "от"}
TOWARD_PREP = {"на", "до", "к", "бік", "напрямку", "напрямок", "курсом"}

# (тип, regex, назва, іконка, колір, швидкість км/год, макс. сек екстраполяції)
# Екстраполяція (dead reckoning) — лише з явним курсом/ціллю і лише обмежений час.
SUB_RULES = [
    ("drone", r"реактивн\w*\s+(?:шахед|шахид|герань|геран|бпла|дрон)|геран-?3|герань-?3", "Реактивний Шахед", "jet", "#ff6b3d", 520, 900),
    ("drone", r"гербер\w*|італмас|италмас|імітатор\w*|имитатор\w*", "Імітатор (Гербера/Італмас)", "decoy", "#c9a227", 160, 900),
    ("drone", r"шахед\w*|шахид\w*|герань|геран\w*|мопед\w*|газонокосарк\w*", "Шахед", "delta", "#e3b341", 180, 900),
    ("drone", r"\bfpv\b|\bфпв\w*|оптоволокн\w*", "FPV-дрон", "quad", "#f2cc60", 110, 300),
    ("drone", r"орлан\w*|суперкам\w*|елерон\w*|форпост\w*|розвід\w*", "Розвідувальний БпЛА", "plane", "#9aa7b8", 110, 900),
    ("drone", r".", "БпЛА", "delta", None, 165, 900),
    ("kab", r"фаб-?\s?(?:1500|3000)", "ФАБ-1500/3000", "kab", None, 650, 300),
    ("kab", r".", "КАБ", "kab", None, 650, 300),
    ("missile", r"кинджал\w*|кінджал\w*|циркон\w*|баліст\w*|балист\w*|іскандер-?м|искандер-?м", "Балістична/гіперзвукова", "ballistic", "#ff453a", 4800, 200),
    ("missile", r"калібр\w*|калибр\w*|іскандер-?к|х-?(?:101|102|555|55)\b|онікс\w*|оникс\w*|крилат\w*", "Крилата ракета", "cruise", "#ff8a3d", 850, 400),
    ("missile", r".", "Ракета", "dart", "#ff8a3d", 900, 300),
    ("aircraft", r".", "Борт (ТА)", "plane2", "#58a6ff", 750, 0),
]

def classify(s):
    low = s.lower()
    for kind, rx in KIND_RX:
        if rx.search(low):
            return kind
    return None

def status_of(s, kind):
    if CLEAR_RX.search(s.lower()):
        return "downed"
    if LOST_RX.search(s.lower()):
        return "lost"
    if IMPACT_RX.search(s.lower()) or kind == "strike":
        return "impact"
    return "flying"

def sub_of(kind, text):
    low = text.lower()
    for k, rx, name, ico, col, sp, drm in SUB_RULES:
        if k == kind and re.search(rx, low):
            return dict(name=name, ico=ico, col=col, speed=sp, dr_max=drm)
    return dict(name=KIND_NAME[kind], ico=KIND_ICO[kind], col=None, speed=0, dr_max=0)

def rule_events(text):
    """Резервний розбір без ШІ. Місця і числа беруться лише з тексту."""
    out = []
    for sent in re.split(r"[.;!?\n]+", text):
        s = sent.strip()
        if not s:
            continue
        kind = classify(s)
        if not kind:
            continue
        status = status_of(s, kind)
        ev = dict(kind=kind, status=status, at=None, origin=None, toward=None,
                  heading=None, speed=None, count=None, quote=s[:160], src="rules")
        for _, pn, la, lo, prep in scan_places(s):
            p = (pn, la, lo)
            if prep in ORIGIN_PREP and ev["origin"] is None:
                ev["origin"] = p
            elif prep in TOWARD_PREP and ev["toward"] is None:
                ev["toward"] = p
            elif ev["at"] is None:
                ev["at"] = p
        if status == "flying" and not (ev["at"] or ev["origin"] or ev["toward"]):
            continue
        m = DEG_RX.search(s)
        ev["heading"] = int(m.group(1)) if m and int(m.group(1)) <= 359 else None
        m = SPEED_RX.search(s)
        ev["speed"] = float(m.group(1)) if m else None
        m = CNT_RX.search(s)
        ev["count"] = m.group(1) if m else None
        out.append(ev)
    return out

# ─── ШІ-екстракція зі строгою перевіркою ─────────────────────────────────────
EXTRACT_SYSTEM = """Ти витягуєш події про повітряні цілі з повідомлення телеграм-каналу про Запорізьку область і околиці.
Поверни ЛИШЕ JSON виду {"events": [...]} без пояснень. Кожна подія:
{"type": "drone|kab|missile|aircraft|strike|unknown",
 "count": число або null,
 "status": "flying|lost|downed|impact|unknown",
 "origin": "назва" або null,
 "at": "назва" або null,
 "toward": "назва" або null,
 "heading_deg": число 0-359 або null,
 "speed_kmh": число або null,
 "quote": "коротка цитата з тексту"}

Правила:
1. Нічого не вигадуй. Якщо поля немає в тексті, став null. Точку пуску, курс і швидкість не визначай самостійно.
2. origin — ЗВІДКИ запущено або вилетіло: лише після слів «з», «із», «від», «пуск з», «запущено з».
3. at — ДЕ ціль ЗАРАЗ: «над X», «біля X», «у районі X», або «X БпЛА» (назва і тип). Це НЕ origin.
4. toward — КУДИ летить: «на X», «до X», «у бік X», «курсом на X».
5. heading_deg і speed_kmh — лише якщо курс чи швидкість названі числом у тексті.
6. «впали», «зникли», «пропали» без «збито» → status lost. «збито», «знищено», «відбій» → downed. «влучання», «упав», «вибух» → impact.
7. «Запоріжжя:» на початку посту — назва регіону, не місце. Наприклад, «Запоріжжя: КАБ на Оріхів» → toward Оріхів, at і origin null.
8. Новини, коментарі, прогнози без конкретної цілі → {"events": []}.
9. Кілька цілей — кілька подій."""

KNOWN_TYPES = {"drone", "kab", "missile", "aircraft", "strike"}
STATUSES = {"flying", "lost", "downed", "impact"}

def validate_llm(info, text):
    tl = text.lower()

    def name(v):
        if isinstance(v, dict):
            v = v.get("name")
        if not isinstance(v, str):
            return None
        v = v.strip()
        return v if len(v) >= 4 and v.lower()[:4] in tl else None   # назва має бути в тексті

    def num(v, lo, hi):
        if isinstance(v, bool) or not isinstance(v, (int, float)) or not lo <= v <= hi:
            return None
        return int(v) if re.search(rf"(?<![\d.,]){int(v)}(?!\d)", text) else None   # число має бути в тексті

    out = []
    for it in (info.get("events") or [])[:12]:
        if not isinstance(it, dict):
            continue
        kind = it.get("type")
        if kind not in KNOWN_TYPES:
            continue
        status = it.get("status") if it.get("status") in STATUSES else "flying"
        at, origin, toward = name(it.get("at")), name(it.get("origin")), name(it.get("toward"))
        if status == "flying" and not (at or origin or toward):
            continue
        out.append(dict(kind=kind, status=status, at=at, origin=origin, toward=toward,
                        heading=num(it.get("heading_deg"), 0, 359),
                        speed=num(it.get("speed_kmh"), 20, 8000),
                        count=num(it.get("count"), 1, 99),
                        quote=str(it.get("quote") or text)[:160], src="ai"))
    return out

def provider_name():
    if GROQ_KEY: return "Groq"
    if GEMINI_KEY: return "Gemini"
    if OPENAI_KEY: return "OpenAI"
    if ANTHROPIC_KEY: return "Claude"
    return "правила (без ШІ)"

async def llm_raw(s, system, user):
    if GROQ_KEY:
        async with s.post("https://api.groq.com/openai/v1/chat/completions",
                          headers={"Authorization": f"Bearer {GROQ_KEY}"},
                          json={"model": os.getenv("GROQ_MODEL", "llama-3.3-70b-versatile"), "temperature": 0,
                                "max_tokens": 400, "response_format": {"type": "json_object"},
                                "messages": [{"role": "system", "content": system}, {"role": "user", "content": user}]},
                          timeout=aiohttp.ClientTimeout(total=8)) as r:
            j = await r.json()
            return j["choices"][0]["message"]["content"]
    if GEMINI_KEY:
        model = os.getenv("GEMINI_MODEL", "gemini-2.5-flash")
        async with s.post(f"https://generativelanguage.googleapis.com/v1beta/models/{model}:generateContent?key={GEMINI_KEY}",
                          json={"systemInstruction": {"parts": [{"text": system}]},
                                "contents": [{"parts": [{"text": user}]}],
                                "generationConfig": {"temperature": 0, "maxOutputTokens": 400,
                                                     "responseMimeType": "application/json"}},
                          timeout=aiohttp.ClientTimeout(total=8)) as r:
            j = await r.json()
            return j["candidates"][0]["content"]["parts"][0]["text"]
    if OPENAI_KEY:
        async with s.post("https://api.openai.com/v1/chat/completions",
                          headers={"Authorization": f"Bearer {OPENAI_KEY}"},
                          json={"model": os.getenv("OPENAI_MODEL", "gpt-4o-mini"), "temperature": 0,
                                "max_tokens": 400, "response_format": {"type": "json_object"},
                                "messages": [{"role": "system", "content": system}, {"role": "user", "content": user}]},
                          timeout=aiohttp.ClientTimeout(total=8)) as r:
            j = await r.json()
            return j["choices"][0]["message"]["content"]
    if ANTHROPIC_KEY:
        async with s.post("https://api.anthropic.com/v1/messages",
                          headers={"x-api-key": ANTHROPIC_KEY, "anthropic-version": "2023-06-01"},
                          json={"model": os.getenv("LLM_MODEL", "claude-haiku-4-5"), "max_tokens": 400,
                                "system": system, "messages": [{"role": "user", "content": user}]},
                          timeout=aiohttp.ClientTimeout(total=8)) as r:
            j = await r.json()
            return "".join(b.get("text", "") for b in j.get("content", []) if b.get("type") == "text")
    return None

LLM_CACHE, LLM_SEM, GEO_LOCK = {}, None, None
STATS = {"calls": 0, "ok": 0, "ms": 0, "err": None}

async def llm_extract(s, text):
    if text in LLM_CACHE:
        return LLM_CACHE[text]
    async with LLM_SEM:
        STATS["calls"] += 1
        t0 = time.perf_counter()
        try:
            raw = await llm_raw(s, EXTRACT_SYSTEM, text)
            m = re.search(r"\{.*\}", raw or "", re.S)
            info = json.loads(m.group(0)) if m else None
            if not isinstance(info, dict) or not isinstance(info.get("events"), list):
                raise ValueError("немає поля events")
            STATS["ok"] += 1
        except Exception as e:
            STATS["err"] = str(e)[:120]
            return None
        finally:
            STATS["ms"] = round((time.perf_counter() - t0) * 1000)
    LLM_CACHE[text] = info
    if len(LLM_CACHE) > 500:
        LLM_CACHE.pop(next(iter(LLM_CACHE)))
    return info

# ─── Геокодування ────────────────────────────────────────────────────────────
GEO_CACHE = {}

async def geocode(s, name):
    async with GEO_LOCK:
        await asyncio_sleep(1.1)   # Nominatim: не більше ~1 запиту на секунду
        try:
            async with s.get("https://nominatim.openstreetmap.org/search",
                             headers={"User-Agent": "zaporizhzhia-radar/4.0"},
                             params={"q": name, "format": "json", "countrycodes": "ua,ru",
                                     "limit": 5, "accept-language": "uk"},
                             timeout=aiohttp.ClientTimeout(total=6)) as r:
                j = await r.json(content_type=None)
        except Exception:
            return None
    cands = []
    for x in j or []:
        la, lo = float(x["lat"]), float(x["lon"])
        if UA_RU_BOX[0] <= la <= UA_RU_BOX[1] and UA_RU_BOX[2] <= lo <= UA_RU_BOX[3]:
            cands.append((hav(la, lo, HOME[1], HOME[2]), (x["display_name"].split(",")[0].strip(), la, lo)))
    return min(cands)[1] if cands else None

async def asyncio_sleep(sec):
    await asyncio.sleep(sec)

async def geo_place(s, name):
    p = find_place(name)
    if p:
        return p
    key = name.lower()
    if key not in GEO_CACHE:
        GEO_CACHE[key] = await geocode(s, name)
    return GEO_CACHE[key]

async def to_place(s, x):
    if x is None:
        return None
    if isinstance(x, tuple):
        return x
    return await geo_place(s, x)

# ─── Треки ───────────────────────────────────────────────────────────────────
tracks = {}
feed, log = [], []
ALERTED, SEEN, SRC_STATS = set(), set(), {}
clients = set()
_seq = 0

def new_track(kind, sub, ts, ch, src):
    global _seq
    _seq += 1
    t = dict(id=_seq, kind=kind, sub=sub, name=sub["name"], ico=sub["ico"],
             col=sub["col"] or KIND_COL[kind], cnt=None,
             lat=HOME[1], lon=HOME[2], a_ts=ts, a_by="at",
             origin=None, heading=None, speed=None, toward=None,
             last_seen=ts, created=ts, dr_max=sub["dr_max"],
             lost=False, ended=False, end_reason="", expire=None,
             text="", chs=set(), posts=set(), ai=(src == "ai"))
    tracks[_seq] = t
    return t

def make(kind, ev, ts, ch):
    return new_track(kind, sub_of(kind, ev["quote"]), ts, ch, ev["src"])

def anchor(t, pos, ts, by):
    """Нова позиція з тексту. Старіші звіти не перезаписують новіші."""
    if ts < t["a_ts"]:
        return
    t["lat"], t["lon"], t["a_ts"], t["a_by"] = pos[1], pos[2], ts, by

def eff_speed(t):
    if t["speed"]:
        return t["speed"]
    return t["sub"]["speed"] if (t["heading"] is not None or t["toward"]) else None

def dr(t, now):
    """Позиція зараз: (lat, lon, курс|None, досягнуто). Рух — лише з явним курсом/ціллю і лише обмежений час."""
    la, lo = t["lat"], t["lon"]
    if t["lost"] or t["ended"] or (t["heading"] is None and not t["toward"]):
        return la, lo, t["heading"], False
    dt = now - t["a_ts"]
    if dt <= 0 or dt > t["dr_max"]:          # занадто давно — не вгадуємо, стоїмо на останній точці
        hdg = brg(la, lo, t["toward"][1], t["toward"][2]) if t["toward"] else t["heading"]
        return la, lo, hdg, False
    v = eff_speed(t)
    if not v:
        return la, lo, t["heading"], False
    d = v * dt / 3600.0
    if t["toward"]:
        _, tla, tlo = t["toward"]
        total = hav(la, lo, tla, tlo)
        hdg = brg(la, lo, tla, tlo)
        if d >= total:
            return tla, tlo, hdg, True
    else:
        hdg = t["heading"]
    nla, nlo = fwd(la, lo, hdg, d)
    return nla, nlo, hdg, False

def aims_home(t, la, lo, hdg):
    if t["toward"]:
        return hav(t["toward"][1], t["toward"][2], HOME[1], HOME[2]) <= HOME_RADIUS_KM
    if t["heading"] is not None and hdg is not None:
        return (abs(angdiff(hdg, brg(la, lo, HOME[1], HOME[2]))) <= 25
                and hav(la, lo, HOME[1], HOME[2]) <= KAB_RANGE_KM)
    return False

def find_match(kind, pos, ts):
    """Той самий тип поблизу (з урахуванням руху) — це та сама ціль, а не нова."""
    best, bd = None, None
    for t in tracks.values():
        if t["kind"] != kind or t["ended"] or ts - t["last_seen"] > 1200:
            continue
        la, lo, _, _ = dr(t, ts)
        d = hav(la, lo, pos[1], pos[2])
        if d <= MERGE_KM.get(kind, 20) and (bd is None or d < bd):
            best, bd = t, d
    return best

def find_origin(kind, origin, ts):
    """Той самий пуск з того самого місця (дублі з різних каналів)."""
    for t in tracks.values():
        if (t["kind"] == kind and not t["ended"] and t["origin"] and ts - t["created"] < 900
                and hav(t["origin"][1], t["origin"][2], origin[1], origin[2]) < 5):
            return t
    return None

def closest_track(kind, pos, ts):
    cands = [t for t in tracks.values() if t["kind"] == kind and not t["ended"]]
    if not cands:
        return None
    if pos is None:
        return max(cands, key=lambda t: t["last_seen"])
    def dist(t):
        la, lo, _, _ = dr(t, ts)
        return hav(la, lo, pos[1], pos[2])
    best = min(cands, key=dist)
    return best if dist(best) <= MERGE_KM.get(kind, 20) else None

def close_nearest(kind, pos, count, ts, reason):
    for _ in range(int(count) if count else 1):
        t = closest_track(kind, pos, ts)
        if t is None:
            break
        t["ended"], t["end_reason"], t["expire"] = True, reason, ts + 60

def ingest(ev, ts, ch, msg_key):
    """Застосовує одну подію до треків. Повертає список тривог (key, lvl, text)."""
    kind, status = ev["kind"], ev["status"]
    pos = ev["at"] or ev["origin"]

    if status == "downed":
        close_nearest(kind, pos, ev.get("count"), ts, "збито")
        return []
    if status == "impact":
        if pos:
            t = new_track("impact", dict(name="Приліт", ico="star", col="#ff5c5c", speed=0, dr_max=0), ts, ch, ev["src"])
            t["lat"], t["lon"] = pos[1], pos[2]
            t["ended"], t["end_reason"], t["expire"] = True, "приліт", ts + 600
            t["chs"].add(ch); t["posts"].add(msg_key); t["text"] = ev["quote"]
        return []
    if status == "lost":
        t = closest_track(kind, pos, ts)
        if t:
            t["lost"] = True
        return []
    if kind == "strike":
        return []   # обстріл без польоту не відстежуємо, лише в стрічці

    if ev["at"]:
        t = find_match(kind, ev["at"], ts) or make(kind, ev, ts, ch)
        anchor(t, ev["at"], ts, "at")
    elif ev["origin"]:
        t = find_origin(kind, ev["origin"], ts)
        if t is None:
            t = make(kind, ev, ts, ch)
            t["origin"] = ev["origin"]
            anchor(t, ev["origin"], ts, "launch")
    else:
        # Є лише курс/ціль без позиції: не малюємо, але КАБ на місто попереджаємо
        if (ev["toward"] and kind == "kab"
                and hav(ev["toward"][1], ev["toward"][2], HOME[1], HOME[2]) <= HOME_RADIUS_KM):
            return [(("msg", msg_key), 1, f"КАБ курсом на {HOME[0]} (місце пуску не вказано)")]
        return []

    if ev["origin"] and t["origin"] is None:
        t["origin"] = ev["origin"]
    if ev["heading"] is not None:
        t["heading"] = float(ev["heading"])
    if ev["toward"]:
        t["toward"] = ev["toward"]
    if ev["speed"]:
        t["speed"] = float(ev["speed"])
    if ev["count"]:
        t["cnt"] = str(ev["count"])
    if ev["src"] == "ai":
        t["ai"] = True
    t["lost"] = False
    t["last_seen"] = max(t["last_seen"], ts)
    t["chs"].add(ch); t["posts"].add(msg_key); t["text"] = ev["quote"]
    return []

# ─── Відображення, тривоги, SSE ──────────────────────────────────────────────
def uncertainty(t, age):
    base = 1.5 if t["a_by"] == "at" else 2.5
    moving = t["heading"] is not None or t["toward"]
    return min(30.0, base + (min(10.0, age / 60.0 * 0.3) if moving else age / 60.0))

def view(t, now):
    la, lo, hdg, arrived = dr(t, now)
    age = int(max(0, now - t["last_seen"]))
    sp = eff_speed(t)
    goal = t["toward"]
    home = hav(la, lo, HOME[1], HOME[2])
    eta = None
    if goal and sp and not t["ended"] and not arrived:
        eta = int(hav(la, lo, goal[1], goal[2]) / sp * 3600)
    etah = int(home / sp * 3600) if (sp and not t["ended"] and aims_home(t, la, lo, hdg)) else None
    short = bool(t["kind"] == "kab" and t["origin"] and goal
                 and hav(t["origin"][1], t["origin"][2], goal[1], goal[2]) > KAB_RANGE_KM)
    name = t["name"] + (f" ×{t['cnt']}" if t["cnt"] else "")
    if t["ended"]:
        name += f" — {t['end_reason']}"
    elif t["lost"]:
        name += " — втрачено з радару"
    alt = None
    if t["kind"] == "kab" and t["origin"]:
        alt = round(KAB_H0 * max(0.0, 1 - hav(t["origin"][1], t["origin"][2], la, lo) / KAB_RANGE_KM))
    return dict(
        id=t["id"], kind=t["kind"], name=name, ico=t["ico"], col=t["col"], cnt=t["cnt"],
        lat=la, lon=lo, unc=round(uncertainty(t, age), 1), v=round(sp) if sp else None,
        hdg=round(hdg) if hdg is not None else None,
        goal=[goal[1], goal[2]] if (goal and not arrived) else None,
        to=goal[0] if goal else None,
        frm=t["origin"][0] if t["origin"] else "",
        eta=eta, etah=etah, age=age, home=round(home, 1), short=short,
        ended=bool(t["ended"]), n=len(t["posts"]), chs=sorted(t["chs"]),
        text=t["text"][:160], ai=t["ai"], alt=alt, sub=t["sub"]["name"])

def snap(now):
    vis = [view(t, now) for t in tracks.values()]
    return {"now": now, "home": HOME, "tracks": vis, "feed": feed[-80:], "oblast": OBLAST_BBOX,
            "ai": {"provider": provider_name(), "ok": STATS["ok"], "ms": STATS["ms"]}}

async def send(payload, event=None):
    msg = (f"event: {event}\n" if event else "") + f"data: {json.dumps(payload, ensure_ascii=False)}\n\n"
    for c in list(clients):
        try:
            await c.write(msg.encode())
        except Exception:
            clients.discard(c)

async def push():
    await send(snap(time.time()))

async def emit_alert(s, key, lvl, text):
    if key in ALERTED:
        return
    ALERTED.add(key)
    await send({"lvl": lvl, "text": text}, "alert")
    if NTFY:
        try:
            await s.post(f"https://ntfy.sh/{NTFY}", data=text.encode(),
                         headers={"Title": "KAB ALERT", "Priority": "urgent", "Tags": "rotating_light"},
                         timeout=aiohttp.ClientTimeout(total=5))
        except Exception as e:
            print("ntfy", e)

async def check_alerts(s, now):
    for t in list(tracks.values()):
        if t["kind"] != "kab" or t["ended"] or t["lost"] or now - t["last_seen"] > 240:
            continue
        la, lo, hdg, _ = dr(t, now)
        if not aims_home(t, la, lo, hdg):
            continue
        dist = hav(la, lo, HOME[1], HOME[2])
        sp = eff_speed(t)
        eta = int(dist / sp * 3600) if sp else None
        lvl = 2 if eta is not None and eta <= ALERT_ETA_LVL2 else 1
        txt = f"КАБ курсом на {HOME[0]}: ~{dist:.0f} км" + (f", ~{eta} с" if eta is not None else "") \
              + " (за повідомленнями каналів, не підтверджено)"
        await emit_alert(s, (t["id"], lvl), lvl, txt)

def add_feed(ts, ch, text, evs):
    parts = []
    for e in evs:
        bits = [KIND_NAME[e["kind"]]]
        if e["origin"]: bits.append(f"з {e['origin'][0]}")
        if e["at"]: bits.append(f"над {e['at'][0]}")
        if e["toward"]: bits.append(f"→ {e['toward'][0]}")
        if e["heading"] is not None: bits.append(f"курс {e['heading']}°")
        parts.append(" ".join(bits) + f" [{e['status']}]")
    feed.append({"t": ts, "ch": ch, "text": text[:300], "kinds": sorted({e["kind"] for e in evs}),
                 "place": " | ".join(parts) if parts else "немає цілі — пропущено"})
    del feed[:-150]

# ─── Конвеєр обробки поста ───────────────────────────────────────────────────
HEADER_RX = re.compile(r"^\s*(?:запоріжжя|запорожье|запорізька обл\.?|запорожская обл\.?|дніпропетровщина|дніпро)\s*[:\-–—]\s*", re.I)
FOOTER_RX = re.compile(r"підписуй\w*|подпиш\w*|приєднуй\w*|наш канал|донат|monobank|privatbank|t\.me/|https?://|@\w+", re.I)

def clean_post(raw):
    t = HEADER_RX.sub("", " ".join(raw.split()))   # «Запоріжжя: КАБ…» → «КАБ…»
    parts = [p for p in re.split(r"(?<=[.!?])\s+", t) if p and not FOOTER_RX.search(p)]
    return " ".join(parts).strip()[:800]

async def process(s, ts, raw, ch, msg_key):
    text = clean_post(raw)
    if not text or not FLIGHT_HINT.search(text.lower()):
        return
    info = await llm_extract(s, text) if LLM_ON else None
    evs = validate_llm(info, text) if info is not None else rule_events(text)
    resolved = []
    for e in evs:
        for k in ("at", "origin", "toward"):
            e[k] = await to_place(s, e[k])
        resolved.append(e)
    alerts = []
    for e in resolved:
        alerts += ingest(e, ts, ch, msg_key)
    add_feed(ts, ch, text, resolved)
    log.append({"t": time.strftime("%H:%M:%S", time.localtime(ts)), "ch": ch, "text": text[:160],
                "src": "ai" if info is not None else "rules",
                "events": [dict(kind=e["kind"], status=e["status"],
                                at=e["at"] and e["at"][0], origin=e["origin"] and e["origin"][0],
                                toward=e["toward"] and e["toward"][0], heading=e["heading"]) for e in resolved]})
    del log[:-300]
    for key, lvl, txt in alerts:
        await emit_alert(s, key, lvl, txt)
    await push()

async def process_safe(s, ts, raw, ch, msg_key):
    try:
        await process(s, ts, raw, ch, msg_key)
    except Exception as e:
        print("process error:", e)

async def tick(s):
    while True:
        now = time.time()
        for i, t in list(tracks.items()):
            if t["ended"] and t["expire"] and now > t["expire"]:
                tracks.pop(i, None)
                continue
            if not t["ended"] and now - t["last_seen"] > 3600:
                tracks.pop(i, None)
                continue
            if t["kind"] in ("kab", "missile") and not t["ended"]:
                _, _, _, arrived = dr(t, now)
                if arrived:
                    t["ended"], t["end_reason"], t["expire"] = True, "досяг цілі", now + 120
        await check_alerts(s, now)
        await push()
        await asyncio.sleep(1)

# ─── Джерела (Telegram) ──────────────────────────────────────────────────────
async def poll(s, ch):
    first = True
    while True:
        try:
            url = f"https://t.me/s/{ch}?_={int(time.time() * 1000)}"
            headers = {"User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/124.0.0.0 Safari/537.36",
                       "Cache-Control": "no-cache"}
            async with s.get(url, headers=headers, timeout=aiohttp.ClientTimeout(total=6)) as r:
                html, code = await r.text(), r.status
            ws = BeautifulSoup(html, "html.parser").select(".tgme_widget_message")
            SRC_STATS[ch] = {"http": code, "messages_on_page": len(ws), "checked": time.strftime("%H:%M:%S")}
            new = []
            for w in ws:
                pid = w.get("data-post")
                tx = w.select_one(".tgme_widget_message_text")
                tm = w.select_one("time")
                if not (pid and tx and tm) or pid in SEEN:
                    continue
                SEEN.add(pid)
                ts = datetime.fromisoformat(tm["datetime"]).timestamp()
                for br in tx.find_all("br"):
                    br.replace_with("\n")
                raw = tx.get_text(" ", strip=True)
                if not first or time.time() - ts < LOOKBACK:
                    new.append((ts, raw, pid))
            first = False
            for ts, raw, pid in sorted(new):
                asyncio.create_task(process_safe(s, ts, raw, ch, pid))
        except Exception as e:
            SRC_STATS[ch] = {"error": str(e)[:200]}
        await asyncio.sleep(POLL)

# ─── HTTP ────────────────────────────────────────────────────────────────────
async def sim_endpoint(req):
    try:
        data = await req.json()
        if data.get("clear"):
            tracks.clear(); feed.clear(); ALERTED.clear()
            await push()
            return web.json_response({"ok": True, "tracks": 0})
        text = str(data.get("text", "")).strip()
        if not text:
            return web.json_response({"ok": False, "error": "empty text"}, status=400)
        ts = time.time()
        await process(req.app["session"], ts, text, str(data.get("ch") or "тест"), f"sim/{int(ts * 1000)}")
        return web.json_response({"ok": True, "tracks": len(tracks)})
    except Exception as e:
        return web.json_response({"ok": False, "error": str(e)}, status=500)

async def events(req):
    r = web.StreamResponse(headers={"Content-Type": "text/event-stream", "Cache-Control": "no-cache",
                                    "X-Accel-Buffering": "no"})
    await r.prepare(req)
    clients.add(r)
    try:
        await r.write(f"data: {json.dumps(snap(time.time()), ensure_ascii=False)}\n\n".encode())
        while True:
            await asyncio.sleep(3600)
    finally:
        clients.discard(r)

async def debug(req):
    now = time.time()
    body = {"llm": {"on": LLM_ON, "provider": provider_name(), **STATS},
            "channels": SRC_STATS,
            "tracks": [view(t, now) for t in tracks.values()],
            "log": log[-150:]}
    return web.json_response(body, dumps=lambda o: json.dumps(o, ensure_ascii=False, indent=1))

async def main():
    global GEO_LOCK, LLM_SEM
    GEO_LOCK, LLM_SEM = asyncio.Lock(), asyncio.Semaphore(4)
    s = aiohttp.ClientSession()
    app = web.Application()
    app["session"] = s
    D = os.path.dirname(os.path.abspath(__file__))
    static = lambda name: (lambda r: web.FileResponse(os.path.join(D, name)))
    app.add_routes([
        web.get("/", static("index.html")),
        web.get("/events", events),
        web.post("/sim", sim_endpoint),
        web.get("/manifest.json", static("manifest.json")),
        web.get("/sw.js", static("sw.js")),
        web.get("/icon-192.png", static("icon-192.png")),
        web.get("/icon-512.png", static("icon-512.png")),
        web.get("/health", lambda r: web.Response(text="ok")),
        web.get("/debug", debug),
    ])
    for ch in CHANNELS:
        asyncio.create_task(poll(s, ch))
    asyncio.create_task(tick(s))
    run = web.AppRunner(app)
    await run.setup()
    await web.TCPSite(run, "0.0.0.0", PORT).start()
    print(f"Radar v4 on :{PORT} | AI: {provider_name()} | places: {len(PLACES)}")
    await asyncio.Event().wait()

if __name__ == "__main__":
    asyncio.run(main())