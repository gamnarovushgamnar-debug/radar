"""Радар повітряних цілей (Мульти-канальний консенсус-фільтр + Тактичний ШІ).
Запуск: pip install aiohttp beautifulsoup4 && python server.py
Відкрити: http://<IP>:8080

Особливості:
1. Мульти-канальний консенсус (Sensor Fusion): об'єднує зведення з різних Telegram-каналів про одну й ту саму ціль,
   рахує середньозважену позицію (з урахуванням часу публікації та руху цілі вперед) і середній вектор курсу без смикань назад.
2. Жорстка військова географія:
   - КАБи ніколи не летять із Запоріжжя, Дніпра чи Вільнянська на південь (Оріхів/Токмак). Шапки постів «Запоріжжя: КАБ на Оріхів»
     автоматично очищуються від назви регіону в шапці, а пуск КАБ ставиться з південного сектору фронту (Токмак/Пологи/Василівка).
   - Ворожі борти (Су-34/35) ніколи не малюються над Запоріжжям чи вільною територією (вони працюють лише з пускових рубежів на півдні).
3. Підтримка швидких ШІ-ключів: GROQ_API_KEY, GEMINI_API_KEY, OPENAI_API_KEY, ANTHROPIC_API_KEY."""

import asyncio, json, math, os, re, time, itertools
from datetime import datetime
import aiohttp
from aiohttp import web
from bs4 import BeautifulSoup

CHANNELS = [
    c.strip() for c in os.getenv(
        "CHANNELS",
        "eyes_everywhere_ua,operatyvnohlep,gnilayachereha,rotodrellomod,kpszsu,zp_oblig,vanek_nikolaev"
    ).split(",") if c.strip()
]
HOME = ("Запоріжжя", 47.838, 35.139)
POLL = float(os.getenv("POLL_INTERVAL", "1.0"))
INITIAL_LOOKBACK = int(os.getenv("LOOKBACK", "1500"))  # 25 хв при старті
NTFY = os.getenv("NTFY_TOPIC")
KAB_H0, KAB_RANGE = 9000, 85                           # м висота пуску, км дальність планування КАБ
ALERT_ETA2 = 60

# ── ОФЛАЙН-БАЗА НАСЕЛЕНИХ ПУНКТІВ ЗАПОРІЗЬКОЇ ОБЛАСТІ ТА ОКОЛИЦЬ ─────────────
RAW_PLACES = [
    # Райони Запоріжжя та передмістя
    ("Дніпровський р-н / Правий берег (ЗП)", 47.868, 35.062, r"дніпровськ\w+\s+р-?н\w*|дніпровськ\w+\s+район\w*|осипенківськ\w*|осипенковск\w*|бородінськ\w*|бородинск\w*|верхн\w+\s+хортиц\w*|правий\s+берег"),
    ("Хортицький р-н / Бабурка (ЗП)", 47.816, 35.025, r"бабурк\w*|хортицьк\w+\s+р|хортицк\w+\s+р"),
    ("Хортиця (Запоріжжя)", 47.822, 35.075, r"острів\s+хортиц\w*|о\.\s*хортиц\w*|\bхортиц\w*"),
    ("Шевченківський р-н (ЗП)", 47.832, 35.225, r"шевченківськ\w+\s+р|шевченковск\w+\s+р|зелен\w+\s+яр\w*|шевчик\w*"),
    ("Комунарський р-н / Космос (ЗП)", 47.785, 35.185, r"комунарськ\w*|коммунарск\w*|космос\w*|південн\w+\s+мкрн|\bпіски\b"),
    ("Олександрівський р-н (ЗП)", 47.815, 35.152, r"олександрівськ\w+\s+р|александровск\w+\s+р"),
    ("Вознесенівський р-н (ЗП)", 47.842, 35.128, r"вознесенівськ\w*|вознесеновск\w*"),
    ("Заводський р-н / Кічкас (ЗП)", 47.888, 35.158, r"заводськ\w+\s+р|кічкас\w*|кичкас\w*|павло-?кічкас\w*"),
    # Важливо: слово «Запорізький/Запорізька» (напрямок/область/район) НЕ матчиться як місто Запоріжжя!
    ("Запоріжжя", 47.838, 35.139, r"запоріжж\w+|запорожь\w+|\bзп\b"),
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
    # Вільнянський та Новомиколаївський напрямок
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
    # Василівський / Каховський / Енергодарський напрямок
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
    # Гуляйпільський та Пологівський напрямок
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
    # Сусідні прикордонні райони
    ("Марганець", 47.645, 34.604, r"марганц\w*|марганец\w*"),
    ("Нікополь", 47.567, 34.400, r"нікопол\w*|никопол\w*"),
    ("Томаківка", 47.816, 34.742, r"томаківк\w*|томаковк\w*"),
    ("Мирова", 47.765, 34.645, r"миров\w+"),
    ("Солоне", 48.204, 34.872, r"солон\w+"),
    ("Синельникове", 48.318, 35.512, r"синельников\w*|синельників\w*"),
    ("Васильківка", 48.212, 36.032, r"васильківк\w*|васильковк\w*"),
    ("Павлоград", 48.533, 35.867, r"павлоград\w*"),
    ("Каховське водосховище", 47.520, 34.850, r"водосховищ\w*|водохран\w*|акватор\w+|русл\w+\s+дніпр\w*|річк\w+\s+дніпр\w*|(?:через|над|вздовж)\s+дніпр\w*"),
]

BROAD_REGIONS = {"Запорізька обл.", "Запорізький р-н", "Дніпропетровщина", "Дніпро", "Запорізький напрямок"}
PLACES = [(n, la, lo, rx) for n, la, lo, rx in RAW_PLACES]
VIEWBOX = "34.0,48.7,37.3,46.3"

# ── КАТАЛОГ ЦІЛЕЙ ─────────────────────────────────────────────────────────────
def S(id, kind, name, rx, v, merge, ttl, ico, col=None, generic=False):
    return dict(id=id, kind=kind, name=name, rx=rx, v=v, merge=merge, ttl=ttl, ico=ico, col=col, generic=generic)

SUBS = [
    S("banderol", "drone", "Бандероль (S8000)", r"бандерол\w*|\bs-?8000\b", 560, 42, 900, "cruise", "#ff3d81"),
    S("geran3", "drone", "Реактивний Шахед (Герань-3)",
      r"реактивн\w*\s+(?:шахед|шахид|герань|геран|мопед|бпла|бла|дрон|безпілот)\w*|(?:шахед|шахид|герань|геран)\w*[-–]\s?(?:3|238)\b|реактив(?!н\w*\s+(?:систем|залп|артил))",
      520, 42, 900, "jet", "#ff6b3d"),
    S("geran2", "drone", "Шахед (Герань-2)", r"шахед\w*|шахид\w*|герань\w*|геран\w*|мопед\w*|газонокосарк\w*|балалайк\w*|\bударн\w+\s+(?:бпла|дрон|бла)\w*", 175, 28, 1200, "delta", "#e3b341"),
    S("gerbera", "drone", "Гербера (імітатор)", r"гербер\w*|пароді\w*|пароди\w*|імітатор\w*|имитатор\w*|фальш-?ціл\w*", 165, 28, 1200, "decoy", "#c9a227"),
    S("italmas", "drone", "Італмас", r"італмас\w*|италмас\w*|итальмас\w*|виріб\s*54", 155, 28, 1200, "decoy", "#c9a227"),
    S("orion", "drone", "Оріон (ударний)", r"оріон\w*|орион\w*|іноходец\w*", 200, 35, 900, "plane", "#ff9f1c"),
    S("lancet", "drone", "Ланцет", r"ланцет\w*|виріб\s*5[12]|изделие\s*5[12]", 135, 18, 480, "xwing", "#3dd6c6"),
    S("molniya", "drone", "Молнія", r"молні\w*|молни\w*|прив['’]?єт-?82", 115, 18, 480, "flywing", "#7ee787"),
    S("fpv", "drone", "FPV-дрон", r"\bфпв\w*|\bfpv\b|фпвішк\w*|камікадзе\s+на\s+оптоволокн\w*|оптоволокн\w*", 110, 10, 360, "quad", "#f2cc60"),
    S("orlan", "drone", "Орлан-10/30", r"орлан\w*", 115, 24, 900, "plane", "#9aa7b8"),
    S("recon", "drone", "Розвідувальний БпЛА", r"суперкам\w*|supercam|\bzala\b|\bзала\b|елерон\w*|\beleron|мерлін\w*|форпост\w*|розвід\w*\s*(?:бпла|дрон|бла|крил)\w*|крил\w+\s+розвід\w*", 105, 24, 900, "plane", "#9aa7b8"),
    S("drone", "drone", "БпЛА", r"бпла|\bбла\b|дрон\w*|безпілотн\w*|беспилотн\w*|камікадзе|камикадзе|\bуав\b|\bбпак\b|невідом\w+\s+бпла", 165, 28, 1200, "delta", None, True),
    # --- авіабомби (КАБ / УМПК / УМПБ) ---
    S("kab_big", "kab", "ФАБ-1500/3000", r"(?:фаб|каб|рбк)-?\s*(?:1500|3000)", 620, 18, 210, "kab", None),
    S("kab", "kab", "КАБ", r"\bкаб(?:и|ів|ах|ам|ом|у|а|ів)?\b|авіабомб\w*|авиабомб\w*|умпк\w*|умпб\w*|\bфаб\w*|\bодаб\w*|д-?30сн|керован\w+\s+авіабомб\w*|пуск\w*\s+каб\w*", 660, 18, 210, "kab", None),
    # --- ракети ---
    S("kinzhal", "missile", "Х-47М2 Кинджал", r"кинджал\w*|кінжал\w*|кинжал\w*|\b[xх]-?47\w*", 4200, 90, 200, "dart", "#ff2d55"),
    S("zircon", "missile", "3М22 Циркон", r"циркон\w*|3м-?22", 5000, 90, 200, "dart", "#ff2d55"),
    S("iskander_m", "missile", "Іскандер-М (балістика)", r"[іи]скандер-?м|9м723|[іи]скандер(?!-?к)|kn-?23|кн-?23", 5200, 90, 200, "ballistic", "#ff453a"),
    S("iskander_k", "missile", "Іскандер-К (крилата)", r"[іи]скандер-?к|9м72[89]", 850, 50, 500, "cruise", "#ff8a3d"),
    S("ballistic", "missile", "Балістична ракета", r"баліст\w*|балист\w*|швидкісн\w+\s+ціл\w*", 4800, 90, 200, "ballistic", "#ff453a"),
    S("s300", "missile", "С-300/С-400", r"\bс-?(?:300|400)\w*|зкр\s+с-?300", 3400, 50, 200, "dart", "#ff453a"),
    S("kh22", "missile", "Х-22/Х-32", r"\b[xх]-?(?:22|32)\b", 3600, 90, 200, "dart", "#ff453a"),
    S("onyx", "missile", "П-800 Онікс", r"онікс\w*|оникс\w*|\bп-?800\b", 2600, 80, 200, "dart", "#ff453a"),
    S("kh101", "missile", "Х-101/Х-555", r"\b[xх]-?(?:101|102|555|55)\b", 800, 50, 500, "cruise", "#ff8a3d"),
    S("kalibr", "missile", "Калібр", r"калібр\w*|калибр\w*|3м-?14|3м-?54", 850, 50, 500, "cruise", "#ff8a3d"),
    S("kh59", "missile", "Х-35/Х-59/Х-69", r"\b[xх]-?(?:35|38|59|69)\b", 880, 50, 500, "cruise", "#ff8a3d"),
    S("missile", "missile", "Ракета", r"ракет\w*|\bкр\b|крилат\w*", 850, 50, 500, "missile", None, True),
    # --- пілотована авіація (літаки працюють ЛИШЕ з окупованого півдня, не над Запоріжжям!) ---
    S("bomber", "aircraft", "Ракетоносій (Ту-95/160/22)", r"\bту-?(?:95|160|22)\w*|ракетоносц\w*|ракетоносн\w*", 720, 40, 600, "bomber", None),
    S("heli", "aircraft", "Гелікоптер Ка-52/Мі-28", r"вертол\w*|гелікоптер\w*|\bка-?52|\bмі-?(?:8|24|28|35)\b", 220, 20, 480, "heli", "#79c0ff"),
    S("aircraft", "aircraft", "Борт Су-34/35 (ТА)", r"\bборт\w*\s+су|\bсу-?(?:24|25|30|34|35|57)\w*|\bміг-?31\w*", 750, 30, 420, "plane2", None, True),
    # --- обстріли ---
    S("mlrs", "strike", "РСЗО", r"рсзо|рсзв|реактивн\w*\s+систем\w*|\bград(?:и|ів|у|ом|ами)?\b|ураган\w*|смерч\w*|торнадо-?[гс]", 0, 10, 420, "star", None),
    S("arty", "strike", "Артобстріл", r"артобстр\w*|артилер\w*|обстріл\w*|обстрел\w*|приліт\w*|прилет\w*|мінометн\w*|танков\w+\s+обстр\w*", 0, 10, 420, "star", None, True),
]
SUBS.append(S("impact", "impact", "Приліт", r"(?!x)x", 0, 2, 600, "star", "#ff5c5c"))
SUB = {s["id"]: s for s in SUBS}
KIND_COL = {"drone": "#e3b341", "kab": "#f85149", "missile": "#ff8a3d", "aircraft": "#58a6ff", "strike": "#d2a8ff", "impact": "#ff5c5c"}

# ── ОЧИЩЕННЯ ПОСТІВ ТА ВІДСІКАННЯ ШАПОК РЕГІОНУ («Запоріжжя: КАБ на Оріхів») ──
FOOTER_LINE_RX = re.compile(
    r"(?:підписуй|подпиш|приєднуй|наш\s+канал|резервн\w+\s+канал|надіслати\s+інф|бот\s+для|збір\s+на|монобанк|monobank|privatbank|картка|донат|реклам|t\.me/|https?://|@\w+)",
    re.I
)
REAL_NEWS_RX = re.compile(
    r"(?:загинул\w*|загинув\w*|поранен\w+\s+\d|постраждал\w+\s+\d|зведення\s+генштаб|за\s+минулу\s+добу|втрати\s+ворога|загальні\s+бойові\s+втрати|підсумки\s+доби|президент\s+заявив|міністр\s+оборони)",
    re.I
)
GENERAL_WARNING_RX = re.compile(
    r"(?:загроз\w+\s+(?:застосування|пуск\w*|каб|баліст|удар\w*|авіабомб)|активн\w+\s+(?:тактичн\w+\s+авіац|ворож\w+\s+авіац|борт\w*)|повітрян\w+\s+тривог\w+|увага\s*!\s*тривога|зберігається\s+загроза|ймовірн\w+\s+пуск\w*|робота\s+ппо|працює\s+ппо)",
    re.I
)
CONCRETE_FLIGHT_RX = re.compile(
    r"(?:летит\w*|летят|курс\w*\s+на|у\s+бік|в\s+бік|в\s+напрям\w*|у\s+напрям\w*|пряму\w*|вектор\w*\s+на|проліта\w*|повз\s+|через\s+|підліта\w*|заход\w+\s+на|пуск\w*\s+(?:каб\w*\s+)?(?:на|по|в\s+бік|у\s+бік))",
    re.I
)
# Зрізає шапку на початку рядка типу «🔴 Запоріжжя та область: », «Запоріжжя! », «Запорізький напрямок — »
REGION_HEADER_RX = re.compile(
    r"^[\s⚠️🔴❗️⚡️📢🚨•\-–—]*(?:(?:м\.\s*)?(?:запоріжжя|запорізьк\w+\s*(?:обл\w*|район\w*|р-н|напрям\w*)?|дніпро\w*)\s*(?:(?:\([^)]*\))?\s*(?:[,:;|!—–\-]|та\s+область)+)\s*)+",
    re.I
)

def clean_post(raw_text):
    if not raw_text:
        return ""
    lines = []
    for ln in raw_text.replace("\r", "\n").split("\n"):
        s = ln.strip()
        if not s:
            continue
        if FOOTER_LINE_RX.search(s) and len(s) < 95 and not any(re.search(sb["rx"], s.lower()) for sb in SUBS if not sb["generic"]):
            continue
        s = re.sub(r"https?://\S+|t\.me/\S+|@\w+", "", s).strip(" -–—•|")
        # Якщо після назви міста в шапці є інший текст зі сводкою — зрізаємо шапку, щоб «Запоріжжя: КАБ на Оріхів» не читалось як «із Запоріжжя на Оріхів»
        s_no_hdr = REGION_HEADER_RX.sub("", s).strip()
        if len(s_no_hdr) >= 6:
            s = s_no_hdr
        if s:
            lines.append(s)
    return "\n".join(lines).strip()

# ── ПАРСИНГ КІЛЬКОСТІ, ШВИДКОСТІ, АЗИМУТА ТА МАРШРУТУ ─────────────────────────
CNT_BEFORE = re.compile(r"(?<![\d:.,/])(\d{1,2})(?:\s*[-–—]\s*(\d{1,2}))?\s*(?:шт\.?\s*)?(?:[xх×]\s*)?(?:ударн\w*\s+|нов\w*\s+|ворож\w*\s+)?$")
CNT_AFTER = re.compile(r"^\s*(?:в\s+кількості\s+|кількість\s+|до\s+|~|\bх|\bx|×)?\s*(\d{1,2})(?:\s*[-–—]\s*(\d{1,2}))?\s*(?:шт|од|ціл|борт|бпла|дрон)\b")
WORD_NUMS = {"один": "1", "одна": "1", "одиночний": "1", "два": "2", "дві": "2", "пару": "2", "пара": "2", "три": "3", "чотири": "4", "п'ять": "5", "декілька": "2-3", "кілька": "2-3"}
SPEED_RX = re.compile(r"(?:швидк\w*|скорост\w*|\bv\s*=)?\s*[~≈]?\s*(\d{2,4})\s*(?:[-–]\s*(\d{2,4})\s*)?(?:км\s*/\s*(?:год|г|ч)|км\s+(?:на\s+)?год)", re.I)
DEG_RX = re.compile(r"(?:курс\w*|азимут\w*|напрям\w*)\s*(?:на\s*)?(\d{1,3})\s*(?:°|град\w*)", re.I)

CARDINAL_PATTERNS = [
    (315, r"(?:північн\w*[-–\s]*захід\w*|северо[-–\s]*запад\w*|\bпн[-–./]?зх\b|\bсз\b)"),
    (45,  r"(?:північн\w*[-–\s]*схід\w*|северо[-–\s]*восток\w*|северо[-–\s]*восточ\w*|\bпн[-–./]?сх\b|\bсв\b)"),
    (225, r"(?:південн\w*[-–\s]*захід\w*|юго[-–\s]*запад\w*|\bпд[-–./]?зх\b|\bюз\b)"),
    (135, r"(?:південн\w*[-–\s]*схід\w*|юго[-–\s]*восток\w*|юго[-–\s]*восточ\w*|\bпд[-–./]?сх\b|\bюв\b)"),
    (0,   r"(?:(?:на|курс\w*\s+на|у\s+бік|в\s+бік|напрям\w*|вектор\w*)\s+північ\w*|(?:на|курс\w*)\s+север\w*|північн\w+\s+(?:курс|напрям|вектор)\w*|\bна\s+пн\b)"),
    (180, r"(?:(?:на|курс\w*\s+на|у\s+бік|в\s+бік|напрям\w*|вектор\w*)\s+півден\w*|(?:на|курс\w*)\s+юг\w*|південн\w+\s+(?:курс|напрям|вектор)\w*|\bна\s+пд\b)"),
    (270, r"(?:(?:на|курс\w*\s+на|у\s+бік|в\s+бік|напрям\w*|вектор\w*)\s+захід\w*|(?:на|курс\w*)\s+запад\w*|західн\w+\s+(?:курс|напрям|вектор)\w*|\bна\s+зх\b)"),
    (90,  r"(?:(?:на|курс\w*\s+на|у\s+бік|в\s+бік|напрям\w*|вектор\w*)\s+схід\w*|(?:на|курс\w*)\s+восток\w*|східн\w+\s+(?:курс|напрям|вектор)\w*|\bна\s+сх\b)"),
]

TURN_RX = re.compile(r"(?:поверну\w+|розворот\w*|розверну\w*|зміни\w+\s+(?:курс|напрям|вектор)|доверну\w+|маневру\w*)", re.I)

def count_around(tl, st, en):
    m = CNT_BEFORE.search(tl[max(0, st - 24):st])
    if m:
        return m.group(1) + ("-" + m.group(2) if m.group(2) else "")
    m2 = CNT_AFTER.search(tl[en:min(len(tl), en + 20)])
    if m2:
        return m2.group(1) + ("-" + m2.group(2) if m2.group(2) else "")
    pre_words = re.findall(r"[а-яіїєґ'’]+", tl[max(0, st - 24):st])
    for w in reversed(pre_words[-3:]):
        if w in WORD_NUMS:
            return WORD_NUMS[w]
    return None

def parse_speed_hdg(tl):
    v_ovr = None
    sm = SPEED_RX.search(tl)
    if sm:
        v1 = int(sm.group(1))
        v2 = int(sm.group(2)) if sm.group(2) else v1
        v = (v1 + v2) // 2
        if 40 <= v <= 7000:
            v_ovr = v
    hdg_ovr = None
    dm = DEG_RX.search(tl)
    if dm:
        d = int(dm.group(1))
        if 0 <= d <= 360:
            hdg_ovr = d % 360
    if hdg_ovr is None:
        for deg, pat in CARDINAL_PATTERNS:
            if re.search(pat, tl):
                hdg_ovr = deg
                break
    return v_ovr, hdg_ovr

def detect(tl):
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
        done.add(s["id"])
        out.append((s, count_around(tl, st, en)))
    # Якщо одночасно згадано літак (борт Су-34) і КАБ/ракету («Борт Су-34 пустив КАБ на Оріхів») —
    # залишаємо ЛИШЕ КАБ/ракету, щоб не малювати винищувач над містом!
    kinds_present = {s["kind"] for s, _ in out}
    if "aircraft" in kinds_present and ("kab" in kinds_present or "missile" in kinds_present):
        out = [(s, c) for s, c in out if s["kind"] != "aircraft"]
    return out

TO_PRE_RX = re.compile(
    r"(?:(?:→|->|=>|➡|🔜|—>|–>)|"
    r"(?:\bна|\bдо|\bк|\bпо|\bу\s+бік|\bв\s+бік|\bу\s+напрям\w*|\bв\s+напрям\w*|напрям\w*\s+на|направ\w*\s+на|курс\w*\s+на|вектор\w*\s+на|пряму\w*\s+на|летит\w*\s+на|летят\s+на|руха\w*\s+на|заход\w*\s+на|підліта\w*\s+до|поверну\w*\s+на|розворот\w*\s+на|загроз\w*\s+для))"
    r"(?:\s+(?:міст\w*|м\.|с\.|смт|селищ\w*|район\w*|р-н\w*|околиц\w*|кра\w+|північ\w*|півден\w*|захід\w*|схід\w*))*\s*$"
)
FROM_PRE_RX = re.compile(
    r"(?:\bз|\bіз|\bзі|\bвід|\bсо|\bот|з\s+боку|зі\s+сторони|со\s+стороны|виліт\s+з|пуск\w*\s+(?:з|із|від|район\w*))(?:\s+(?:боку|сторони|району|р-ну|напрямку|акваторії|м\.|с\.))*\s*$"
)
VIA_PRE_RX = re.compile(
    r"(?:\bнад|\bбіля|\bпоблизу|\bповз|\bчерез|\bпроліта\w*|\bпролет\w*|\bпройш\w*|\bминув|\bминає|\bв\s+районі|\bу\s+районі|\bрайон\w*|\bфіксу\w*\s+(?:над|біля|в))(?:\s+(?:м\.|с\.|смт|селищ\w*))*\s*$"
)
LIST_CONJ_RX = re.compile(r"^\s*(?:,|/|\\|\+|та|і|й|або|чи)\s*(?:на\s+|до\s+|в\s+бік\s+|район\w*\s+|м\.\s*|с\.\s*|смт\s*)?$")
ARROW_BETWEEN_RX = re.compile(r"^\s*(?:→|->|=>|—>|–>)\s*$")

CLEAR = re.compile(r"відбій|отбой|збит\w*|сбит\w*|знищен\w*|минус|мінус|чисто|не\s+фіксу\w*|не\s+спостеріга\w*|локаційно\s+втрачен\w*|припини\w+\s+існуван\w*")
DOWNED_RX = re.compile(r"збит\w*|сбит\w*|знищен\w*|минус|мінус|приземлил\w*|локаційно\s+втрачен\w*|припини\w+\s+існуван\w*")
IMPACT = re.compile(r"(?:упал|впал|падал|падінн|падіння|падение|влуч|попад|приліт|прилет|прильот|вибух|взрыв|бахн|бахк|гучн|вдарил|вдарив|накрил|ударил|уражен)\w*")
FLIGHT = re.compile(r"летить|летят|летит|курс|напрям|вектор|рухаєть|движ|в\s+бік|у\s+бік|підлітає|заходить|заходят|прямує|прямують|повз|через|поверну|маневр")

def intent_of(tl):
    if CLEAR.search(tl):
        return "downed" if DOWNED_RX.search(tl) else "clear"
    if IMPACT.search(tl) and not FLIGHT.search(tl):
        return "impact"
    return "flight"

def extract_routes(tl):
    """Повертає список чистих маршрутів [(pos, tgt), ...] без хибного склеювання списків цілей."""
    best = {}
    for n, la, lo, rx in PLACES:
        for m in re.finditer(r"(?<![а-яіїєґ'’])(?:" + rx + r")\w*", tl):
            st, en = m.start(), m.end()
            if st not in best or (en - st, len(rx)) > (best[st][0] - st, best[st][1]):
                best[st] = (en, len(rx), n, la, lo)
    spans = []
    for st, (en, lrx, n, la, lo) in sorted(best.items(), key=lambda x: (x[0], -(x[1][0] - x[0]))):
        if all(en <= s0 or st >= s1 for s0, s1, *_ in spans):
            spans.append((st, en, n, la, lo))

    if not spans:
        return []

    roles = []
    for idx, (st, en, n, la, lo) in enumerate(spans):
        pre = tl[max(0, st - 45):st]
        between = tl[spans[idx - 1][1]:st] if idx > 0 else ""
        role = "unk"
        if ARROW_BETWEEN_RX.search(between):
            role = "to"
            if roles and roles[-1][0] == "unk":
                roles[-1] = ("from", roles[-1][1])
        elif TO_PRE_RX.search(pre):
            role = "to"
        elif VIA_PRE_RX.search(pre):
            role = "at"
        elif FROM_PRE_RX.search(pre):
            role = "from"
        elif idx > 0 and LIST_CONJ_RX.search(between) and roles[-1][0] in ("to", "from", "at"):
            role = roles[-1][0]
        roles.append((role, (n, la, lo)))

    dedup = []
    for r, p in roles:
        if not dedup or dedup[-1][1][0] != p[0]:
            dedup.append((r, p))

    from_pts = [p for r, p in dedup if r == "from"]
    at_pts = [p for r, p in dedup if r == "at"]
    to_pts = [p for r, p in dedup if r == "to"]
    unk_pts = [p for r, p in dedup if r == "unk"]

    origin = at_pts[-1] if at_pts else (from_pts[-1] if from_pts else None)

    if to_pts:
        distinct_to = []
        for tp in to_pts:
            if origin and tp[0] == origin[0]:
                continue
            if all(hav(tp[1], tp[2], ex[1], ex[2]) > 12.0 for ex in distinct_to):
                distinct_to.append(tp)
        if distinct_to:
            return [(origin, tp) for tp in distinct_to[:2]]

    if origin:
        return [(origin, None)]
    if unk_pts:
        return [(unk_pts[0], None)]
    return []

def hav(a, b, c, d):
    p = math.pi / 180
    x = math.sin((c - a) * p / 2) ** 2 + math.cos(a * p) * math.cos(c * p) * math.sin((d - b) * p / 2) ** 2
    return 12742 * math.asin(min(1.0, math.sqrt(max(0.0, x))))

def brg(a, b, c, d):
    p = math.pi / 180
    y = math.sin((d - b) * p) * math.cos(c * p)
    x = math.cos(a * p) * math.sin(c * p) - math.sin(a * p) * math.cos(c * p) * math.cos((d - b) * p)
    return (math.degrees(math.atan2(y, x)) + 360) % 360

tracks, ids, seen, clients, alerted, stats = {}, itertools.count(1), set(), set(), set(), {}

OBLAST = [(34.30, 47.40), (34.46, 47.56), (34.80, 47.72), (35.02, 47.98), (35.20, 48.14), (35.50, 48.10), (35.80, 47.98),
          (36.20, 48.02), (36.50, 47.95), (36.78, 47.72), (36.96, 47.40), (37.05, 47.05), (36.95, 46.80), (36.55, 46.58),
          (36.00, 46.52), (35.45, 46.42), (35.00, 46.38), (34.75, 46.55), (34.78, 46.90), (34.50, 47.15)]
OBL_MARGIN = 18.0

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
    if _pip(lo, la, OBLAST):
        return True
    if margin <= 0:
        return False
    k = 111.2 * math.cos(math.radians(la))
    pts = [(x * k, y * 111.2) for x, y in OBLAST]
    px, py = lo * k, la * 111.2
    return any(_segd(px, py, *pts[i - 1], *pts[i]) <= margin for i in range(len(pts)))

RAD = {
    "Запоріжжя": 5.0, "Балабине": 2.5, "Кушугум": 2.5, "Малокатеринівка": 2.5, "Комишуваха": 3.0,
    "Вільнянськ": 3.0, "Новомиколаївка": 3.0, "Василівка": 3.5, "Степногірськ": 2.5, "Кам'янське": 2.5,
    "Кам'янка-Дніпровська": 3.5, "Енергодар": 4.0, "Оріхів": 3.0, "Гуляйполе": 3.0, "Пологи": 3.5,
    "Токмак": 3.5, "Молочанськ": 3.5, "Мелітополь": 4.5, "Бердянськ": 4.5,
}
def sigma_of(name):
    return max(1.8, RAD.get(name, 2.8))

# ── ВІЙСЬКОВО-ТАКТИЧНА ГЕОГРАФІЯ ТА МУЛЬТИ-КАНАЛЬНИЙ КОНСЕНСУС ────────────────
REPORT_LAG = 5.0
ATTACK_END = {"kab", "missile"}

def ang(a):
    return (a + 180) % 360 - 180

def fwd(la, lo, h, km):
    r = math.radians(h)
    return la + km * math.cos(r) / 111.2, lo + km * math.sin(r) / (111.2 * math.cos(math.radians(la)))

def frontline_lat_at(lo):
    """Приблизна широта лінії бойового зіткнення (ЛБЗ) у Запорізькій області залежно від довготи."""
    # Західніше Василівки (~35.2): по Дніпру ~47.48; Оріхів (35.78): ~47.50; Гуляйполе (36.26): ~47.60
    if lo < 35.35:
        return 47.48
    if lo < 36.0:
        return 47.48 + (lo - 35.35) * 0.08
    return 47.53 + (lo - 36.0) * 0.18

def is_free_territory(la, lo):
    """Точка знаходиться на підконтрольній Україні території (на північ від ЛБЗ)."""
    return la > frontline_lat_at(lo) + 0.03

def sanitize_tactical_route(sub_kind, pos, tgt, hdg_ovr=None):
    """Жорстка перевірка реалістичності маршруту:
    1. Борт (aircraft): російські Су-34/35 НЕ літають над Запоріжжям, Оріховом чи Вільнянськом!
       Якщо точка на вільній території — борт відкидається (або ставиться в зону пусків біля Токмака/Молочанська).
    2. КАБ (kab): пуски ТІЛЬКИ з півдня/сходу у північному/північно-західному секторі (курс 250°..360° або 0°..45°).
       КАБ НІКОЛИ не летить із Запоріжжя на Оріхів (курс ~121°) або з півночі на південь!"""
    if pos and pos[0] in BROAD_REGIONS:
        pos = None
    if tgt and tgt[0] in BROAD_REGIONS:
        tgt = None

    if sub_kind == "aircraft":
        # Ворожа авіація знаходиться лише в глибині окупованої території на півдні (lat <= 47.32)
        if pos and pos[1] > 47.35:
            pos = None
        if tgt and tgt[1] > 47.35:
            tgt = None
        return pos, tgt, hdg_ovr

    if sub_kind == "kab":
        # Якщо вказана лише одна точка на вільній території (напр. «КАБ Оріхів», «КАБ Запоріжжя», «КАБ Комишуваха») —
        # це 100% ЦІЛЬ (tgt), куди летить КАБ, а НЕ місце пуску!
        if pos and not tgt and is_free_territory(pos[1], pos[2]):
            tgt, pos = pos, None

        # Якщо є і pos, і tgt, перевіряємо азимут: КАБ не може летіти на південь/схід (азимут 50°..245°) або стартувати з тилу (Запоріжжя/Вільнянськ)!
        if pos and tgt:
            b = brg(pos[1], pos[2], tgt[1], tgt[2])
            if (50.0 <= b <= 245.0) or (pos[1] > 47.62):
                # Якщо pos було «Запоріжжя», а tgt «Оріхів» (через шапку «Запоріжжя: КАБ на Оріхів»),
                # відкидаємо фейковий pos «Запоріжжя», залишаючи справжню ціль tgt («Оріхів»)!
                pos = None

        # Якщо тепер є ціль tgt, але немає точки пуску pos — ставимо реалістичну точку підльоту з півдня (сектор Токмак/Пологи/Василівка)
        if not pos and tgt:
            # Напрямок звідки летить КАБ: для Запоріжжя/Кушугума/Комишувахи — з Пд-Сх (155°), для Оріхова/Гуляйполя — з Пд (170°)
            from_brg = 155.0 if tgt[2] < 35.65 else 172.0
            sla, slo = fwd(tgt[1], tgt[2], from_brg, 28.0)
            pos = ("підліт з півдня", sla, slo)
            hdg_ovr = brg(sla, slo, tgt[1], tgt[2])

        # Блокуємо південний курс для КАБ, навіть якщо задано словами
        if hdg_ovr is not None and (50.0 <= hdg_ovr <= 245.0):
            hdg_ovr = 335.0

    return pos, tgt, hdg_ovr

def recompute_consensus(t, now):
    """МУЛЬТИ-КАНАЛЬНИЙ КОНСЕНСУС (Sensor Fusion):
    Бере всі свіжі повідомлення різних каналів про цю ціль, екстраполює кожне повідомлення вперед до моменту `now`,
    і обчислює середньозважену позицію, середню швидкість та середній вектор на ціль БЕЗ відкатів назад!"""
    obs_list = [o for o in t["obs"].values() if now - o["ts"] <= min(t["ttl"], 420)]
    if not obs_list:
        return
    # Середня швидкість між каналами
    w_sum = 0.0
    v_sum = 0.0
    sin_h, cos_h = 0.0, 0.0
    gla_sum, glo_sum, gw_sum = 0.0, 0.0, 0.0
    best_to_name = t["to_name"]
    best_to_w = -1.0

    for o in obs_list:
        age = max(0.0, now - o["ts"])
        w = o["w"] * math.exp(-age / 180.0)
        w_sum += w
        v_sum += w * o["v"]
        if o["goal"]:
            gw_sum += w
            gla_sum += w * o["goal"][1]
            glo_sum += w * o["goal"][2]
            if w > best_to_w:
                best_to_w = w
                best_to_name = o["goal"][0]
        if o["hdg"] is not None:
            rad = math.radians(o["hdg"])
            sin_h += w * math.sin(rad)
            cos_h += w * math.cos(rad)

    if w_sum > 0 and t["v"] > 0:
        t["v"] = round(v_sum / w_sum)

    if gw_sum > 0:
        cg_lat, cg_lon = gla_sum / gw_sum, glo_sum / gw_sum
        t["goal"] = (best_to_name or "ціль", cg_lat, cg_lon)

    if sin_h or cos_h:
        cons_hdg = (math.degrees(math.atan2(sin_h, cos_h)) + 360) % 360
        t["hdg"] = cons_hdg

    # Тепер екстраполюємо позицію кожного каналу вперед до моменту `now` вздовж узгодженого курсу
    plat_sum, plon_sum, pw_sum = 0.0, 0.0, 0.0
    for o in obs_list:
        age = max(0.0, now - o["ts"])
        w = o["w"] * math.exp(-age / 150.0)
        h = brg(o["lat"], o["lon"], t["goal"][1], t["goal"][2]) if t["goal"] else (o["hdg"] if o["hdg"] is not None else t["hdg"])
        if h is not None and t["v"] > 0:
            dist = t["v"] * (age / 3600.0)
            if t["goal"]:
                dist = min(dist, max(0.0, hav(o["lat"], o["lon"], t["goal"][1], t["goal"][2]) - 0.5))
            pla, plo = fwd(o["lat"], o["lon"], h, dist)
        else:
            pla, plo = o["lat"], o["lon"]
        plat_sum += w * pla
        plon_sum += w * plo
        pw_sum += w

    if pw_sum > 0:
        target_lat = plat_sum / pw_sum
        target_lon = plon_sum / pw_sum
        # Не дозволяємо новому каналу смикати мітку НАЗАД проти руху (лише плавне усереднення вбоки та вперед)
        if t["goal"]:
            cur_rem = hav(t["lat"], t["lon"], t["goal"][1], t["goal"][2])
            new_rem = hav(target_lat, target_lon, t["goal"][1], t["goal"][2])
            if new_rem > cur_rem + 1.5 and len(obs_list) > 1:
                # Запізнілий пост іншого каналу позаду: беремо лише бокове усереднення (25%), не відкочуючи прогрес вперед
                t["lat"] = 0.8 * t["lat"] + 0.2 * target_lat
                t["lon"] = 0.8 * t["lon"] + 0.2 * target_lon
            else:
                t["lat"] = 0.45 * t["lat"] + 0.55 * target_lat
                t["lon"] = 0.45 * t["lon"] + 0.55 * target_lon
        else:
            t["lat"] = 0.5 * t["lat"] + 0.5 * target_lat
            t["lon"] = 0.5 * t["lon"] + 0.5 * target_lon

        if t["goal"] and hav(t["lat"], t["lon"], t["goal"][1], t["goal"][2]) > 0.4:
            t["hdg"] = brg(t["lat"], t["lon"], t["goal"][1], t["goal"][2])

    # Чим більше незалежних каналів підтвердили ціль, тим вища точність (менший радіус кола невизначеності unc)
    ch_count = len(t["channels"])
    base_unc = min(o["sg"] for o in obs_list)
    t["unc"] = max(1.2, round(base_unc / (1.0 + 0.28 * (ch_count - 1)), 1))

def step(t, now):
    """Плавний рух консенсус-треку вперед точно за вектором на ціль."""
    dt = min(now - t["ls"], 1800.0)
    if dt <= 0:
        return
    t["ls"] = now
    if t.get("ended"):
        return
    v = t["v"]
    if not v or t["hdg"] is None:
        return
    g = t["goal"]
    if g:
        rem_g = hav(t["lat"], t["lon"], g[1], g[2])
        if rem_g <= 1.0 or (v * dt / 3600.0) >= rem_g:
            t["lat"], t["lon"] = g[1], g[2]
            if t["kind"] in ATTACK_END or (t["kind"] == "drone" and t["sub"] not in ("recon", "orlan")):
                t["ended"] = True
            else:
                t["goal"] = None
            return
        t["hdg"] = brg(t["lat"], t["lon"], g[1], g[2])
    t["lat"], t["lon"] = fwd(t["lat"], t["lon"], t["hdg"], v * dt / 3600.0)
    if t["kind"] == "kab" and hav(*t["o"], t["lat"], t["lon"]) >= KAB_RANGE:
        t["ended"] = True
    umax = 10.0 if v < 400 else 14.0
    t["unc"] = min(umax, t["unc"] + dt * v / 3600 * 0.05)

def can_fuse_into_track(c, sub, pos, tgt, hdg_ovr, te):
    """Визначає, чи повідомлення з іншого (або того ж) каналу описує ТУ САМУ повітряну ціль."""
    if c["kind"] != sub["kind"] or c.get("ended"):
        return False, 999.0

    # Для КАБів: об'єднуємо повідомлення різних каналів, якщо вони летять в один і той самий сектор/ціль (<14 км між цілями)
    if sub["kind"] == "kab":
        if tgt and c["goal"]:
            g_dist = hav(tgt[1], tgt[2], c["goal"][1], c["goal"][2])
            if g_dist <= 14.0:
                return True, g_dist / 14.0
            return False, 999.0
        if pos:
            p_dist = hav(pos[1], pos[2], c["lat"], c["lon"])
            if p_dist <= 18.0:
                return True, p_dist / 18.0
        return False, 999.0

    # Для БпЛА та ракет:
    # 1. Перевіряємо відстань між поточною екстрапольованою позицією треку і новою точкою
    p_dist = hav(pos[1], pos[2], c["lat"], c["lon"]) if pos else 999.0
    # 2. Якщо обидва канали вказують куди летить (напр. один каже «на Комишуваху», інший «на Запоріжжя» з одного району Оріхова)
    g_dist = hav(tgt[1], tgt[2], c["goal"][1], c["goal"][2]) if (tgt and c["goal"]) else 0.0

    max_r = sub["merge"]
    if p_dist <= max_r:
        # Перевіряємо, щоб вектори не були протилежними (різниця курсів не більше 75°)
        new_hdg = brg(pos[1], pos[2], tgt[1], tgt[2]) if (pos and tgt and hav(pos[1], pos[2], tgt[1], tgt[2]) > 1.0) else hdg_ovr
        if new_hdg is not None and c["hdg"] is not None:
            if abs(ang(new_hdg - c["hdg"])) > 75.0:
                return False, 999.0
        return True, (p_dist + 0.35 * g_dist) / (max_r * 1.4)

    # Якщо другий канал назвав лише ціль («Шахед курсом на Запоріжжя») і трек вже летить туди ж
    if (not pos or pos[0].startswith("підліт")) and tgt and c["goal"]:
        if hav(tgt[1], tgt[2], c["goal"][1], c["goal"][2]) <= 15.0:
            return True, 0.4

    return False, 999.0

def upsert(sub, pos, tgt, ts, text, ch="канал", cnt=None, v_ovr=None, hdg_ovr=None, is_turn=False, msg_key=None, ai_flag=False):
    now = time.time()
    te = min(ts + REPORT_LAG, now)

    # Крок 1: Військово-тактична валідація маршруту (блокує КАБи з Запоріжжя на Оріхів та борти над містом!)
    pos, tgt, hdg_ovr = sanitize_tactical_route(sub["kind"], pos, tgt, hdg_ovr=hdg_ovr)

    if sub["kind"] == "aircraft" and not (pos or tgt):
        return None

    live = [t for t in tracks.values() if t["kind"] == sub["kind"] and not t.get("ended") and te - t["seen"] < t["ttl"]]
    for c in live:
        step(c, te)

    # Якщо вказано тільки КУДИ летить («Шахед на Вільнянськ»), спочатку шукаємо існуючий трек поруч
    synthetic_pos = False
    if not pos and tgt:
        for c in live:
            ok, _ = can_fuse_into_track(c, sub, None, tgt, hdg_ovr, te)
            if ok:
                pos = (c["frm"], c["lat"], c["lon"])
                break
        if not pos:
            from_brg = 150.0 if tgt[2] < 35.6 else 165.0
            dist_km = 26.0 if sub["kind"] == "kab" else 16.0
            sla, slo = fwd(tgt[1], tgt[2], from_brg, dist_km)
            pos = (f"підліт до {tgt[0]}", sla, slo)
            synthetic_pos = True
    elif pos and pos[0].startswith("підліт"):
        synthetic_pos = True

    if not pos:
        return None

    # Якщо це ШІ-уточнення для того самого msg_key — оновлюємо спостереження цього каналу в треку
    if msg_key:
        for c in tracks.values():
            if msg_key in c.get("msg_keys", set()) and c["kind"] == sub["kind"]:
                if not sub.get("generic") and SUB[c["sub"]].get("generic"):
                    c["sub"] = sub["id"]
                    c["ttl"] = sub["ttl"]
                if ai_flag:
                    c["ai"] = True
                return c

    # Шукаємо найкращий трек для мульти-канального злиття (Sensor Fusion)
    best_t, best_cost = None, 1.0
    for c in live:
        ok, cost = can_fuse_into_track(c, sub, pos, tgt, hdg_ovr, te)
        if ok and cost < best_cost:
            best_cost, best_t = cost, c

    obs_hdg = brg(pos[1], pos[2], tgt[1], tgt[2]) if (tgt and hav(pos[1], pos[2], tgt[1], tgt[2]) >= 0.8) else hdg_ovr
    sg = sigma_of(pos[0])
    obs_weight = 0.65 if synthetic_pos else (1.35 if (pos and tgt) else 1.0)

    if best_t is None:
        i = next(ids)
        v = v_ovr or sub["v"]
        if obs_hdg is None and v > 0 and sub["kind"] not in ("strike", "impact"):
            obs_hdg = brg(pos[1], pos[2], HOME[1], HOME[2]) if hav(pos[1], pos[2], HOME[1], HOME[2]) > 6.0 else 330.0
        t = tracks[i] = dict(
            id=i, kind=sub["kind"], sub=sub["id"], ttl=sub["ttl"], v=v,
            o=(pos[1], pos[2]), frm=pos[0], to_name=tgt[0] if tgt else None,
            lat=pos[1], lon=pos[2], hdg=obs_hdg, thdg=obs_hdg, hq=0.85 if obs_hdg is not None else 0.0,
            unc=sg, goal=tgt, ls=te, seen=te, n=1,
            fx=[(te, pos[1], pos[2], sg)], text=text, cnt=cnt,
            channels={ch} if ch else set(),
            msg_keys={msg_key} if msg_key else set(),
            obs={}
        )
        t["obs"][ch or f"c{i}"] = dict(
            ts=te, lat=pos[1], lon=pos[2], goal=tgt, hdg=obs_hdg,
            v=v, sg=sg, w=obs_weight, frm=pos[0]
        )
        step(t, now)
    else:
        t = best_t
        if not sub.get("generic") and SUB[t["sub"]].get("generic"):
            t["sub"] = sub["id"]
            t["ttl"] = sub["ttl"]
        if ch:
            t["channels"].add(ch)
        if msg_key:
            t["msg_keys"].add(msg_key)
        t["n"] = max(t["n"] + 1, len(t["channels"]))
        t["seen"] = max(t["seen"], te)
        if not synthetic_pos:
            t["frm"] = pos[0]
        if tgt:
            t["to_name"] = tgt[0]
        t["obs"][ch or f"u{t['n']}"] = dict(
            ts=te, lat=pos[1], lon=pos[2], goal=tgt or t["goal"],
            hdg=obs_hdg if obs_hdg is not None else t["hdg"],
            v=v_ovr or t["v"] or sub["v"], sg=sg, w=obs_weight, frm=pos[0]
        )
        # Перераховуємо середній консенсус між усіма каналами!
        recompute_consensus(t, now)
        t["fx"] = [(now, t["lat"], t["lon"], t["unc"])]

    if cnt:
        t["cnt"] = cnt
    if ai_flag:
        t["ai"] = True
    t["text"] = text
    return t

def view(t, now):
    la, lo, v, g = t["lat"], t["lon"], t["v"], t["goal"]
    home = hav(la, lo, HOME[1], HOME[2])
    eta, short = None, False
    if v and g and not t.get("ended"):
        dg = hav(la, lo, g[1], g[2])
        lim = dg
        if t["kind"] == "kab":
            rem = max(0, KAB_RANGE - hav(*t["o"], la, lo))
            lim = min(dg, rem)
            short = rem < dg - 0.5
        eta = int(lim / v * 3600)
    etah = None
    if v and t["hdg"] is not None and not t.get("ended"):
        tol = 22 + math.degrees(math.atan2(t["unc"], max(home, 1)))
        if abs(ang(brg(la, lo, HOME[1], HOME[2]) - t["hdg"])) <= tol:
            etah = int(home / v * 3600)
    sb = SUB[t["sub"]]
    hdg_out = round(brg(la, lo, g[1], g[2])) if (g and hav(la, lo, g[1], g[2]) > 0.3) else (None if t["hdg"] is None else round(t["hdg"]))
    chs = sorted(t.get("channels") or [])
    r = dict(
        id=t["id"], kind=t["kind"], lat=la, lon=lo, unc=round(t["unc"], 1), v=v,
        hdg=hdg_out, thdg=hdg_out, turn=45.0,
        goal=g[1:] if g else None, to=g[0] if g else None, frm=t["frm"],
        fx=[[la, lo]], fs=round(t["unc"], 1),
        n=max(t["n"], len(chs)), chs=chs,
        age=int(max(0, now - t["seen"])), text=t["text"][:160],
        eta=eta, etah=etah, ended=bool(t.get("ended")),
        home=round(home, 1), short=short, sub=t["sub"],
        name=t.get("label") or sb["name"], ico=sb["ico"],
        col=sb["col"] or KIND_COL[t["kind"]], cnt=t.get("cnt"),
        ai=bool(t.get("ai"))
    )
    if t["kind"] == "kab":
        r["alt"] = round(max(0, KAB_H0 * (1 - hav(*t["o"], la, lo) / KAB_RANGE)))
    return r

log, GEO_CACHE = [], {}
feed, fed = [], set()
SKIP = re.compile(r"^(курс|курсом|напрям|балістик|групи|група|рухаєть|також|увага|реактив|швидкіс|фіксуєть|повітрян|загроз|тривог|відбій|област|район|дніпро|запоріж)")

def note(m):
    log.append(f"[{time.strftime('%H:%M:%S')}] " + m[:180])
    del log[:-80]

GEO_Q, GEO_LOCK = None, None
NOMI = "https://nominatim.openstreetmap.org/search"

def geo_words(text):
    out = []
    for w in dict.fromkeys(re.findall(r"[А-ЯІЇЄҐ][а-яіїєґ'’\-]{3,}", text)):
        lw = w.lower()
        if lw in GEO_CACHE or SKIP.match(lw) or any(re.match(rx, lw) for *_, rx in PLACES) or any(re.search(sb["rx"], lw) for sb in SUBS):
            continue
        out.append(w)
    return out

async def geo_one(s, w):
    lw = w.lower()
    GEO_CACHE[lw] = None
    async with GEO_LOCK:
        variants = list(dict.fromkeys([w, w[:-1] + "а", w[:-1] + "я", w[:-1] + "е", w[:-2] + "ка"]))
        for q in variants:
            try:
                async with s.get(
                    NOMI,
                    headers={"User-Agent": "zaporizhzhia-radar/3.0"},
                    params={"q": q, "format": "json", "countrycodes": "ua", "limit": 6, "accept-language": "uk", "viewbox": VIEWBOX, "bounded": 1},
                    timeout=aiohttp.ClientTimeout(total=4)
                ) as r:
                    j = await r.json()
            except Exception:
                GEO_CACHE.pop(lw, None)
                return False
            await asyncio.sleep(0.8)
            if not j:
                continue
            c = [x for x in j if in_oblast(float(x["lat"]), float(x["lon"]), 10)]
            if not c:
                continue
            x = min(c, key=lambda x: hav(float(x["lat"]), float(x["lon"]), HOME[1], HOME[2]))
            nm = x["display_name"].split(",")[0].strip()
            stem = re.escape(lw[:-2] if len(lw) > 6 else lw[:-1])
            PLACES.append((nm, float(x["lat"]), float(x["lon"]), stem + r"\w*"))
            GEO_CACHE[lw] = True
            RAD[nm] = 2.8
            return True
    return False

async def geo_worker(s):
    while True:
        ts, text, ch, msg_key = await GEO_Q.get()
        added = False
        for w in geo_words(text):
            added = await geo_one(s, w) or added
        if added:
            ingest(ts, text, ch, msg_key=msg_key)
            await push()

def feed_add(ts, ch, text, kinds, pos, tgt):
    key = (int(ts // 10), text[:55])
    if key in fed:
        return
    if pos or tgt:
        if not any(in_oblast(p[1], p[2], 20) for p in (pos, tgt) if p):
            return
    fed.add(key)
    place_str = " → ".join(p[0] for p in (pos, tgt) if p)
    feed.insert(0, dict(t=ts, ch=ch, text=text[:220], kinds=kinds or ["drone"], place=place_str))
    del feed[40:]

def do_clear(kinds, loc, one=False):
    cand = [t for t in tracks.values() if (not kinds or t["kind"] in kinds) and (not loc or hav(t["lat"], t["lon"], loc[1], loc[2]) < 45)]
    if one and loc and cand:
        cand = [min([t for t in cand if t["kind"] == k], key=lambda t: hav(t["lat"], t["lon"], loc[1], loc[2])) for k in {t["kind"] for t in cand}]
    for t in cand:
        tracks.pop(t["id"], None)

def do_impact(subs, loc, ts, text, ch):
    for k in {sb["kind"] for sb, _ in subs} & {"kab", "missile", "drone"}:
        cand = [t for t in tracks.values() if t["kind"] == k and not t.get("ended")]
        if not cand:
            continue
        t = min(cand, key=lambda t: hav(t["lat"], t["lon"], loc[1], loc[2]))
        if hav(t["lat"], t["lon"], loc[1], loc[2]) < 35 or (t["goal"] and hav(t["goal"][1], t["goal"][2], loc[1], loc[2]) < 15):
            t["ended"] = True
            t["lat"], t["lon"] = loc[1], loc[2]
            t["seen"] = max(t["seen"], time.time())
    names = list(dict.fromkeys(sb["name"] for sb, _ in subs if sb["kind"] not in ("strike", "impact")))
    m = upsert(SUB["impact"], loc, None, ts, text, ch=ch, cnt=next((c for _, c in subs if c), None))
    if m:
        m["label"] = ("Приліт: " + ", ".join(names)) if names else "Приліт/вибух"

def act(intent, subs, pos, tgt, ts, text, ch, how="", v_ovr=None, hdg_ovr=None, is_turn=False, msg_key=None, ai_flag=False):
    now, loc = time.time(), pos or tgt
    kinds = list(dict.fromkeys(sb["kind"] for sb, _ in subs))
    tag = f"{ch}{how}: «{text[:55]}» → "
    if intent in ("clear", "downed"):
        do_clear(kinds, loc, one=(intent == "downed"))
        return note(tag + ("збито" if intent == "downed" else "відбій"))
    if intent == "impact":
        if not loc or loc[0] in BROAD_REGIONS:
            return note(tag + "приліт без конкретного місця")
        do_impact(subs, loc, ts, text, ch)
        feed_add(ts, ch, text, ["impact"] + [k for k in kinds if k != "impact"], pos, tgt)
        return note(tag + f"ПРИЛІТ {[sb['id'] for sb, _ in subs]} у {loc[0]}")
    if not subs:
        ref = loc
        live = [t for t in tracks.values() if t["kind"] not in ("strike", "impact") and now - t["seen"] < t["ttl"]]
        if ref and live:
            near = min(live, key=lambda t: hav(t["lat"], t["lon"], ref[1], ref[2]))
            subs, kinds = [(SUB[near["sub"]], None)], [near["kind"]]
        elif ref and tgt:
            subs, kinds = [(SUB["drone"], None)], ["drone"]
        else:
            return note(tag + "без типу цілі, пропуск")
    for sb, cnt in subs:
        if sb["kind"] == "strike":
            if loc and loc[0] not in BROAD_REGIONS:
                upsert(sb, loc, None, ts, text, ch=ch, cnt=cnt, msg_key=msg_key, ai_flag=ai_flag)
        else:
            upsert(sb, pos, tgt, ts, text, ch=ch, cnt=cnt, v_ovr=v_ovr, hdg_ovr=hdg_ovr, is_turn=is_turn, msg_key=msg_key, ai_flag=ai_flag)
    feed_add(ts, ch, text, kinds, pos, tgt)
    note(tag + f"{[(sb['id'], c) for sb, c in subs]} поз={pos and pos[0]} ціль={tgt and tgt[0]}")

def ingest_segment(ts, seg, ch="", msg_key=None, inherited_subs=None):
    tl = seg.lower()
    if GENERAL_WARNING_RX.search(tl) and not CONCRETE_FLIGHT_RX.search(tl):
        subs = detect(tl)
        kinds = list(dict.fromkeys(sb["kind"] for sb, _ in subs)) or ["kab"]
        feed_add(ts, ch, seg, kinds, ("Запоріжжя", HOME[1], HOME[2]), None)
        note(f"{ch}: «{seg[:50]}» → загальне попередження (лише в стрічку)")
        return subs
    subs = detect(tl) or inherited_subs or []
    v_ovr, hdg_ovr = parse_speed_hdg(tl)
    is_turn = bool(TURN_RX.search(tl))
    routes = extract_routes(tl)
    intent = intent_of(tl)
    if intent in ("clear", "downed") and not routes:
        act(intent, subs, None, None, ts, seg, ch, msg_key=msg_key)
        return subs
    if not routes:
        note(f"{ch}: «{seg[:50]}» → конкретних місць не знайдено")
        return subs
    if intent == "impact" and not [1 for sb, _ in subs if sb["kind"] != "strike"]:
        subs = [x for x in subs if x[0]["id"] != "arty"]
    for r_idx, (pos, tgt) in enumerate(routes):
        r_key = f"{msg_key}:r{r_idx}" if msg_key else None
        act(intent, subs, pos, tgt, ts, seg, ch, v_ovr=v_ovr, hdg_ovr=hdg_ovr, is_turn=is_turn, msg_key=r_key)
    return subs

def ingest(ts, text, ch="", msg_key=None):
    now = time.time()
    for t in tracks.values():
        step(t, now)
    cleaned = clean_post(text)
    if not cleaned or REAL_NEWS_RX.search(cleaned.lower()) or len(cleaned) > 650:
        return note(f"{ch}: «{text[:50]}» → новина/не зведення, пропуск")
    lines = [ln.strip(" •▪▫-*—–") for ln in cleaned.split("\n") if len(ln.strip()) >= 4]
    if len(lines) > 1:
        inherited = detect(lines[0].lower())
        for idx, ln in enumerate(lines):
            sub_key = f"{msg_key}:{idx}" if msg_key else None
            cur_subs = ingest_segment(ts, ln, ch, msg_key=sub_key, inherited_subs=inherited)
            if cur_subs:
                inherited = cur_subs
    else:
        ingest_segment(ts, cleaned, ch, msg_key=msg_key)

# ── ШІ-РУШІЙ (Вмикається при наявності GROQ / GEMINI / OPENAI / ANTHROPIC ключа) ──
GROQ_KEY = os.getenv("GROQ_API_KEY")
GEMINI_KEY = os.getenv("GEMINI_API_KEY")
OPENAI_KEY = os.getenv("OPENAI_API_KEY")
ANTHROPIC_KEY = os.getenv("ANTHROPIC_API_KEY")
# Публічний безключовий Pollinations вимкнено за замовчуванням для уникнення галюцинацій («КАБ із Запоріжжя на Оріхів»),
# оскільки вбудований тактичний консенсус-аналізатор розбирає маршрути точніше і за <1 мс.
FREE_AI = os.getenv("FREE_AI", "0") == "1"

LLM_CACHE, LLM_SEM = {}, None
INTENTS = {"flight", "impact", "downed", "clear", "other"}
ai_stats = {"provider": "consensus-nlp", "calls": 0, "ok": 0, "last_ms": 0, "last_err": None}

def active_ai_provider():
    if GROQ_KEY: return "Groq AI + Консенсус каналів"
    if GEMINI_KEY: return "Gemini AI + Консенсус каналів"
    if OPENAI_KEY: return "OpenAI + Консенсус каналів"
    if ANTHROPIC_KEY: return "Claude AI + Консенсус каналів"
    return "Мульти-канальний консенсус ШІ"

def llm_system():
    types = "\n".join(f"- {k}: {v['name']}" for k, v in SUB.items() if k not in ("impact", "aircraft"))
    return f"""Ти — тактичний радар ППО Запорізької області. Проаналізуй повідомлення і поверни ЛИШЕ валідний JSON:
{{"items": [{{"intent": "flight|impact|downed|clear|other", "type": "<id>", "count": "<число або null>", "at": "<де зараз або null>", "to": "<куди летить або null>", "from": "<звідки або null>", "hdg": <0..359 або null>, "speed": <км/год або null>}}]}}

КРИТИЧНІ ПРАВИЛА:
1. КАБи запускаються ТІЛЬКИ з півдня/сходу (Токмак, Молочанськ, Василівка, Пологи) на північ/північний захід (Оріхів, Гуляйполе, Комишуваха, Кушугум, Запоріжжя, Вільнянськ). КАБ НІКОЛИ не летить із Запоріжжя на Оріхів!
2. Якщо пост починається з «Запоріжжя: КАБ на Оріхів» — слово «Запоріжжя» це назва регіону (шапка), тому "from": null, "at": null, "to": "Оріхів".
3. Не створюй цілі типу "aircraft" (борти Су-34) над Запоріжжям чи вільною територією.
4. Доступні id для "type":
{types}"""

async def _call_llm_raw(s, text):
    sys_prompt = llm_system()
    t0 = time.perf_counter()
    raw = None
    if GROQ_KEY:
        model = os.getenv("GROQ_MODEL", "llama-3.3-70b-versatile")
        async with s.post(
            "https://api.groq.com/openai/v1/chat/completions",
            headers={"Authorization": f"Bearer {GROQ_KEY}", "Content-Type": "application/json"},
            json={"model": model, "temperature": 0.0, "max_tokens": 300, "response_format": {"type": "json_object"},
                  "messages": [{"role": "system", "content": sys_prompt}, {"role": "user", "content": text[:600]}]},
            timeout=aiohttp.ClientTimeout(total=4.0)
        ) as r:
            j = await r.json()
            raw = j["choices"][0]["message"]["content"]
    elif GEMINI_KEY:
        model = os.getenv("GEMINI_MODEL", "gemini-2.5-flash")
        url = f"https://generativelanguage.googleapis.com/v1beta/models/{model}:generateContent?key={GEMINI_KEY}"
        async with s.post(
            url,
            headers={"Content-Type": "application/json"},
            json={"systemInstruction": {"parts": [{"text": sys_prompt}]},
                  "contents": [{"parts": [{"text": text[:600]}]}],
                  "generationConfig": {"temperature": 0.0, "maxOutputTokens": 300, "responseMimeType": "application/json"}},
            timeout=aiohttp.ClientTimeout(total=4.0)
        ) as r:
            j = await r.json()
            raw = j["candidates"][0]["content"]["parts"][0]["text"]
    elif OPENAI_KEY:
        model = os.getenv("OPENAI_MODEL", "gpt-4o-mini")
        async with s.post(
            "https://api.openai.com/v1/chat/completions",
            headers={"Authorization": f"Bearer {OPENAI_KEY}", "Content-Type": "application/json"},
            json={"model": model, "temperature": 0.0, "max_tokens": 300, "response_format": {"type": "json_object"},
                  "messages": [{"role": "system", "content": sys_prompt}, {"role": "user", "content": text[:600]}]},
            timeout=aiohttp.ClientTimeout(total=4.5)
        ) as r:
            j = await r.json()
            raw = j["choices"][0]["message"]["content"]
    elif ANTHROPIC_KEY:
        model = os.getenv("LLM_MODEL", "claude-haiku-4-5")
        if model == "claude-haiku-5-5":
            model = "claude-haiku-4-5"
        async with s.post(
            "https://api.anthropic.com/v1/messages",
            headers={"x-api-key": ANTHROPIC_KEY, "anthropic-version": "2023-06-01", "content-type": "application/json"},
            json={"model": model, "max_tokens": 300, "system": sys_prompt, "messages": [{"role": "user", "content": text[:600]}]},
            timeout=aiohttp.ClientTimeout(total=5.0)
        ) as r:
            j = await r.json()
            raw = "".join(b.get("text", "") for b in j.get("content", []) if b.get("type") == "text")
    ai_stats["last_ms"] = round((time.perf_counter() - t0) * 1000)
    return raw

async def llm_parse(s, text):
    if not (GROQ_KEY or GEMINI_KEY or OPENAI_KEY or ANTHROPIC_KEY):
        return None
    if text in LLM_CACHE:
        return LLM_CACHE[text]
    async with LLM_SEM:
        ai_stats["calls"] += 1
        ai_stats["provider"] = active_ai_provider()
        try:
            raw = await _call_llm_raw(s, text)
            if not raw:
                return None
            m = re.search(r"\{.*\}", raw, re.S)
            info = json.loads(m.group(0) if m else raw)
            assert isinstance(info.get("items"), list)
            ai_stats["ok"] += 1
        except Exception as e:
            ai_stats["last_err"] = str(e)[:80]
            return None
    LLM_CACHE[text] = info
    if len(LLM_CACHE) > 600:
        LLM_CACHE.pop(next(iter(LLM_CACHE)))
    return info

def find_place(name):
    if not name or not isinstance(name, str):
        return None
    n = re.sub(r"^(?:м\.|с\.|смт|селище|місто|район|р-н)\s+", "", name.lower().strip())
    if n in ("дніпро", "днепр", "річка дніпро", "р. дніпро", "запорізька область", "запорізький напрямок"):
        return None
    best = None
    for pn, la, lo, rx in PLACES:
        if pn.lower() == n or re.match(r"(?:" + rx + r")", n):
            if best is None or len(rx) > best[0]:
                best = (len(rx), (pn, la, lo))
    return best and best[1]

async def resolve(s, name):
    p = find_place(name)
    if p or not isinstance(name, str) or len(name.strip()) < 3:
        return p
    w = name.strip()
    if w.lower() not in GEO_CACHE and await geo_one(s, w[0].upper() + w[1:]):
        return find_place(name)
    return None

async def apply_llm(s, info, ts, text, ch, msg_key=None):
    tl = text.lower()
    if GENERAL_WARNING_RX.search(tl) and not CONCRETE_FLIGHT_RX.search(tl):
        return
    now = time.time()
    for t in tracks.values():
        step(t, now)
    items = info.get("items") or []
    for idx, item in enumerate(items):
        if not isinstance(item, dict):
            continue
        intent = item.get("intent", "flight")
        if intent not in INTENTS or intent == "other":
            continue
        cur = await resolve(s, item.get("at"))
        to = await resolve(s, item.get("to"))
        org = await resolve(s, item.get("from"))
        sb = SUB.get(str(item.get("type", "")).strip())
        if sb and sb["kind"] == "aircraft":
            continue
        cnt = str(item["count"]) if item.get("count") not in (None, "", "null") else None
        subs = [(sb, cnt)] if sb and sb["id"] != "impact" else []
        if not subs:
            subs = [x for x in detect(tl) if x[0]["id"] != "arty" or intent != "impact"]
        v_ovr = item.get("speed")
        v_ovr = int(v_ovr) if isinstance(v_ovr, (int, float)) and 40 <= v_ovr <= 7000 else None
        hdg_ovr = item.get("hdg")
        hdg_ovr = int(hdg_ovr) % 360 if isinstance(hdg_ovr, (int, float)) and 0 <= hdg_ovr <= 360 else None
        pos, tgt = cur or org, to
        if cur and org and not to and cur[0] != org[0]:
            pos, tgt = org, cur
        if not (pos or tgt) and hdg_ovr is None:
            continue
        sub_key = f"{msg_key}:r0" if (msg_key and len(items) == 1) else (f"{msg_key}:r{idx}" if msg_key else None)
        act(intent, subs, pos, tgt, ts, text, ch, " [ШІ]", v_ovr=v_ovr, hdg_ovr=hdg_ovr, msg_key=sub_key or msg_key, ai_flag=True)

async def process(s, ts, text, ch, msg_key=None):
    try:
        cleaned = clean_post(text)
        if not cleaned or REAL_NEWS_RX.search(cleaned.lower()) or len(cleaned) > 650:
            return note(f"{ch}: «{text[:50]}» → новина/не зведення, пропуск")
        ingest(ts, cleaned, ch, msg_key=msg_key)
        await push()
        if geo_words(cleaned):
            GEO_Q.put_nowait((ts, cleaned, ch, msg_key))
        if GROQ_KEY or GEMINI_KEY or OPENAI_KEY or ANTHROPIC_KEY:
            info = await llm_parse(s, cleaned)
            if info:
                await apply_llm(s, info, ts, cleaned, ch, msg_key=msg_key)
                await push()
    except Exception as e:
        print("process error:", e)

def snap(now):
    vis = []
    for t in tracks.values():
        step(t, now)
        if in_oblast(t["lat"], t["lon"]):
            vis.append(view(t, now))
    return {
        "now": now,
        "home": HOME,
        "tracks": vis,
        "feed": feed,
        "oblast": [[la, lo] for lo, la in OBLAST],
        "ai": {"provider": active_ai_provider(), "ok": ai_stats["ok"], "ms": ai_stats["last_ms"]}
    }

async def send(payload, event=None):
    msg = (f"event: {event}\n" if event else "") + f"data: {json.dumps(payload, ensure_ascii=False)}\n\n"
    for c in list(clients):
        try:
            await c.write(msg.encode())
        except Exception:
            clients.discard(c)

async def push():
    await send(snap(time.time()))

async def tick(s):
    while True:
        now = time.time()
        for i, t in list(tracks.items()):
            if now - t["seen"] > t["ttl"] or (t.get("ended") and now - t["seen"] > 60):
                tracks.pop(i, None)
        for t in list(tracks.values()):
            step(t, now)
            if t["kind"] != "kab" or t.get("ended") or not t["goal"] or hav(*t["goal"][1:], HOME[1], HOME[2]) > 25:
                continue
            v = view(t, now)
            if v["short"]:
                continue
            lvl = 2 if v["eta"] is not None and v["eta"] <= ALERT_ETA2 else 1
            if (t["id"], lvl) not in alerted:
                alerted.add((t["id"], lvl))
                txt = f"КАБ на {HOME[0]}! {v['home']} км, висота {v.get('alt')} м, ~{v['eta']} с"
                await send({"lvl": lvl, "text": txt}, "alert")
                if NTFY:
                    try:
                        await s.post(
                            f"https://ntfy.sh/{NTFY}",
                            data=txt.encode(),
                            headers={"Title": "KAB ALERT", "Priority": "urgent", "Tags": "rotating_light"}
                        )
                    except Exception as e:
                        print("ntfy", e)
        await push()
        await asyncio.sleep(1)

async def poll(s, ch):
    first = True
    while True:
        try:
            url = f"https://t.me/s/{ch}?_={int(time.time() * 1000)}"
            headers = {
                "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/124.0.0.0 Safari/537.36",
                "Cache-Control": "no-cache, no-store, must-revalidate",
                "Pragma": "no-cache"
            }
            async with s.get(url, headers=headers, timeout=aiohttp.ClientTimeout(total=6)) as r:
                html, code = await r.text(), r.status
            new = []
            ws = BeautifulSoup(html, "html.parser").select(".tgme_widget_message")
            lt = ws[-1].select_one(".tgme_widget_message_text") if ws else None
            stats[ch] = {
                "http": code, "html_len": len(html), "messages_on_page": len(ws),
                "last_post": lt.get_text(" ", strip=True)[:200] if lt else None,
                "checked": time.strftime("%H:%M:%S")
            }
            for w in ws:
                pid, tx, tm = w.get("data-post"), w.select_one(".tgme_widget_message_text"), w.select_one("time")
                if not (pid and tx and tm) or pid in seen:
                    continue
                seen.add(pid)
                ts = datetime.fromisoformat(tm["datetime"]).timestamp()
                for br in tx.find_all("br"):
                    br.replace_with("\n")
                raw_text = tx.get_text(" ", strip=True)
                if not first or time.time() - ts < INITIAL_LOOKBACK:
                    new.append((ts, raw_text, pid))
            first = False
            for ts, text, pid in sorted(new):
                asyncio.create_task(process(s, ts, text, ch, msg_key=pid))
        except Exception as e:
            print(ch, e)
            stats[ch] = {"error": str(e)}
        await asyncio.sleep(POLL)

async def sim_endpoint(req):
    try:
        data = await req.json()
        if data.get("clear"):
            tracks.clear()
            await push()
            return web.json_response({"ok": True, "tracks": 0})
        text = str(data.get("text", "")).strip()
        ch = str(data.get("ch", "тест")).strip() or "тест"
        if not text:
            return web.json_response({"ok": False, "error": "empty text"}, status=400)
        ts = time.time()
        await process(req.app["session"], ts, text, ch, msg_key=f"sim/{int(ts*1000)}")
        return web.json_response({"ok": True, "tracks": len(tracks)})
    except Exception as e:
        return web.json_response({"ok": False, "error": str(e)}, status=500)

async def events(req):
    r = web.StreamResponse(headers={"Content-Type": "text/event-stream", "Cache-Control": "no-cache", "X-Accel-Buffering": "no"})
    await r.prepare(req)
    clients.add(r)
    try:
        await r.write(f"data: {json.dumps(snap(time.time()), ensure_ascii=False)}\n\n".encode())
        while True:
            await asyncio.sleep(3600)
    finally:
        clients.discard(r)

async def main():
    global GEO_Q, LLM_SEM, GEO_LOCK
    LLM_SEM, GEO_LOCK = asyncio.Semaphore(6), asyncio.Lock()
    s = aiohttp.ClientSession()
    GEO_Q = asyncio.Queue()
    asyncio.create_task(geo_worker(s))
    app = web.Application()
    app["session"] = s
    D = os.path.dirname(os.path.abspath(__file__))
    f = lambda n: (lambda r: web.FileResponse(os.path.join(D, n)))
    app.add_routes([
        web.get("/", f("index.html")),
        web.get("/events", events),
        web.post("/sim", sim_endpoint),
        web.get("/manifest.json", f("manifest.json")),
        web.get("/sw.js", f("sw.js")),
        web.get("/icon-192.png", f("icon-192.png")),
        web.get("/icon-512.png", f("icon-512.png")),
        web.get("/health", lambda r: web.Response(text="ok")),
        web.get("/debug", lambda r: web.json_response({
            "ai": ai_stats,
            "places_count": len(PLACES),
            "channels": stats,
            "tracks": [
                dict(id=t["id"], sub=t["sub"], v=t["v"], hdg=t["hdg"] and round(t["hdg"]),
                     unc=round(t["unc"], 1), n=t["n"], chs=sorted(t.get("channels") or []),
                     pos=[round(t["lat"], 3), round(t["lon"], 3)], frm=t["frm"], to=t["goal"] and t["goal"][0],
                     shown=in_oblast(t["lat"], t["lon"]), ai=bool(t.get("ai")))
                for t in tracks.values()
            ],
            "log": log,
            "geo": [k for k, v in GEO_CACHE.items() if v]
        }, dumps=lambda o: json.dumps(o, ensure_ascii=False, indent=1)))
    ])
    for ch in CHANNELS:
        asyncio.create_task(poll(s, ch))
    asyncio.create_task(tick(s))
    run = web.AppRunner(app)
    await run.setup()
    await web.TCPSite(run, "0.0.0.0", int(os.getenv("PORT", 8080))).start()
    print(f"Radar started on http://localhost:8080 | AI: {active_ai_provider()} | Places: {len(PLACES)}")
    await asyncio.Event().wait()

if __name__ == "__main__":
    asyncio.run(main())