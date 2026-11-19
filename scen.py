import sys, types, time, importlib.util
for n in ("aiohttp","aiohttp.web","bs4"):
    sys.modules[n]=types.ModuleType(n)
sys.modules["aiohttp"].web=sys.modules["aiohttp.web"]
sys.modules["bs4"].BeautifulSoup=object
path = sys.argv[1] if len(sys.argv)>1 else 'server.py'
verbose = len(sys.argv)>2
_real=time.time; CLK=[_real()]
time.time=lambda: CLK[0]
spec=importlib.util.spec_from_file_location('server',path)
m=importlib.util.module_from_spec(spec); spec.loader.exec_module(m)
def live(): return [t for t in m.tracks.values() if not t.get('ended')]
def run(label, msgs, expect):
    m.tracks.clear(); m.log.clear(); m.ch_last_track.clear(); m.ch_last_sub.clear() if hasattr(m,'ch_last_sub') else None
    CLK[0]=_real()
    for i,(dt,ch,tx) in enumerate(msgs):
        CLK[0]+=dt
        m.ingest(CLK[0], tx, ch, msg_key=f"{ch}{i}")
        if verbose:
            print(f"   +{dt:>3}s [{ch}] {tx}\n        =>", [(t['sub'],round(t['lat'],2),round(t['lon'],2),t['hdg'] and round(t['hdg']),(t['goal'] or ['-'])[0]) for t in live()])
    n=len(live()); ok = "OK " if n==expect else "BAD"
    print(f"{ok} {label}: tracks={n} expected={expect}", [(t['sub'],round(t['lat'],2),round(t['lon'],2),t['hdg'] and round(t['hdg']),(t['goal'] or ['-'])[0]) for t in live()])
M=60
run("A одна ціль різні канали", [(0,"A","Шахед біля Мелітополя"),(15*M,"B","БпЛА в районі Токмака, курс північ"),(12*M,"C","Шахед на Оріхів"),(9*M,"A","БпЛА над Комишувахою"),(4*M,"B","Шахед курс на Запоріжжя")],1)
run("B лише курси", [(0,"A","Шахед в районі Токмака"),(60,"A","курс на північ"),(90,"B","шахед курс на Запоріжжя"),(60,"C","БпЛА курс 350"),(60,"A","на Запоріжжя")],1)
run("C без типу", [(0,"A","Шахед з Мелітополя на Запоріжжя"),(8*M,"B","Токмак"),(6*M,"B","Оріхів, курс на Запоріжжя"),(4*M,"C","на Запоріжжя з півдня")],1)
run("D дві цілі", [(0,"A","Шахед біля Мелітополя"),(30,"A","Шахед біля Бердянська"),(120,"A","Шахед курс на Запоріжжя"),(120,"B","БпЛА Бердянськ курс північ")],2)
run("E один КАБ", [(0,"A","КАБ на Запоріжжя"),(20,"B","КАБ по Запоріжжю"),(20,"C","пуск КАБ на Запоріжжя"),(30,"A","КАБ курс на Запоріжжя")],1)
run("F маршрут над містами", [(0,"A","Шахед над Оріховом"),(8*M,"B","БпЛА над Комишувахою"),(8*M,"A","Шахед над Запоріжжям")],1)
run("G шахед+цей же шахед з різних каналів", [(0,"A","Шахед на Запоріжжя з Мелітополя"),(60,"B","БпЛА на Запоріжжя"),(60,"C","шахед курс Запоріжжя"),(60,"D","Шахеди на Запоріжжя")],1)
run("H курс чужій цілі", [(0,"A","Шахед біля Мелітополя"),(10,"A","Шахед біля Бердянська"),(300,"B","Шахед біля Бердянська курс на північ"),(60,"A","Шахед Мелітополь курс на Запоріжжя")],2)
run("I реактив уточнення", [(0,"A","Шахед на Запоріжжя з Мелітополя"),(60,"B","реактивний на Запоріжжя")],1)
