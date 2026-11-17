import sys, types
for n in ("aiohttp","aiohttp.web","bs4"):
    sys.modules[n]=types.ModuleType(n)
sys.modules["aiohttp"].web=sys.modules["aiohttp.web"]
sys.modules["bs4"].BeautifulSoup=object
import sys, time, importlib.util
spec=importlib.util.spec_from_file_location('server','/mnt/user-data/outputs/server.py')
m=importlib.util.module_from_spec(spec); spec.loader.exec_module(m)

def run(label, msgs):
    m.tracks.clear()
    for tx in msgs:
        m.ingest(time.time(), tx, "test", msg_key=f"k{abs(hash(tx))}")
    print("==",label)
    for t in m.tracks.values():
        print("  ",t["sub"], "v=",m.effective_speed(t), "hdg=",t["hdg"] and round(t["hdg"]),
              "pos=",round(t["lat"],2),round(t["lon"],2), "goal=",t["goal"] and t["goal"][0], "hold=",t.get("holding"))
    if not m.tracks: print("   (нічого не створено)")
    print("   log:", m.log[-2:] if m.log else "")

run("шахед курс на Запоріжжя", ["Шахед в районі Оріхова, курс на Запоріжжя"])
run("шахеди курс на північ", ["Шахеди в районі Токмака", "Шахеди курс на північ"])
run("курс градусів", ["БпЛА біля Василівки", "БпЛА курс 340°"])
run("балістика", ["Балістика на Запоріжжя з Криму"])
run("балістика з півдня", ["Балістика з півдня на Запоріжжя"])
run("КАБ", ["КАБ на Запоріжжя"])
run("шахед з півдня", ["Шахеди з півдня на Запоріжжя"])
run("2 шахеди Мелітополь -> Запоріжжя", ["2 шахеди з Мелітополя на Запоріжжя"])
