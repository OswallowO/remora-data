# -*- coding: utf-8 -*-
"""雲端 Zenodo 每日同步 + 補齊(2026-07-14 建;goal:全自動、電腦關著也跑)。

跑在 GitHub Actions(不依賴任何本機 DB)。對「一段日期範圍」的每個交易日:
  ① FinMind 抓全市場分點(TaiwanStockTradingDailyReport)
     → 聚合成月包列 [date,stock,trader,buy,sell,buy_vwap] + 算 achip 特徵
  ② TWSE/TPEx 當日(或歷史 MI_INDEX)報價 → 漲停價 map;TWSE punish → 處置期間
之後把每個涉及的「當月分點月包」下載回來 → 塞當日新列(dedup) → 重傳 Zenodo 新版本(Z2);
特徵 json、散資料(漲停+處置)json 同法 merge 後重傳。

Zenodo 資料集(concept id):
  Z1 散資料(漲停價+處置股)= 20800839    Z2 分點(月包 raw + 特徵 json)= 20800285

安全:預設 dry-run(只抓不傳)。加 --go 才真的上傳 Zenodo。
用法:
  python cloud_zenodo_sync.py --start 2026-07-01 --end 2026-07-11 --go     # 補齊
  python cloud_zenodo_sync.py --latest --go                                # 每日:自動抓 FinMind 最新分點日
需要 env:FINMIND_TOKEN、ZENODO_TOKEN。Py3.10+。"""
import argparse, datetime, gzip, io, json, os, sys, time, urllib.parse, urllib.request

HERE = os.path.dirname(os.path.abspath(__file__)); sys.path.insert(0, HERE)
import cloud_chip_pipeline as ccp   # 複用官方/FinMind 抓取器 + 特徵/漲停公式(其 import 已把 stdout 轉 utf-8)
# 注意:不要在此再包一次 sys.stdout —— ccp import 時已包過一層;重複包會讓前一層 wrapper 被 GC 關閉共用 buffer。
try: sys.stdout.reconfigure(encoding='utf-8')   # 冪等:若尚未 utf-8 就地調整(不新建物件)
except Exception: pass

Z1_CONCEPT = '20800839'    # 散資料
Z2_CONCEPT = '20800285'    # 分點(月包 + 特徵)
PACK_HDR = '#schema branch/1 cols=date,stock,trader,buy,sell,buy_vwap\n'
PACK_NAME = lambda ym: f'分點月包_{ym}.jsonl.gz'
FEAT_NAME = '分點籌碼_全市場特徵v2.json.gz'   # ★須與 Zenodo 現有檔名逐字相同,否則會多傳一個新檔而非更新
SAN_NAME = '散資料_漲停價處置股.json.gz'
UA = {'User-Agent': 'Mozilla/5.0', 'accept': 'application/json'}


def log(m): print(f"[{time.strftime('%m-%d %H:%M:%S')}] {m}", flush=True)


# FinMind 限速:sponsor 版 6000 req/hr → 0.66s/req ≈ 5450/hr(留 headroom,對齊本機 orchestrator)。
# 補齊多日時這個很關鍵:太快會撞 FinMind 配額 → 回錯 → 資料破洞。可用 env FINMIND_INTERVAL 覆寫。
_FM_INTERVAL = float(os.environ.get('FINMIND_INTERVAL', '0.66'))
_FM_BASE = 'https://api.finmindtrade.com/api/v4/data'


def _fetch_branch_paced(token, syms, date_str):
    """逐檔抓 FinMind 分點(自帶限速)。回 {sym: [raw rows]}。"""
    out = {}
    for i, sym in enumerate(syms):
        q = urllib.parse.urlencode({'dataset': 'TaiwanStockTradingDailyReport',
                                    'data_id': sym, 'start_date': date_str, 'end_date': date_str, 'token': token})
        try:
            d = ccp._get_json(f'{_FM_BASE}?{q}', timeout=30)
            rows = d.get('data', [])
            if rows: out[sym] = rows
        except Exception as e:
            print(f'    [FinMind] {sym} {date_str}: {e}')
        time.sleep(_FM_INTERVAL)
        if i and i % 500 == 0:
            log(f"    …{date_str} 進度 {i}/{len(syms)}")
    return out


# ═══════════════ 分點:抓 + 聚合成月包列 ═══════════════
def branch_pack_and_feats(token, syms, date_str):
    """回 (pack_lines[list], feats{sym:featdict})。pack line = [date,stock,trader,buy,sell,buy_vwap]。"""
    raw = _fetch_branch_paced(token, syms, date_str)        # {sym: [FinMind raw rows]}(自帶限速)
    feats = ccp.compute_branch_features(raw)                 # 特徵(公式對齊研究產生器)
    lines = []
    for sym, rows in raw.items():
        agg = {}   # tid -> [buy, sell, Σbuy*price]
        for r in rows:
            tid = str(r.get('securities_trader_id', '') or '').strip()
            if not tid: continue
            b = int(r.get('buy', 0) or 0); se = int(r.get('sell', 0) or 0); p = float(r.get('price', 0) or 0)
            a = agg.setdefault(tid, [0, 0, 0.0]); a[0] += b; a[1] += se; a[2] += b * p
        for tid, (b, se, sbp) in agg.items():
            bv = round(sbp / b, 4) if b else None
            lines.append([date_str, str(sym), tid, b, se, bv])
    return lines, feats


# ═══════════════ 漲停:當日(live)或歷史(MI_INDEX)═══════════════
def _num(x):
    try: return float(str(x).replace(',', '').replace('+', '').replace('%', ''))
    except Exception: return 0.0


def lup_map_for_date(date_str, is_today):
    """回 {sym: 漲停價}。今天→用 OpenAPI STOCK_DAY_ALL(live);歷史→TWSE MI_INDEX?date=。"""
    if is_today:
        return ccp.compute_lup_map(ccp.fetch_twse_quotes(), ccp.fetch_tpex_quotes())
    ymd = date_str.replace('-', '')
    lup = {}
    # TWSE 歷史全市場(MI_INDEX ALLBUT0999)回多張 tables;個股表 = 同時含「證券代號」+「收盤價」的那張。
    # 欄位:證券代號 / … / 收盤價 / 漲跌(+/-) / 漲跌價差。漲跌(+/-)含 color:green 或內容為 '-' = 下跌。
    try:
        url = f'https://www.twse.com.tw/exchangeReport/MI_INDEX?response=json&date={ymd}&type=ALLBUT0999'
        d = json.load(urllib.request.urlopen(urllib.request.Request(url, headers=UA), timeout=30))
        stock_tab = None
        for t in (d.get('tables') or []):
            fields = [str(f) for f in (t.get('fields') or [])]
            if any('證券代號' in f for f in fields) and any(f == '收盤價' for f in fields):
                stock_tab = (fields, t.get('data') or []); break
        if stock_tab:
            fields, data = stock_tab
            si = fields.index('證券代號'); ci = fields.index('收盤價')
            chi = next((i for i, f in enumerate(fields) if '漲跌價差' in f), None)
            sgn = next((i for i, f in enumerate(fields) if '漲跌(+/-)' in f), None)
            for row in data:
                code = str(row[si]).strip()
                if not (len(code) == 4 and code.isdigit()): continue   # 只取 4 位普通股(排除權證/ETF 5-6 碼)
                close = _num(row[ci]); ch = _num(row[chi]) if chi is not None else 0.0
                if sgn is not None and ('green' in str(row[sgn]) or '-' in str(row[sgn])): ch = -abs(ch)
                prev = close - ch
                if prev > 0: lup[code] = ccp._floor_tick(prev * 1.10)
        else:
            log(f"  ⚠️ MI_INDEX({ymd})找不到個股表(可能非交易日)")
    except Exception as e:
        log(f"  ⚠️ 歷史漲停(TWSE {ymd})抓取失敗:{e}")
    return {k: v for k, v in lup.items() if k and v > 0}


def dispo_on_date(punish_rows, date_str):
    """從 TWSE punish 快照(含處置期間 start~end)判斷 date 當日在處置的股號清單。
    punish 每筆有處置起訖(民國)。只要 起 ≤ date ≤ 迄 就算當日處置。"""
    out = set()
    for r in (punish_rows or []):
        code = str(r.get('Code') or r.get('股票代號') or '').strip()
        period = str(r.get('DispositionPeriod') or r.get('處置期間') or '')
        # 期間格式常見「115年07月01日至115年07月14日」
        import re
        m = re.findall(r'(\d{2,3})\D+(\d{1,2})\D+(\d{1,2})', period)
        if len(m) >= 2 and code:
            def roc(y, mo, d): return f"{int(y)+1911:04d}-{int(mo):02d}-{int(d):02d}"
            s = roc(*m[0]); e = roc(*m[1])
            if s <= date_str <= e: out.add(code)
        elif code and not period:
            out.add(code)   # 無期間資訊→保守列入
    return sorted(out)


# ═══════════════ Zenodo:新版本 → merge 檔 → 發佈 ═══════════════
def _zget(url, token, binary=False, timeout=120):
    sep = '&' if '?' in url else '?'
    req = urllib.request.Request(f'{url}{sep}access_token={token}', headers=UA)
    r = urllib.request.urlopen(req, timeout=timeout).read()
    return r if binary else json.loads(r)


def zenodo_new_draft(concept, token):
    """對 concept 開新版本草稿,回 (dep_id, bucket, files[])。"""
    import requests
    p = {'access_token': token}
    r = requests.post(f'https://zenodo.org/api/deposit/depositions/{concept}/actions/newversion', params=p)
    r.raise_for_status()
    dep = requests.get(r.json()['links']['latest_draft'], params=p).json()
    return dep['id'], dep['links']['bucket'], dep.get('files', [])


def _draft_file_url(dep_files, name):
    for f in dep_files:
        nm = f.get('filename') or f.get('key', '')
        if nm == name:
            links = f.get('links', {})
            return links.get('download') or links.get('self'), f.get('id')
    return None, None


def zenodo_replace(dep_id, bucket, dep_files, name, local_path, token):
    import requests
    p = {'access_token': token}
    _, fid = _draft_file_url(dep_files, name)
    if fid:
        requests.delete(f'https://zenodo.org/api/deposit/depositions/{dep_id}/files/{fid}', params=p)
    with open(local_path, 'rb') as fp:
        requests.put(f'{bucket}/{urllib.parse.quote(name)}', data=fp, params=p)
    log(f"  ✓ 已上傳 {name} ({os.path.getsize(local_path)/1e6:.1f} MB)")


def zenodo_publish(dep_id, token):
    import requests
    r = requests.post(f'https://zenodo.org/api/deposit/depositions/{dep_id}/actions/publish',
                      params={'access_token': token})
    r.raise_for_status()
    return r.json().get('doi')


# ═══════════════ merge 檔內容 ═══════════════
def merge_pack(old_bytes, new_lines):
    """月包 merge:舊 gz bytes + 新列 → 新 gz bytes(dedup by date|stock|trader,新覆舊)。"""
    seen = {}
    if old_bytes:
        for ln in gzip.decompress(old_bytes).decode('utf-8').splitlines():
            if not ln or ln.startswith('#'): continue
            try: rec = json.loads(ln)
            except Exception: continue
            seen[(rec[0], rec[1], rec[2])] = rec
    for rec in new_lines:
        seen[(rec[0], rec[1], rec[2])] = rec
    buf = io.BytesIO()
    with gzip.GzipFile(fileobj=buf, mode='wb', compresslevel=6) as g:
        g.write(PACK_HDR.encode('utf-8'))
        for rec in sorted(seen.values()):
            g.write((json.dumps(rec, ensure_ascii=False) + '\n').encode('utf-8'))
    return buf.getvalue(), len(seen)


def merge_json_gz(old_bytes, updates):
    d = {}
    if old_bytes:
        try: d = json.loads(gzip.decompress(old_bytes).decode('utf-8'))
        except Exception: d = {}
    d.update(updates)
    buf = io.BytesIO()
    with gzip.GzipFile(fileobj=buf, mode='wb', compresslevel=6) as g:
        g.write(json.dumps(d, ensure_ascii=False).encode('utf-8'))
    return buf.getvalue(), len(d)


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument('--start', default=''); ap.add_argument('--end', default='')
    ap.add_argument('--latest', action='store_true', help='自動抓 FinMind 最新分點日(每日排程用)')
    ap.add_argument('--universe', default=os.path.join(HERE, 'universe_fullmarket.txt'))
    ap.add_argument('--go', action='store_true', help='真的上傳 Zenodo(預設 dry-run)')
    ap.add_argument('--work', default=os.path.join(HERE, '_zsync_tmp'))
    args = ap.parse_args()
    ftok = os.environ.get('FINMIND_TOKEN', ''); ztok = os.environ.get('ZENODO_TOKEN', '')
    if not ftok: log('⚠️ 無 FINMIND_TOKEN'); sys.exit(1)
    if args.go and not ztok: log('⚠️ --go 但無 ZENODO_TOKEN'); sys.exit(1)
    os.makedirs(args.work, exist_ok=True)
    today = datetime.date.today().isoformat()

    # 目標日期
    if args.latest:
        d = ccp.latest_branch_date(ftok)
        if not d: log('⚠️ FinMind 最新分點日偵測失敗'); sys.exit(1)
        # 每日 cron 一晚會觸發多次 → 若該日已在 Zenodo(特徵 json 有 sym|date 鍵)就跳過,
        # 免得重抓 2508 檔 + 重傳一個多餘版本(對齊 chip_pipeline 的 skip-if-good)。
        try:
            rec = json.load(urllib.request.urlopen(
                f'https://zenodo.org/api/records/{Z2_CONCEPT}/versions/latest', timeout=60))
            fu = next((f['links']['self'] for f in rec.get('files', []) if f['key'] == FEAT_NAME), None)
            if fu:
                cur = json.loads(gzip.decompress(_zget(fu, ztok or '', binary=True)))
                if any(k.endswith('|' + d) for k in cur):
                    log(f"✅ {d} 已在 Zenodo(特徵 json 已含)→ 本次跳過(每日 cron 去重)"); return
        except Exception as e:
            log(f"  (跳過檢查失敗,照常上傳):{e}")
        dates = [d]
    else:
        s = datetime.date.fromisoformat(args.start); e = datetime.date.fromisoformat(args.end)
        dates = [(s + datetime.timedelta(days=i)).isoformat()
                 for i in range((e - s).days + 1)
                 if (s + datetime.timedelta(days=i)).weekday() < 5]
    syms = [l.strip() for l in open(args.universe, encoding='utf-8') if l.strip()]
    log(f"目標 {len(dates)} 日:{dates[0]}~{dates[-1]} | universe {len(syms)} 檔 | {'上傳' if args.go else 'DRY-RUN'}")

    # 逐日抓 → 累積(按月分組)
    packs_by_month = {}   # ym -> [lines]
    all_feats = {}        # 'sym|date' -> featdict
    san_lup = {}          # date -> {sym: lup}
    san_dispo = {}        # date -> [syms]
    punish_snapshot = ccp.fetch_twse_punish()
    for ds in dates:
        is_today = (ds == today)
        lines, feats = branch_pack_and_feats(ftok, syms, ds)
        if not lines:
            log(f"  {ds}:分點空(非交易日/未公佈)→ skip"); continue
        ym = ds[:7]; packs_by_month.setdefault(ym, []).extend(lines)
        for sym, fv in feats.items(): all_feats[f'{sym}|{ds}'] = fv
        san_lup[ds] = lup_map_for_date(ds, is_today)
        san_dispo[ds] = dispo_on_date(punish_snapshot, ds)
        log(f"  {ds}:分點 {len(lines):,} 列 / {len(feats)} 檔特徵 / 漲停 {len(san_lup[ds])} 檔 / 處置 {len(san_dispo[ds])} 檔")

    if not packs_by_month:
        log('本次無任何資料(全非交易日或未公佈)→ 結束'); return
    if not args.go:
        log('DRY-RUN 完成(未上傳)。加 --go 才會傳 Zenodo。'); return

    # ── 上傳 Z2:分點月包(逐月)+ 特徵 json ──
    dep_id, bucket, dfiles = zenodo_new_draft(Z2_CONCEPT, ztok)
    for ym, lines in packs_by_month.items():
        url, _ = _draft_file_url(dfiles, PACK_NAME(ym))
        old = _zget(url, ztok, binary=True) if url else None
        merged, n = merge_pack(old, lines)
        lp = os.path.join(args.work, PACK_NAME(ym)); open(lp, 'wb').write(merged)
        log(f"  Z2 月包 {ym}:merge 後 {n:,} 列")
        zenodo_replace(dep_id, bucket, dfiles, PACK_NAME(ym), lp, ztok)
    furl, _ = _draft_file_url(dfiles, FEAT_NAME)
    fold = _zget(furl, ztok, binary=True) if furl else None
    fmerged, fn = merge_json_gz(fold, all_feats)
    flp = os.path.join(args.work, FEAT_NAME); open(flp, 'wb').write(fmerged)
    log(f"  Z2 特徵:merge 後 {fn:,} 筆(sym|date)")
    zenodo_replace(dep_id, bucket, dfiles, FEAT_NAME, flp, ztok)
    doi2 = zenodo_publish(dep_id, ztok)
    log(f"✅ Z2 發佈 DOI={doi2}")

    # ── 上傳 Z1:散資料(漲停 + 處置,以 date 為鍵 merge)──
    dep1, bucket1, dfiles1 = zenodo_new_draft(Z1_CONCEPT, ztok)
    surl, _ = _draft_file_url(dfiles1, SAN_NAME)
    sold = _zget(surl, ztok, binary=True) if surl else None
    san = {}
    if sold:
        try: san = json.loads(gzip.decompress(sold).decode('utf-8'))
        except Exception: san = {}
    # 現有 schema:{schema, generated, range, fields:{lup,punish}, lup:{date:{sym:lup}}, punish:{date:[syms]}}
    san.setdefault('schema', 'remora-san-data/1')
    san.setdefault('fields', {'lup': '{date:{sym:漲停價}}', 'punish': '{date:[處置股號]}'})
    san.setdefault('lup', {}); san.setdefault('punish', {})
    for ds in san_lup: san['lup'][ds] = san_lup[ds]
    for ds in san_dispo: san['punish'][ds] = san_dispo[ds]   # 處置 → punish 鍵(對齊現有 schema)
    _alld = sorted(set(san['lup']) | set(san['punish']))
    san['range'] = f"{_alld[0]}~{_alld[-1]}" if _alld else ''
    san['generated'] = time.strftime('%Y-%m-%d %H:%M')
    buf = io.BytesIO()
    with gzip.GzipFile(fileobj=buf, mode='wb', compresslevel=6) as g:
        g.write(json.dumps(san, ensure_ascii=False).encode('utf-8'))
    slp = os.path.join(args.work, SAN_NAME); open(slp, 'wb').write(buf.getvalue())
    log(f"  Z1 散資料:漲停 {len(san['lup'])} 日 / 處置 {len(san['punish'])} 日")
    zenodo_replace(dep1, bucket1, dfiles1, SAN_NAME, slp, ztok)
    doi1 = zenodo_publish(dep1, ztok)
    log(f"✅ Z1 發佈 DOI={doi1}")
    log('全部完成。')


if __name__ == '__main__':
    main()
