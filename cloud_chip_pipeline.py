# -*- coding: utf-8 -*-
"""v2.1.0 L2 (2026-06-12) 雲端籌碼資料管線原型 — 每日盤後跑一次

目的:讓發佈版客戶端不依賴 FinMind token / 85克報告,
     由雲端每日產出三類日檔 JSON,客戶端下載即用。

資料源(探測結果 2026-06-12):
  ① TWSE OpenAPI 融資融券(MI_MARGN)+ 處置股(punish)— 官方、免費、無 CAPTCHA ✓
  ② TPEx OpenAPI(上櫃對應資料)
  ③ 分點隔日沖(FinMind TaiwanStockTradingDailyReport)— 集中用「一個」雲端 token,
     客戶端不需自備;CAPTCHA 牆使 bsr.twse.com.tw 直爬不可靠(已探測:CaptchaControl)
  ④ 跑 achip 排名(2×z(nbr) + z(churn) + z(套牢))→ 產出「明日自動 stocklist」

部署選項(擇一):
  A. GitHub Actions cron(免費、零維運;artifacts 或 commit 到 data repo)
  B. Cloudflare Workers Cron + R2(免費額度內;Workers 呼叫此腳本的 HTTP 版)
  C. 任何一台常開機器的工作排程器(最簡單,本機即可先跑)

輸出:out_dir/YYYY-MM-DD/
  margin.json     融資融券(上市+上櫃)
  punish.json     處置股
  branch.json     分點特徵(nbr/churn_r/dt_buy_avg/top5_net_r)— 需 FINMIND_TOKEN
  stocklist.json  明日自動 stocklist(achip top-N)— 需 branch.json

用法:
  py -3.10 services/cloud_chip_pipeline.py --out cloud_data [--token-env FINMIND_TOKEN] [--top-n 20]
"""
import argparse, datetime, io, json, os, sys, time, urllib.parse, urllib.request

sys.stdout = io.TextIOWrapper(sys.stdout.buffer, encoding='utf-8')
UA = {'User-Agent': 'Mozilla/5.0', 'accept': 'application/json'}


def _get_json(url, timeout=20):
    req = urllib.request.Request(url, headers=UA)
    with urllib.request.urlopen(req, timeout=timeout) as r:
        return json.load(r)


def fetch_twse_margin():
    """① 上市融資融券(官方 OpenAPI,當日盤後更新)"""
    return _get_json('https://openapi.twse.com.tw/v1/exchangeReport/MI_MARGN')


def fetch_twse_punish():
    """① 處置股(官方 OpenAPI)"""
    return _get_json('https://openapi.twse.com.tw/v1/announcement/punish')


def fetch_tpex_margin():
    """② 上櫃融資融券(TPEx OpenAPI;endpoint 由 swagger.json 確認)"""
    try:
        return _get_json('https://www.tpex.org.tw/openapi/v1/tpex_mainboard_margin_balance')
    except Exception as e:
        print(f'[TPEx margin] 失敗(非致命): {e}')
        return []


def fetch_twse_quotes():
    """①b 上市全部個股當日行情(STOCK_DAY_ALL:Code/ClosingPrice/Change …)→ lup 反推用"""
    return _get_json('https://openapi.twse.com.tw/v1/exchangeReport/STOCK_DAY_ALL')


def fetch_tpex_quotes():
    """②b 上櫃全部個股當日行情(SecuritiesCompanyCode/Close/Change …)"""
    try:
        return _get_json('https://www.tpex.org.tw/openapi/v1/tpex_mainboard_quotes')
    except Exception as e:
        print(f'[TPEx quotes] 失敗(非致命): {e}')
        return []


def _tick(p):
    if p < 10: return 0.01
    if p < 50: return 0.05
    if p < 100: return 0.1
    if p < 500: return 0.5
    if p < 1000: return 1.0
    return 5.0


def _floor_tick(p):
    t = _tick(p)
    return round(int(p / t + 1e-9) * t, 2)


def _num(x):
    try:
        return float(str(x).replace(',', '').replace('+', ''))
    except Exception:
        return 0.0


def compute_lup_map(twse_rows, tpex_rows):
    """當日漲停價 map {sym: lup}:前收 = Close − Change,lup = floor_tick(前收 × 1.10)。
    (achip 的套牢度分母 = 特徵同日的漲停價,與 分點籌碼_全市場特徵v2 的回測用法一致)"""
    lup = {}
    for r in (twse_rows or []):
        c = _num(r.get('ClosingPrice')); ch = _num(r.get('Change'))
        # TWSE Change 可能帶正負字串;'X' 等非數字 → 0
        prev = c - ch
        if prev > 0:
            lup[str(r.get('Code', '')).strip()] = _floor_tick(prev * 1.10)
    for r in (tpex_rows or []):
        c = _num(r.get('Close')); ch = _num(r.get('Change'))
        prev = c - ch
        if prev > 0:
            lup.setdefault(str(r.get('SecuritiesCompanyCode', '')).strip(), _floor_tick(prev * 1.10))
    return {k: v for k, v in lup.items() if k and v > 0}


def fetch_finmind_branch(token, syms, date_str):
    """③ 分點買賣日報(FinMind);回 {sym: rows}。
    產品化注意:全市場逐檔抓量大,僅抓族群清單股(~450 檔);
    free tier 配額不足時需付費版或分批,token 集中放雲端。"""
    out = {}
    base = 'https://api.finmindtrade.com/api/v4/data'
    for i, sym in enumerate(syms):
        q = urllib.parse.urlencode({
            'dataset': 'TaiwanStockTradingDailyReport',
            'data_id': sym, 'start_date': date_str, 'end_date': date_str,
            'token': token})
        try:
            d = _get_json(f'{base}?{q}', timeout=30)
            rows = d.get('data', [])
            if rows: out[sym] = rows
        except Exception as e:
            print(f'  [FinMind] {sym}: {e}')
        if i % 20 == 19:
            time.sleep(1.0)   # 禮貌限速
    return out


def compute_branch_features(branch_rows):
    """把分點原始列轉成 achip 特徵 — 公式逐行對齊原研究
    verify_scripts/finmind_universe_fetch_v2.py feats()(= 分點籌碼_全市場特徵v2.json 產生器):
    churn_r    = Σ min(買,賣) / 總買張(分點當沖比)
    top5_net_r = 淨買前 5 名合計 / 總買張
    dt_buy_avg = 淨買前 5 分點「買量加權均價」(套牢度分子)
    nbr        = 分點列數(該股當日有交易的分點家數)"""
    feats = {}
    for sym, rows in branch_rows.items():
        try:
            if not rows or len(rows) < 3:
                continue
            B = [(float(r.get('buy', 0) or 0), float(r.get('sell', 0) or 0),
                  float(r.get('price', 0) or 0)) for r in rows]
            nets = sorted((b - s) for b, s, p in B)
            churns = [min(b, s) for b, s, p in B]
            tb = sum(b for b, s, p in B) or 1
            Bn = sorted(B, key=lambda t: -(t[0] - t[1]))[:5]
            wb = sum(b for b, s, p in Bn) or 1
            dt_buy_avg = sum(b * p for b, s, p in Bn) / wb
            feats[sym] = {
                'churn_r': round(sum(churns) / tb, 5),
                'top5_net_r': round(sum(nets[-5:]) / tb, 5),
                'dt_buy_avg': round(dt_buy_avg, 3),
                'nbr': len(rows),
            }
        except Exception:
            continue
    return feats


def achip_stocklist(feats, lup_map, top_n=20):
    """④ achip 排名:2×z(nbr) + z(churn_r) + z(dt_buy_avg/漲停價) → top-N
    (公式對齊 交易程式 _build_chip_diff_branch achip 模式)"""
    base = {s: m for s, m in feats.items() if lup_map.get(s, 0) > 0}
    if not base: return []
    def z(d):
        vs = list(d.values())
        mu = sum(vs) / len(vs)
        sd = (sum((x - mu) ** 2 for x in vs) / len(vs)) ** 0.5 or 1.0
        return {k: (x - mu) / sd for k, x in d.items()}
    zs = z({s: m['dt_buy_avg'] / lup_map[s] for s, m in base.items()})
    zn = z({s: m.get('nbr', 0.0) for s, m in base.items()})
    zh = z({s: m.get('churn_r', 0.0) for s, m in base.items()})
    ranked = sorted(((s, zn[s] + 2.0 * zs[s]) for s in base), key=lambda t: -t[1])
    return [s for s, _ in ranked[:top_n]]


def latest_branch_date(token, probe='2330', lookback=12):
    """從今天單日往回探,回最新「有分點」的交易日。
    TaiwanStockTradingDailyReport 只支援單日查(範圍會 400)→ 逐日探。
    根治用:不靠執行時鐘 → 免疫 GitHub 排程延遲/跨午夜抓錯天/盤前空抓。"""
    today = datetime.date.today()
    for back in range(lookback):
        d = today - datetime.timedelta(days=back)
        if d.weekday() >= 5:  # 週六日跳過
            continue
        ds = d.isoformat()
        q = urllib.parse.urlencode({'dataset': 'TaiwanStockTradingDailyReport', 'data_id': probe,
                                    'start_date': ds, 'end_date': ds, 'token': token})
        try:
            r = _get_json(f'https://api.finmindtrade.com/api/v4/data?{q}', timeout=40)
            if r.get('data'):
                return ds
        except Exception:
            pass
        time.sleep(0.3)
    return None


def _append_attempt_log(out_root, status, finmind_latest, target):
    """每次排程嘗試都記一行(UTC+台北 + FinMind 最新分點日 + 結果)→ commit 回 repo,
    供事後分析『FinMind 實際何時公佈當日分點』。對應 22:30~00:00 每 10 分重試排程。"""
    try:
        now = datetime.datetime.utcnow()
        rec = {'utc': now.strftime('%Y-%m-%d %H:%M:%S'),
               'taipei': (now + datetime.timedelta(hours=8)).strftime('%Y-%m-%d %H:%M:%S'),
               'finmind_latest': finmind_latest, 'target': target, 'status': status}
        os.makedirs(out_root, exist_ok=True)
        with open(os.path.join(out_root, '_fetch_attempts.jsonl'), 'a', encoding='utf-8') as f:
            f.write(json.dumps(rec, ensure_ascii=False) + '\n')
        print(f"[attempt] 台北 {rec['taipei']} | FinMind最新={finmind_latest} | {status}")
    except Exception:
        pass


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument('--out', default='cloud_data')
    ap.add_argument('--token-env', default='FINMIND_TOKEN')
    ap.add_argument('--top-n', type=int, default=10,
                    help='achip top-N(生產參數 10:4 季套風控 +729%%、2026Q2 OOS +134%%)')
    ap.add_argument('--syms-file', default='', help='族群清單股票檔(每行一檔);省略則跳過分點層')
    ap.add_argument('--date', default='', help='覆寫日期 YYYY-MM-DD(回補用;預設今天)')
    args = ap.parse_args()
    token = os.environ.get(args.token_env, '')

    # 根治日期:不用「執行當下 UTC 日期」(GitHub 排程延遲會跨午夜抓錯天),
    # 改問 FinMind「現在最新有分點的是哪天」就抓那天 → 免疫排程時間。
    _finmind_latest = None
    if args.date:
        today = args.date
    elif token and args.syms_file and os.path.exists(args.syms_file):
        _finmind_latest = latest_branch_date(token)
        today = _finmind_latest or datetime.date.today().strftime('%Y-%m-%d')
        print(f'[date] FinMind 最新分點日 = {today}' + ('' if _finmind_latest else '(偵測失敗→退回系統日期)'))
    else:
        today = datetime.date.today().strftime('%Y-%m-%d')
    out_dir = os.path.join(args.out, today)
    os.makedirs(out_dir, exist_ok=True)
    print(f'[cloud_chip_pipeline] {today} → {out_dir}')

    # 重試友善:排程(無 --date)時,若今日 stocklist 已就緒(codes 非空)→ 直接跳過,
    # 不重抓、不蓋掉好資料。供「每 10 分排程重試直到分點公佈」用。
    _sl_path = os.path.join(out_dir, 'stocklist.json')
    if (not args.date) and os.path.exists(_sl_path):
        try:
            _ex = json.load(open(_sl_path, encoding='utf-8'))
            if _ex.get('codes'):
                print(f"  OK {today} stocklist 已就緒({len(_ex['codes'])} 檔)→ 本次跳過")
                _append_attempt_log(args.out, 'skip-already-ready', _finmind_latest, today)
                return
        except Exception:
            pass

    # 第一層:官方 OpenAPI(零依賴,一定要成功)
    margin = {'twse': fetch_twse_margin(), 'tpex': fetch_tpex_margin()}
    json.dump(margin, open(os.path.join(out_dir, 'margin.json'), 'w', encoding='utf-8'), ensure_ascii=False)
    print(f'  margin.json: 上市 {len(margin["twse"])} + 上櫃 {len(margin["tpex"])} 筆')
    punish = fetch_twse_punish()
    json.dump(punish, open(os.path.join(out_dir, 'punish.json'), 'w', encoding='utf-8'), ensure_ascii=False)
    print(f'  punish.json: {len(punish)} 筆')

    # 第一層b:全市場行情 → 當日漲停價 map(achip 套牢度分母;免 token)
    quotes_twse, quotes_tpex = fetch_twse_quotes(), fetch_tpex_quotes()
    lup_map = compute_lup_map(quotes_twse, quotes_tpex)
    json.dump(lup_map, open(os.path.join(out_dir, 'lup.json'), 'w', encoding='utf-8'), ensure_ascii=False)
    print(f'  lup.json: {len(lup_map)} 檔漲停價(上市 {len(quotes_twse)} + 上櫃 {len(quotes_tpex)} 行情)')

    # 第二層:分點(token 已於頂部讀取)
    if token and args.syms_file and os.path.exists(args.syms_file):
        syms = [l.strip() for l in open(args.syms_file, encoding='utf-8') if l.strip()]
        print(f'  分點層:抓 {len(syms)} 檔...')
        rows = fetch_finmind_branch(token, syms, today)
        feats = compute_branch_features(rows)
        if not feats:
            print(f'  WAIT 分點尚未公佈或無資料({today})→ 本次不產出 branch/stocklist,等下次排程重試')
            _append_attempt_log(args.out, 'wait-not-published', _finmind_latest, today)
            return
        json.dump(feats, open(os.path.join(out_dir, 'branch.json'), 'w', encoding='utf-8'), ensure_ascii=False)
        print(f'  OK branch.json: {len(feats)} 檔特徵 — 分點就緒 @ {datetime.datetime.utcnow():%Y-%m-%d %H:%M} UTC')

        # 第三層:自動 stocklist(achip top-N;字母序 = 客戶端引擎 day_stocks 順序)
        # 此清單供「下一個交易日」使用;客戶端 _resolve_daily_stocklist 會往回
        # 找最近一份(≤12 天)並驗新鮮度
        sl = sorted(achip_stocklist(feats, lup_map, args.top_n))
        json.dump({'date': today, 'codes': sl, 'top_n': args.top_n,
                   'source': 'cloud_chip_pipeline', 'formula': 'achip=2z(nbr)+z(churn)+z(sutao)',
                   'generated_at': datetime.datetime.now().isoformat(timespec='seconds')},
                  open(os.path.join(out_dir, 'stocklist.json'), 'w', encoding='utf-8'),
                  ensure_ascii=False, indent=1)
        print(f'  stocklist.json: top-{args.top_n} → {sl}')
        _append_attempt_log(args.out, 'produced', _finmind_latest, today)
    else:
        print('  分點層跳過(無 token 或無 syms-file)— 官方層已完成,客戶端融資券/處置/漲停價可用')


if __name__ == '__main__':
    main()
