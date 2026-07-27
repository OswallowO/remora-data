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
  py -3.10 cloud_chip_pipeline.py --out data [--token-env FINMIND_TOKEN] [--top-n 20]

★★R12-2(2026-07-27):這一份是【唯一權威】。
  原本 services/ 底下還有一份 243 行的舊複本(這一份 496 行),而且這段用法說明
  本來就寫著「py -3.10 services/cloud_chip_pipeline.py」—— 指向舊的那份。
  舊複本【缺少整套行情日期對帳】(match_quote_day / verified.json),
  照著文件部署就等於把剛修好的「漲停價用錯日期」bug 原樣放回去。
  → 舊複本已刪除(git 有歷史);部署一律以 cloud_pipeline_deploy/ 這一份為準,
    步驟見 cloud_pipeline_deploy/部署步驟_20260727_修漲停價與公式.md。
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


def _prev_jsons(out_root, today, name, k=6):
    """回「早於 today 的最近 k 份 <name>」,由新到舊:[(日期, 內容), ...]。

    ★2026-07-27 第二版:原本只取【最近一份】,那會讓守衛把自己鎖死 ——
      守衛一旦擋下某天就不產出、也就不寫 close.json;隔天比對的仍是更舊那份,
      於是又對不上、又擋下……永遠解不開。
      改成在最近幾天的收盤裡找「對到哪一天」:
        對到 D-1 → 行情是今天的
        對到更舊 → 知道落後幾天(而且下次仍比得到,不會卡死)
    """
    try:
        days = sorted((d for d in os.listdir(out_root)
                       if len(d) == 10 and d[4] == '-' and d < today
                       and os.path.exists(os.path.join(out_root, d, name))), reverse=True)[:k]
        out = []
        for d in days:
            try:
                with open(os.path.join(out_root, d, name), encoding='utf-8') as f:
                    out.append((d, json.load(f)))
            except Exception:
                pass
        return out
    except Exception:
        return []


def roc_to_iso(v):
    """民國 YYYMMDD(如 '1150724')→ '2026-07-24';解析不了回 None。"""
    t = str(v or '').strip()
    if len(t) != 7 or not t.isdigit():
        return None
    try:
        return '%04d-%s-%s' % (int(t[:3]) + 1911, t[3:5], t[5:7])
    except Exception:
        return None


def quote_date_of(rows, field='Date'):
    """這批行情【自己說】是哪一天。回 (日期, 佔比, 各日期筆數)。

    ★★2026-07-28 重大更正:R11 的根因分析是【錯的】。
      當時我在這支的註解寫著「STOCK_DAY_ALL 的回應【沒有日期欄位】」,
      並據此做了一整套「靠反推前收與歷史 close.json 比對」的自我一致性守衛,
      還寫下「穩定落後一天在數學上無法偵測」的結論。
      實測(2026-07-28 00:53)兩個端點【都有 Date】:
          TWSE STOCK_DAY_ALL        欄位含 'Date',值如 '1150724'
          TPEx tpex_mainboard_quotes 欄位含 'Date',值如 '1150727'
      → 完全不需要推論,直接讀就好。

    ★而且讀了才發現問題比原本以為的更嚴重:那個時點
      TWSE 是 07-24、TPEx 是 07-27 —— 【兩個端點不同天】,
      而管線把它們合併成同一份 lup.json 並貼上「今天」的日期
      → 那份漲停價是【兩個交易日的混合物】,不只是「落後一天」。
    """
    import collections as _c
    cnt = _c.Counter()
    for r in rows or []:
        d = roc_to_iso(r.get(field))
        if d:
            cnt[d] += 1
    if not cnt:
        return None, 0.0, {}
    top, n = cnt.most_common(1)[0]
    return top, n / max(sum(cnt.values()), 1), dict(cnt)


def compute_close_map(twse_rows, tpex_rows):
    """{sym: 當日收盤價}。存起來給【下一次執行】對帳用(見 quotes_are_lagged)。"""
    out = {}
    for r in twse_rows or []:
        c = _num(r.get('ClosingPrice'))
        s = str(r.get('Code', '')).strip()
        if s and c > 0:
            out[s] = c
    for r in tpex_rows or []:
        c = _num(r.get('Close'))
        s = str(r.get('SecuritiesCompanyCode', '')).strip()
        if s and c > 0:
            out.setdefault(s, c)
    return out


def implied_prev_close(twse_rows, tpex_rows):
    """{sym: 反推的前一日收盤} = Close − Change(與 compute_lup_map 用的是同一個推法)。"""
    out = {}
    for r in twse_rows or []:
        c, ch = _num(r.get('ClosingPrice')), _num(r.get('Change'))
        s = str(r.get('Code', '')).strip()
        if s and c > 0:
            out[s] = c - ch
    for r in tpex_rows or []:
        c, ch = _num(r.get('Close')), _num(r.get('Change'))
        s = str(r.get('SecuritiesCompanyCode', '')).strip()
        if s and c > 0:
            out.setdefault(s, c - ch)
    return out


def match_quote_day(implied_prev, prev_list, min_common=100, need_ratio=0.80):
    """這批行情的「前收」對到 prev_list 裡的哪一天?

    prev_list = [(日期, {sym: 收盤}), ...] 由新到舊。
    回 (對到的日期 or None, 對上的檔數, 共同檔數, 是否樣本不足)。

    ★用「對到哪一天」取代「跟最近一份像不像」的理由:
      後者會讓守衛自我鎖死(擋下 → 不寫 close.json → 隔天比更舊的 → 又擋下)。
      前者不論被擋幾天都還比得到,而且直接告訴你落後幾天。
    """
    best = (None, 0, 0)
    thin = True
    for d, cmap in prev_list:
        common = [s for s in implied_prev if s in cmap]
        if len(common) < min_common:
            continue
        thin = False
        hit = sum(1 for s in common
                  if abs(float(implied_prev[s]) - float(cmap[s])) < 0.005)
        if hit > best[1]:
            best = (d, hit, len(common))
        if hit / len(common) >= need_ratio:
            return d, hit, len(common), False
    return None, best[1], best[2], thin


def quotes_are_lagged(implied_prev, prev_close_map, min_common=100, need_ratio=0.80):
    """判斷這次抓到的行情是不是【落後一個交易日】。

    ★2026-07-27 加。為什麼需要:
      ⚠2026-07-28 更正:「STOCK_DAY_ALL 沒有日期欄位」是【錯的】——
        兩個端點都有 Date(民國 YYYMMDD)。這段推論式守衛因此降為【備援】,
        主判定改用端點自報的 Date(見 quote_date_of)。以下描述保留供理解舊設計。
      實測(客戶端側,cloud_data/2026-07-20/lup.json):
        228 檔裡 152 檔(66.7%)與券商來源不一致;
        反推它的「前收」= 2026-07-16 的收盤,而 07-20 的前一交易日是 07-17
        → 它拿到的是【07-17 那一列行情】,卻被寫進 07-20 的資料夾。
      而漲停價是 achip 套牢度的【分母】→ 選股直接被改掉。
      ⚠ 檔位算法本身沒問題:與客戶端 calculate_limit_up_price 對 20 萬個價格 0 不一致,
        所以【唯一】的錯就是「用了哪一天的收盤價」。

    ★為什麼不是比對「與前一份 lup 像不像」:我先做過那個版本,實測擋不住 ——
      相鄰兩份雲端 lup 只有 4~10% 相同,因為快照【每天都有前進】,
      只是穩定落後一天。所以要比的不是「有沒有變」,而是「對不對得上」。

    正確關係:今天這批行情反推出來的「前收」,應該等於【上一個交易日收盤】。
    對得上 → 行情是今天的;對不上(多半會對到再前一天)→ 落後。
    回 (是否落後, 對上的檔數, 共同檔數)。
    """
    common = [s for s in implied_prev if s in prev_close_map]
    if len(common) < min_common:
        return False, 0, len(common)          # 樣本太少不下判斷(寧可放行,不誤擋)
    hit = sum(1 for s in common
              if abs(float(implied_prev[s]) - float(prev_close_map[s])) < 0.005)
    return (hit / len(common)) < need_ratio, hit, len(common)


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
    nbr        = 分點列數(該股當日有交易的分點家數)

    ★R12-3(2026-07-27):原本逐檔 `except Exception: continue`,**沒有任何計數或日誌**
      → 「這檔本來就沒有分點資料」與「算它的時候炸了」在輸出上長得一模一樣。
      實測後果:branch.json 五天都恰好 228 檔(送進去 231 檔),固定少 3 檔,
      而要查出那 3 檔為什麼不見,只能反過來比對歷史特徵庫 —— 管線自己什麼都沒說。
      (查證結果:3383/5765 在歷史庫 509 天全是 null = FinMind 真的沒資料,跳過正確;
       6806 停在 2026-06-22,原因待查 —— 有了下面這幾行就會直接印出來。)
    ⚠ 另一個潛在分岔:本函式用 float(x or 0),而產出歷史特徵庫的
      _archive/verify_historical/finmind_universe_fetch_v2.py 用 num()=float(str(v).replace(',','')).
      對 '1,234' / '-' / 'N/A' 兩者行為不同(前者算得出來、後者【整檔丟掉】)。
      兩份產物餵進同一個特徵庫。目前 FinMind 回的是數值型別所以沒踩到,
      但一旦回傳格式變了,差別會是「整檔消失」而不是報錯 → 所以更需要下面的計數。
    """
    feats = {}
    _skip_thin = _skip_err = 0
    _err_syms = []
    for sym, rows in branch_rows.items():
        try:
            if not rows or len(rows) < 3:
                _skip_thin += 1
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
        except Exception as _e:
            _skip_err += 1
            if len(_err_syms) < 8:
                _err_syms.append('%s(%s)' % (sym, type(_e).__name__))
            continue
    # ★R12-3:把「少了幾檔、為什麼少」講出來。沉默的跳過會被當成正常運作。
    if _skip_thin or _skip_err:
        print('  branch 特徵:收到 %d 檔 → 產出 %d 檔;'
              '跳過 分點列數<3 %d 檔 / 計算失敗 %d 檔%s'
              % (len(branch_rows), len(feats), _skip_thin, _skip_err,
                 ('  失敗例:' + ', '.join(_err_syms)) if _err_syms else ''))
        if _skip_err:
            print('  ⚠ 「計算失敗」不是「沒資料」—— 若持續出現同一批股號,'
                  '很可能是 FinMind 回傳格式變了(例:千分位字串),需要修轉換而不是忽略。')
    return feats


def achip_stocklist(feats, lup_map, top_n=20):
    """④ achip 排名(A 原式,2026-07-26 定版):z(nbr) + z(churn_r) + z(dt_buy_avg/前一日漲停價) → top-N
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
    # ★2026-07-26 修:這裡原本寫 zn + 2*zs —— 那是 _ACHIP_FORMULAS['now'],
    #   正是 2.2.2 因為「五分位 Q4/Q5 反轉」而【刻意換掉】的那一式,
    #   churn(zh)算了卻完全沒用到。而 live 走的是雲端清單(路徑①優先),
    #   等於「回測用 a 算出權威數字、真錢下的是 now 選的股」——
    #   client 端的 chip_achip_formula='a' 與開機正典自檢對這條路完全管不到,
    #   綠燈綠得毫無意義。這是 achip 靜默退化家族的第四次。
    #   實測 07-24:a 與 now 的 top-10 只重疊 6/10(最差 07-20 只有 4/10)。
    ranked = sorted(((s, zn[s] + zh[s] + zs[s]) for s in base), key=lambda t: -t[1])
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
    # ★★2026-07-27 修:「已就緒」不能只看【檔案在不在】,還要看【那次產出是否通過行情日期對帳】。
    #   實測踩到:台北 17:02 手動跑了一次(TWSE 每日彙總要晚上才發布,所以排程訂在 22:30~00:00),
    #   拿到的是【前一個交易日】的行情 → lup 反推基準對到 2026-07-23 而非 07-24(66.7% vs 36.8%)。
    #   那次是第一次執行、沒有 close.json 基準,守衛依設計略過檢查 → 錯資料就寫出去了。
    #   而舊的 skip 條件只看 stocklist.json 存不存在 → 今晚排程會【整天跳過】,
    #   那份錯的漲停價就永遠留著。
    #   → 一次太早的執行足以【永久毒化】那一天。
    #   修法:只有通過日期對帳的那次會寫 verified.json;skip 必須兩者皆備。
    _sl_path = os.path.join(out_dir, 'stocklist.json')
    _vf_path = os.path.join(out_dir, 'verified.json')
    if (not args.date) and os.path.exists(_sl_path):
        try:
            _ex = json.load(open(_sl_path, encoding='utf-8'))
            if _ex.get('codes') and os.path.exists(_vf_path):
                print(f"  OK {today} stocklist 已就緒({len(_ex['codes'])} 檔)且行情日期已對帳 → 本次跳過")
                _append_attempt_log(args.out, 'skip-already-ready', _finmind_latest, today)
                return
            if _ex.get('codes'):
                print(f"  REDO {today} stocklist 存在但【沒有 verified.json】"
                      f"(= 產出時行情日期未通過對帳)→ 本次重做,不跳過")
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

    # ★★★2026-07-28 根因直接修:兩個端點【都有 Date 欄位】,直接讀,不用推論。
    #   (R11 的註解寫「沒有日期欄位」是錯的 —— 見 quote_date_of 的說明。)
    #   實測 2026-07-28 00:53:TWSE=2026-07-24、TPEx=2026-07-27 —— 【兩邊不同天】,
    #   而原本的程式把它們直接合併成一份 lup.json 並貼上「今天」的日期
    #   → 產出的漲停價是【兩個交易日的混合物】。
    #   ⚠ 漲停價是 achip 套牢度的分母,混到不同天 = 選股直接錯,而且看不出來。
    _qd_tw, _rt_tw, _all_tw = quote_date_of(quotes_twse)
    _qd_tp, _rt_tp, _all_tp = quote_date_of(quotes_tpex)
    print(f'  行情日期(端點自報):TWSE={_qd_tw}({_rt_tw:.0%}) '
          f'TPEx={_qd_tp}({_rt_tp:.0%})')

    # ① 兩邊不同天 → 【不混合】。只留與上市(筆數多、是主市場)同一天的上櫃列。
    if _qd_tw and _qd_tp and _qd_tw != _qd_tp:
        _n0 = len(quotes_tpex)
        quotes_tpex = [r for r in quotes_tpex if roc_to_iso(r.get('Date')) == _qd_tw]
        print(f'  ★兩個端點不同天(TWSE={_qd_tw} / TPEx={_qd_tp})→ '
              f'只保留與 TWSE 同一天的上櫃列:{_n0} → {len(quotes_tpex)} 筆。'
              f'【不混合不同交易日的行情】')

    # ② 行情日期 ≠ 今天 → 【不是丟掉,是寫進它真正屬於的那一天】
    #   ★2026-07-28 二修:第一版寫成「日期不符就不產出」,實測會變成【永遠不產出】——
    #     TWSE 的 OpenAPI 在 07-28 00:56 還停在 07-24(TPEx 已是 07-27),
    #     也就是它本來就穩定落後。「正確但永遠沒有資料」不是可用的設計。
    #   ★關鍵認知:那批行情【對它自己那一天是正確的】,只是來得晚。
    #     lup.json 的語意是「這一天的漲停價」,所以寫進 data/<行情日期>/ 才是對的;
    #     客戶端本來就會往回找最近一份(≤12 天),所以它讀得到。
    #   → 漲停價:寫進真正的日期資料夾(有用且正確)
    #     stocklist:仍然只在【全部日期對齊】時才產(它同時吃 branch 與 lup,
    #                兩者不同天就不該合成一份清單)
    _qdate = _qd_tw or _qd_tp
    _date_ok = (_qdate == today)
    if _qdate and not _date_ok:
        out_dir = os.path.join(args.out, _qdate)
        os.makedirs(out_dir, exist_ok=True)
        print(f'  ★行情日期 {_qdate} ≠ 執行日 {today} —— 端點尚未更新當日彙總。')
        print(f'    → lup.json 改寫進【它真正屬於的日期】 data/{_qdate}/(資料本身是對的,只是來得晚)')
        print(f'    → stocklist 仍不產出(它同時吃 branch 與 lup,不同天不該合成)')
    elif not _qdate:
        print(f'  ⚠ 兩個端點都讀不到 Date 欄位 → 退回舊的自我一致性對帳(見下)。')

    lup_map = compute_lup_map(quotes_twse, quotes_tpex)

    # ★2026-07-27 行情日期對帳(★2026-07-28 降為備援:端點其實有 Date 欄位)。
    #   正確關係 =「這批行情反推的前收」必須等於「上一個交易日的收盤」。
    #   對不上 → 行情落後 → 【不產出】,等下次排程重試。
    #   寧可今天沒有雲端清單(客戶端本來就會自動退回本機重算),
    #   也不要產出一份「頂著今天日期的昨天資料」讓真錢照著它選股。
    _close_map = compute_close_map(quotes_twse, quotes_tpex)
    _impl_prev = implied_prev_close(quotes_twse, quotes_tpex)
    _prevs = _prev_jsons(args.out, today, 'close.json')
    _newest = _prevs[0][0] if _prevs else None
    _match, _hit, _common, _thin = match_quote_day(_impl_prev, _prevs)

    # ★★【無論放行或擋下,都要寫 close.json】—— 這是不自我鎖死的關鍵。
    #   第一版只在放行時寫,結果:擋一次 → 沒寫 → 隔天比到更舊的那份 → 又擋 → 永久鎖死。
    #   為什麼寫在 today 目錄是對的(即使這批行情其實是昨天的):
    #     D 天拿到 D-1 的行情 → 存 close.json[D] = close(D-1)
    #     D+1 天端點恢復、拿到 D 的行情 → 反推前收 = close(D-1)
    #       → 對上剛存的 close.json[D],而且它是最新 → 放行 ✔ 自動恢復
    #     D+1 天端點仍落後、又拿到 D-1 的行情 → 反推前收 = close(D-2)
    #       → 對到更舊那份、不是最新 → 繼續擋 ✔ 仍然正確
    #   也就是說 close.json 記的是「這次抓到什麼」,不是「今天的官方收盤」——
    #   它的用途只有一個:給下一次比對用的參照點。
    json.dump(_close_map, open(os.path.join(out_dir, 'close.json'), 'w', encoding='utf-8'),
              ensure_ascii=False)

    # ★★這道守衛【能做到什麼、做不到什麼】(踩了兩次錯設計才想清楚,寫下來):
    #   【只在讀不到 Date 時才用】。沒有日期欄位時,「穩定落後一天」在數學上無法只靠自我一致性偵測 ——
    #   因為「每天都晚一天」與「每天都正確」產生的資料在內部完全一致。
    #   能可靠證明的只有一件事:【這批資料比我已經有的還舊】。
    #   所以判定只有三種,絕不擴張:
    #     · 對到的不是最新 → 可【證明】變舊 → 擋下(這正好抓住「開始落後」的那一天)
    #     · 對到最新       → 放行
    #     · 對不到任何一天 → 【不知道】→ 放行但示警
    #       (第二版把這種當成擋下,結果任何資料缺口之後都會把好資料擋掉 = 誤擋)
    # ★★★2026-07-28:端點【自報的日期】是直接證據,優先於下面的自我一致性推論。
    #   有 Date 就用 Date 判定;讀不到 Date 才退回舊的 match_quote_day。
    #   (舊那套是在「以為端點沒有日期」的錯誤前提下做的 —— 留著當備援,
    #    因為端點欄位哪天又改了,至少還有一條路。)
    if _qdate:
        if not _date_ok:
            # 漲停價寫進真正的日期(上面已把 out_dir 換過去),然後結束 ——
            # branch/stocklist 那兩層需要「今天的」分點資料,跟這批舊行情對不起來。
            json.dump(lup_map, open(os.path.join(out_dir, 'lup.json'), 'w', encoding='utf-8'),
                      ensure_ascii=False)
            print(f'  lup.json → data/{_qdate}/({len(lup_map)} 檔漲停價,'
                  f'上市 {len(quotes_twse)} + 上櫃 {len(quotes_tpex)} 行情)')
            try:
                json.dump({'date': _qdate, 'quote_date': _qdate, 'run_date': today,
                           'source': 'endpoint Date field(直接判定,非推論)',
                           'twse_date': _qd_tw, 'tpex_date': _qd_tp,
                           'note': '行情晚到:寫進它真正屬於的日期;stocklist 未產出',
                           'checked_at': datetime.datetime.now().isoformat(timespec='seconds')},
                          open(os.path.join(out_dir, 'verified.json'), 'w', encoding='utf-8'),
                          ensure_ascii=False, indent=1)
            except Exception:
                pass
            _append_attempt_log(args.out,
                                f'quote-late(endpoint={_qdate}, run={today}) → lup 已寫進 {_qdate}',
                                _finmind_latest, today)
            return
        print(f'  ✔ 行情日期直接判定:端點自報 {_qdate} = 目標日期 → 產出')
        try:
            json.dump({'date': today, 'quote_date': _qdate,
                       'source': 'endpoint Date field(直接判定,非推論)',
                       'twse_date': _qd_tw, 'tpex_date': _qd_tp,
                       'checked_at': datetime.datetime.now().isoformat(timespec='seconds')},
                      open(os.path.join(out_dir, 'verified.json'), 'w', encoding='utf-8'),
                      ensure_ascii=False, indent=1)
        except Exception as _e_vf2:
            print(f'  (verified.json 寫入失敗,不致命:{_e_vf2})')
    elif _thin:
        print('  行情日期對帳:沒有足夠的歷史收盤可比(第一次執行 / 樣本太少)→ 本次略過檢查')
    elif _match is None:
        print(f'  WARN 行情反推前收對不上最近 {len(_prevs)} 份收盤的任何一份'
              f'(最佳 {_hit}/{_common})—— 可能是中間有交易日沒跑到。')
        print('     【不擋】:對不上只代表不知道,不代表變舊;擋下會把好資料誤殺。')
        _append_attempt_log(args.out, f'quotes-unmatched(best {_hit}/{_common})',
                            _finmind_latest, today)
    elif _match != _newest:
        print(f'  LAGGED 行情落後:反推前收對到 {_match} 的收盤,而最新一份是 {_newest}'
              f'({_hit}/{_common})→ 這批行情不是 {today} 的')
        print('  → 本次【不產出】lup.json / stocklist.json,等下次排程重試。')
        print('     若確認今天不是交易日,這就是預期行為。')
        _append_attempt_log(args.out, f'lagged-quotes(matched {_match}, newest {_newest})',
                            _finmind_latest, today)
        return
    else:
        print(f'  行情日期對帳:反推前收 {_hit}/{_common} 檔 = {_match} 收盤 → 行情是今天的 OK')
        # ★只有【通過對帳】才寫 verified.json —— 上面的 skip 條件靠它判斷
        #   「這一天已經有可信的產出了」。沒通過(略過檢查/落後/對不到)就不寫,
        #   下次排程才會重做,不會被一次太早的執行永久卡住。
        try:
            json.dump({'date': today, 'matched': _match, 'hit': _hit, 'common': _common,
                       'checked_at': datetime.datetime.now().isoformat(timespec='seconds')},
                      open(os.path.join(out_dir, 'verified.json'), 'w', encoding='utf-8'),
                      ensure_ascii=False, indent=1)
        except Exception as _e_vf:
            print(f'  (verified.json 寫入失敗,不致命:{_e_vf})')
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
        # ★R12-1(2026-07-27):branch.json 是【公式的產物】,卻沒有任何版本/公式標記。
        #   stocklist.json 有 formula 欄位 —— 當初能抓到「雲端跑的是 2z(nbr) 不是定版 A 式」
        #   就是靠它。branch.json 沒有對應的東西,所以特徵定義一改,
        #   舊檔會靜默混進客戶端的特徵庫,而且【沒有任何檢查抓得到】。
        #   ⚠ 不把 meta 塞進 branch.json 內部:客戶端把它整個當 {股號: 特徵} 迭代,
        #     加保留鍵是破壞性變更。改寫【旁邊一個獨立檔】,舊客戶端完全不受影響。
        try:
            json.dump({'formula': 'branch_v2=churn_r|top5_net_r|dt_buy_avg|nbr',
                       'impl': 'compute_branch_features',
                       'date': today,
                       'n_in': len(rows), 'n_out': len(feats),
                       'generated_at': datetime.datetime.now().isoformat(timespec='seconds')},
                      open(os.path.join(out_dir, 'branch_meta.json'), 'w', encoding='utf-8'),
                      ensure_ascii=False, indent=1)
        except Exception as _e_bm:
            print(f'  (branch_meta.json 寫入失敗,不致命:{_e_bm})')
        print(f'  OK branch.json: {len(feats)} 檔特徵 — 分點就緒 @ {datetime.datetime.utcnow():%Y-%m-%d %H:%M} UTC')

        # 第三層:自動 stocklist(achip top-N;字母序 = 客戶端引擎 day_stocks 順序)
        # 此清單供「下一個交易日」使用;客戶端 _resolve_daily_stocklist 會往回
        # 找最近一份(≤12 天)並驗新鮮度
        sl = sorted(achip_stocklist(feats, lup_map, args.top_n))
        json.dump({'date': today, 'codes': sl, 'top_n': args.top_n,
                   'source': 'cloud_chip_pipeline', 'formula': 'achip_a=z(nbr)+z(churn)+z(sutao)',
                   'generated_at': datetime.datetime.now().isoformat(timespec='seconds')},
                  open(os.path.join(out_dir, 'stocklist.json'), 'w', encoding='utf-8'),
                  ensure_ascii=False, indent=1)
        print(f'  stocklist.json: top-{args.top_n} → {sl}')
        _append_attempt_log(args.out, 'produced', _finmind_latest, today)
    else:
        print('  分點層跳過(無 token 或無 syms-file)— 官方層已完成,客戶端融資券/處置/漲停價可用')


if __name__ == '__main__':
    main()
