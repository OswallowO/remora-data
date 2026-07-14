# REMORA 雲端 Zenodo 自動上傳(電腦關著也跑)

把原本靠**本機 taskdeck 腳本**的每日資料上傳,搬到 **GitHub Actions 雲端**。
本機那條會因為電腦沒開而停(2026-07 就是這樣停在 6/30);雲端版與你的電腦無關。

## 這條 workflow 做什麼

`cloud_zenodo_sync.py` 對指定日期的每個交易日:
1. **FinMind** 抓全市場分點(2508 檔)→ 聚合成月包列 `[date,stock,trader,buy,sell,buy_vwap]` + 算 achip 特徵
2. **TWSE/TPEx** 當日(或歷史 MI_INDEX)報價 → **漲停價**;**TWSE punish** → **處置期間**
3. 下載 Zenodo 現有的「當月分點月包 / 特徵 json / 散資料 json」→ 塞進當日新資料(dedup)→ **重傳新版本**

對應三個 Zenodo 資料集(沿用原 concept id,不會另開新集):
- **Z2 分點** `20800285`:`分點月包_YYYY-MM.jsonl.gz`(逐月)+ `分點籌碼_全市場特徵v2.json.gz`
- **Z1 散資料** `20800839`:`散資料_漲停價處置股.json.gz`(`lup`=漲停、`punish`=處置)

## 一次性部署(3 步)

1. **放檔**:把本資料夾(`cloud_pipeline_deploy/`)的內容放到你的 data repo(remora-data)根目錄。
   關鍵檔:`cloud_zenodo_sync.py`、`cloud_chip_pipeline.py`、`universe_fullmarket.txt`、`.github/workflows/zenodo_sync.yml`。
2. **加 secret**:repo → Settings → Secrets and variables → Actions → New repository secret
   - `FINMIND_TOKEN`(你已經有,沿用即可)
   - `ZENODO_TOKEN`(從本機 `OPT8d/.zenodo_token.txt` 貼上;⚠️ 只貼進 GitHub secret,別 commit 進任何檔)
3. **push**。之後每交易日自動跑(台北 23:00 起數次 + 隔日盤前保險)。

## 補齊 07/01~07/11(到 07/13 前最後交易日)

repo → Actions → **daily-zenodo-sync** → Run workflow:
- `start` = `2026-07-01`,`end` = `2026-07-11` → 按 Run。

雲端會自己抓 9 個交易日 × 2508 檔並上傳,PC 不用開。
⚠️ **FinMind 限速**:內建 0.66s/檔(~5450/hr,低於 sponsor 6000/hr 上限)。全 9 日約 4 小時,workflow timeout 設 350 分足夠。
若想更保險,可分兩段跑(`07-01~07-04`、`07-07~07-11`)——**merge 是冪等的**(同 date|stock|trader 新蓋舊),重跑/重疊都安全。

## 每日模式(自動)

排程觸發時不帶日期 → `--latest`:自動問 FinMind「最新有分點的是哪天」就抓那天,
且若該日已在 Zenodo(特徵 json 已含)→ **自動跳過**(一晚多次 cron 不會重傳)。

## 安全

- 預設 dry-run(只抓不傳);workflow 用 `--go` 才真的上傳。
- Zenodo/FinMind token 只存在 GitHub secret,腳本不寫進 log、不 commit。
- 只讀 FinMind/TWSE、只上傳到你自己的 Zenodo 資料集,不動別的。
