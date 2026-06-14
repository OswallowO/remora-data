# 雲端籌碼管線部署(GitHub Actions)— 電腦關機也自動抓資料

> 目的:每個交易日盤後,**在 GitHub 的雲端伺服器**自動抓分點 → 算 achip → 產出隔日 `stocklist.json`。
> 因為跑在雲端,**你的電腦開不開機完全沒影響**。

---

## 📦 data repo 根目錄需要的檔案(只有這 4 個 + 1 個資料夾)

```
<你的 data repo>/
├─ .github/workflows/chip_pipeline.yml   ← 排程(每交易日自動跑)
├─ cloud_chip_pipeline.py                 ← 管線本體(抓分點→算 achip→產 stocklist)
├─ group_syms.txt                         ← 231 檔 universe(achip 排名母體)
├─ README_部署步驟.md                     ← 本檔(可選)
└─ data/                                  ← 不用自己建;workflow 每天自動產 data/<日期>/
```

> **歷史分點(2024-2026)不放這裡** —— 它是靜態已驗證資料,已**打包進 app**(`Remora.spec` 內
> `分點籌碼_全市場特徵v2.json`),客戶端離線就有、回測即用。repo 只負責「forward 每日新資料」,
> 所以 repo 很輕(< 1 MB)。客戶端每天用 `_merge_cloud_branch_features` 把雲端新日併入本機檔。

---

## 一、你要做的(約 10 分鐘,一次性)

### 1. 建一個 data repo
- 到 GitHub 按 **New repository**,取名例如 `remora-data`。
- 建議 **Private(私有)**(只有籌碼清單,無機密;設私有客戶端下載需帶 token,設 Public 可免 token)。

### 2. 把這個資料夾的 3 樣放進去
把 `cloud_pipeline_deploy/` 裡的:
```
cloud_chip_pipeline.py
group_syms.txt
.github/workflows/chip_pipeline.yml
```
放到 data repo 根目錄(git push 或 GitHub 網頁直接上傳)。

### 3. 設 FINMIND_TOKEN secret
- data repo → **Settings → Secrets and variables → Actions → New repository secret**
- Name `FINMIND_TOKEN`,Value 你的 FinMind token,存檔。
- ⚠️ token 只放這裡(加密儲存),**不要寫進任何檔案 / 不要 commit**。

### 4. 開 Actions 寫入權限
- data repo → **Settings → Actions → General → Workflow permissions** → **Read and write permissions** → Save。

### 5. 測一次
- data repo → **Actions** → `daily-chip-pipeline` → **Run workflow**。
- 成功後 repo 會多出 `data/<日期>/stocklist.json` 等檔。

完成後就**每交易日自動跑**(台北 19:00 主跑、隔日 07:30 補跑),你電腦關著也會更新。

---

## 二、客戶端怎麼拿到資料

- **歷史(2024-2026)**:已打包在 app 內(`分點籌碼_全市場特徵v2.json`)→ 開箱即有,免下載。
- **forward(每日新資料)**:在客戶端「系統參數設定」填入 data repo 的 raw base URL,
  盤中監控啟動時自動下載當日 `data/<日期>/` 併入本機。
  - URL 格式:`https://raw.githubusercontent.com/<帳號>/<data-repo>/main/data`
  - 留空 = 不下載,維持本機自算 achip(仍可跑,只是少了「關機也更新」)。

> 客戶端 stocklist 解析鏈:① 雲端當日 `stocklist.json` → ② 本機自算 achip top-N(用打包的歷史特徵)→ ③ 空。

---

## 三、排程時間

- 台股 13:30 收盤,FinMind 分點日報傍晚才齊。
- 故排 **台北 19:00 主跑** + **隔日 07:30 補跑**(都在 09:00 開盤前 → 盤中一定有當日清單)。

## 四、產出內容(data/<日期>/)

| 檔 | 內容 |
|---|---|
| `stocklist.json` | **明日 achip top-10 自動選股清單**(盤中/回測進場候選池) |
| `branch.json` | 分點特徵(nbr / churn_r / dt_buy_avg / top5_net_r)— 客戶端併入本機歷史檔 |
| `margin.json` | 融資融券(上市+上櫃) |
| `punish.json` | 處置股名單 |
