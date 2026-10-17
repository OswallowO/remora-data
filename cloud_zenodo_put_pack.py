# -*- coding: utf-8 -*-
u"""(GitHub Actions 用)把一個月包上傳到 Zenodo 分點 concept,並核對。

為什麼:本機往 Zenodo 的上傳有時只有 20~30 KB/s(2026-10-16 實測),一個 100 MB 的月包傳不完;
GitHub 的機房往 Zenodo 快得多。本機先把月包放到 GitHub Release,再由這支在雲端轉傳。

安全流程(與本機回補同一套):
  ① 有別的程序的草稿(<2 小時)→ 不動它、本次失敗;>2 小時的殘骸才丟棄
  ② newversion → 草稿繼承全部既有檔 → 刪掉同名舊檔 → PUT 新檔
  ③ 草稿檔案清單核對:既有檔一個都不能少、新檔 MD5 必須等於 --md5
  ④ 發佈 → 公開端點再核對一次
任何一步失敗:丟棄草稿,絕不發佈,以非 0 結束。
用法:python zenodo_put_pack.py --file 分點月包_2026-07.jsonl.gz --md5 <hex>
需要 env ZENODO_TOKEN。
"""
import argparse, datetime, hashlib, json, os, sys, time, urllib.parse, urllib.request

import requests

CONCEPT = '20800284'


def log(m):
    print('[%s] %s' % (time.strftime('%H:%M:%S'), m), flush=True)


def public_latest():
    req = urllib.request.Request('https://zenodo.org/api/records/%s' % CONCEPT, headers={'User-Agent': 'python-urllib/3'})
    return json.load(urllib.request.urlopen(req, timeout=120))


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument('--file', required=True)
    ap.add_argument('--md5', required=True)
    ap.add_argument('--concept', default='20800284', help='20800284=分點(Z2) / 20800838=散資料(Z1)')
    a = ap.parse_args()
    global CONCEPT
    CONCEPT = a.concept
    if CONCEPT not in ('20800284', '20800838'):
        log('★ 未知的 concept %s → 中止' % CONCEPT); return 5
    name = os.path.basename(a.file)
    md5 = hashlib.md5(open(a.file, 'rb').read()).hexdigest()
    if md5 != a.md5:
        log('★ 下載到的檔案 MD5 %s ≠ 預期 %s → 中止' % (md5, a.md5)); return 2
    size = os.path.getsize(a.file)
    S = requests.Session()
    S.headers.update({'Authorization': 'Bearer %s' % os.environ['ZENODO_TOKEN'], 'User-Agent': 'remora-uploader/1.0'})
    rec = public_latest()
    prev_keys = set(f['key'] for f in rec['files'])
    deps = S.get('https://zenodo.org/api/deposit/depositions',
                 params={'size': 50, 'sort': 'mostrecent', 'all_versions': 'true'}, timeout=60).json()
    for d in deps:
        if str(d.get('conceptrecid')) != CONCEPT or d.get('submitted'):
            continue
        age = time.time() - datetime.datetime.strptime(str(d.get('modified'))[:19], '%Y-%m-%dT%H:%M:%S') \
            .replace(tzinfo=datetime.timezone.utc).timestamp()
        if age < 7200:
            log('★ 有其他程序的草稿正在進行(%s)→ 本次不動' % d['id']); return 3
        S.post('https://zenodo.org/api/deposit/depositions/%s/actions/discard' % d['id'], timeout=60)
        log('丟棄殘留草稿 %s' % d['id'])
    r = S.post('https://zenodo.org/api/deposit/depositions/%s/actions/newversion' % rec['id'], timeout=180)
    r.raise_for_status()
    dep = S.get(r.json()['links']['latest_draft'], timeout=60).json()
    dep_id, bucket = dep['id'], dep['links']['bucket']
    log('新草稿 %s(繼承 %d 檔)' % (dep_id, len(dep.get('files', []))))
    try:
        for f in dep.get('files', []):
            if (f.get('filename') or f.get('key')) == name:
                S.delete('https://zenodo.org/api/deposit/depositions/%s/files/%s' % (dep_id, f['id']), timeout=120)
        t0 = time.time()
        for i in range(4):
            try:
                with open(a.file, 'rb') as fh:
                    pr = S.put('%s/%s' % (bucket, urllib.parse.quote(name)), data=fh, timeout=(30, 300))
                pr.raise_for_status()
                break
            except Exception as e:
                if i == 3:
                    raise
                log('上傳重試 %d:%s' % (i + 1, type(e).__name__)); time.sleep(20 * (i + 1))
        log('上傳完成 %.1f MB / %.0f 秒' % (size / 1e6, time.time() - t0))
        fl = S.get('https://zenodo.org/api/deposit/depositions/%s/files' % dep_id, timeout=120).json()
        fm = dict(((x.get('filename') or x.get('key')), x) for x in fl)
        miss = sorted(prev_keys - set(fm))
        if miss:
            raise RuntimeError('草稿少了既有檔案:%s' % miss)
        if str(fm.get(name, {}).get('checksum', '')).split(':')[-1] != md5:
            raise RuntimeError('草稿裡的檔案 MD5 不符')
        pub = S.post('https://zenodo.org/api/deposit/depositions/%s/actions/publish' % dep_id, timeout=600)
        pub.raise_for_status()
    except Exception:
        S.post('https://zenodo.org/api/deposit/depositions/%s/actions/discard' % dep_id, timeout=60)
        raise
    for _ in range(12):
        time.sleep(10)
        rec2 = public_latest()
        f = next((x for x in rec2['files'] if x['key'] == name), None)
        if str(rec2['id']) != str(rec['id']) and f and f['checksum'].endswith(md5) and int(f['size']) == size \
                and prev_keys <= set(x['key'] for x in rec2['files']):
            log('✔ 已發佈並核對:版本 %s,%s,MD5 一致' % (rec2['id'], name)); return 0
    log('★ 發佈後公開端點核對失敗'); return 4


if __name__ == '__main__':
    sys.exit(main())
