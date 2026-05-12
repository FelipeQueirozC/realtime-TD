#!/usr/bin/env python3
# -*- coding: utf-8 -*-

import json
import os
import subprocess
import re
from datetime import datetime
from zoneinfo import ZoneInfo

OUTPUT_HIST = os.path.join("output", "td_hist.json")
REALTIME_JSON = os.path.join("output", "td_realtime_resgatar.json")
GIT_REALTIME_JSON = "output/td_realtime_resgatar.json" # <-- ADD THIS LINE

REVERSE_TICKER_MAP = {
    "NTN-B1 E+": "Tesouro Educa+",
    "LFT": "Tesouro Selic",
    "LTN": "Tesouro Prefixado",
    "NTN-B": "Tesouro IPCA+ com Juros Semestrais",
    "NTN-B P": "Tesouro IPCA+",
    "NTN-C": "Tesouro IGPM+ com Juros Semestrais",
    "NTN-F": "Tesouro Prefixado com Juros Semestrais",
    "NTN-B1 R+": "Tesouro Renda+ Aposentadoria Extra",
}

def parse_ticker(ticker_str):
    m = re.match(r"^(.*?)\s+(\d{4}-\d{2}-\d{2})$", ticker_str)
    if m:
        return m.group(1), m.group(2)
    return ticker_str, ""

def convert_realtime_to_hist(realtime_data, date_base):
    items = []
    for row in realtime_data:
        base, venc = parse_ticker(row["Ticker"])
        tipo = REVERSE_TICKER_MAP.get(base, base)
        items.append({
            "TipoTitulo": tipo,
            "Vencimento": venc,
            "Ticker": row["Ticker"],
            "TaxaVenda": row["Yield_Atual"],
            "PUVenda": row["Preco_Atual"]
        })
    items.sort(key=lambda x: x["Ticker"])
    return {
        "DataBase": date_base,
        "is_scraped": True,
        "items": items
    }

def get_git_history_since(date_str):
    # Change REALTIME_JSON to GIT_REALTIME_JSON here:
    cmd = ['git', 'log', f'--since={date_str}', '--format=%H|%cI', '--', GIT_REALTIME_JSON]
    try:
        res = subprocess.check_output(cmd, text=True)
    except Exception as e:
        print(f"Git log error: {e}")
        return {}
    
    day_commits = {}
    for line in res.strip().split('\n'):
        if not line: continue
        commit, timestamp = line.split('|')
        dt = datetime.fromisoformat(timestamp)
        date_ymd = dt.astimezone(ZoneInfo("America/Sao_Paulo")).strftime("%Y-%m-%d")
        
        # Newest commit first, so the first time we see date_ymd, it is the last commit of that day!
        if date_ymd not in day_commits:
            day_commits[date_ymd] = commit

    history_data = {}
    for date_ymd, commit in day_commits.items():
        if date_ymd < date_str:
            continue
        try:
            # Change REALTIME_JSON to GIT_REALTIME_JSON here:
            file_content = subprocess.check_output(['git', 'show', f'{commit}:{GIT_REALTIME_JSON}'], text=True)
            j = json.loads(file_content)
            history_data[date_ymd] = j.get("data", [])
        except Exception as e:
            print(f"Error reading commit {commit}: {e}")
            
    return history_data

def merge_hist(hist_path, new_entries):
    if os.path.exists(hist_path):
        with open(hist_path, "r", encoding="utf-8") as f:
            hist_json = json.load(f)
    else:
        hist_json = {"meta": {}, "data": []}
        
    data = hist_json.get("data", [])
    data_dict = {d["DataBase"]: d for d in data}
    
    for entry in new_entries:
        db = entry["DataBase"]
        # ONLY append if the date is missing completely, OR if the existing date is just a scraped placeholder.
        # This protects official dates from being overwritten by scraped data.
        if db not in data_dict or data_dict[db].get("is_scraped"):
            data_dict[db] = entry
            print(f"Merged scraped data for {db}")
        else:
            print(f"Ignored {db} (official Tesouro data already exists)")
            
    sorted_data = sorted(data_dict.values(), key=lambda x: x["DataBase"])
    if len(sorted_data) > 180:
        sorted_data = sorted_data[-180:]
        
    hist_json["data"] = sorted_data
    
    if sorted_data:
        if "meta" not in hist_json:
            hist_json["meta"] = {}
        hist_json["meta"]["range"] = {
            "from": sorted_data[0]["DataBase"],
            "to": sorted_data[-1]["DataBase"]
        }
        hist_json["meta"]["unique_databases"] = len(sorted_data)
        
    with open(hist_path, "w", encoding="utf-8") as f:
        json.dump(hist_json, f, ensure_ascii=False, indent=2)

def main():
    import argparse
    parser = argparse.ArgumentParser()
    parser.add_argument("--backfill-since", type=str, help="YYYY-MM-DD to backfill from Git history")
    parser.add_argument("--daily", action="store_true", help="Append current td_realtime_resgatar.json for today")
    args = parser.parse_args()

    new_entries = []

    if args.backfill_since:
        print(f"Backfilling from {args.backfill_since}...")
        history = get_git_history_since(args.backfill_since)
        for date_ymd, r_data in history.items():
            new_entries.append(convert_realtime_to_hist(r_data, date_ymd))
            
    if args.daily:
        if os.path.exists(REALTIME_JSON):
            with open(REALTIME_JSON, "r", encoding="utf-8") as f:
                r_json = json.load(f)
                r_data = r_json.get("data", [])
                
                # Fetching date precisely as it was tracked in metadata or current SP time
                last_run = r_json.get("meta", {}).get("last_run_at", "")
                if last_run:
                    date_ymd = last_run[:10]
                else:
                    date_ymd = datetime.now(ZoneInfo("America/Sao_Paulo")).strftime("%Y-%m-%d")
                new_entries.append(convert_realtime_to_hist(r_data, date_ymd))
                
    if new_entries:
        merge_hist(OUTPUT_HIST, new_entries)
        print("Done.")
    else:
        print("No entries to merge.")

if __name__ == "__main__":
    main()