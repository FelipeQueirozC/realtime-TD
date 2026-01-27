#!/usr/bin/env python3
# -*- coding: utf-8 -*-

import csv
import io
import json
import os
import re
from datetime import datetime
from typing import Dict, Any, List, Optional, Set

import requests
from zoneinfo import ZoneInfo


URL_TD_FULL_CSV = (
    "https://www.tesourotransparente.gov.br/ckan/dataset/"
    "df56aa42-484a-4a59-8184-7676580c81e3/resource/"
    "796d2059-14e9-44e3-80c9-2d9e30b405c1/download/precotaxatesourodireto.csv"
)

OUTPUT_PATH = os.path.join("output", "td_hist.json")

HEADERS = {
    "User-Agent": "Mozilla/5.0",
    "Accept": "text/csv,*/*",
}

SP_TZ = ZoneInfo("America/Sao_Paulo")

n_dias = 180 # numero de dias para buscar no historico

# ===== ticker map (padrão do seu Sheets) =====
TICKER_MAP = {
    "Tesouro Educa+": "NTN-B1 E+",
    "Tesouro Selic": "LFT",  # <- corrigido (LFT)
    "Tesouro Prefixado": "LTN",
    "Tesouro IPCA+ com Juros Semestrais": "NTN-B",
    "Tesouro IPCA+": "NTN-B P",
    "Tesouro IGPM+ com Juros Semestrais": "NTN-C",
    "Tesouro Prefixado com Juros Semestrais": "NTN-F",
    "Tesouro Renda+ Aposentadoria Extra": "NTN-B1 R+",
}


def now_sp_iso() -> str:
    return datetime.now(SP_TZ).isoformat(timespec="seconds")


def parse_ptbr_date_to_ymd(s: str) -> Optional[str]:
    s = (s or "").strip()
    m = re.match(r"^(\d{2})/(\d{2})/(\d{4})$", s)
    if not m:
        return None
    dd, mm, yyyy = m.group(1), m.group(2), m.group(3)
    return f"{yyyy}-{mm}-{dd}"


def parse_pt_number(s: str) -> Optional[float]:
    """
    '7,33' -> 7.33
    '1.842,01' -> 1842.01
    """
    s = (s or "").strip()
    if not s:
        return None
    s = s.replace(".", "").replace(",", ".")
    try:
        return float(s)
    except ValueError:
        return None


def fetch_csv_text(url: str) -> str:
    r = requests.get(url, headers=HEADERS, timeout=60)
    r.raise_for_status()
    return r.text


def load_previous_json(path: str) -> Optional[Dict[str, Any]]:
    if not os.path.exists(path):
        return None
    try:
        with open(path, "r", encoding="utf-8") as f:
            return json.load(f)
    except Exception:
        return None


def normalize_grouped_for_compare(grouped: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
    """
    Normaliza para comparação determinística:
    - ordena datas
    - ordena items por Ticker
    - arredonda floats
    """
    norm = []
    for d in grouped:
        items = []
        for it in d.get("items", []):
            items.append({
                "TipoTitulo": it["TipoTitulo"],
                "Vencimento": it["Vencimento"],
                "Ticker": it["Ticker"],
                "TaxaVenda": round(float(it["TaxaVenda"]), 6),
                "PUVenda": round(float(it["PUVenda"]), 6),
            })
        items.sort(key=lambda x: x["Ticker"])
        norm.append({"DataBase": d["DataBase"], "items": items})
    norm.sort(key=lambda x: x["DataBase"])
    return norm


def main():
    run_ts = now_sp_iso()
    csv_text = fetch_csv_text(URL_TD_FULL_CSV)

    reader = csv.DictReader(io.StringIO(csv_text), delimiter=";")

    required_cols = [
        "Tipo Titulo",
        "Data Vencimento",
        "Data Base",
        "Taxa Venda Manha",
        "PU Venda Manha",
    ]

    parsed_rows: List[Dict[str, Any]] = []
    data_bases: Set[str] = set()

    for row in reader:
        if any(col not in row for col in required_cols):
            raise RuntimeError(f"CSV não contém colunas esperadas. Exemplo chaves: {list(row.keys())}")

        tipo = (row.get("Tipo Titulo") or "").strip()
        venc = parse_ptbr_date_to_ymd(row.get("Data Vencimento"))
        base = parse_ptbr_date_to_ymd(row.get("Data Base"))
        taxa_venda = parse_pt_number(row.get("Taxa Venda Manha"))
        pu_venda = parse_pt_number(row.get("PU Venda Manha"))

        if not tipo or not venc or not base:
            continue
        if taxa_venda is None or pu_venda is None:
            continue

        ticker_base = TICKER_MAP.get(tipo, tipo)
        ticker = f"{ticker_base} {venc}"

        parsed_rows.append({
            "DataBase": base,
            "TipoTitulo": tipo,
            "Vencimento": venc,
            "Ticker": ticker,
            "TaxaVenda": taxa_venda,
            "PUVenda": pu_venda,
        })
        data_bases.add(base)

    sorted_bases = sorted(data_bases)
    last_180 = set(sorted_bases[-n_dias:]) if len(sorted_bases) > n_dias else set(sorted_bases)

    # Filtra e agrupa por DataBase
    grouped_map: Dict[str, List[Dict[str, Any]]] = {}
    for r in parsed_rows:
        if r["DataBase"] not in last_180:
            continue
        grouped_map.setdefault(r["DataBase"], []).append({
            "TipoTitulo": r["TipoTitulo"],
            "Vencimento": r["Vencimento"],
            "Ticker": r["Ticker"],
            "TaxaVenda": r["TaxaVenda"],
            "PUVenda": r["PUVenda"],
        })

    grouped = [{"DataBase": db, "items": items} for db, items in grouped_map.items()]
    grouped = normalize_grouped_for_compare(grouped)

    # Detecta mudança vs último JSON (meta.last_data_change_at)
    prev = load_previous_json(OUTPUT_PATH)
    prev_data = None
    prev_last_change = None
    if isinstance(prev, dict):
        prev_data = prev.get("data")
        prev_last_change = (prev.get("meta") or {}).get("last_data_change_at")

    changed = True
    if isinstance(prev_data, list):
        try:
            prev_norm = normalize_grouped_for_compare(prev_data)
            changed = (prev_norm != grouped)
        except Exception:
            changed = True

    last_change_at = run_ts if changed else (prev_last_change or run_ts)

    payload = {
        "meta": {
            "source": "tesourotransparente.gov.br (CKAN)",
            "source_url": URL_TD_FULL_CSV,
            "last_run_at": run_ts,
            "last_data_change_at": last_change_at,
            "unique_databases": len(last_180),
            "range": {
                "from": min(last_180) if last_180 else None,
                "to": max(last_180) if last_180 else None,
            },
        },
        # data = lista de datas, cada uma com items
        "data": grouped,
    }

    os.makedirs(os.path.dirname(OUTPUT_PATH), exist_ok=True)
    with open(OUTPUT_PATH, "w", encoding="utf-8") as f:
        json.dump(payload, f, ensure_ascii=False, indent=2)

    print(
        f"OK: wrote {OUTPUT_PATH} dates={len(grouped)} rows_total={sum(len(d['items']) for d in grouped)} "
        f"changed={changed} range={payload['meta']['range']}"
    )


if __name__ == "__main__":
    main()
