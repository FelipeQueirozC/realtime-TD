#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
Investidor10 (Tesouro Direto - RESGATAR) -> JSON para consumo no Google Sheets.

O que este script faz:
1) Faz GET em https://investidor10.com.br/tesouro-direto/resgatar/
2) Extrai a tabela HTML #rankigns (tbody tr) com:
   - Título
   - Rentabilidade anual
   - Preço (unid.)
   - Vencimento
3) Converte para o formato do seu Sheets:
   - data: [{ "Ticker", "Preco_Atual", "Yield_Atual" }, ...]
     onde Ticker = "<ticker_base> YYYY-MM-DD"
4) Salva em output/td_realtime_resgatar.json
5) Mantém metadados:
   - meta.last_run_at (sempre atualiza, timezone SP)
   - meta.last_price_change_at (só muda quando algum preço/yield muda vs JSON anterior)
   - meta.source / meta.source_url

Observações:
- Não tenta 'source_updated_at' (Investidor10 não fornece de forma confiável)
- Timezone explicitamente America/Sao_Paulo (para consistência no GitHub Actions)
"""

import json
import os
import re
from datetime import datetime
from typing import Optional, List, Dict, Any

import requests
from bs4 import BeautifulSoup
from zoneinfo import ZoneInfo


# ===== Config =====
URL_RESGATAR = "https://investidor10.com.br/tesouro-direto/resgatar/"
OUTPUT_PATH = os.path.join("output", "td_realtime_resgatar.json")

HEADERS = {
    "User-Agent": (
        "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 "
        "(KHTML, like Gecko) Chrome/120 Safari/537.36"
    ),
    "Accept-Language": "pt-BR,pt;q=0.9,en;q=0.8",
    "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
}


# ===== Helpers =====

def now_sp_iso() -> str:
    """Timestamp ISO em America/Sao_Paulo (independente do ambiente)."""
    return datetime.now(ZoneInfo("America/Sao_Paulo")).isoformat(timespec="seconds")


def clean(txt: str) -> str:
    return re.sub(r"\s+", " ", (txt or "")).strip()


def parse_ptbr_date_to_ymd(s: str) -> Optional[str]:
    """'dd/mm/yyyy' -> 'yyyy-mm-dd'"""
    s = clean(s)
    m = re.match(r"^(\d{2})/(\d{2})/(\d{4})$", s)
    if not m:
        return None
    dd, mm, yyyy = m.group(1), m.group(2), m.group(3)
    return f"{yyyy}-{mm}-{dd}"


def parse_money_brl_to_float(s: str) -> Optional[float]:
    """
    'R$ 4.612,51' -> 4612.51
    Aceita também '4.612,51' sem R$.
    """
    s = clean(s)
    m = re.search(r"([0-9]{1,3}(\.[0-9]{3})*,[0-9]{2})", s)
    if not m:
        return None
    num = m.group(1).replace(".", "").replace(",", ".")
    try:
        return float(num)
    except ValueError:
        return None


def parse_yield_percent_to_float(s: str) -> Optional[float]:
    """
    Extrai o primeiro percentual:
      '13,61%' -> 13.61
      'SELIC + 0,0711%' -> 0.0711
      'IPCA + 7,80%' -> 7.80
    """
    s = clean(s)
    m = re.search(r"([0-9]+,[0-9]+|[0-9]+)\s*%", s)
    if not m:
        return None
    try:
        return float(m.group(1).replace(",", "."))
    except ValueError:
        return None


def infer_ticker_base_from_titulo_investidor10(titulo: str) -> str:
    """
    Mapeia o nome do Investidor10 pro seu padrão de ticker base.
    """
    t = (titulo or "").lower()

    # Selic
    if "selic" in t:
        return "LFT"

    # Prefixados
    if "prefixado" in t and "juros" in t:
        return "NTN-F"
    if "prefixado" in t:
        return "LTN"

    # IPCA
    if "ipca" in t and "juros" in t:
        return "NTN-B"
    if "ipca" in t:
        return "NTN-B P"

    # Outros
    if "igpm" in t and "juros" in t:
        return "NTN-C"
    if "renda+" in t:
        return "NTN-B1 R+"
    if "educa" in t or "educa+" in t:
        return "NTN-B1 E+"

    return "TD"


def fetch_html(url: str) -> str:
    r = requests.get(url, headers=HEADERS, timeout=30)
    r.raise_for_status()
    return r.text


def extract_rankigns_rows(html: str) -> List[List[str]]:
    """
    Extrai linhas de table#rankigns tbody tr como lista de colunas (texto).
    Estrutura típica (RESGATAR):
      ['#', 'Título', 'Rentabilidade anual', 'Preço (unid.)', 'Vencimento']
    """
    soup = BeautifulSoup(html, "html.parser")
    table = soup.select_one("table#rankigns")
    if table is None:
        raise RuntimeError("Não encontrei table#rankigns no HTML")

    out = []
    for tr in table.select("tbody tr"):
        tds = tr.find_all("td")
        cols = [clean(td.get_text(" ", strip=True)) for td in tds]
        if cols:
            out.append(cols)
    return out


def load_previous_json(path: str) -> Optional[Dict[str, Any]]:
    if not os.path.exists(path):
        return None
    try:
        with open(path, "r", encoding="utf-8") as f:
            return json.load(f)
    except Exception:
        return None


def normalize_data_for_compare(data: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
    """
    Normaliza para comparação determinística:
    - ordena por Ticker
    - arredonda floats para evitar ruído
    """
    norm = []
    for row in data:
        norm.append(
            {
                "Ticker": row["Ticker"],
                "Preco_Atual": round(float(row["Preco_Atual"]), 6),
                "Yield_Atual": round(float(row["Yield_Atual"]), 6),
            }
        )
    norm.sort(key=lambda x: x["Ticker"])
    return norm


# ===== Main =====

def main():
    run_ts = now_sp_iso()

    html = fetch_html(URL_RESGATAR)
    rows = extract_rankigns_rows(html)

    data: List[Dict[str, Any]] = []

    for cols in rows:
        # Esperado: ['#', 'Título', 'Rentabilidade anual', 'Preço (unid.)', 'Vencimento']
        if len(cols) < 5:
            continue

        titulo = cols[1]
        yield_txt = cols[2]
        preco_txt = cols[3]
        venc_txt = cols[4]

        venc_ymd = parse_ptbr_date_to_ymd(venc_txt)
        if not venc_ymd:
            continue

        preco = parse_money_brl_to_float(preco_txt)
        yld = parse_yield_percent_to_float(yield_txt)
        if preco is None or yld is None:
            continue

        base = infer_ticker_base_from_titulo_investidor10(titulo)
        ticker = f"{base} {venc_ymd}"

        data.append(
            {
                "Ticker": ticker,
                "Preco_Atual": preco,
                "Yield_Atual": yld,
            }
        )

    # Ordena e normaliza para estabilidade (commits limpos + comparação)
    data = normalize_data_for_compare(data)

    # ---- Lógica de last_price_change_at ----
    prev = load_previous_json(OUTPUT_PATH)

    prev_data = None
    prev_last_change = None
    if isinstance(prev, dict):
        prev_data = prev.get("data")
        prev_last_change = (prev.get("meta") or {}).get("last_price_change_at")

    changed = True
    if isinstance(prev_data, list):
        try:
            prev_norm = normalize_data_for_compare(prev_data)
            changed = (prev_norm != data)
        except Exception:
            changed = True

    last_price_change_at = run_ts if changed else (prev_last_change or run_ts)

    payload = {
        "meta": {
            "source": "investidor10",
            "source_url": URL_RESGATAR,
            "last_run_at": run_ts,                 # sempre atualiza
            "last_price_change_at": last_price_change_at,  # só muda se mudou preço/yield
            "rows": len(data),
        },
        "data": data,
    }

    os.makedirs(os.path.dirname(OUTPUT_PATH), exist_ok=True)
    with open(OUTPUT_PATH, "w", encoding="utf-8") as f:
        json.dump(payload, f, ensure_ascii=False, indent=2)

    print(
        f"OK: wrote {OUTPUT_PATH} rows={len(data)} changed={changed} "
        f"last_price_change_at={last_price_change_at}"
    )


if __name__ == "__main__":
    main()
