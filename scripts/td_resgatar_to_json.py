import json
import os
import re
from datetime import datetime, timezone

from playwright.sync_api import sync_playwright


OUTPUT_PATH = os.path.join("output", "td_realtime_resgatar.json")

URL_CSV_RESGATAR = (
    "https://www.tesourodireto.com.br/documents/d/guest/rendimento-resgatar-csv?download=true"
)
URL_STATUS_PAGE = (
    "https://www.tesourodireto.com.br/produtos/dados-sobre-titulos/rendimento-dos-titulos"
)


def infer_ticker_base_from_titulo(titulo: str) -> str:
    """
    Infere o 'ticker base' a partir do nome do título exibido no site.
    Mantém coerência com seu padrão no Sheets.
    """
    t = (titulo or "").lower()

    if "selic" in t:
        return "LFT"
    if "educa" in t:
        return "NTN-B1 E+"
    if "renda+" in t:
        return "NTN-B1 R+"
    if "igpm" in t and "juros" in t:
        return "NTN-C"
    if "ipca" in t and "juros" in t:
        return "NTN-B"
    if "ipca" in t:
        return "NTN-B P"
    if "prefixado" in t and "juros" in t:
        return "NTN-F"
    if "prefixado" in t:
        return "LTN"

    return "TD"


def br_date_to_iso(ddmmyyyy: str) -> str:
    """
    Converte 'dd/mm/yyyy' -> 'yyyy-mm-dd'.
    Retorna "" se não casar.
    """
    m = re.match(r"^(\d{2})/(\d{2})/(\d{4})$", (ddmmyyyy or "").strip())
    if not m:
        return ""
    dd, mm, yyyy = m.group(1), m.group(2), m.group(3)
    return f"{yyyy}-{mm}-{dd}"


def parse_brl_money_to_float(s: str) -> float:
    """
    'R$ 18.275,65' -> 18275.65
    Retorna float('nan') se não casar.
    """
    s = (s or "").strip()
    m = re.search(r"R\$\s*([0-9\.\,]+)", s, flags=re.I)
    if not m:
        return float("nan")
    num = m.group(1).replace(".", "").replace(",", ".")
    try:
        return float(num)
    except ValueError:
        return float("nan")


def parse_yield_to_float(s: str) -> float:
    """
    Extrai o primeiro número percentual que aparecer:
      '13,61%' -> 13.61
      'SELIC + 0,0711%' -> 0.0711
      'IPCA + 7,80%' -> 7.80
    Retorna float('nan') se não casar.
    """
    s = (s or "").strip()
    m = re.search(r"([0-9]+,[0-9]+|[0-9]+)\s*%", s)
    if not m:
        return float("nan")
    try:
        return float(m.group(1).replace(",", "."))
    except ValueError:
        return float("nan")


def parse_semicolon_csv(text: str) -> list[list[str]]:
    """
    Parser simples para CSV separado por ';'.
    Se no futuro aparecerem aspas com ';' dentro, a gente troca para csv module com dialect custom.
    """
    lines = (text or "").replace("\r", "").split("\n")
    lines = [ln for ln in lines if ln.strip()]
    return [[cell.strip() for cell in ln.split(";")] for ln in lines]


def main():
    # Timestamp do momento em UTC (bom para auditoria / debug)
    run_ts = datetime.now(timezone.utc).isoformat()

    with sync_playwright() as p:
        # 1) Inicia Chromium headless
        browser = p.chromium.launch(headless=True)

        # 2) Contexto com locale e user-agent "de navegador"
        context = browser.new_context(
            locale="pt-BR",
            user_agent=(
                "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
                "AppleWebKit/537.36 (KHTML, like Gecko) "
                "Chrome/122.0.0.0 Safari/537.36"
            ),
        )
        page = context.new_page()

        # 3) Best-effort: pega o timestamp de atualização na página
        source_update = ""
        try:
            page.goto(URL_STATUS_PAGE, wait_until="domcontentloaded", timeout=60_000)
            # A classe que vimos no HTML é .lastMarketPricingDate
            loc = page.locator(".lastMarketPricingDate").first
            if loc.count() > 0:
                source_update = loc.inner_text().strip()
        except Exception:
            source_update = ""

        # 4) Baixa o CSV via request do contexto (melhor que page.goto para downloads)
        resp = context.request.get(URL_CSV_RESGATAR, timeout=60_000)
        if not resp.ok:
            raise RuntimeError(f"Falha ao baixar CSV: HTTP {resp.status} {resp.status_text}")

        csv_text = resp.text()

        # 5) Parse do CSV e mapeamento de colunas
        rows = parse_semicolon_csv(csv_text)
        if len(rows) < 2:
            raise RuntimeError("CSV veio vazio ou sem dados.")

        header = rows[0]
        try:
            idx_titulo = header.index("Título")
            idx_venc = header.index("Vencimento do Título")
            idx_yield = header.index("Rendimento anual do título")
            idx_pu = header.index("Preço unitário de resgate")
        except ValueError as e:
            raise RuntimeError(f"Colunas esperadas não encontradas. Header={header}") from e

        # 6) Monta linhas finais no formato desejado
        data = []
        for r in rows[1:]:
            if len(r) < len(header):
                continue

            titulo = r[idx_titulo]
            venc = r[idx_venc]
            yield_str = r[idx_yield]
            pu_str = r[idx_pu]

            venc_iso = br_date_to_iso(venc)
            base = infer_ticker_base_from_titulo(titulo)
            ticker = f"{base} {venc_iso}" if venc_iso else base

            preco = parse_brl_money_to_float(pu_str)
            yld = parse_yield_to_float(yield_str)

            if not ticker:
                continue
            if preco != preco:  # NaN check
                continue
            if yld != yld:  # NaN check
                continue

            data.append(
                {
                    "Ticker": ticker,
                    "Preco_Atual": preco,
                    "Yield_Atual": yld,
                }
            )

        # ordena por ticker pra commits estáveis
        data.sort(key=lambda x: x["Ticker"])

        payload = {
            "run_ts": run_ts,
            "source_update": source_update,  # pode vir vazio
            "source": "tesourodireto.com.br rendimento-resgatar",
            "rows": len(data),
            "data": data,
        }

        # 7) Salva JSON
        os.makedirs(os.path.dirname(OUTPUT_PATH), exist_ok=True)
        with open(OUTPUT_PATH, "w", encoding="utf-8") as f:
            json.dump(payload, f, ensure_ascii=False, indent=2)

        browser.close()

    print(f"OK: wrote {OUTPUT_PATH} with {len(data)} rows")


if __name__ == "__main__":
    main()
