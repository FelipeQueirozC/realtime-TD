import json
import xml.etree.ElementTree as ET
from datetime import datetime
import pandas as pd
import requests
from playwright.sync_api import sync_playwright
import os

# Garante que a pasta output exista
os.makedirs("output", exist_ok=True)

def get_current_vna_anbima():
    """Acessa o site moderno da ANBIMA e extrai o VNA atual, retornando (data_iso, valor)."""
    with sync_playwright() as p:
        browser = p.chromium.launch(headless=True)
        page = browser.new_page()
        page.goto("https://data.anbima.com.br/titulos-publicos/valor-nominal-atualizado")
        page.wait_for_selector("table")
        
        rows = page.locator("table tbody tr").all()
        dados_vna = None
        
        for row in rows:
            if "NTN-B" in row.inner_text():
                cols = row.locator("td").all_inner_texts()
                vna_str = cols[1].strip()
                data_ref_str = cols[2].strip() # Formato esperado: DD/MM/YYYY
                
                # Formatação dos dados
                vna_float = float(vna_str.replace('.', '').replace(',', '.'))
                # Converte DD/MM/YYYY para YYYY-MM-DD
                data_iso = datetime.strptime(data_ref_str, "%d/%m/%Y").strftime("%Y-%m-%d")
                
                dados_vna = (data_iso, vna_float)
                break
                
        browser.close()
        
        if not dados_vna:
            raise Exception("Não foi possível encontrar a NTN-B na tabela da ANBIMA.")
            
        return dados_vna

def get_historical_vna_tesouro():
    """Baixa o Excel do Tesouro e retorna um dicionário { 'YYYY-MM-DD': valor }"""
    url = "https://thot-arquivos.tesouro.gov.br/publicacao/53360"
    headers = {'User-Agent': 'Mozilla/5.0'}
    
    response = requests.get(url, headers=headers)
    response.raise_for_status()
    
    file_path = "temp_tesouro.xlsx"
    with open(file_path, 'wb') as f:
        f.write(response.content)
    
    historico_dict = {}
    
    try:
        df = pd.read_excel(file_path, sheet_name=0, skiprows=10, names=['data_competencia', 'vna_historico'])
        df = df.dropna(subset=['data_competencia', 'vna_historico'])
        
        for index, row in df.iterrows():
            data_str = str(row['data_competencia']).split(' ')[0] # Extrai apenas YYYY-MM-DD
            historico_dict[data_str] = float(row['vna_historico'])
            
        if os.path.exists(file_path):
            os.remove(file_path)
            
        return historico_dict
        
    except Exception as e:
        print(f"Aviso: Erro ao processar o histórico do Tesouro: {e}")
        if os.path.exists(file_path):
            os.remove(file_path)
        return {}

def save_diario_json(data_iso, vna_float, filename="output/vna_diario.json", limite_dias=30):
    """Lê o histórico diário, adiciona o novo valor, ordena e mantém apenas os últimos 30 dias."""
    diario_dict = {}
    
    # Se o arquivo já existir, carrega os dados atuais
    if os.path.exists(filename):
        with open(filename, 'r', encoding='utf-8') as f:
            try:
                diario_dict = json.load(f)
            except json.JSONDecodeError:
                pass # Começa vazio se o arquivo estiver corrompido ou vazio
                
    # Adiciona/Atualiza o valor de "hoje"
    diario_dict[data_iso] = vna_float
    
    # Ordena as datas cronologicamente
    datas_ordenadas = sorted(diario_dict.keys())
    
    # Corta para manter apenas os últimos N dias (30)
    if len(datas_ordenadas) > limite_dias:
        datas_ordenadas = datas_ordenadas[-limite_dias:]
        
    # Reconstrói o dicionário final
    diario_final = {data: diario_dict[data] for data in datas_ordenadas}
    
    # Salva
    with open(filename, 'w', encoding='utf-8') as f:
        json.dump(diario_final, f, ensure_ascii=False, indent=4)

def save_historico_json(historico_dict, filename="output/vna_historico.json"):
    """Salva o dicionário completo do Tesouro."""
    with open(filename, 'w', encoding='utf-8') as f:
        json.dump(historico_dict, f, ensure_ascii=False, indent=4)

def save_xml(data_iso, vna_float, filename="output/vna_atual.xml"):
    """Mantém a geração do XML do valor mais atualizado para compatibilidade."""
    root = ET.Element("VNA")
    ET.SubElement(root, "Titulo").text = "NTN-B"
    ET.SubElement(root, "Valor").text = str(vna_float)
    ET.SubElement(root, "DataReferencia").text = data_iso
    ET.SubElement(root, "UltimaAtualizacao").text = datetime.now().isoformat()
    
    tree = ET.ElementTree(root)
    tree.write(filename, encoding="utf-8", xml_declaration=True)

if __name__ == "__main__":
    print("Iniciando extração do VNA Atual (ANBIMA)...")
    data_atual, vna_atual = get_current_vna_anbima()
    print(f"VNA Atual Encontrado: {vna_atual} (Ref: {data_atual})")
    
    print("Iniciando download do Histórico (Tesouro)...")
    historico = get_historical_vna_tesouro()
    print(f"Foram extraídos {len(historico)} registros históricos.")
    
    print("Gravando vna_atual.xml...")
    save_xml(data_atual, vna_atual)
    
    print("Gravando vna_diario.json (Máximo 30 dias)...")
    save_diario_json(data_atual, vna_atual)
    
    if historico:
        print("Gravando vna_historico.json...")
        save_historico_json(historico)
    
    print("Rotina finalizada com sucesso!")
