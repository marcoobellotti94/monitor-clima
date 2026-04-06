"""
Robô de coleta climática — Open-Meteo + Climatempo + INMET
Roda semanalmente via GitHub Actions e salva JSONs em /docs/data/
"""

import json, os, urllib.request, urllib.parse, math
from datetime import datetime, timedelta, date

# ─────────────────────────────────────────────────────────────────
# LOCALIDADES — com estação primária e fallback INMET
# ─────────────────────────────────────────────────────────────────
LOCALIDADES = [
    {
        "id": "tome_acu", "nome": "Tomé-Açu", "estado": "PA",
        "lat": -2.418, "lon": -48.151,
        "inmet_estacoes": ["A213"],                    # estação no município
    },
    {
        "id": "salvaterra", "nome": "Salvaterra", "estado": "PA",
        "lat": -0.758, "lon": -48.519,
        "inmet_estacoes": ["A215"],                    # Soure, 5 km — Marajó leste
    },
    {
        "id": "soure", "nome": "Soure", "estado": "PA",
        "lat": -0.716, "lon": -48.521,
        "inmet_estacoes": ["A215"],                    # estação no município
    },
    {
        "id": "cachoeira_arari", "nome": "Cachoeira do Arari", "estado": "PA",
        "lat": -1.003, "lon": -48.958,
        "inmet_estacoes": ["A215"],                    # Soure, 58 km — mesmo corredor climático leste Marajó
        # Descartado: A214 BREVES (oeste do Marajó, clima completamente diferente)
    },
    {
        "id": "itapiranga", "nome": "Itapiranga", "estado": "AM",
        "lat": -2.746, "lon": -58.026,
        "inmet_estacoes": ["A117", "A103"],            # A117 Itapiranga (verificar se ativa); fallback A103 Itacoatiara 64km
    },
    {
        "id": "sao_raimundo_nonato", "nome": "São Raimundo Nonato", "estado": "PI",
        "lat": -9.012, "lon": -42.697,
        "inmet_estacoes": ["A325"],                    # estação no município
    },
    {
        "id": "picos", "nome": "Picos", "estado": "PI",
        "lat": -7.077, "lon": -41.467,
        "inmet_estacoes": ["A341"],                    # estação no município
    },
    {
        "id": "altamira", "nome": "Altamira", "estado": "PA",
        "lat": -3.204, "lon": -52.208,
        "inmet_estacoes": ["A253"],                    # estação no município
    },
]

CLIMATEMPO_TOKEN = os.environ.get("CLIMATEMPO_TOKEN", "")

# ─────────────────────────────────────────────────────────────────
# UTILITÁRIOS
# ─────────────────────────────────────────────────────────────────

def fetch_json(url, timeout=30):
    try:
        req = urllib.request.Request(url, headers={"User-Agent": "robo-clima/1.0"})
        with urllib.request.urlopen(req, timeout=timeout) as resp:
            return json.loads(resp.read().decode("utf-8"))
    except Exception as e:
        print(f"  [ERRO] {url[:80]}... → {e}")
        return None

def salvar_json(caminho, dados):
    os.makedirs(os.path.dirname(caminho), exist_ok=True)
    with open(caminho, "w", encoding="utf-8") as f:
        json.dump(dados, f, ensure_ascii=False, indent=2)
    print(f"  [OK] Salvo: {caminho}")

def safe_float(v):
    try:
        return float(str(v).replace(",", ".")) if v not in (None, "", "null") else None
    except (TypeError, ValueError):
        return None

# ─────────────────────────────────────────────────────────────────
# PREVISÃO — OPEN-METEO (16 dias)
# ─────────────────────────────────────────────────────────────────

def coletar_previsao_openmeteo(loc):
    params = urllib.parse.urlencode({
        "latitude": loc["lat"], "longitude": loc["lon"],
        "daily": ",".join([
            "precipitation_sum", "precipitation_probability_max",
            "temperature_2m_max", "temperature_2m_min",
            "relative_humidity_2m_max", "relative_humidity_2m_min",
            "wind_speed_10m_max", "wind_gusts_10m_max",
            "shortwave_radiation_sum", "et0_fao_evapotranspiration",
        ]),
        "timezone": "America/Sao_Paulo", "forecast_days": 16,
    })
    dados = fetch_json(f"https://api.open-meteo.com/v1/forecast?{params}")
    if not dados or "daily" not in dados:
        return None
    d = dados["daily"]
    dias = [{
        "data": d["time"][i],
        "chuva_mm": d["precipitation_sum"][i],
        "prob_chuva_pct": d["precipitation_probability_max"][i],
        "temp_max_c": d["temperature_2m_max"][i],
        "temp_min_c": d["temperature_2m_min"][i],
        "umidade_max_pct": d["relative_humidity_2m_max"][i],
        "umidade_min_pct": d["relative_humidity_2m_min"][i],
        "vento_max_kmh": d["wind_speed_10m_max"][i],
        "rajada_max_kmh": d["wind_gusts_10m_max"][i],
        "radiacao_mj_m2": d["shortwave_radiation_sum"][i],
        "evapotranspiracao_mm": d["et0_fao_evapotranspiration"][i],
    } for i in range(len(d["time"]))]
    return {"fonte": "Open-Meteo", "coletado_em": datetime.now().isoformat(), "horizonte_dias": 16, "dias": dias}

# ─────────────────────────────────────────────────────────────────
# PREVISÃO — CLIMATEMPO (15 dias)
# ─────────────────────────────────────────────────────────────────

def coletar_previsao_climatempo(loc):
    if not CLIMATEMPO_TOKEN:
        print("  [SKIP] CLIMATEMPO_TOKEN não configurado")
        return None
    resultado = fetch_json(
        f"http://apiadvisor.climatempo.com.br/api/v1/locale/city"
        f"?name={urllib.parse.quote(loc['nome'])}&state={loc['estado']}&token={CLIMATEMPO_TOKEN}"
    )
    if not resultado or not isinstance(resultado, list) or not resultado:
        return None
    cidade_id = resultado[0].get("id")
    if not cidade_id:
        return None
    try:
        data = urllib.parse.urlencode({"localeId[]": cidade_id}).encode()
        req = urllib.request.Request(
            f"http://apiadvisor.climatempo.com.br/api-manager/user-token/{CLIMATEMPO_TOKEN}/locales",
            data=data, method="PUT", headers={"User-Agent": "robo-clima/1.0"}
        )
        with urllib.request.urlopen(req, timeout=15): pass
    except Exception as e:
        print(f"  [AVISO] Climatempo registro: {e}")
    dados = fetch_json(
        f"http://apiadvisor.climatempo.com.br/api/v1/forecast/locale/{cidade_id}/days/15?token={CLIMATEMPO_TOKEN}"
    )
    if not dados or "data" not in dados:
        return None
    dias = [{
        "data": d.get("date_br", d.get("date", "")),
        "chuva_mm": d.get("rain", {}).get("precipitation"),
        "prob_chuva_pct": d.get("rain", {}).get("probability"),
        "temp_max_c": d.get("temperature", {}).get("max"),
        "temp_min_c": d.get("temperature", {}).get("min"),
        "umidade_max_pct": d.get("humidity", {}).get("max"),
        "umidade_min_pct": d.get("humidity", {}).get("min"),
        "vento_max_kmh": d.get("wind", {}).get("velocity_max"),
        "condicao": d.get("text_icon", {}).get("text", {}).get("pt"),
    } for d in dados["data"]]
    return {"fonte": "Climatempo", "cidade_id": cidade_id,
            "coletado_em": datetime.now().isoformat(), "horizonte_dias": 15, "dias": dias}

# ─────────────────────────────────────────────────────────────────
# HISTÓRICO — OPEN-METEO / ERA5  (URL corrigida)
# ─────────────────────────────────────────────────────────────────

def coletar_historico_openmeteo(loc):
    data_fim   = date.today() - timedelta(days=5)   # ERA5 tem ~5 dias de atraso
    data_inicio = data_fim - timedelta(days=35)
    params = urllib.parse.urlencode({
        "latitude": loc["lat"], "longitude": loc["lon"],
        "start_date": data_inicio.isoformat(), "end_date": data_fim.isoformat(),
        "daily": ",".join([
            "precipitation_sum", "temperature_2m_max", "temperature_2m_min",
            "relative_humidity_2m_max", "relative_humidity_2m_min",
            "wind_speed_10m_max", "shortwave_radiation_sum", "et0_fao_evapotranspiration",
        ]),
        "timezone": "America/Sao_Paulo",
    })
    # URL CORRETA: archive-api (não historical-api)
    dados = fetch_json(f"https://archive-api.open-meteo.com/v1/archive?{params}")
    if not dados or "daily" not in dados:
        return None
    d = dados["daily"]
    dias = [{
        "data": d["time"][i],
        "chuva_mm": d["precipitation_sum"][i],
        "temp_max_c": d["temperature_2m_max"][i],
        "temp_min_c": d["temperature_2m_min"][i],
        "umidade_max_pct": d["relative_humidity_2m_max"][i],
        "umidade_min_pct": d["relative_humidity_2m_min"][i],
        "vento_max_kmh": d["wind_speed_10m_max"][i],
        "radiacao_mj_m2": d["shortwave_radiation_sum"][i],
        "evapotranspiracao_mm": d["et0_fao_evapotranspiration"][i],
    } for i in range(len(d["time"]))]
    return {
        "fonte": "Open-Meteo / ERA5",
        "periodo_inicio": data_inicio.isoformat(), "periodo_fim": data_fim.isoformat(),
        "coletado_em": datetime.now().isoformat(), "dias": dias,
    }

# ─────────────────────────────────────────────────────────────────
# HISTÓRICO — INMET com fallback automático entre estações
# ─────────────────────────────────────────────────────────────────

def coletar_historico_inmet(loc):
    """
    Tenta cada código em inmet_estacoes na ordem. Usa a primeira que retornar dados.
    Isso permite fallback automático (ex: A117 Itapiranga → A103 Itacoatiara).
    """
    estacoes = loc.get("inmet_estacoes", [])
    if not estacoes:
        return None

    data_fim    = date.today()
    data_inicio = data_fim - timedelta(days=35)

    for codigo in estacoes:
        url = (f"https://apitempo.inmet.gov.br/estacao/"
               f"{data_inicio.isoformat()}/{data_fim.isoformat()}/{codigo}")
        dados = fetch_json(url)
        if not dados or not isinstance(dados, list) or len(dados) == 0:
            print(f"  [SKIP] INMET {codigo} — sem dados, tentando próxima...")
            continue

        print(f"  [OK] INMET usando estação {codigo} ({len(dados)} leituras)")

        # Agregar leituras horárias por dia
        por_dia = {}
        for obs in dados:
            data_hora = obs.get("DT_MEDICAO", "")
            data_obs  = data_hora[:10] if data_hora else None
            if not data_obs:
                continue
            if data_obs not in por_dia:
                por_dia[data_obs] = {"chuva_mm": 0.0, "temp_max_c": None, "temp_min_c": None,
                                     "umidade_max_pct": None, "umidade_min_pct": None,
                                     "vento_max_kmh": None, "leituras": 0}
            e = por_dia[data_obs]
            e["leituras"] += 1
            chuva = safe_float(obs.get("CHUVA"))
            if chuva is not None:
                e["chuva_mm"] = (e["chuva_mm"] or 0) + chuva
            temp = safe_float(obs.get("TEM_INS"))
            if temp is not None:
                e["temp_max_c"] = max(e["temp_max_c"] or temp, temp)
                e["temp_min_c"] = min(e["temp_min_c"] or temp, temp)
            umid = safe_float(obs.get("UMD_INS"))
            if umid is not None:
                e["umidade_max_pct"] = max(e["umidade_max_pct"] or umid, umid)
                e["umidade_min_pct"] = min(e["umidade_min_pct"] or umid, umid)
            vento = safe_float(obs.get("VEN_VEL"))
            if vento is not None:
                kmh = vento * 3.6
                e["vento_max_kmh"] = max(e["vento_max_kmh"] or kmh, kmh)

        dias = [{"data": k, **v} for k, v in sorted(por_dia.items())]
        return {
            "fonte": f"INMET — estação {codigo}",
            "estacao": codigo,
            "periodo_inicio": data_inicio.isoformat(), "periodo_fim": data_fim.isoformat(),
            "coletado_em": datetime.now().isoformat(), "dias": dias,
        }

    print(f"  [AVISO] Nenhuma estação INMET retornou dados para {loc['nome']}")
    return None

# ─────────────────────────────────────────────────────────────────
# RESUMOS + ANÁLISE DE CONCENTRAÇÃO DE CHUVA
# ─────────────────────────────────────────────────────────────────

def calcular_resumos(dias, fonte=""):
    def agregar(janela):
        if not janela:
            return {}
        chuvas = [d["chuva_mm"] for d in janela if d.get("chuva_mm") is not None]
        t_max  = [d["temp_max_c"] for d in janela if d.get("temp_max_c") is not None]
        t_min  = [d["temp_min_c"] for d in janela if d.get("temp_min_c") is not None]
        umid   = [d.get("umidade_max_pct") for d in janela if d.get("umidade_max_pct") is not None]
        vento  = [d.get("vento_max_kmh") for d in janela if d.get("vento_max_kmh") is not None]
        total  = sum(chuvas)
        intensos = [c for c in chuvas if c > 20]
        return {
            "chuva_total_mm":       round(total, 1) if chuvas else None,
            "dias_com_chuva":       sum(1 for c in chuvas if c > 0.5),
            "dias_chuva_intensa":   len(intensos),
            "pct_chuva_concentrada": round(sum(intensos)/total*100, 1) if total > 0 else 0,
            "temp_max_periodo_c":   round(max(t_max), 1) if t_max else None,
            "temp_min_periodo_c":   round(min(t_min), 1) if t_min else None,
            "temp_max_media_c":     round(sum(t_max)/len(t_max), 1) if t_max else None,
            "umidade_media_pct":    round(sum(umid)/len(umid), 1) if umid else None,
            "vento_max_kmh":        round(max(vento), 1) if vento else None,
        }

    distribuicao = [{"data": d["data"], "chuva_mm": round(d.get("chuva_mm") or 0, 1),
                     "intensa": (d.get("chuva_mm") or 0) > 20,
                     "moderada": 5 < (d.get("chuva_mm") or 0) <= 20,
                     "fraca": 0.5 < (d.get("chuva_mm") or 0) <= 5} for d in dias]
    return {
        "ultimos_7_dias":    agregar(dias[-7:]),
        "ultimos_15_dias":   agregar(dias[-15:]),
        "ultimos_30_dias":   agregar(dias[-30:]),
        "distribuicao_diaria": distribuicao,
        "fonte_historico":   fonte,
    }

def resumo_proxima_semana(prev):
    if not prev or not prev.get("dias"):
        return {}
    janela = prev["dias"][:7]
    chuvas = [d["chuva_mm"] for d in janela if d.get("chuva_mm") is not None]
    t_max  = [d["temp_max_c"] for d in janela if d.get("temp_max_c") is not None]
    return {
        "chuva_total_mm": round(sum(chuvas), 1) if chuvas else None,
        "dias_com_chuva": sum(1 for c in chuvas if c > 0.5),
        "temp_max_c": round(max(t_max), 1) if t_max else None,
        "fonte": prev.get("fonte", ""),
    }

# ─────────────────────────────────────────────────────────────────
# LOOP PRINCIPAL
# ─────────────────────────────────────────────────────────────────

def main():
    agora = datetime.now().isoformat()
    print(f"\n=== Coleta iniciada em {agora} ===\n")

    resumo_geral = {"atualizado_em": agora, "localidades": []}

    for loc in LOCALIDADES:
        print(f"\n── {loc['nome']}, {loc['estado']} ──")
        base = f"docs/data/{loc['id']}"

        print("  Coletando previsão Open-Meteo...")
        prev_om = coletar_previsao_openmeteo(loc)
        if prev_om:
            salvar_json(f"{base}/previsao_openmeteo.json", prev_om)

        print("  Coletando previsão Climatempo...")
        prev_ct = coletar_previsao_climatempo(loc)
        if prev_ct:
            salvar_json(f"{base}/previsao_climatempo.json", prev_ct)

        print("  Coletando histórico ERA5...")
        hist_om = coletar_historico_openmeteo(loc)
        if hist_om:
            salvar_json(f"{base}/historico_era5.json", hist_om)

        # INMET: tenta estações na ordem, usa primeira com dados
        hist_inmet = None
        if loc.get("inmet_estacoes"):
            hist_inmet = coletar_historico_inmet(loc)
            if hist_inmet:
                salvar_json(f"{base}/historico_inmet.json", hist_inmet)

        # Resumos: prefere INMET (observado), usa ERA5 como fallback
        fonte_hist = hist_inmet or hist_om
        if fonte_hist:
            resumos = calcular_resumos(fonte_hist["dias"], fonte_hist.get("fonte", ""))
            resumos.update({
                "localidade": loc["nome"], "estado": loc["estado"],
                "atualizado_em": agora,
                "proxima_semana": resumo_proxima_semana(prev_om),
            })
            salvar_json(f"{base}/resumos.json", resumos)

        # Fontes disponíveis para exibição no dashboard
        fontes = ["Open-Meteo"]
        if prev_ct:
            fontes.append("Climatempo")
        estacao_inmet = hist_inmet.get("estacao") if hist_inmet else None
        if estacao_inmet:
            fontes.append(f"INMET {estacao_inmet}")

        resumo_geral["localidades"].append({
            "id": loc["id"], "nome": loc["nome"], "estado": loc["estado"],
            "lat": loc["lat"], "lon": loc["lon"],
            "tem_inmet": bool(hist_inmet),
            "estacao_inmet": estacao_inmet,
            "tem_climatempo": prev_ct is not None,
            "fontes": fontes,
        })

    salvar_json("docs/data/index.json", resumo_geral)
    print(f"\n=== Coleta concluída. {len(LOCALIDADES)} localidades processadas. ===\n")

if __name__ == "__main__":
    main()
