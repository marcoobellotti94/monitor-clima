"""
Robô de coleta climática — ibel Monitor Climático
Fontes: Open-Meteo (multi-modelo), Climatempo, INMET, ERA5
Roda semanalmente via GitHub Actions → salva JSONs em /docs/data/
"""

import json, os, urllib.request, urllib.parse, math
from datetime import datetime, timedelta, date

# ─────────────────────────────────────────────────────────────────
# LOCALIDADES
# ─────────────────────────────────────────────────────────────────
LOCALIDADES = [
    {
        "id": "tome_acu", "nome": "Tomé-Açu", "estado": "PA",
        "lat": -2.418, "lon": -48.151,
        "inmet_estacoes": ["A213"],
    },
    {
        "id": "salvaterra", "nome": "Salvaterra", "estado": "PA",
        "lat": -0.758, "lon": -48.519,
        "inmet_estacoes": ["A215"],
    },
    {
        "id": "soure", "nome": "Soure", "estado": "PA",
        "lat": -0.716, "lon": -48.521,
        "inmet_estacoes": ["A215"],
    },
    {
        "id": "cachoeira_arari", "nome": "Cachoeira do Arari", "estado": "PA",
        "lat": -1.003, "lon": -48.958,
        "inmet_estacoes": ["A215"],
    },
    {
        "id": "itapiranga", "nome": "Itapiranga", "estado": "AM",
        "lat": -2.746, "lon": -58.026,
        "inmet_estacoes": ["A117", "A103"],
    },
    {
        "id": "sao_raimundo_nonato", "nome": "São Raimundo Nonato", "estado": "PI",
        "lat": -9.012, "lon": -42.697,
        "inmet_estacoes": ["A325"],
    },
    {
        "id": "picos", "nome": "Picos", "estado": "PI",
        "lat": -7.077, "lon": -41.467,
        "inmet_estacoes": ["A341"],
    },
    {
        "id": "altamira", "nome": "Altamira", "estado": "PA",
        "lat": -3.204, "lon": -52.208,
        "inmet_estacoes": ["A253"],
    },
]

# Modelos para comparação de convergência
# ECMWF IFS (melhor global), GFS (americano), ICON (alemão) — todos gratuitos via Open-Meteo
MODELOS_COMPARACAO = [
    # Nomes corretos para o parâmetro models= no endpoint /v1/forecast do Open-Meteo
    # "seamless" combina automaticamente as variantes do modelo (global + regional)
    {"id": "ecmwf_ifs",    "nome": "ECMWF IFS", "param": "ecmwf_ifs"},
    {"id": "gfs",          "nome": "GFS",        "param": "gfs_seamless"},
    {"id": "icon",         "nome": "ICON",       "param": "icon_seamless"},
]

CLIMATEMPO_TOKEN = os.environ.get("CLIMATEMPO_TOKEN", "")

# ─────────────────────────────────────────────────────────────────
# UTILITÁRIOS
# ─────────────────────────────────────────────────────────────────

def fetch_json(url, timeout=30):
    try:
        req = urllib.request.Request(url, headers={"User-Agent": "ibel-robo-clima/1.0"})
        with urllib.request.urlopen(req, timeout=timeout) as resp:
            return json.loads(resp.read().decode("utf-8"))
    except Exception as e:
        print(f"  [ERRO] {url[:80]}... → {e}")
        return None

def salvar_json(caminho, dados):
    os.makedirs(os.path.dirname(caminho), exist_ok=True)
    with open(caminho, "w", encoding="utf-8") as f:
        json.dump(dados, f, ensure_ascii=False, indent=2)
    print(f"  [OK] {caminho}")

def safe_float(v):
    try:
        return float(str(v).replace(",", ".")) if v not in (None, "", "null") else None
    except (TypeError, ValueError):
        return None

# ─────────────────────────────────────────────────────────────────
# PREVISÃO — OPEN-METEO BEST MATCH (fonte principal, 16 dias)
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
    return {"fonte": "Open-Meteo", "coletado_em": datetime.now().isoformat(),
            "horizonte_dias": 16, "dias": dias}

# ─────────────────────────────────────────────────────────────────
# PREVISÃO — MÚLTIPLOS MODELOS (para convergência)
# Coleta ECMWF IFS, GFS e ICON separadamente via Open-Meteo
# ─────────────────────────────────────────────────────────────────

def coletar_modelos_comparacao(loc, prev_ct=None):
    """
    Retorna dicionário com previsão de chuva diária de cada modelo
    para os próximos 10 dias. Usado para calcular convergência.
    Inclui Climatempo como 4ª fonte quando disponível.
    """
    resultados = {}

    for modelo in MODELOS_COMPARACAO:
        params = urllib.parse.urlencode({
            "latitude": loc["lat"], "longitude": loc["lon"],
            "daily": "precipitation_sum,temperature_2m_max,temperature_2m_min",
            "timezone": "America/Sao_Paulo",
            "forecast_days": 10,
            "models": modelo["param"],
        })
        dados = fetch_json(f"https://api.open-meteo.com/v1/forecast?{params}")
        if not dados or "daily" not in dados:
            print(f"  [SKIP] Modelo {modelo['nome']} sem dados")
            continue
        d = dados["daily"]
        dias = [{
            "data": d["time"][i],
            "chuva_mm": d["precipitation_sum"][i],
            "temp_max_c": d["temperature_2m_max"][i],
            "temp_min_c": d["temperature_2m_min"][i],
        } for i in range(len(d["time"]))]
        resultados[modelo["id"]] = {
            "nome": modelo["nome"],
            "dias": dias,
        }
        print(f"  [OK] Modelo {modelo['nome']}: {len(dias)} dias")

    # Adiciona Climatempo como 4ª fonte quando disponível
    if prev_ct and prev_ct.get("dias"):
        ct_dias = []
        for d in prev_ct["dias"][:10]:
            ct_dias.append({
                "data": d.get("data", ""),
                "chuva_mm": d.get("chuva_mm"),
                "temp_max_c": d.get("temp_max_c"),
                "temp_min_c": d.get("temp_min_c"),
            })
        resultados["climatempo"] = {
            "nome": "Climatempo",
            "dias": ct_dias,
        }
        print(f"  [OK] Climatempo adicionado à comparação: {len(ct_dias)} dias")

    if not resultados:
        return None

    # Calcula convergência por dia: média, desvio padrão e classificação
    # Usa o primeiro modelo como referência de datas
    primeira_chave = list(resultados.keys())[0]
    datas = [d["data"] for d in resultados[primeira_chave]["dias"]]

    convergencia = []
    for i, data in enumerate(datas):
        valores_chuva = []
        for mid, mdata in resultados.items():
            if i < len(mdata["dias"]):
                v = mdata["dias"][i].get("chuva_mm")
                if v is not None:
                    valores_chuva.append(v)

        if len(valores_chuva) < 2:
            convergencia.append({"data": data, "media_mm": None, "desvio_mm": None,
                                  "nivel": "insuficiente", "n_modelos": len(valores_chuva)})
            continue

        media = sum(valores_chuva) / len(valores_chuva)
        variancia = sum((v - media) ** 2 for v in valores_chuva) / len(valores_chuva)
        desvio = math.sqrt(variancia)

        # Classifica convergência: boa se desvio < 5mm, moderada < 15mm, ruim acima
        if desvio < 5:
            nivel = "boa"
        elif desvio < 15:
            nivel = "moderada"
        else:
            nivel = "baixa"

        convergencia.append({
            "data": data,
            "media_mm": round(media, 1),
            "desvio_mm": round(desvio, 1),
            "nivel": nivel,
            "n_modelos": len(valores_chuva),
            "valores": {mid: round(resultados[mid]["dias"][i].get("chuva_mm") or 0, 1)
                        for mid in resultados if i < len(resultados[mid]["dias"])},
        })

    return {
        "coletado_em": datetime.now().isoformat(),
        "modelos": {mid: {"nome": mdata["nome"]} for mid, mdata in resultados.items()},
        "dias_por_modelo": resultados,
        "convergencia": convergencia,
    }

# ─────────────────────────────────────────────────────────────────
# PREVISÃO — CLIMATEMPO (validação secundária)
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
            data=data, method="PUT", headers={"User-Agent": "ibel-robo-clima/1.0"}
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
# HISTÓRICO — OPEN-METEO / ERA5 (URL correta: archive-api)
# ─────────────────────────────────────────────────────────────────

def coletar_historico_openmeteo(loc):
    data_fim    = date.today() - timedelta(days=5)
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
# HISTÓRICO — INMET com fallback entre estações
# ─────────────────────────────────────────────────────────────────

def coletar_historico_inmet(loc):
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
        print(f"  [OK] INMET estação {codigo} ({len(dados)} leituras)")

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
            "fonte": f"INMET — estação {codigo}", "estacao": codigo,
            "periodo_inicio": data_inicio.isoformat(), "periodo_fim": data_fim.isoformat(),
            "coletado_em": datetime.now().isoformat(), "dias": dias,
        }

    print(f"  [AVISO] Nenhuma estação INMET disponível para {loc['nome']}")
    return None

# ─────────────────────────────────────────────────────────────────
# RESUMOS + ANÁLISE DE CONCENTRAÇÃO
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
            "chuva_total_mm":        round(total, 1) if chuvas else None,
            "dias_com_chuva":        sum(1 for c in chuvas if c > 0.5),
            "dias_chuva_intensa":    len(intensos),
            # % da chuva total que caiu em dias com >20mm (concentração)
            "pct_chuva_concentrada": round(sum(intensos)/total*100, 1) if total > 0 else 0,
            "temp_max_periodo_c":    round(max(t_max), 1) if t_max else None,
            "temp_min_periodo_c":    round(min(t_min), 1) if t_min else None,
            "temp_max_media_c":      round(sum(t_max)/len(t_max), 1) if t_max else None,
            "umidade_media_pct":     round(sum(umid)/len(umid), 1) if umid else None,
            "vento_max_kmh":         round(max(vento), 1) if vento else None,
        }

    distribuicao = [{"data": d["data"], "chuva_mm": round(d.get("chuva_mm") or 0, 1),
                     "intensa":  (d.get("chuva_mm") or 0) > 20,
                     "moderada": 5 < (d.get("chuva_mm") or 0) <= 20,
                     "fraca":    0.5 < (d.get("chuva_mm") or 0) <= 5} for d in dias]
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
        "chuva_total_mm":  round(sum(chuvas), 1) if chuvas else None,
        "dias_com_chuva":  sum(1 for c in chuvas if c > 0.5),
        "chuva_max_dia":   round(max(chuvas), 1) if chuvas else None,
        "temp_max_c":      round(max(t_max), 1) if t_max else None,
        "fonte":           prev.get("fonte", ""),
    }

# ─────────────────────────────────────────────────────────────────
# LOOP PRINCIPAL
# ─────────────────────────────────────────────────────────────────

def main():
    agora = datetime.now().isoformat()
    print(f"\n=== ibel · Coleta climática iniciada em {agora} ===\n")

    resumo_geral = {"atualizado_em": agora, "localidades": []}

    for loc in LOCALIDADES:
        print(f"\n── {loc['nome']}, {loc['estado']} ──")
        base = f"docs/data/{loc['id']}"

        # Previsão principal (Open-Meteo best match)
        print("  Previsão Open-Meteo...")
        prev_om = coletar_previsao_openmeteo(loc)
        if prev_om:
            salvar_json(f"{base}/previsao_openmeteo.json", prev_om)

        # Climatempo — coletado antes dos modelos para poder incluir na comparação
        print("  Climatempo...")
        prev_ct = coletar_previsao_climatempo(loc)
        if prev_ct:
            salvar_json(f"{base}/previsao_climatempo.json", prev_ct)

        # Comparação de modelos (ECMWF IFS / GFS / ICON / Climatempo)
        print("  Comparação de modelos (ECMWF IFS / GFS / ICON / Climatempo)...")
        modelos = coletar_modelos_comparacao(loc, prev_ct)
        if modelos:
            salvar_json(f"{base}/modelos_comparacao.json", modelos)

        # Histórico ERA5
        print("  Histórico ERA5...")
        hist_om = coletar_historico_openmeteo(loc)
        if hist_om:
            salvar_json(f"{base}/historico_era5.json", hist_om)

        # Histórico INMET (com fallback)
        hist_inmet = None
        if loc.get("inmet_estacoes"):
            hist_inmet = coletar_historico_inmet(loc)
            if hist_inmet:
                salvar_json(f"{base}/historico_inmet.json", hist_inmet)

        # Resumos — prioriza INMET sobre ERA5
        fonte_hist = hist_inmet or hist_om
        if fonte_hist:
            resumos = calcular_resumos(fonte_hist["dias"], fonte_hist.get("fonte", ""))
            resumos.update({
                "localidade": loc["nome"], "estado": loc["estado"],
                "atualizado_em": agora,
                "proxima_semana": resumo_proxima_semana(prev_om),
            })
            salvar_json(f"{base}/resumos.json", resumos)

        # Fontes disponíveis para o índice
        fontes = ["Open-Meteo"]
        if modelos:
            fontes.extend([m["nome"] for m in MODELOS_COMPARACAO
                           if m["id"] in modelos.get("modelos", {})])
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
            "tem_modelos": bool(modelos),
            "fontes": fontes,
        })

    salvar_json("docs/data/index.json", resumo_geral)
    print(f"\n=== Coleta concluída. {len(LOCALIDADES)} localidades. ===\n")

if __name__ == "__main__":
    main()
