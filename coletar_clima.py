"""
Robô de coleta climática — Open-Meteo + Climatempo + INMET
Roda semanalmente via GitHub Actions e salva JSONs em /docs/data/
"""

import json
import os
import urllib.request
import urllib.parse
from datetime import datetime, timedelta, date

# ─────────────────────────────────────────────
# CONFIGURAÇÃO DAS LOCALIDADES
# ─────────────────────────────────────────────
LOCALIDADES = [
    {
        "id": "tome_acu",
        "nome": "Tomé-Açu",
        "estado": "PA",
        "lat": -2.418,
        "lon": -48.151,
        "inmet_estacao": None,          # sem estação INMET confirmada
        "cptec_codigo": None,
    },
    {
        "id": "salvaterra",
        "nome": "Salvaterra",
        "estado": "PA",
        "lat": -0.758,
        "lon": -48.519,
        "inmet_estacao": None,
        "cptec_codigo": None,
    },
    {
        "id": "soure",
        "nome": "Soure",
        "estado": "PA",
        "lat": -0.716,
        "lon": -48.521,
        "inmet_estacao": None,
        "cptec_codigo": None,
    },
    {
        "id": "cachoeira_arari",
        "nome": "Cachoeira do Arari",
        "estado": "PA",
        "lat": -1.003,
        "lon": -48.958,
        "inmet_estacao": None,
        "cptec_codigo": None,
    },
    {
        "id": "itapiranga",
        "nome": "Itapiranga",
        "estado": "AM",
        "lat": -2.746,
        "lon": -58.026,
        "inmet_estacao": None,
        "cptec_codigo": None,
    },
    {
        "id": "sao_raimundo_nonato",
        "nome": "São Raimundo Nonato",
        "estado": "PI",
        "lat": -9.012,
        "lon": -42.697,
        "inmet_estacao": None,
        "cptec_codigo": None,
    },
    {
        "id": "picos",
        "nome": "Picos",
        "estado": "PI",
        "lat": -7.077,
        "lon": -41.467,
        "inmet_estacao": "A341",        # estação INMET confirmada
        "cptec_codigo": None,
    },
    {
        "id": "altamira",
        "nome": "Altamira",
        "estado": "PA",
        "lat": -3.204,
        "lon": -52.208,
        "inmet_estacao": "A253",        # estação INMET confirmada
        "cptec_codigo": None,
    },
]

# Chave Climatempo (plan free — 300 req/dia)
# Obtenha em https://advisor.climatempo.com.br/
CLIMATEMPO_TOKEN = os.environ.get("CLIMATEMPO_TOKEN", "")

# ─────────────────────────────────────────────
# UTILITÁRIOS
# ─────────────────────────────────────────────

def fetch_json(url: str, timeout: int = 30) -> dict | None:
    """Faz requisição HTTP e retorna JSON ou None em caso de erro."""
    try:
        req = urllib.request.Request(url, headers={"User-Agent": "robo-clima/1.0"})
        with urllib.request.urlopen(req, timeout=timeout) as resp:
            return json.loads(resp.read().decode("utf-8"))
    except Exception as e:
        print(f"  [ERRO] {url[:80]}... → {e}")
        return None


def salvar_json(caminho: str, dados: dict):
    os.makedirs(os.path.dirname(caminho), exist_ok=True)
    with open(caminho, "w", encoding="utf-8") as f:
        json.dump(dados, f, ensure_ascii=False, indent=2)
    print(f"  [OK] Salvo: {caminho}")


# ─────────────────────────────────────────────
# COLETA — OPEN-METEO PREVISÃO (16 dias)
# ─────────────────────────────────────────────

def coletar_previsao_openmeteo(loc: dict) -> dict | None:
    """
    Retorna previsão diária para os próximos 16 dias via Open-Meteo.
    Variáveis: precipitação, temp. máx/mín, umidade, vento, radiação solar.
    """
    params = urllib.parse.urlencode({
        "latitude": loc["lat"],
        "longitude": loc["lon"],
        "daily": ",".join([
            "precipitation_sum",
            "precipitation_probability_max",
            "temperature_2m_max",
            "temperature_2m_min",
            "relative_humidity_2m_max",
            "relative_humidity_2m_min",
            "wind_speed_10m_max",
            "wind_gusts_10m_max",
            "shortwave_radiation_sum",
            "et0_fao_evapotranspiration",
        ]),
        "timezone": "America/Sao_Paulo",
        "forecast_days": 16,
    })
    url = f"https://api.open-meteo.com/v1/forecast?{params}"
    dados = fetch_json(url)
    if not dados or "daily" not in dados:
        return None

    d = dados["daily"]
    dias = []
    for i, data in enumerate(d.get("time", [])):
        dias.append({
            "data": data,
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
        })

    return {
        "fonte": "Open-Meteo",
        "coletado_em": datetime.now().isoformat(),
        "horizonte_dias": 16,
        "dias": dias,
    }


# ─────────────────────────────────────────────
# COLETA — CLIMATEMPO PREVISÃO (15 dias)
# ─────────────────────────────────────────────

def coletar_previsao_climatempo(loc: dict) -> dict | None:
    """
    Coleta previsão via Climatempo Advisor (requer token).
    Etapa 1: busca ID da localidade. Etapa 2: registra. Etapa 3: busca previsão.
    Retorna None se token não configurado.
    """
    if not CLIMATEMPO_TOKEN:
        print("  [SKIP] CLIMATEMPO_TOKEN não configurado")
        return None

    # Etapa 1: busca ID da cidade
    url_busca = (
        f"http://apiadvisor.climatempo.com.br/api/v1/locale/city"
        f"?name={urllib.parse.quote(loc['nome'])}"
        f"&state={loc['estado']}"
        f"&token={CLIMATEMPO_TOKEN}"
    )
    resultado = fetch_json(url_busca)
    if not resultado or not isinstance(resultado, list) or len(resultado) == 0:
        print(f"  [SKIP] Climatempo: cidade não encontrada para {loc['nome']}")
        return None

    cidade_id = resultado[0].get("id")
    if not cidade_id:
        return None

    # Etapa 2: registrar localidade (necessário antes de consultar previsão)
    url_reg = (
        f"http://apiadvisor.climatempo.com.br/api-manager/user-token/"
        f"{CLIMATEMPO_TOKEN}/locales"
    )
    try:
        data = urllib.parse.urlencode({"localeId[]": cidade_id}).encode("utf-8")
        req = urllib.request.Request(url_reg, data=data, method="PUT",
                                     headers={"User-Agent": "robo-clima/1.0"})
        with urllib.request.urlopen(req, timeout=15):
            pass
    except Exception as e:
        print(f"  [AVISO] Climatempo registro: {e}")

    # Etapa 3: buscar previsão 15 dias
    url_prev = (
        f"http://apiadvisor.climatempo.com.br/api/v1/forecast/locale/"
        f"{cidade_id}/days/15?token={CLIMATEMPO_TOKEN}"
    )
    dados = fetch_json(url_prev)
    if not dados or "data" not in dados:
        return None

    dias = []
    for d in dados["data"]:
        rain = d.get("rain", {})
        temp = d.get("temperature", {})
        humidity = d.get("humidity", {})
        wind = d.get("wind", {})
        dias.append({
            "data": d.get("date_br", d.get("date", "")),
            "chuva_mm": rain.get("precipitation", None),
            "prob_chuva_pct": rain.get("probability", None),
            "temp_max_c": temp.get("max", None),
            "temp_min_c": temp.get("min", None),
            "umidade_max_pct": humidity.get("max", None),
            "umidade_min_pct": humidity.get("min", None),
            "vento_max_kmh": wind.get("velocity_max", None),
            "direcao_vento": wind.get("direction", None),
            "condicao": d.get("text_icon", {}).get("text", {}).get("pt", None),
        })

    return {
        "fonte": "Climatempo",
        "cidade_id": cidade_id,
        "coletado_em": datetime.now().isoformat(),
        "horizonte_dias": 15,
        "dias": dias,
    }


# ─────────────────────────────────────────────
# COLETA — OPEN-METEO HISTÓRICO (ERA5)
# ─────────────────────────────────────────────

def coletar_historico_openmeteo(loc: dict) -> dict | None:
    """
    Coleta histórico dos últimos 35 dias via Open-Meteo Historical API (ERA5).
    Cobre: semana passada, 15 dias e mês.
    """
    data_fim = date.today() - timedelta(days=5)   # ERA5 tem ~5 dias de atraso
    data_inicio = data_fim - timedelta(days=35)

    params = urllib.parse.urlencode({
        "latitude": loc["lat"],
        "longitude": loc["lon"],
        "start_date": data_inicio.isoformat(),
        "end_date": data_fim.isoformat(),
        "daily": ",".join([
            "precipitation_sum",
            "temperature_2m_max",
            "temperature_2m_min",
            "relative_humidity_2m_max",
            "relative_humidity_2m_min",
            "wind_speed_10m_max",
            "shortwave_radiation_sum",
            "et0_fao_evapotranspiration",
        ]),
        "timezone": "America/Sao_Paulo",
        "models": "era5",
    })
    url = f"https://historical-api.open-meteo.com/v1/archive?{params}"
    dados = fetch_json(url)
    if not dados or "daily" not in dados:
        return None

    d = dados["daily"]
    dias = []
    for i, data in enumerate(d.get("time", [])):
        dias.append({
            "data": data,
            "chuva_mm": d["precipitation_sum"][i],
            "temp_max_c": d["temperature_2m_max"][i],
            "temp_min_c": d["temperature_2m_min"][i],
            "umidade_max_pct": d["relative_humidity_2m_max"][i],
            "umidade_min_pct": d["relative_humidity_2m_min"][i],
            "vento_max_kmh": d["wind_speed_10m_max"][i],
            "radiacao_mj_m2": d["shortwave_radiation_sum"][i],
            "evapotranspiracao_mm": d["et0_fao_evapotranspiration"][i],
        })

    return {
        "fonte": "Open-Meteo Historical (ERA5)",
        "periodo_inicio": data_inicio.isoformat(),
        "periodo_fim": data_fim.isoformat(),
        "coletado_em": datetime.now().isoformat(),
        "dias": dias,
    }


# ─────────────────────────────────────────────
# COLETA — INMET (estações físicas, 90 dias)
# ─────────────────────────────────────────────

def coletar_historico_inmet(loc: dict) -> dict | None:
    """
    Coleta dados observados da estação INMET (onde disponível).
    Retorna últimos 35 dias de observações horárias agregadas por dia.
    """
    if not loc.get("inmet_estacao"):
        return None

    codigo = loc["inmet_estacao"]
    data_fim = date.today()
    data_inicio = data_fim - timedelta(days=35)

    url = (
        f"https://apitempo.inmet.gov.br/estacao/"
        f"{data_inicio.isoformat()}/{data_fim.isoformat()}/{codigo}"
    )
    dados = fetch_json(url)
    if not dados or not isinstance(dados, list):
        return None

    # Agregar leituras horárias por dia
    por_dia: dict[str, dict] = {}
    for obs in dados:
        data_hora = obs.get("DT_MEDICAO", "")
        data_obs = data_hora[:10] if data_hora else None
        if not data_obs:
            continue

        if data_obs not in por_dia:
            por_dia[data_obs] = {
                "chuva_mm": 0.0,
                "temp_max_c": None,
                "temp_min_c": None,
                "umidade_max_pct": None,
                "umidade_min_pct": None,
                "vento_max_kmh": None,
                "leituras": 0,
            }

        entrada = por_dia[data_obs]
        entrada["leituras"] += 1

        def safe_float(v):
            try:
                return float(v) if v not in (None, "", "null") else None
            except (TypeError, ValueError):
                return None

        chuva = safe_float(obs.get("CHUVA"))
        if chuva is not None:
            entrada["chuva_mm"] = (entrada["chuva_mm"] or 0) + chuva

        temp = safe_float(obs.get("TEM_INS"))
        if temp is not None:
            entrada["temp_max_c"] = max(entrada["temp_max_c"] or temp, temp)
            entrada["temp_min_c"] = min(entrada["temp_min_c"] or temp, temp)

        umid = safe_float(obs.get("UMD_INS"))
        if umid is not None:
            entrada["umidade_max_pct"] = max(entrada["umidade_max_pct"] or umid, umid)
            entrada["umidade_min_pct"] = min(entrada["umidade_min_pct"] or umid, umid)

        vento = safe_float(obs.get("VEN_VEL"))
        if vento is not None:
            kmh = vento * 3.6
            entrada["vento_max_kmh"] = max(entrada["vento_max_kmh"] or kmh, kmh)

    dias = [{"data": k, **v} for k, v in sorted(por_dia.items())]
    return {
        "fonte": f"INMET — estação {codigo}",
        "estacao": codigo,
        "periodo_inicio": data_inicio.isoformat(),
        "periodo_fim": data_fim.isoformat(),
        "coletado_em": datetime.now().isoformat(),
        "dias": dias,
    }


# ─────────────────────────────────────────────
# RESUMOS (semana / 15 dias / mês)
# ─────────────────────────────────────────────

def calcular_resumos(dias: list[dict]) -> dict:
    """Agrega totais e médias para 7, 15 e 30 dias a partir de lista de dias."""

    def agregar(janela: list[dict]) -> dict:
        if not janela:
            return {}
        chuvas = [d["chuva_mm"] for d in janela if d.get("chuva_mm") is not None]
        t_max = [d["temp_max_c"] for d in janela if d.get("temp_max_c") is not None]
        t_min = [d["temp_min_c"] for d in janela if d.get("temp_min_c") is not None]
        umid = [d.get("umidade_max_pct") for d in janela if d.get("umidade_max_pct") is not None]
        vento = [d.get("vento_max_kmh") for d in janela if d.get("vento_max_kmh") is not None]
        return {
            "chuva_total_mm": round(sum(chuvas), 1) if chuvas else None,
            "dias_com_chuva": sum(1 for c in chuvas if c > 0.5),
            "temp_max_periodo_c": round(max(t_max), 1) if t_max else None,
            "temp_min_periodo_c": round(min(t_min), 1) if t_min else None,
            "temp_max_media_c": round(sum(t_max) / len(t_max), 1) if t_max else None,
            "umidade_media_pct": round(sum(umid) / len(umid), 1) if umid else None,
            "vento_max_kmh": round(max(vento), 1) if vento else None,
        }

    return {
        "ultimos_7_dias": agregar(dias[-7:]),
        "ultimos_15_dias": agregar(dias[-15:]),
        "ultimos_30_dias": agregar(dias[-30:]),
    }


# ─────────────────────────────────────────────
# LOOP PRINCIPAL
# ─────────────────────────────────────────────

def main():
    agora = datetime.now().isoformat()
    print(f"\n=== Coleta iniciada em {agora} ===\n")

    resumo_geral = {
        "atualizado_em": agora,
        "localidades": [],
    }

    for loc in LOCALIDADES:
        print(f"\n── {loc['nome']}, {loc['estado']} ──")
        base = f"docs/data/{loc['id']}"

        # — Previsão Open-Meteo
        print("  Coletando previsão Open-Meteo...")
        prev_om = coletar_previsao_openmeteo(loc)
        if prev_om:
            salvar_json(f"{base}/previsao_openmeteo.json", prev_om)

        # — Previsão Climatempo
        print("  Coletando previsão Climatempo...")
        prev_ct = coletar_previsao_climatempo(loc)
        if prev_ct:
            salvar_json(f"{base}/previsao_climatempo.json", prev_ct)

        # — Histórico Open-Meteo/ERA5
        print("  Coletando histórico ERA5...")
        hist_om = coletar_historico_openmeteo(loc)
        if hist_om:
            salvar_json(f"{base}/historico_era5.json", hist_om)
            resumos = calcular_resumos(hist_om["dias"])
            salvar_json(f"{base}/resumos.json", {
                "localidade": loc["nome"],
                "estado": loc["estado"],
                "atualizado_em": agora,
                **resumos,
            })

        # — Histórico INMET (onde disponível)
        if loc.get("inmet_estacao"):
            print(f"  Coletando histórico INMET ({loc['inmet_estacao']})...")
            hist_inmet = coletar_historico_inmet(loc)
            if hist_inmet:
                salvar_json(f"{base}/historico_inmet.json", hist_inmet)

        # Registro para índice geral
        resumo_geral["localidades"].append({
            "id": loc["id"],
            "nome": loc["nome"],
            "estado": loc["estado"],
            "lat": loc["lat"],
            "lon": loc["lon"],
            "tem_inmet": bool(loc.get("inmet_estacao")),
            "tem_climatempo": prev_ct is not None,
        })

    # Índice geral — lido pelo dashboard
    salvar_json("docs/data/index.json", resumo_geral)
    print(f"\n=== Coleta concluída. {len(LOCALIDADES)} localidades processadas. ===\n")


if __name__ == "__main__":
    main()
