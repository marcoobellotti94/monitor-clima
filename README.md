# Monitor Climático

Sistema automático de coleta e visualização de dados climáticos para regiões específicas do Brasil.

## O que o sistema faz

- **Toda segunda-feira**, o robô coleta automaticamente dados de:
  - **Open-Meteo** — previsão 16 dias + histórico ERA5
  - **Climatempo** — previsão 15 dias (validação cruzada)
  - **INMET** — dados observados onde há estação física
- Gera um **dashboard web** acessível por link, com previsão e histórico por localidade
- Tudo roda na nuvem, gratuitamente, via GitHub

---

## Configuração inicial (passo a passo)

### 1. Criar conta no GitHub

Acesse https://github.com e crie uma conta gratuita se ainda não tiver.

### 2. Criar o repositório

1. Clique em **New repository**
2. Nome sugerido: `monitor-clima`
3. Marque **Public** (necessário para o GitHub Pages gratuito)
4. Clique em **Create repository**

### 3. Subir os arquivos

Faça upload dos seguintes arquivos para o repositório:

```
monitor-clima/
├── coletar_clima.py          ← robô de coleta
├── .github/
│   └── workflows/
│       └── coletar.yml       ← agendador automático
└── docs/
    ├── index.html            ← página inicial do dashboard
    └── localidade.html       ← página de detalhe por localidade
```

> **Dica**: Na interface do GitHub, clique em "Add file → Upload files" para cada pasta.

### 4. Ativar o GitHub Pages

1. Vá em **Settings → Pages**
2. Em "Source", selecione **Deploy from a branch**
3. Branch: `main` · Pasta: `/docs`
4. Clique em **Save**

Após alguns minutos, o dashboard estará disponível em:
`https://SEU-USUARIO.github.io/monitor-clima/`

### 5. Configurar o token do Climatempo (opcional)

Se quiser usar a segunda fonte de previsão (Climatempo):

1. Crie conta gratuita em https://advisor.climatempo.com.br
2. Copie seu token de acesso
3. No repositório, vá em **Settings → Secrets and variables → Actions**
4. Clique em **New repository secret**
5. Nome: `CLIMATEMPO_TOKEN` · Valor: seu token
6. Clique em **Add secret**

> Se não configurar, o sistema usa apenas Open-Meteo (já excelente cobertura).

### 6. Rodar o robô pela primeira vez

1. Vá em **Actions** no repositório
2. Clique em **Coleta Climática Semanal**
3. Clique em **Run workflow → Run workflow**

Aguarde ~2 minutos. Os dados aparecerão na pasta `docs/data/` e o dashboard será atualizado.

---

## Localidades configuradas

| Localidade | Estado | Estação INMET |
|---|---|---|
| Tomé-Açu | PA | — |
| Salvaterra | PA | — |
| Soure | PA | — |
| Cachoeira do Arari | PA | — |
| Itapiranga | AM | — |
| São Raimundo Nonato | PI | — |
| Picos | PI | A341 |
| Altamira | PA | A253 |

### Adicionar ou remover localidades

Edite o arquivo `coletar_clima.py`, seção `LOCALIDADES`:

```python
{
    "id": "nome_sem_acento",       # identificador único (sem espaços)
    "nome": "Nome da Cidade",      # nome exibido no dashboard
    "estado": "XX",                # sigla do estado
    "lat": -0.000,                 # latitude (negativo = sul)
    "lon": -00.000,                # longitude (negativo = oeste)
    "inmet_estacao": None,         # código INMET (ex: "A253") ou None
    "cptec_codigo": None,          # reservado para uso futuro
},
```

Para encontrar o código da estação INMET: https://mapas.inmet.gov.br

---

## Fontes de dados

| Fonte | Uso | Custo |
|---|---|---|
| Open-Meteo | Previsão 16 dias · Histórico ERA5 | Gratuito |
| Climatempo | Previsão 15 dias (validação) | Gratuito (300 req/dia) |
| INMET | Dados observados onde há estação | Gratuito |
| ERA5 / ECMWF | Histórico climático desde 1940 | Gratuito via Open-Meteo |

**Custo total da infraestrutura: R$ 0,00**

---

## Solução de problemas

**O dashboard abre mas não mostra dados**
→ O robô ainda não rodou. Vá em Actions e execute manualmente (passo 6).

**Erro no Actions: "Permission denied"**
→ Vá em Settings → Actions → General → Workflow permissions → marque "Read and write permissions".

**Climatempo retorna erro 401**
→ Verifique se o token foi salvo corretamente em Secrets (passo 5).

**Dados do INMET aparecem como "—"**
→ A estação pode estar temporariamente offline. O sistema usa ERA5 como fallback automaticamente.
