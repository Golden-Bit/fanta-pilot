# -*- coding: utf-8 -*-
# asta_manager.py — Console Live (coerente col Modello 4) con:
# - Identità iniziale Statico (PrezzoRicalcolato) = Dinamico (PrezzoEquoDyn)
# - Parametri per governare l'impatto dell'evoluzione (scarcity/temperatura/mix)
# - Aggiunta giocatori fuori lista (default listino=1)
# - Tabelle leggibili in dark mode (testo nero su bianco)
#
# Requisiti: streamlit, pandas, numpy, openpyxl/xlsxwriter

import streamlit as st
import pandas as pd
import numpy as np
import json
from io import BytesIO
from typing import Dict, Any, Tuple, Optional
import time

# --------------------------
# Costanti e utility
# --------------------------

ROLE_SHEETS = {"P": "Portieri", "D": "Difensori", "C": "Centrocampisti", "A": "Attaccanti"}
ROLE_ORDER = ["P", "D", "C", "A"]
TIERS = ["Top", "High", "Medium", "Low"]

# Colonne minime richieste nello sheet "Tutti"
REQ_COLS_MIN = ["Nome", "Squadra", "Ruolo", "Tier", "Peso", "PrezzoRicalcolato"]

# Default appetibilità fasce
DEFAULT_TIER_WEIGHTS = {"Top": 3.0, "High": 2.0, "Medium": 1.2, "Low": 0.8}

# Parametri di sicurezza / estetica
TEMP_MIN, TEMP_MAX = 0.70, 1.30       # clip della temperatura

# --------------------------
# Stato & inizializzazione
# --------------------------

def _init_state():
    ss = st.session_state
    # dati principali
    ss.setdefault("listone", None)              # dict sheet_name -> DataFrame
    ss.setdefault("teams", None)                # dict team_id -> {name,budget,slots,players}
    ss.setdefault("config", {})                 # json completo caricato
    ss.setdefault("my_team_id", None)
    # dinamica & log
    ss.setdefault("log", [])
    ss.setdefault("role_temp", {r: 1.0 for r in ROLE_ORDER})
    ss.setdefault("global_sales_count", 0)
    # parametri console (default scelti per IDENTITÀ INIZIALE = 0 impatto dinamiche)
    ss.setdefault("alpha_role", 0.0)
    ss.setdefault("alpha_tier", 0.0)
    ss.setdefault("ema_beta", 0.0)
    ss.setdefault("tier_weights", DEFAULT_TIER_WEIGHTS.copy())
    ss.setdefault("dyn_floor", 1.0)             # floor minimo del prezzo live
    ss.setdefault("impact_mix", 0.0)            # 0=perfetto statico, 1=full dinamico
    # baseline statiche per clearing a inizio asta
    ss.setdefault("baseline_B_role", {r: 0.0 for r in ROLE_ORDER})
    # undo/salvataggi
    ss.setdefault("last_snapshot", None)
    ss.setdefault("autosave_json_path", "")
    ss.setdefault("autosave_xlsx_path", "")
    ss.setdefault("loaded_state_name", None)

def _coalesce(*args):
    for x in args:
        if x is not None and not (isinstance(x, float) and np.isnan(x)):
            return x
    return None

def _take_snapshot():
    ss = st.session_state
    if ss.listone is not None and ss.teams is not None:
        snap = {
            "listone": {k: v.copy(deep=True) for k, v in ss.listone.items()},
            "teams": json.loads(json.dumps(ss.teams)),
            "log": list(ss.log),
            "role_temp": dict(ss.role_temp),
            "global_sales_count": int(ss.global_sales_count),
            "alpha_role": float(ss.alpha_role),
            "alpha_tier": float(ss.alpha_tier),
            "ema_beta": float(ss.ema_beta),
            "tier_weights": dict(ss.tier_weights),
            "dyn_floor": float(ss.dyn_floor),
            "impact_mix": float(ss.impact_mix),
            "baseline_B_role": dict(ss.baseline_B_role),
        }
        ss.last_snapshot = snap

def _undo():
    ss = st.session_state
    if not ss.last_snapshot:
        st.warning("Nessuno stato precedente disponibile per UNDO.")
        return
    snap = ss.last_snapshot
    ss.listone = {k: v.copy(deep=True) for k, v in snap["listone"].items()}
    ss.teams = json.loads(json.dumps(snap["teams"]))
    ss.log = list(snap["log"])
    ss.role_temp = dict(snap["role_temp"])
    ss.global_sales_count = int(snap["global_sales_count"])
    ss.alpha_role = float(snap["alpha_role"])
    ss.alpha_tier = float(snap["alpha_tier"])
    ss.ema_beta = float(snap["ema_beta"])
    ss.tier_weights = dict(snap["tier_weights"])
    ss.dyn_floor = float(snap["dyn_floor"])
    ss.impact_mix = float(snap["impact_mix"])
    ss.baseline_B_role = dict(snap["baseline_B_role"])
    st.success("Ripristinato ultimo stato (UNDO).")
    _autosave_if_needed()

# --------------------------
# Caricamento / Salvataggio
# --------------------------

def _ensure_core_cols(df: pd.DataFrame) -> pd.DataFrame:
    # colonne di gestione asta
    for c in ["Acquistato", "PrezzoFinale", "Team"]:
        if c not in df.columns:
            df[c] = np.nan if c != "Acquistato" else False
    # ID univoco (se manca, generato sequenziale)
    if "PlayerID" not in df.columns:
        df["PlayerID"] = df.get("Id", pd.Series(range(1, len(df) + 1)))
    # colonne numeriche base
    df["Peso"] = pd.to_numeric(df.get("Peso", 1.0), errors="coerce").fillna(1.0)
    df["PrezzoRicalcolato"] = pd.to_numeric(df.get("PrezzoRicalcolato", 1.0), errors="coerce").fillna(1.0)
    # PrezzoEquoDyn iniziale = Statico (identità a inizio asta)
    df["PrezzoEquoDyn"] = pd.to_numeric(df.get("PrezzoEquoDyn", df["PrezzoRicalcolato"]), errors="coerce").fillna(1.0)
    # tipologia tier
    df["Tier"] = df.get("Tier", "Medium").fillna("Medium").astype(str)
    return df

def load_listone_xlsx(file) -> Dict[str, pd.DataFrame]:
    data = pd.read_excel(file, sheet_name=None)
    for name, df in data.items():
        # correzione header in riga 0
        if 0 in df.index and "Nome" not in df.columns and "Nome" in df.iloc[0].values:
            df.columns = df.iloc[0]
            df = df.drop(df.index[0]).reset_index(drop=True)
        data[name] = _ensure_core_cols(df.copy())
    ok, msg = _validate_listone(data)
    if not ok:
        raise ValueError(msg)
    return data

def _validate_listone(listone: Dict[str, pd.DataFrame]) -> Tuple[bool, str]:
    if "Tutti" not in listone:
        return False, "Nel file listone manca lo sheet 'Tutti'."
    missing = [c for c in REQ_COLS_MIN if c not in listone["Tutti"].columns]
    if missing:
        return False, f"Nel foglio 'Tutti' mancano le colonne: {missing}"
    return True, ""

def load_teams_json(file) -> Tuple[Dict[str, Any], Dict[str, Any]]:
    conf = json.load(file)
    teams = {
        t["id"]: {
            "name": t["name"],
            "budget": float(conf["budget_per_team"]),
            "slots": dict(conf["slots_per_role"]),  # {'P':x, 'D':y, 'C':z, 'A':k}
            "players": [],
        }
        for t in conf["teams"]
    }
    return conf, teams

def export_state_xlsx() -> BytesIO:
    ss = st.session_state
    buffer = BytesIO()
    with pd.ExcelWriter(buffer, engine="xlsxwriter") as writer:
        ss.listone["Tutti"].to_excel(writer, sheet_name="Tutti", index=False)
        for r, sheet in ROLE_SHEETS.items():
            if sheet in ss.listone:
                ss.listone[sheet].to_excel(writer, sheet_name=sheet, index=False)
        if ss.log:
            pd.DataFrame(ss.log).to_excel(writer, sheet_name="Log", index=False)
        pd.DataFrame([{"role": r, "temperature": t} for r, t in ss.role_temp.items()]).to_excel(writer, sheet_name="Temperature", index=False)
        meta = {"global_sales_count": [st.session_state.global_sales_count]}
        pd.DataFrame(meta).to_excel(writer, sheet_name="Meta", index=False)
    buffer.seek(0)
    return buffer

def _bundle_state() -> Dict[str, Any]:
    ss = st.session_state
    tutti_records = ss.listone["Tutti"].to_dict(orient="records") if ss.listone else []
    return {
        "config": ss.config,
        "teams": ss.teams,
        "log": ss.log,
        "role_temp": ss.role_temp,
        "global_sales_count": ss.global_sales_count,
        "listone_tutti": tutti_records,
        "alpha_role": ss.alpha_role,
        "alpha_tier": ss.alpha_tier,
        "ema_beta": ss.ema_beta,
        "tier_weights": ss.tier_weights,
        "dyn_floor": ss.dyn_floor,
        "impact_mix": ss.impact_mix,
        "baseline_B_role": ss.baseline_B_role,
        "ts": time.time(),
    }

def _restore_from_bundle(bundle: Dict[str, Any], per_role_from_tutti: bool = True):
    ss = st.session_state
    ss.config = bundle.get("config", {})
    ss.teams = bundle.get("teams", {})
    ss.log = bundle.get("log", [])
    ss.role_temp = bundle.get("role_temp", {r: 1.0 for r in ROLE_ORDER})
    ss.global_sales_count = int(bundle.get("global_sales_count", 0))
    ss.alpha_role = float(bundle.get("alpha_role", 0.0))
    ss.alpha_tier = float(bundle.get("alpha_tier", 0.0))
    ss.ema_beta = float(bundle.get("ema_beta", 0.0))
    ss.tier_weights = dict(bundle.get("tier_weights", DEFAULT_TIER_WEIGHTS))
    ss.dyn_floor = float(bundle.get("dyn_floor", 1.0))
    ss.impact_mix = float(bundle.get("impact_mix", 0.0))
    ss.baseline_B_role = dict(bundle.get("baseline_B_role", {r: 0.0 for r in ROLE_ORDER}))
    # listone
    df_tutti = pd.DataFrame(bundle.get("listone_tutti", []))
    if df_tutti.empty:
        st.error("Il pacchetto di stato non contiene il foglio 'Tutti'.")
        return
    df_tutti = _ensure_core_cols(df_tutti)
    listone = {"Tutti": df_tutti}
    if per_role_from_tutti:
        for r, sh in ROLE_SHEETS.items():
            listone[sh] = df_tutti[df_tutti["Ruolo"] == r].copy()
    ss.listone = listone

def _autosave_if_needed():
    ss = st.session_state
    try:
        if ss.autosave_json_path:
            bundle = _bundle_state()
            with open(ss.autosave_json_path, "w", encoding="utf-8") as f:
                json.dump(bundle, f, ensure_ascii=False)
        if ss.autosave_xlsx_path:
            buf = export_state_xlsx()
            with open(ss.autosave_xlsx_path, "wb") as f:
                f.write(buf.getvalue())
    except Exception as e:
        st.warning(f"Autosalvataggio fallito ({e}).")

# --------------------------
# Dinamica prezzi (nuovo schema con IMPACT MIX)
# --------------------------

def _role_budget_residual(teams: Dict[str, Any], ruolo: str) -> float:
    """
    Budget residuo allocabile al ruolo r, proporzionale agli slot residui di ogni squadra.
    """
    B_r = 0.0
    for t in teams.values():
        slots_tot = sum(max(0, v) for v in t["slots"].values())
        if slots_tot <= 0:
            continue
        share = max(0, t["slots"].get(ruolo, 0)) / slots_tot
        B_r += max(0.0, float(t["budget"])) * share
    return float(B_r)

def _tier_scarcity_multiplier(df_role_free: pd.DataFrame, slots_r: int, tier_weights: Dict[str, float], alpha_tier: float) -> Dict[str, float]:
    """
    m_{r,t} = ( D_{r,t} / max(S_{r,t}, 1) ) ** alpha_tier
    con D_{r,t} proporzionale a tier_weights e agli slots_r.
    """
    a = np.array([tier_weights.get(t, 1.0) for t in TIERS], dtype=float)
    a = np.where(a <= 0, 1e-9, a)
    q = a / a.sum()
    D = {TIERS[i]: q[i] * max(0, slots_r) for i in range(len(TIERS))}
    S = {t: int((df_role_free["Tier"] == t).sum()) for t in TIERS}
    m = {t: (D[t] / max(S[t], 1)) ** float(alpha_tier) for t in TIERS}
    return m

def _apply_decay_to_temperature(role_temp: Dict[str, float], factor: float = 0.05) -> Dict[str, float]:
    """
    Decadimento morbido verso 1.0: temp <- temp + factor*(1-temp)
    """
    out = {}
    for r, v in role_temp.items():
        v = float(v)
        v = v + factor * (1.0 - v)
        out[r] = float(np.clip(v, TEMP_MIN, TEMP_MAX))
    return out

def _compute_baseline_clearing_from_static(df_all: pd.DataFrame) -> Dict[str, float]:
    """
    Calcola la baseline di clearing per ruolo come somma dei PrezzoRicalcolato (solo liberi).
    Questo garantisce IDENTITÀ iniziale: somma dinamica per ruolo == somma statica.
    """
    base = {}
    free = ~df_all["Acquistato"].fillna(False)
    for r in ROLE_ORDER:
        m = (df_all["Ruolo"] == r) & free
        base[r] = float(pd.to_numeric(df_all.loc[m, "PrezzoRicalcolato"], errors="coerce").fillna(0.0).sum())
    return base

def _compute_clearing_mixed(df_all: pd.DataFrame, teams: Dict[str, Any], baseline_B_role: Dict[str, float], impact_mix: float) -> Dict[str, float]:
    """
    Clearing per ruolo con miscela:
      B_mix[r] = (1-impact_mix) * BaselineStatic[r]  +  impact_mix * ResidualBudget[r]
    - impact_mix = 0.0 -> identità perfetta iniziale
    - impact_mix = 1.0 -> completamente guidato dal budget residuo
    """
    impact_mix = float(np.clip(impact_mix, 0.0, 1.0))
    out = {}
    for r in ROLE_ORDER:
        B_res = _role_budget_residual(teams, r)
        B_base = float(baseline_B_role.get(r, 0.0))
        out[r] = (1.0 - impact_mix) * B_base + impact_mix * B_res
    return out

def recalc_prices_dynamic(listone: Dict[str, pd.DataFrame],
                          teams: Dict[str, Any],
                          alpha_role: float,
                          alpha_tier: float,
                          tier_weights: Dict[str, float],
                          role_temp: Dict[str, float],
                          baseline_B_role: Dict[str, float],
                          impact_mix: float,
                          dyn_floor: float) -> Dict[str, pd.DataFrame]:
    """
    PrezzoEquoDyn = (Clearing_misto per ruolo) * (Peso / somma_pesi_ruolo)
                    * (Scarsità ruolo)^alpha_role * (Scarsità fascia)^alpha_tier * (Temperatura ruolo)
    poi applica un floor (>= dyn_floor).
    """
    df_all = listone["Tutti"].copy()

    # garantisci tipi
    df_all["Acquistato"] = df_all["Acquistato"].fillna(False).astype(bool)
    df_all["Peso"] = pd.to_numeric(df_all["Peso"], errors="coerce").fillna(1.0)
    df_all["PrezzoRicalcolato"] = pd.to_numeric(df_all["PrezzoRicalcolato"], errors="coerce").fillna(1.0)
    df_all["PrezzoEquoDyn"] = pd.to_numeric(df_all.get("PrezzoEquoDyn", df_all["PrezzoRicalcolato"]), errors="coerce").fillna(1.0)

    # clearing misto tra baseline statico e residuo
    B_mix = _compute_clearing_mixed(df_all, teams, baseline_B_role, impact_mix)

    for r in ROLE_ORDER:
        role_mask = (df_all["Ruolo"] == r)
        free_mask = role_mask & (~df_all["Acquistato"])
        if free_mask.sum() == 0:
            continue

        slots_r = sum(max(0, t["slots"].get(r, 0)) for t in teams.values())
        players_r = int(free_mask.sum())

        # scarsità ruolo
        scarcity_role = (slots_r / max(players_r, 1)) ** float(alpha_role)

        # scarsità fascia
        df_role_free = df_all.loc[free_mask, ["Tier", "Peso"]].copy()
        tier_mult = _tier_scarcity_multiplier(df_role_free, slots_r, tier_weights, alpha_tier)

        # temperatura
        temp = float(np.clip(float(role_temp.get(r, 1.0)), TEMP_MIN, TEMP_MAX))

        # pesi relativi
        w = df_all.loc[free_mask, "Peso"].clip(lower=1e-9).astype(float)
        wsum = w.sum()
        if wsum <= 0:
            base_prices = np.full(int(free_mask.sum()), B_mix[r] / max(int(free_mask.sum()), 1))
        else:
            base_prices = B_mix[r] * (w / wsum)

        tiers_series = df_all.loc[free_mask, "Tier"].astype(str).map(lambda t: tier_mult.get(t, 1.0)).astype(float)
        dyn_prices = base_prices * scarcity_role * tiers_series.values * temp
        dyn_prices = np.maximum(dyn_prices, float(dyn_floor))

        df_all.loc[free_mask, "PrezzoEquoDyn"] = dyn_prices

    listone["Tutti"] = df_all

    # Propagazione nei fogli per-ruolo
    for r, sheet in ROLE_SHEETS.items():
        if sheet in listone:
            role_df = listone[sheet].copy()
            key_cols = ["PlayerID"] if "PlayerID" in role_df.columns else ["Nome", "Squadra"]
            merged = role_df.merge(
                df_all[key_cols + ["PrezzoEquoDyn", "Acquistato", "PrezzoFinale", "Team"]],
                on=key_cols, how="left", suffixes=("", "_upd")
            )
            for c in ["PrezzoEquoDyn", "Acquistato", "PrezzoFinale", "Team"]:
                if c + "_upd" in merged.columns:
                    merged[c] = merged[c + "_upd"]
            dropcols = [c for c in merged.columns if c.endswith("_upd")]
            listone[sheet] = merged.drop(columns=dropcols)

    return listone

# --------------------------
# Temperatura (EMA) — aggiornamento su vendita
# --------------------------

def update_role_temperature(role_temp: Dict[str, float], ruolo: str, paid_price: float, fair_at_sale: float, beta: float) -> Dict[str, float]:
    """
    Temp_r <- (1 - beta) * Temp_r + beta * (paid_price / fair_at_sale), con clip a [TEMP_MIN, TEMP_MAX].
    """
    if fair_at_sale is None or fair_at_sale <= 0:
        return role_temp
    ratio = float(paid_price) / float(fair_at_sale)
    prev = float(role_temp.get(ruolo, 1.0))
    newv = (1.0 - float(beta)) * prev + float(beta) * ratio
    role_temp[ruolo] = float(np.clip(newv, TEMP_MIN, TEMP_MAX))
    return role_temp

# --------------------------
# Verifiche e controlli acquisto
# --------------------------

def _check_purchase_preconditions(teams: Dict[str, Any], team_id: str, ruolo: str, price: float) -> Tuple[bool, str]:
    t = teams.get(team_id)
    if t is None:
        return False, "Squadra selezionata inesistente."
    if t["slots"].get(ruolo, 0) <= 0:
        return False, f"Slot esauriti per il ruolo {ruolo} in {t['name']}."
    if t["budget"] < price:
        return False, f"Budget insufficiente in {t['name']} (budget={int(t['budget'])}, prezzo={int(price)})."
    return True, ""

def _verify_global_consistency(teams: Dict[str, Any]) -> Tuple[bool, str]:
    for tid, t in teams.items():
        if t["budget"] < -1e-6:
            return False, f"La squadra {t['name']} ha budget negativo."
        for r in ROLE_ORDER:
            if t["slots"].get(r, 0) < 0:
                return False, f"La squadra {t['name']} ha slots negativi sul ruolo {r}."
    return True, "OK"

# --------------------------
# UI helpers (tabelle leggibili in dark mode)
# --------------------------

def _delta_cols(df: pd.DataFrame) -> pd.DataFrame:
    out = df.copy()
    out["Delta%"] = ((out["PrezzoEquoDyn"] - out["PrezzoRicalcolato"]) / out["PrezzoRicalcolato"].replace(0, np.nan)) * 100.0
    out["OffertaMin"] = np.round(out["PrezzoEquoDyn"] * 0.95).astype(float)
    out["OffertaMax"] = np.round(out["PrezzoEquoDyn"] * 1.10).astype(float)
    return out

def colored_table(df: pd.DataFrame):
    """
    Tabella con bordo rosso (acquistati) / verde (liberi), Delta% evidenziato,
    e TESTO NERO forzato (compatibilità dark mode).
    """
    df2 = df.copy()
    if "Acquistato" not in df2.columns:
        df2["Acquistato"] = False

    def _style_row(row):
        bought = bool(row.get("Acquistato", False))
        border = "2px solid red" if bought else "2px solid green"
        bg = "#ffe5e5" if bought else "#e8ffe8"
        # testo nero SEMPRE
        return [f"border:{border}; background-color:{bg}; color:black;" for _ in row]

    styled = df2.style.apply(_style_row, axis=1)

    def _color_delta(val):
        try:
            if np.isnan(val):
                return "color:black;"
            # rosso scuro < -5%, verde scuro > +5%, altrimenti testo nero
            return "color:#b00020;" if val < -5 else ("color:#006400;" if val > 5 else "color:black;")
        except Exception:
            return "color:black;"

    if "Delta%" in df2.columns:
        styled = styled.format({"Delta%": "{:.1f}"}).applymap(_color_delta, subset=["Delta%"])

    # Forza testo nero anche per tutte le celle
    styled = styled.set_properties(**{"color": "black"})
    return styled

# --------------------------
# Aggiunta giocatore fuori lista
# --------------------------

def _add_offlist_player(name: str, team: str, ruolo: str, tier: str, prezzo_listino: float = 1.0, peso: float = 1.0):
    ss = st.session_state
    if ss.listone is None or "Tutti" not in ss.listone:
        st.error("Carica prima un listone valido.")
        return
    df_all = ss.listone["Tutti"]
    # genera nuovo PlayerID univoco
    new_id = int(df_all["PlayerID"].max()) + 1 if "PlayerID" in df_all.columns and not df_all["PlayerID"].isna().all() else 1
    row = {
        "PlayerID": new_id,
        "Nome": name.strip(),
        "Squadra": team.strip(),
        "Ruolo": ruolo,
        "Tier": tier if tier in TIERS else "Medium",
        "Peso": float(peso),
        "PrezzoRicalcolato": float(prezzo_listino),
        "PrezzoEquoDyn": float(prezzo_listino),
        "Acquistato": False,
        "PrezzoFinale": np.nan,
        "Team": np.nan,
    }
    # aggiungi a Tutti
    df_all = pd.concat([df_all, pd.DataFrame([row])], ignore_index=True)
    ss.listone["Tutti"] = _ensure_core_cols(df_all)

    # aggiorna foglio per-ruolo
    sheet = ROLE_SHEETS.get(ruolo)
    if sheet:
        if sheet in ss.listone:
            ss.listone[sheet] = ss.listone["Tutti"][ss.listone["Tutti"]["Ruolo"] == ruolo].copy()
        else:
            ss.listone[sheet] = ss.listone["Tutti"][ss.listone["Tutti"]["Ruolo"] == ruolo].copy()

    st.success(f"Aggiunto giocatore fuori lista: {name} ({team}) [{ruolo}], listino={int(prezzo_listino)}.")

# --------------------------
# Logica acquisti
# --------------------------

def _find_player_row(df_all: pd.DataFrame, player_id: Optional[Any], name: Optional[str], squad: Optional[str]) -> pd.Index:
    if player_id is not None and "PlayerID" in df_all.columns:
        return df_all.index[df_all["PlayerID"] == player_id]
    if name is not None:
        if squad is not None:
            return df_all.index[(df_all["Nome"] == name) & (df_all["Squadra"] == squad)]
        return df_all.index[df_all["Nome"] == name]
    return pd.Index([])

def register_purchase(player_id: Optional[Any], name: Optional[str], squad: Optional[str], price: float, team_id: str):
    ss = st.session_state
    if ss.listone is None or ss.teams is None:
        st.error("Carica listone e squadre prima di registrare un acquisto.")
        return
    df_all = ss.listone["Tutti"]
    rows = _find_player_row(df_all, player_id, name, squad)
    if len(rows) == 0:
        st.error("Giocatore non trovato. Usa la sezione 'Fuori lista' se serve inserirlo.")
        return
    idx = rows[0]
    if bool(_coalesce(df_all.at[idx, "Acquistato"], False)):
        st.warning("Questo giocatore risulta già acquistato.")
        return

    ruolo = str(df_all.at[idx, "Ruolo"])
    ok, msg = _check_purchase_preconditions(ss.teams, team_id, ruolo, float(price))
    if not ok:
        st.error(msg)
        return

    # snapshot per UNDO
    _take_snapshot()

    fair_now = float(_coalesce(df_all.at[idx, "PrezzoEquoDyn"], df_all.at[idx, "PrezzoRicalcolato"], 0.0))
    # segna acquisto
    df_all.at[idx, "Acquistato"] = True
    df_all.at[idx, "PrezzoFinale"] = float(price)
    df_all.at[idx, "Team"] = ss.teams[team_id]["name"]
    ss.listone["Tutti"] = df_all

    # aggiorna team
    ss.teams[team_id]["budget"] = max(0.0, ss.teams[team_id]["budget"] - float(price))
    ss.teams[team_id]["slots"][ruolo] = max(0, ss.teams[team_id]["slots"][ruolo] - 1)
    ss.teams[team_id]["players"].append(
        {"id": int(_coalesce(df_all.at[idx, "PlayerID"], -1)), "nome": str(df_all.at[idx, "Nome"]),
         "ruolo": ruolo, "prezzo": float(price)}
    )

    # log
    ss.log.append({
        "player_id": int(_coalesce(df_all.at[idx, "PlayerID"], -1)),
        "nome": str(df_all.at[idx, "Nome"]),
        "squadra_gioc": str(df_all.at[idx, "Squadra"]),
        "ruolo": ruolo,
        "prezzo": float(price),
        "team_id": team_id,
        "team_name": ss.teams[team_id]["name"],
        "fair_at_sale": fair_now
    })
    ss.global_sales_count += 1

    # aggiorna temperatura ruolo (EMA)
    ss.role_temp = update_role_temperature(ss.role_temp, ruolo, float(price), fair_now, beta=ss.ema_beta)

    # decadimento periodico (opzionale; rendilo piccolo o disattivalo alzando l'intervallo)
    DECAY_EVERY_N_STEPS, DECAY_FACTOR = 10, 0.05
    if ss.global_sales_count % DECAY_EVERY_N_STEPS == 0:
        ss.role_temp = _apply_decay_to_temperature(ss.role_temp, factor=DECAY_FACTOR)

    # ricalcolo dinamica (con mix)
    recalc_prices_dynamic(
        ss.listone, ss.teams,
        alpha_role=ss.alpha_role,
        alpha_tier=ss.alpha_tier,
        tier_weights=ss.tier_weights,
        role_temp=ss.role_temp,
        baseline_B_role=ss.baseline_B_role,
        impact_mix=ss.impact_mix,
        dyn_floor=ss.dyn_floor
    )

    st.success(f"{df_all.at[idx, 'Nome']} acquistato da {ss.teams[team_id]['name']} per {price} crediti.")
    _autosave_if_needed()

# --------------------------
# Simulazione (dry-run)
# --------------------------

def _simulate_on_copies(sim_df: pd.DataFrame,
                        listone: Dict[str, pd.DataFrame],
                        teams: Dict[str, Any],
                        alpha_role: float, alpha_tier: float,
                        tier_weights: Dict[str, float], role_temp: Dict[str, float],
                        baseline_B_role: Dict[str, float], impact_mix: float,
                        ema_beta: float, dyn_floor: float) -> Tuple[Dict[str, pd.DataFrame], Dict[str, Any], Dict[str, float]]:
    listone_c = {k: v.copy(deep=True) for k, v in listone.items()}
    teams_c = json.loads(json.dumps(teams))
    role_temp_c = dict(role_temp)

    def _register_on_copy(pid, name, squad, price, team_id):
        df_all = listone_c["Tutti"]
        rows = _find_player_row(df_all, pid, name, squad)
        if len(rows) == 0:
            return
        idx = rows[0]
        if bool(_coalesce(df_all.at[idx, "Acquistato"], False)):
            return
        ruolo = str(df_all.at[idx, "Ruolo"])

        # precondizioni
        t = teams_c.get(team_id)
        if t is None or t["slots"].get(ruolo, 0) <= 0 or t["budget"] < price:
            return

        fair_now = float(_coalesce(df_all.at[idx, "PrezzoEquoDyn"], df_all.at[idx, "PrezzoRicalcolato"], 0.0))
        df_all.at[idx, "Acquistato"] = True
        df_all.at[idx, "PrezzoFinale"] = float(price)
        df_all.at[idx, "Team"] = teams_c[team_id]["name"]

        teams_c[team_id]["budget"] = max(0.0, teams_c[team_id]["budget"] - float(price))
        teams_c[team_id]["slots"][ruolo] = max(0, teams_c[team_id]["slots"][ruolo] - 1)
        teams_c[team_id]["players"].append({"id": int(_coalesce(df_all.at[idx,"PlayerID"], -1)),
                                            "nome": str(df_all.at[idx,"Nome"]),
                                            "ruolo": ruolo, "prezzo": float(price)})

        # temperatura su copia
        if fair_now > 0:
            ratio = float(price) / fair_now
            prev = float(role_temp_c.get(ruolo, 1.0))
            role_temp_c[ruolo] = float(np.clip((1-ema_beta)*prev + ema_beta*ratio, TEMP_MIN, TEMP_MAX))

        # ricalcolo su copia
        recalc_prices_dynamic(
            listone_c, teams_c,
            alpha_role=alpha_role,
            alpha_tier=alpha_tier,
            tier_weights=tier_weights,
            role_temp=role_temp_c,
            baseline_B_role=baseline_B_role,
            impact_mix=impact_mix,
            dyn_floor=dyn_floor
        )

    for _, row in sim_df.iterrows():
        pid = _coalesce(row.get("PlayerID", None), None)
        name = row.get("Nome", None)
        squad = row.get("Squadra", None)
        price = float(row.get("Prezzo", 0))
        team = row.get("TeamID", None)
        if team is None or price <= 0:
            continue
        _register_on_copy(pid, name, squad, price, team)

    return listone_c, teams_c, role_temp_c

# --------------------------
# Streamlit APP
# --------------------------

def main():
    st.set_page_config(page_title="Gestione Asta Fantacalcio", layout="wide")
    _init_state()

    st.title("🏆 Gestione Asta Fantacalcio — Console Live (Static=Dynamic @start)")

    # Sidebar — caricamenti, parametri e stato
    st.sidebar.header("⚙️ Configurazione")
    st.sidebar.markdown("**Carica listone ricalcolato (Modello 4) e config squadre**")

    # Caricamento stato salvato (bundle JSON) opzionale
    saved_state = st.sidebar.file_uploader("Carica STATO (bundle .json) opzionale", type=["json"])
    if saved_state and st.sidebar.button("Carica stato salvato"):
        try:
            bundle = json.load(saved_state)
            _restore_from_bundle(bundle)
            st.session_state.loaded_state_name = getattr(saved_state, "name", "stato_caricato.json")
            st.success(f"Stato caricato da {st.session_state.loaded_state_name}")
        except Exception as e:
            st.error(f"Errore nel caricamento stato: {e}")

    xlsx_file = st.sidebar.file_uploader("Listone ricalcolato (.xlsx)", type=["xlsx"])
    json_file = st.sidebar.file_uploader("Config squadre (.json)", type=["json"])

    # Parametri dinamica
    st.sidebar.subheader("📈 Dinamiche (impatto dell'asta)")
    st.sidebar.slider("Impatto dinamiche (mix statico↔residuo)", 0.0, 1.0, key="impact_mix", value=st.session_state.impact_mix, step=0.05,
                      help="0 = prezzi guidati dalla baseline statica (identici allo XLSX all'inizio). 1 = completamente guidati dal budget residuo/slots.")
    st.sidebar.number_input("α ruolo (scarsità ruolo)", min_value=0.0, max_value=2.0, step=0.1,
                            value=st.session_state.alpha_role, key="alpha_role")
    st.sidebar.number_input("α fascia (scarsità fascia)", min_value=0.0, max_value=2.0, step=0.1,
                            value=st.session_state.alpha_tier, key="alpha_tier")
    st.sidebar.number_input("EMA β (temperatura ruolo)", min_value=0.0, max_value=1.0, step=0.05,
                            value=st.session_state.ema_beta, key="ema_beta")
    st.sidebar.number_input("Floor minimo prezzo live", min_value=0.0, max_value=10.0, step=0.5,
                            value=st.session_state.dyn_floor, key="dyn_floor")

    with st.sidebar.expander("Pesi appetibilità per fascia"):
        for t in TIERS:
            st.session_state.tier_weights[t] = st.number_input(
                f"{t}", min_value=0.0, step=0.1,
                value=float(st.session_state.tier_weights.get(t, 1.0)), key=f"tier_{t}"
            )

    st.sidebar.text_input("Autosave JSON path", value=st.session_state.autosave_json_path, key="autosave_json_path")
    st.sidebar.text_input("Autosave XLSX path (opz.)", value=st.session_state.autosave_xlsx_path, key="autosave_xlsx_path")

    # Carica listone e squadre (se non si è caricato stato)
    if xlsx_file and st.session_state.listone is None:
        try:
            st.session_state.listone = load_listone_xlsx(xlsx_file)
        except Exception as e:
            st.error(f"Errore nel listone: {e}")

    if json_file and st.session_state.teams is None:
        try:
            conf, teams = load_teams_json(json_file)
            st.session_state.config = conf
            st.session_state.teams = teams
            st.session_state.my_team_id = conf.get("our_team_id", list(teams.keys())[0])
        except Exception as e:
            st.error(f"Errore nel JSON squadre: {e}")

    # Inizializzazione: baseline clearing statico per identità iniziale
    if st.session_state.listone is not None:
        df_all = st.session_state.listone["Tutti"]
        # al primo giro, se tutte le baseline sono 0, calcola dalle statiche
        if all(v == 0.0 for v in st.session_state.baseline_B_role.values()):
            st.session_state.baseline_B_role = _compute_baseline_clearing_from_static(df_all)

    # Ricalcolo dinamico prezzi (se abbiamo tutto)
    if st.session_state.listone is not None and st.session_state.teams is not None:
        # assicura che PrezzoEquoDyn riparta = PrezzoRicalcolato quando impact_mix = 0 e α=0, β=0
        recalc_prices_dynamic(
            st.session_state.listone,
            st.session_state.teams,
            alpha_role=st.session_state.alpha_role,
            alpha_tier=st.session_state.alpha_tier,
            tier_weights=st.session_state.tier_weights,
            role_temp=st.session_state.role_temp,
            baseline_B_role=st.session_state.baseline_B_role,
            impact_mix=st.session_state.impact_mix,
            dyn_floor=st.session_state.dyn_floor
        )

        # Selezione squadra nostra
        st.sidebar.selectbox(
            "La mia squadra",
            options=list(st.session_state.teams.keys()),
            index=list(st.session_state.teams.keys()).index(st.session_state.my_team_id)
            if st.session_state.my_team_id in st.session_state.teams else 0,
            format_func=lambda x: st.session_state.teams[x]["name"],
            key="my_team_id"
        )

        # KPIs generali
        colA, colB, colC, colD = st.columns(4)
        Btot = sum(t["budget"] for t in st.session_state.teams.values())
        acquired = int(st.session_state.listone["Tutti"]["Acquistato"].fillna(False).sum())
        remaining = len(st.session_state.listone["Tutti"]) - acquired
        colA.metric("Budget residuo Totale", f"{int(Btot)}")
        colB.metric("Giocatori già assegnati", f"{acquired}")
        colC.metric("Giocatori ancora liberi", f"{remaining}")
        colD.metric("Vendite (global)", f"{st.session_state.global_sales_count}")

        # Tabs
        tabs = st.tabs([
            "Portieri", "Difensori", "Centrocampisti", "Attaccanti",
            "Operazioni", "Fuori lista", "Simulazione", "Squadre/Monitor", "Verifica", "Salva/Carica"
        ])
        role_map = {"Portieri": "P", "Difensori": "D", "Centrocampisti": "C", "Attaccanti": "A"}

        # --- Tab Ruoli (con testo nero nelle tabelle)
        for tab_name in ["Portieri", "Difensori", "Centrocampisti", "Attaccanti"]:
            with tabs[list(role_map.keys()).index(tab_name)]:
                r = role_map[tab_name]
                df_r = st.session_state.listone["Tutti"]
                df_r = df_r[df_r["Ruolo"] == r].copy()

                # KPI ruolo
                slots_r = sum(max(0, t["slots"][r]) for t in st.session_state.teams.values())
                free_r = int((~df_r["Acquistato"].fillna(False)).sum())
                scarcity_role = (slots_r / max(free_r, 1)) ** float(st.session_state.alpha_role)
                temp = float(st.session_state.role_temp.get(r, 1.0))
                col1, col2, col3, col4 = st.columns(4)
                col1.metric(f"Slots residui {r}", f"{slots_r}")
                col2.metric(f"Libero in {r}", f"{free_r}")
                col3.metric(f"Scarsità ruolo×α", f"{scarcity_role:.2f}")
                col4.metric(f"Temperatura {r}", f"{temp:.2f}")

                # Tabella (liberi on top) con delta e range offerta
                show_cols = ["PlayerID", "Nome", "Squadra", "Tier",
                             "PrezzoRicalcolato", "PrezzoEquoDyn", "Delta%", "OffertaMin", "OffertaMax",
                             "Acquistato", "PrezzoFinale", "Team"]
                df_show = _delta_cols(df_r)
                df_show = df_show.sort_values(["Acquistato", "PrezzoEquoDyn"], ascending=[True, False]).reset_index(drop=True)
                st.dataframe(colored_table(df_show[show_cols]), use_container_width=True)

        # --- Tab Operazioni (acquisti)
        with tabs[4]:
            st.subheader("➕ Inserisci acquisto")
            df_all = st.session_state.listone["Tutti"]
            remaining_df = df_all[~df_all["Acquistato"].fillna(False)].copy()
            use_id = "PlayerID" in remaining_df.columns
            if remaining_df.empty:
                st.info("Nessun giocatore libero.")
            else:
                player_key = st.selectbox(
                    "Giocatore",
                    options=remaining_df.index.tolist(),
                    format_func=lambda idx: (
                        f"[{int(remaining_df.at[idx,'PlayerID'])}] {remaining_df.at[idx,'Nome']} - "
                        f"{remaining_df.at[idx,'Squadra']} ({remaining_df.at[idx,'Ruolo']})"
                        if use_id else
                        f"{remaining_df.at[idx,'Nome']} - {remaining_df.at[idx,'Squadra']} ({remaining_df.at[idx,'Ruolo']})"
                    ),
                )
                price = st.number_input("Prezzo pagato", min_value=1, value=1, step=1)
                team_id = st.selectbox("Squadra acquirente", list(st.session_state.teams.keys()),
                                       format_func=lambda x: st.session_state.teams[x]["name"])

                # Mostra equo live e range consigliato per quel giocatore
                row = remaining_df.loc[player_key]
                live_now = float(_coalesce(row.get("PrezzoEquoDyn"), row.get("PrezzoRicalcolato"), 1.0))
                st.caption(f"Equo live: **{live_now:.0f}**  — Range offerta: **{int(live_now*0.95)}–{int(live_now*1.10)}**")

                col_ok, col_undo = st.columns([3, 1])
                if col_ok.button("Registra acquisto", use_container_width=True):
                    pid = int(_coalesce(row.get("PlayerID", None), None)) if use_id else None
                    register_purchase(pid, str(row["Nome"]), str(row["Squadra"]), float(price), str(team_id))
                if col_undo.button("↩️ UNDO ultimo", use_container_width=True):
                    _undo()

            st.divider()
            st.subheader("🧾 Log operazioni")
            if st.session_state.log:
                st.dataframe(pd.DataFrame(st.session_state.log), use_container_width=True)
            else:
                st.caption("Nessuna operazione ancora registrata.")

        # --- Tab Fuori lista (inserimento giocatori non presenti)
        with tabs[5]:
            st.subheader("➕ Aggiungi giocatore fuori lista")
            colL, colR = st.columns(2)
            with colL:
                name = st.text_input("Nome", "")
                team = st.text_input("Squadra", "")
                ruolo = st.selectbox("Ruolo", ROLE_ORDER, index=2)  # default "C"
                tier = st.selectbox("Tier", TIERS, index=2)         # default "Medium"
            with colR:
                prezzo = st.number_input("Prezzo di listino (default=1)", min_value=0.0, value=1.0, step=1.0)
                peso = st.number_input("Peso (default=1)", min_value=0.0, value=1.0, step=0.1)

            if st.button("Aggiungi"):
                if not name.strip() or not team.strip():
                    st.error("Compila almeno Nome e Squadra.")
                else:
                    _add_offlist_player(name, team, ruolo, tier, prezzo_listino=prezzo, peso=peso)
                    # aggiorna i prezzi (rispetta identità se impact_mix=0)
                    recalc_prices_dynamic(
                        st.session_state.listone, st.session_state.teams,
                        alpha_role=st.session_state.alpha_role,
                        alpha_tier=st.session_state.alpha_tier,
                        tier_weights=st.session_state.tier_weights,
                        role_temp=st.session_state.role_temp,
                        baseline_B_role=st.session_state.baseline_B_role,
                        impact_mix=st.session_state.impact_mix,
                        dyn_floor=st.session_state.dyn_floor
                    )

        # --- Tab Simulazione
        with tabs[6]:
            st.subheader("🧪 Simulazione da CSV")
            st.caption("Colonne: PlayerID (preferita) oppure Nome,Squadra; Prezzo; TeamID")
            sim_file = st.file_uploader("Carica CSV simulazione", type=["csv"], key="sim_csv")
            dry = st.checkbox("Esegui in dry-run (non modifica lo stato reale)", value=True, key="sim_dry")
            if sim_file is not None:
                try:
                    sim_df = pd.read_csv(sim_file)
                    st.dataframe(sim_df.head(20), use_container_width=True)
                    if st.button("Esegui simulazione"):
                        if dry:
                            lc, tc, rc = _simulate_on_copies(
                                sim_df,
                                st.session_state.listone,
                                st.session_state.teams,
                                st.session_state.alpha_role,
                                st.session_state.alpha_tier,
                                st.session_state.tier_weights,
                                st.session_state.role_temp,
                                st.session_state.baseline_B_role,
                                st.session_state.impact_mix,
                                st.session_state.ema_beta,
                                st.session_state.dyn_floor
                            )
                            st.success("Simulazione (dry-run) eseguita. Stato reale NON modificato.")
                            st.dataframe(lc["Tutti"].head(25))
                        else:
                            # live
                            for _, row in sim_df.iterrows():
                                pid = _coalesce(row.get("PlayerID", None), None)
                                name = row.get("Nome", None)
                                squad = row.get("Squadra", None)
                                price = float(row.get("Prezzo", 0))
                                team = row.get("TeamID", None)
                                if team is None or price <= 0:
                                    continue
                                register_purchase(pid, name, squad, price, team)
                except Exception as e:
                    st.error(f"Errore lettura CSV simulazione: {e}")

        # --- Tab Squadre/Monitor
        with tabs[7]:
            st.subheader("📊 Monitor squadre")
            my = st.session_state.teams[st.session_state.my_team_id]
            st.markdown(f"### ⭐ La mia squadra: **{my['name']}**")
            colA, colB = st.columns(2)
            with colA:
                st.write(f"**Budget residuo:** {int(my['budget'])}")
                st.write(f"**Slots residui:** {my['slots']}")
                st.write("**Giocatori presi:**")
                st.dataframe(pd.DataFrame(my["players"]) if my["players"] else pd.DataFrame(columns=["id","nome","ruolo","prezzo"]))
            with colB:
                st.write("**Suggerimenti (Top-5 per ruolo mancante):**")
                df_all = st.session_state.listone["Tutti"]
                for r in ROLE_ORDER:
                    need = my["slots"].get(r, 0)
                    if need <= 0:
                        continue
                    free_r = df_all[(df_all["Ruolo"] == r) & (~df_all["Acquistato"].fillna(False))].copy()
                    if free_r.empty:
                        continue
                    free_r = free_r.sort_values("PrezzoEquoDyn", ascending=False).head(5)
                    small = _delta_cols(free_r)[["PlayerID","Nome","Squadra","Tier","PrezzoEquoDyn","PrezzoRicalcolato","Delta%"]]
                    st.caption(f"Ruolo {r} — mancano {need}")
                    st.dataframe(colored_table(small), use_container_width=True)

            st.divider()
            st.markdown("### 🧭 Avversari")
            for tid, t in st.session_state.teams.items():
                if tid == st.session_state.my_team_id:
                    continue
                with st.expander(f"⚽ {t['name']}"):
                    st.write(f"**Budget residuo:** {int(t['budget'])}")
                    st.write(f"**Slots residui:** {t['slots']}")
                    st.dataframe(pd.DataFrame(t["players"]) if t["players"] else pd.DataFrame(columns=["id","nome","ruolo","prezzo"]))

        # --- Tab Verifica
        with tabs[8]:
            st.subheader("🔎 Verifica coerenza")
            ok, msg = _verify_global_consistency(st.session_state.teams)
            if ok:
                st.success("OK: nessuna anomalia rilevata.")
            else:
                st.error(msg)
            st.caption("Controlla budget < 0 o slots negativi per ruolo/squadra.")

        # --- Tab Salva/Carica
        with tabs[9]:
            st.subheader("💾 Salva / Carica stato")
            buf_xlsx = export_state_xlsx()
            st.download_button("Scarica XLSX stato corrente", data=buf_xlsx, file_name="asta_state.xlsx",
                               mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet")

            bundle = _bundle_state()
            st.download_button("Scarica STATO (bundle .json)", data=json.dumps(bundle, ensure_ascii=False).encode("utf-8"),
                               file_name="asta_state.json", mime="application/json")
            st.caption("Imposta in sidebar i percorsi per autosalvataggi continui (JSON e opz. XLSX).")

    else:
        st.info("📥 Carica **listone** ricalcolato (.xlsx) e **config squadre** (.json) dalla sidebar, oppure carica uno **stato salvato (.json)**.")

if __name__ == "__main__":
    main()
