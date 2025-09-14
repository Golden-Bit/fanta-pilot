# The previous execution reset unexpectedly (likely due to environment constraints).
# Re-run the full code to generate outputs again.
import os
import pandas as pd
import numpy as np
from typing import Dict, Tuple, List

INPUT_PATH = "../data/Quotazioni_Fantacalcio_Stagione_2025_26.xlsx"
OUT_DIR = "../data/out_data"

ROLE_SHEETS = {"P":"Portieri","D":"Difensori","C":"Centrocampisti","A":"Attaccanti"}
ROLE_ORDER = ["P","D","C","A"]

DEFAULT_BUDGET_PER_ROLE = {"P": 0.10, "D": 0.20, "C": 0.30, "A": 0.40}
DEFAULT_QUANTILES = (0.0, 0.15, 0.45, 0.80, 1.0)
DEFAULT_TIER_BUDGET_QUOTA = {"Top": 0.50, "High": 0.30, "Medium": 0.15, "Low": 0.05}
DEFAULT_TIER_WEIGHTS = {"Top": 3.0, "High": 2.0, "Medium": 1.2, "Low": 0.8}

def read_role_sheet(xlsx_path: str, sheet_name: str) -> pd.DataFrame:
    df = pd.read_excel(xlsx_path, sheet_name=sheet_name)
    df.columns = df.iloc[0]
    df = df.drop(0).reset_index(drop=True)
    rename_map = {"R":"Ruolo","RM":"RuoloMantra","Nome":"Nome","Squadra":"Squadra",
                  "Qt.A":"QuotAttuale","Qt.I":"QuotIniziale","Diff.":"Differenza","FVM":"FVM"}
    df = df.rename(columns=rename_map)
    keep = ["Nome","Squadra","Ruolo","RuoloMantra","QuotAttuale","QuotIniziale","Differenza","FVM"]
    for k in keep:
        if k not in df.columns: df[k]=np.nan
    df = df[keep].copy()
    for c in ["QuotAttuale","QuotIniziale","Differenza","FVM"]:
        df[c]=pd.to_numeric(df[c], errors="coerce")
    df = df[~df["Nome"].isna() & ~df["Ruolo"].isna()].copy()
    return df.reset_index(drop=True)

def load_all_roles(path):
    return {r: read_role_sheet(path, sheet) for r, sheet in ROLE_SHEETS.items()}

def assign_tiers_by_quantiles(df_role: pd.DataFrame, q_breaks=DEFAULT_QUANTILES) -> pd.DataFrame:
    x = df_role.copy()
    score = x["QuotAttuale"].fillna(x["QuotIniziale"]).fillna(x["FVM"])
    x = x.assign(_score=score).sort_values("_score", ascending=False).reset_index(drop=True)
    n=len(x); idx=[int(np.floor(n*q)) for q in q_breaks]; idx=sorted(set(idx+[0,n]))
    labels=["Top","High","Medium","Low"]; tiers=[""]*n
    for k in range(len(labels)):
        start,end=idx[k],idx[k+1]
        for i in range(start,end): tiers[i]=labels[k]
    x["Tier"]=tiers
    return x.drop(columns=["_score"])

def compute_weights_linear(df,col="QuotAttuale"):
    return df[col].fillna(df["QuotIniziale"]).fillna(df["FVM"]).astype(float).values

def microscala_to_budget_per_role(df_role: pd.DataFrame, price_col: str, target_budget: float) -> pd.Series:
    vals = df_role[price_col].astype(float).values
    vals = np.maximum(1.0, vals)
    s=vals.sum()
    if s<=0:
        out=np.full(len(vals),1.0)
    else:
        k=target_budget/s
        out=np.maximum(1.0, np.round(vals*k))
    # fine tuning
    out=out.astype(int)
    diff=int(target_budget - out.sum())
    if diff!=0 and len(out)>0:
        step=1 if diff>0 else -1
        for j in range(abs(diff)):
            pos=j%len(out)
            out[pos]=max(1, out[pos]+step)
    return pd.Series(out, index=df_role.index)

def concat_roles_dict(d: Dict[str,pd.DataFrame]) -> pd.DataFrame:
    frames=[d[r].copy() for r in ROLE_ORDER if r in d]
    return pd.concat(frames, ignore_index=True) if frames else pd.DataFrame()

def write_output_xlsx(dframes: Dict[str,pd.DataFrame], out_path: str):
    with pd.ExcelWriter(out_path, engine="xlsxwriter") as writer:
        concat_roles_dict(dframes).to_excel(writer, sheet_name="Tutti", index=False)
        for r, sheet in ROLE_SHEETS.items():
            if r in dframes:
                dframes[r].to_excel(writer, sheet_name=sheet, index=False)

class BaseModel:
    def __init__(self, budget_split=None):
        self.budget_split = budget_split or DEFAULT_BUDGET_PER_ROLE
        s=sum(self.budget_split.values()); self.budget_split={k:v/s for k,v in self.budget_split.items()}
        self.roles_data={}; self.total_budget=None
    def load_xlsx(self, path): self.roles_data=load_all_roles(path); return self
    def set_total_budget(self, total_budget): self.total_budget=float(total_budget); return self
    def _B(self, r):
        if self.total_budget is None: raise ValueError("Set total budget first");
        return self.budget_split.get(r,0.0)*self.total_budget
    def run(self): raise NotImplementedError

class Model1Proporzionale(BaseModel):
    def run(self):
        out={}
        for r, df in self.roles_data.items():
            df=df.copy(); w=compute_weights_linear(df); S=float(np.nansum(w)); B_r=self._B(r)
            if S<=0: prices=np.full(len(df),1)
            else:
                raw=B_r*(w/S)
                tmp=df[["Nome"]].copy(); tmp["p"]=raw
                tmp["p"]=microscala_to_budget_per_role(tmp,"p",B_r)
                prices=tmp["p"].values
            df["Tier"] = ""
            df["Peso"] = w
            df["MoltiplicatoreScarsita"]=1.0
            df["BoosterMercato"]=1.0
            df["PrezzoRicalcolato"]=prices
            out[r]=df
        return out

class Model2Fasce(BaseModel):
    def __init__(self, budget_split=None, quantiles=DEFAULT_QUANTILES, tier_budget_quota=None):
        super().__init__(budget_split); self.quantiles=quantiles
        self.tier_budget_quota=tier_budget_quota or DEFAULT_TIER_BUDGET_QUOTA
    def run(self):
        out={}
        for r, df in self.roles_data.items():
            df=assign_tiers_by_quantiles(df,self.quantiles); B_r=self._B(r)
            df["Peso"]=compute_weights_linear(df); df["MoltiplicatoreScarsita"]=1.0; df["BoosterMercato"]=1.0
            df["PrezzoRicalcolato"]=1
            for tier,quota in self.tier_budget_quota.items():
                mask=df["Tier"]==tier; B_rt=quota*B_r; S_rt=df.loc[mask,"Peso"].sum()
                if S_rt>0 and mask.sum()>0:
                    raw=B_rt*(df.loc[mask,"Peso"]/S_rt)
                    tmp=df.loc[mask,["Peso"]].copy(); tmp["p"]=raw.values
                    tmp["p"]=microscala_to_budget_per_role(tmp,"p",B_rt)
                    df.loc[mask,"PrezzoRicalcolato"]=tmp["p"].values
            out[r]=df
        return out

class Model3PesiScarsita(BaseModel):
    def __init__(self, budget_split=None, quantiles=DEFAULT_QUANTILES, appetibilita=None, alpha=0.5, gamma=1.1):
        super().__init__(budget_split); self.quantiles=quantiles;
        self.appetibilita=appetibilita or DEFAULT_TIER_WEIGHTS; self.alpha=alpha; self.gamma=gamma
    def run(self):
        out={}
        for r, df in self.roles_data.items():
            df=assign_tiers_by_quantiles(df,self.quantiles); B_r=self._B(r)
            base=compute_weights_linear(df); w=np.power(np.maximum(0.0,base), self.gamma); df["Peso"]=w
            tiers=["Top","High","Medium","Low"]; a=np.array([self.appetibilita.get(t,1.0) for t in tiers]); a=np.where(a<=0,1e-9,a)
            q=a/a.sum(); M_r=len(df); D={t:q[i]*M_r for i,t in enumerate(tiers)}; S={t:int((df["Tier"]==t).sum()) for t in tiers}
            m={t:(D[t]/max(S[t],1e-9))**self.alpha for t in tiers}
            df["MoltiplicatoreScarsita"]=df["Tier"].map(m).astype(float)
            df["_weff"]=df["Peso"]*df["MoltiplicatoreScarsita"]
            W=df["_weff"].sum()
            if W>0:
                raw=B_r*(df["_weff"]/W); tmp=df[["_weff"]].copy(); tmp["p"]=raw.values
                tmp["p"]=microscala_to_budget_per_role(tmp,"p",B_r); df["PrezzoRicalcolato"]=tmp["p"].values
            else:
                df["PrezzoRicalcolato"]=1
            df["BoosterMercato"]=1.0; df=df.drop(columns=["_weff"]); out[r]=df
        return out

class Model4DebiasMercato(BaseModel):
    def __init__(self,
                 budget_split=None,
                 quantiles=DEFAULT_QUANTILES,
                 appetibilita=None,
                 alpha=0.5,
                 gamma_map=None, eta_map=None, kappa_map=None, lambda_map=None, delta=0.8,
                 # --- NEW: parametri per filtrare gli effettivi
                 n_teams: int = 10,
                 slots_per_role: Dict[str, int] = None,
                 overshoot: float = 0.10,                 # +10% margine
                 selection_metric: str = "QuotAttuale"    # metrica per ordinare i migliori
                 ):
        super().__init__(budget_split)
        self.quantiles = quantiles
        self.appetibilita = appetibilita or DEFAULT_TIER_WEIGHTS
        self.alpha = alpha
        self.gamma_map = gamma_map or {"A":1.25,"C":1.15,"D":1.10,"P":1.05}
        self.eta_map   = eta_map   or {"A":1.0,"C":1.0,"D":1.0,"P":1.0}
        self.kappa_map = kappa_map or {"A":0.18,"C":0.12,"D":0.08,"P":0.05}
        self.lambda_map= lambda_map or {"A":2.2,"C":2.0,"D":1.8,"P":1.5}
        self.delta = delta
        # --- NEW
        self.n_teams = int(n_teams)
        self.slots_per_role = slots_per_role or {"P":3, "D":8, "C":8, "A":6}
        self.overshoot = float(overshoot)
        self.selection_metric = selection_metric

    # --- NEW: selezione effettivi per ruolo
    def _best_n_per_role(self, df_role: pd.DataFrame, r: str) -> pd.DataFrame:
        n_target = int(self.n_teams * self.slots_per_role.get(r, 0))
        n_keep = max(1, int(np.ceil(n_target * (1.0 + self.overshoot))))
        # metrica: selection_metric -> fallback su QuotAttuale -> QuotIniziale -> FVM
        score = pd.to_numeric(
            df_role.get(self.selection_metric, df_role.get("QuotAttuale", np.nan)),
            errors="coerce"
        ).fillna(df_role.get("QuotIniziale", np.nan)).fillna(df_role.get("FVM", np.nan))
        # ordina per punteggio desc e prendi i primi n_keep (clamp a len)
        df_role = df_role.assign(_score=score).sort_values("_score", ascending=False)
        n_keep = min(n_keep, len(df_role))
        df_role = df_role.head(n_keep).drop(columns=["_score"])
        return df_role.reset_index(drop=True)

    def _debias_role_scores(self, df_role, r):
        base = compute_weights_linear(df_role)
        if len(base) == 0:
            return base
        mn, mx = np.nanmin(base), np.nanmax(base)
        x = np.zeros_like(base) if (mx - mn) <= 0 else (base - mn) / (mx - mn)
        gamma = self.gamma_map.get(r, 1.1)
        x = np.power(np.maximum(0.0, x), gamma)
        eta = self.eta_map.get(r, 1.0)
        return np.power(x, eta)

    # --- NEW: util per percentile continuo u in [0,1] (ruolo-centrico)
    def _percentile_rank(self, df_role: pd.DataFrame, score_col: str, descending: bool = True) -> np.ndarray:
        s = pd.to_numeric(df_role.get(score_col, np.nan), errors="coerce") \
              .fillna(df_role.get("QuotIniziale", np.nan)) \
              .fillna(df_role.get("FVM", np.nan))
        order = s.rank(method="average", ascending=not descending).values  # 1..n
        n = max(len(df_role), 1)
        u = (order - 0.5) / n  # (0,1)
        return np.clip(u, 0.0, 1.0)

    # --- NEW: blending tra moltiplicatori di fascia
    def _blend_tier_multiplier(self,
                               u: np.ndarray,
                               quantiles: Tuple[float, ...],
                               tier_mult_map: Dict[str, float]) -> np.ndarray:
        """
        quantiles: come (0.00, 0.15, 0.45, 0.80, 1.00)
        tier_mult_map: es. {"Top": m_top, "High": m_high, "Medium": m_med, "Low": m_low}
        Ritorna un vettore m_soft con blending lineare tra i tier adiacenti.
        """
        tiers_order = ["Top", "High", "Medium", "Low"]
        mvals = np.array([tier_mult_map.get(t, 1.0) for t in tiers_order], dtype=float)

        # --- robustezza quantili ---
        if quantiles is None or len(quantiles) < 5:
            quantiles = (0.0, 0.15, 0.45, 0.80, 1.0)
        q = np.array(sorted(quantiles), dtype=float)
        q[0], q[-1] = 0.0, 1.0  # clamp ai bordi

        m_soft = np.empty_like(u, dtype=float)
        for i, ui in enumerate(u):
            if ui <= q[0]:
                m_soft[i] = mvals[0]
                continue
            if ui >= q[-1]:
                m_soft[i] = mvals[-1]
                continue
            k = np.searchsorted(q, ui, side="right") - 1
            k = np.clip(k, 0, len(mvals) - 1)
            if k == len(mvals) - 1:
                m_soft[i] = mvals[-1]
            else:
                t = (ui - q[k]) / max(q[k + 1] - q[k], 1e-9)
                m_soft[i] = (1 - t) * mvals[k] + t * mvals[k + 1]
        return m_soft

    def run(self):
        """
        Modello 4 con:
        - filtro 'effettivi' per ruolo (n_teams * slots_per_role[ruolo] * (1+overshoot))
        - tier assegnati per quantili
        - pesi de-biasati per ruolo
        - scarsità per fascia con BLENDING morbido (niente scalini)
        - booster di mercato
        - normalizzazione al budget di ruolo con microscala_to_budget_per_role
        """
        out = {}
        tiers_order = ["Top", "High", "Medium", "Low"]

        for r, df in self.roles_data.items():
            # 1) Tieni solo gli effettivi per il ruolo r (prima di ogni calcolo)
            df = self._best_n_per_role(df.copy(), r)

            # 2) Tier per quantili (informativi; il blending userà i quantili come nodi)
            df = assign_tiers_by_quantiles(df, self.quantiles)

            # 3) Budget di ruolo
            B_r = self._B(r)

            # 4) Pesi de-biasati (ruolo-specifici) + clamp per sicurezza numerica
            w = self._debias_role_scores(df, r)

            # Se viene fuori array numpy, assicurati che non contenga NaN/inf
            w = np.asarray(w, dtype=float)
            w = np.where(np.isnan(w), 0.0, w)  # rimpiazza NaN con 0
            w = np.clip(w, 0.0, None)  # forza non-negativi

            # fallback se tutto zero
            if not np.isfinite(w.sum()) or w.sum() <= 0:
                w = np.ones(len(df), dtype=float)

            df["Peso"] = np.where(w <= 0.0, 1e-9, w)  # evitare somme zero

            # 5) Scarsità per fascia (D/S)^alpha, calcolata sui soli effettivi del ruolo
            #    Domanda target per fascia: proporzionale a self.appetibilita
            a = np.array([self.appetibilita.get(t, 1.0) for t in tiers_order], dtype=float)
            a = np.where(a <= 0, 1e-9, a)
            q = a / a.sum()

            M_r_target = max(1, int(self.n_teams * self.slots_per_role.get(r, 0)))
            D = {t: q[i] * M_r_target for i, t in enumerate(tiers_order)}
            S = {t: int((df["Tier"] == t).sum()) for t in tiers_order}
            m_discrete = {t: (D[t] / max(S[t], 1)) ** float(self.alpha) for t in tiers_order}

            # 6) BLENDING morbido: niente salto tra fasce ai confini
            #    - percentile continuo u in [0,1] sul ruolo, basato su selection_metric (fallback su QuotAttuale/Qt.I/FVM)
            u = self._percentile_rank(df, score_col=self.selection_metric, descending=True)
            #    - blending lineare dei moltiplicatori m_discrete sui nodi dei quantili
            m_soft_vec = self._blend_tier_multiplier(u, self.quantiles, m_discrete)
            m_soft_vec = np.asarray(m_soft_vec, dtype=float)
            # rimpiazza NaN/inf con 1.0
            m_soft_vec = np.where(~np.isfinite(m_soft_vec), 1.0, m_soft_vec)
            df["MoltiplicatoreScarsita"] = m_soft_vec
            # 7) Booster di mercato (rank su Peso, con decadimento esponenziale)
            tmp = df.sort_values("Peso", ascending=False).reset_index(drop=True)
            n = len(tmp)
            tmp["u"] = (tmp.index + 0.5) / max(n, 1)  # rank normalizzato in (0,1]
            kappa = float(self.kappa_map.get(r, 0.1))
            lam = float(self.lambda_map.get(r, 2.0))
            tmp["BoosterMercato"] = (1.0 + kappa * np.exp(-lam * tmp["u"])) ** float(self.delta)
            key_cols = ["PlayerID"] if "PlayerID" in df.columns else ["Nome", "Squadra"]
            df = df.merge(tmp[key_cols + ["BoosterMercato"]], on=key_cols, how="left")

            # 8) Peso effettivo, normalizzazione a B_r e microscala intera
            df["_weff"] = df["Peso"] * df["MoltiplicatoreScarsita"] * df["BoosterMercato"]
            W = float(pd.to_numeric(df["_weff"], errors="coerce").fillna(0.0).sum())

            if W > 0 and B_r > 0:
                raw = B_r * (df["_weff"] / W)  # prezzi reali (float) prima della microscala
                # passaggio alla microscala in modo stabile (serie con stessa index)
                tmp2 = df[["_weff"]].copy()
                tmp2["p"] = np.asarray(raw, dtype=float)
                tmp2["p"] = np.where(~np.isfinite(tmp2["p"]), 0.0, tmp2["p"])

                target_int = int(np.round(B_r))  # microscala più stabile
                df["PrezzoRicalcolato"] = np.maximum(
                    1,
                    microscala_to_budget_per_role(tmp2, "p", target_int).astype(int).values
                )

            else:
                # fallback robusto
                df["PrezzoRicalcolato"] = 1

            # 9) Pulizia colonne temporanee e ordinamento finale
            df = df.drop(columns=["_weff"], errors="ignore").reset_index(drop=True)
            out[r] = df

        return out


def run_demo(input_path=INPUT_PATH, total_budget=10000):
    models=[("model1_proporzionale", Model1Proporzionale()),
            ("model2_fasce", Model2Fasce()),
            ("model3_pesi_scarsita", Model3PesiScarsita(alpha=0.5,gamma=1.1)),
            ("model4_debias_mercato", Model4DebiasMercato(alpha=0.5))]
    out_paths=[]
    for tag, m in models:
        m.load_xlsx(input_path).set_total_budget(total_budget)
        res=m.run()
        # ordina colonne
        for r, df in res.items():
            for c in ["Tier","Peso","MoltiplicatoreScarsita","BoosterMercato","PrezzoRicalcolato"]:
                if c not in df.columns: df[c]=np.nan
            cols=["Nome","Squadra","Ruolo","RuoloMantra","QuotAttuale","QuotIniziale","Differenza","FVM",
                  "Tier","Peso","MoltiplicatoreScarsita","BoosterMercato","PrezzoRicalcolato"]
            res[r]=df[cols]
        out_xlsx=os.path.join(OUT_DIR, f"{tag}_output.xlsx")
        write_output_xlsx(res, out_xlsx); out_paths.append(out_xlsx)
    return out_paths

""""# Run the demo
paths = run_demo(INPUT_PATH, total_budget=10000)
preview = pd.read_excel(paths[-1], sheet_name="Tutti")
print(paths)"""
