import os
import pandas as pd

from app.api.utils.math_models import Model4DebiasMercato, write_output_xlsx

# --- parametri della tua lega
N_TEAMS = 10
SLOTS_PER_ROLE = {"P": 3, "D": 8, "C": 8, "A": 6}  # adatta ai tuoi roster
TOTAL_BUDGET_ALL_TEAMS = 10000   # esempio: 10 squadre * 1000 crediti
OUT_DIR = "./data/out_data"
INPUT_PATH = "./data/Quotazioni_Fantacalcio_Stagione_2025_26.xlsx"

# --- istanzia Model4 con i settaggi “buoni”
m4 = Model4DebiasMercato(
    # split del budget per ruolo (se vuoi personalizzarlo):
    budget_split={"P": 0.10, "D": 0.20, "C": 0.30, "A": 0.40},
    # quantili per i tier (Top/High/Med/Low):
    quantiles=(0.0, 0.15, 0.45, 0.80, 1.0),
    # appetibilità target tra le fasce (usata nella scarsità per fascia):
    appetibilita={"Top": 3.0, "High": 2.0, "Medium": 1.2, "Low": 0.8},
    # esponente scarsità per fascia:
    alpha=0.5,

    # >>> parametri “effettivi” + BLENDING morbido:
    n_teams=N_TEAMS,
    slots_per_role=SLOTS_PER_ROLE,
    overshoot=0.15,                     # considera ~15% di giocatori in più del minimo necessario
    selection_metric="QuotAttuale",     # metrica per ordinare e tagliare i migliori
    # blending morbido tra i moltiplicatori di fascia:
    #blend_slope=30.0,                   # morbidezza della transizione (più alto = transizione più “larga”)

    # (facoltativi) curve per ruolo:
    #gamma_map={"A": 1.25, "C": 1.15, "D": 1.10, "P": 1.05},
    #eta_map={"A": 1.0, "C": 1.0, "D": 1.0, "P": 1.0},
    #kappa_map={"A": 0.18, "C": 0.12, "D": 0.08, "P": 0.05},
    #lambda_map={"A": 2.2, "C": 2.0, "D": 1.8, "P": 1.5},
    #delta=0.8,

    # >>> SUPER COMPRESS D/C: abbassa i top e azzera quasi il booster
    gamma_map = {"A": 1.25, "C": 0.80, "D": 0.62, "P": 1.05},   # <1 = curva concava (schiaccia i top)
    eta_map   = {"A": 1.00, "C": 0.90, "D": 0.70, "P": 1.00},   # <1 = ulteriore compressione
    kappa_map = {"A": 0.18, "C": 0.10, "D": 0.00, "P": 0.05},   # 0 = niente “premio” ai primissimi
    lambda_map= {"A": 2.20, "C": 4.00, "D": 6.00, "P": 1.50},   # alto = booster svanisce subito
    delta=0.8,
)

# --- esecuzione
m4.load_xlsx(INPUT_PATH).set_total_budget(TOTAL_BUDGET_ALL_TEAMS)
res = m4.run()

# --- ordinamento colonne ed export
for r, df in res.items():
    for c in ["Tier", "Peso", "MoltiplicatoreScarsita", "BoosterMercato", "PrezzoRicalcolato"]:
        if c not in df.columns:
            df[c] = pd.NA
    cols = [
        "Nome","Squadra","Ruolo","RuoloMantra","QuotAttuale","QuotIniziale","Differenza","FVM",
        "Tier","Peso","MoltiplicatoreScarsita","BoosterMercato","PrezzoRicalcolato"
    ]
    # tieni solo le colonne presenti (se il listone non le ha tutte)
    res[r] = df[[c for c in cols if c in df.columns]]

os.makedirs(OUT_DIR, exist_ok=True)
out_xlsx = os.path.join(OUT_DIR, "model4_debias_mercato_output.xlsx")
write_output_xlsx(res, out_xlsx)

print("Output salvato in:", out_xlsx)
preview = pd.read_excel(out_xlsx, sheet_name="Tutti")
print("Righe in 'Tutti':", len(preview))
