import pandas as pd
import sys
sys.path.insert(0, '.')
from dag_final_etl import valider_dataframe, enrichir_donnees, calculer_kpis

def test_pipeline_fonctions():
    df = pd.DataFrame({
        'date':['2024-01-01']*3,
        'produit':['Laptop Pro','Smartphone X','Écouteurs BT'],
        'vendeur':['Alice','Karim','Lucie'],
        'montant':[2400, 1300, 240],
        'quantite':[2,2,2],
    })
    val = valider_dataframe(df, min_lignes=2)
    assert val['valid']

    df_e = enrichir_donnees(df)
    assert 'marge' in df_e.columns

    kpis = calculer_kpis(df_e, objectif=5000)
    assert kpis['ca_total'] == 3940.0
    print("✓ Tous les tests passent")

test_pipeline_fonctions()
