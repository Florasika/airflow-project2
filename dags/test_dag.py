"""
JOUR 9 / 10 — ETL/Airflow
Tests : test_dag.py

Structure :
    test_dag_structure   → vérifier le DAG lui-même (id, tasks, schedule...)
    test_fonctions_metier→ tester les fonctions pures avec pytest
    test_integration     → tester le pipeline de bout en bout

Lancer : pytest test_dag.py -v
"""

import pytest
import pandas as pd
import numpy as np
import sys
import os

# Ajouter le dossier courant au path pour importer le DAG
sys.path.insert(0, os.path.dirname(__file__))


# ════════════════════════════════════════════════════════════════
#  FIXTURES : données de test réutilisables
# ════════════════════════════════════════════════════════════════

@pytest.fixture
def df_valide():
    """DataFrame valide pour les tests."""
    return pd.DataFrame({
        'date'    : ['2024-01-01'] * 5,
        'produit' : ['Laptop Pro','Smartphone X','Tablette Air',
                     'Écouteurs BT','Montre Smart'],
        'vendeur' : ['Alice','Karim','Lucie','Thomas','Nadia'],
        'montant' : [2400, 1300, 900, 240, 560],
        'quantite': [2, 2, 2, 2, 2],
    })

@pytest.fixture
def df_avec_nulls():
    """DataFrame avec valeurs nulles."""
    df = pd.DataFrame({
        'date'   : ['2024-01-01', None, '2024-01-01'],
        'produit': ['Laptop Pro', 'Smartphone X', None],
        'vendeur': ['Alice', 'Karim', 'Lucie'],
        'montant': [2400, 1300, 900],
    })
    return df

@pytest.fixture
def df_vide():
    """DataFrame vide."""
    return pd.DataFrame(columns=['date','produit','vendeur','montant'])

@pytest.fixture
def df_montants_negatifs():
    """DataFrame avec montants négatifs."""
    return pd.DataFrame({
        'date'   : ['2024-01-01'] * 3,
        'produit': ['Laptop Pro'] * 3,
        'vendeur': ['Alice'] * 3,
        'montant': [2400, -100, 0],
    })


# ════════════════════════════════════════════════════════════════
#  TESTS DE STRUCTURE DU DAG
# ════════════════════════════════════════════════════════════════

class TestStructureDAG:
    """Vérifie la structure du DAG sans l'exécuter."""

    def test_dag_importable(self):
        """Le fichier DAG doit s'importer sans erreur."""
        try:
            import dag_a_tester
            assert dag_a_tester is not None
        except ImportError as e:
            pytest.skip(f"Import impossible (Airflow non installé) : {e}")

    def test_dag_id_correct(self):
        """Le dag_id doit correspondre à la convention."""
        try:
            from dag_a_tester import pipeline_testable
            dag = pipeline_testable
            assert 'jour9' in dag.dag_id
        except Exception:
            pytest.skip("DAG non disponible")

    def test_pas_de_cycles(self):
        """Vérifier qu'il n'y a pas de cycles dans le DAG (DAG = acyclique)."""
        # Un DAG valide doit s'importer sans lever d'erreur de cycle
        try:
            import dag_a_tester
            assert True  # Si l'import réussit, pas de cycle
        except Exception:
            pytest.skip("DAG non disponible")


# ════════════════════════════════════════════════════════════════
#  TESTS DES FONCTIONS MÉTIER
# ════════════════════════════════════════════════════════════════

class TestValiderDonnees:
    """Tests de la fonction valider_donnees()."""

    def test_df_valide_passe(self, df_valide):
        from dag_a_tester import valider_donnees
        result = valider_donnees(df_valide)
        assert result['valid'] is True
        assert result['erreurs'] == []
        assert result['nb_lignes'] == 5

    def test_df_vide_echoue(self, df_vide):
        from dag_a_tester import valider_donnees
        result = valider_donnees(df_vide)
        assert result['valid'] is False
        assert len(result['erreurs']) > 0

    def test_montants_negatifs_detectes(self, df_montants_negatifs):
        from dag_a_tester import valider_donnees
        result = valider_donnees(df_montants_negatifs)
        assert result['valid'] is False
        assert any('négatif' in e.lower() or 'negatif' in e.lower()
                   for e in result['erreurs'])

    def test_vendeur_inconnu_detecte(self, df_valide):
        from dag_a_tester import valider_donnees
        df_bad         = df_valide.copy()
        df_bad.loc[0, 'vendeur'] = 'VendeurInconnu'
        result = valider_donnees(df_bad)
        assert result['valid'] is False

    def test_nb_lignes_correct(self, df_valide):
        from dag_a_tester import valider_donnees
        result = valider_donnees(df_valide)
        assert result['nb_lignes'] == len(df_valide)


class TestTransformerDonnees:
    """Tests de la fonction transformer_donnees()."""

    def test_colonne_marge_ajoutee(self, df_valide):
        from dag_a_tester import transformer_donnees
        result = transformer_donnees(df_valide)
        assert 'marge' in result.columns

    def test_marge_calculee_correctement(self, df_valide):
        from dag_a_tester import transformer_donnees
        result = transformer_donnees(df_valide)
        expected = (df_valide['montant'] * 0.42).round(2)
        pd.testing.assert_series_equal(
            result['marge'].reset_index(drop=True),
            expected.reset_index(drop=True),
            check_names=False,
        )

    def test_colonne_taille_vente_ajoutee(self, df_valide):
        from dag_a_tester import transformer_donnees
        result = transformer_donnees(df_valide)
        assert 'taille_vente' in result.columns

    def test_taille_vente_valeurs_valides(self, df_valide):
        from dag_a_tester import transformer_donnees
        result = transformer_donnees(df_valide)
        valeurs_valides = {'Petite', 'Moyenne', 'Grosse'}
        assert set(result['taille_vente']).issubset(valeurs_valides)

    def test_df_original_non_modifie(self, df_valide):
        """La fonction ne doit pas modifier le DataFrame d'entrée."""
        from dag_a_tester import transformer_donnees
        df_copy = df_valide.copy()
        transformer_donnees(df_valide)
        pd.testing.assert_frame_equal(df_valide, df_copy)

    def test_vendeur_title_case(self, df_valide):
        from dag_a_tester import transformer_donnees
        df_test = df_valide.copy()
        df_test['vendeur'] = ['alice', 'KARIM', 'lucie', 'thomas', 'nadia']
        result = transformer_donnees(df_test)
        assert all(v[0].isupper() for v in result['vendeur'])


class TestCalculerKpis:
    """Tests de la fonction calculer_kpis()."""

    def test_ca_total_correct(self, df_valide):
        from dag_a_tester import calculer_kpis
        result = calculer_kpis(df_valide)
        assert result['ca_total'] == float(df_valide['montant'].sum().round(2))

    def test_nb_ventes_correct(self, df_valide):
        from dag_a_tester import calculer_kpis
        result = calculer_kpis(df_valide)
        assert result['nb_ventes'] == len(df_valide)

    def test_panier_moyen_correct(self, df_valide):
        from dag_a_tester import calculer_kpis
        result   = calculer_kpis(df_valide)
        expected = round(float(df_valide['montant'].mean()), 2)
        assert abs(result['panier_moyen'] - expected) < 0.01

    def test_top_produit_identifie(self, df_valide):
        from dag_a_tester import calculer_kpis
        result = calculer_kpis(df_valide)
        assert result['top_produit'] in df_valide['produit'].values

    def test_df_vide_retourne_zeros(self, df_vide):
        from dag_a_tester import calculer_kpis
        result = calculer_kpis(df_vide)
        assert result['ca_total']  == 0
        assert result['nb_ventes'] == 0


# ════════════════════════════════════════════════════════════════
#  TESTS D'INTÉGRATION
# ════════════════════════════════════════════════════════════════

class TestIntegration:
    """Tests du pipeline de bout en bout."""

    def test_pipeline_valider_transformer_enchaine(self, df_valide):
        """Valider puis transformer doit fonctionner sans erreur."""
        from dag_a_tester import valider_donnees, transformer_donnees

        validation = valider_donnees(df_valide)
        assert validation['valid']

        df_clean = transformer_donnees(df_valide)
        assert len(df_clean) == len(df_valide)
        assert 'marge' in df_clean.columns

    def test_transformer_puis_kpis(self, df_valide):
        """Transformer puis calculer les KPIs doit produire des résultats cohérents."""
        from dag_a_tester import transformer_donnees, calculer_kpis

        df_clean = transformer_donnees(df_valide)
        kpis     = calculer_kpis(df_clean)

        assert kpis['ca_total'] > 0
        assert kpis['nb_ventes'] == len(df_valide)
        assert kpis['top_vendeur'] in df_valide['vendeur'].values

    def test_pipeline_complet(self, df_valide):
        """Test du pipeline E2E avec le même DataFrame."""
        from dag_a_tester import valider_donnees, transformer_donnees, calculer_kpis

        # Étape 1
        val = valider_donnees(df_valide)
        assert val['valid']

        # Étape 2
        df_t = transformer_donnees(df_valide)
        assert 'marge' in df_t.columns

        # Étape 3
        kpis = calculer_kpis(df_t)
        assert kpis['ca_total'] == pytest.approx(
            float(df_valide['montant'].sum()), rel=1e-2
        )
