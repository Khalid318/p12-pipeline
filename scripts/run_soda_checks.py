"""
P12 - Sport Data Solution
Exécution des tests de qualité SODA Core.
Lance les checks définis dans checks.yml sur les tables raw.

Installation : pip install soda-core-postgres -- pip install setuptools
"""

import os
import sys
from dotenv import load_dotenv

load_dotenv()

try:
    from soda.scan import Scan
except ImportError:
    print("ERREUR : soda-core-postgres n'est pas installé")
    print("Installe avec : pip install soda-core-postgres")
    sys.exit(1)

# Chemins des fichiers de config
SCRIPT_DIR = os.path.dirname(os.path.abspath(__file__))
PROJECT_DIR = os.path.dirname(SCRIPT_DIR)
CONFIG_FILE = os.path.join(PROJECT_DIR, "soda", "soda_config.yml")
CHECKS_FILE = os.path.join(PROJECT_DIR, "soda", "checks.yml")


def run_checks():
    """Exécute les checks SODA et retourne le résultat."""
    scan = Scan()
    scan.set_data_source_name("sport_data")
    scan.add_configuration_yaml_file(CONFIG_FILE)
    scan.add_sodacl_yaml_file(CHECKS_FILE)

    # Passer les variables d'environnement à SODA
    scan.add_variables({
        "DB_HOST": os.getenv("DB_HOST", "localhost"),
        "DB_PORT": os.getenv("DB_PORT", "5432"),
        "DB_NAME": os.getenv("DB_NAME", "sport_data"),
        "DB_USER": os.getenv("DB_USER", ""),
        "DB_PASSWORD": os.getenv("DB_PASSWORD", ""),
    })

    scan.execute()

    # Afficher les résultats
    print("\n" + "=" * 60)
    print("RESULTATS DES TESTS DE QUALITE (SODA)")
    print("=" * 60)

    results = scan.get_scan_results()
    checks = results.get("checks", [])

    passed = 0
    failed = 0
    for check in checks:
        name = check.get("name", "sans nom")
        outcome = check.get("outcome", "unknown")
        if outcome == "pass":
            print(f"  [PASS] {name}")
            passed += 1
        else:
            print(f"  [FAIL] {name}")
            diag = check.get("diagnostics", {})
            for key, val in diag.items():
                print(f"         {key}: {val}")
            failed += 1

    print(f"\n  Total : {passed} passed, {failed} failed")
    print("=" * 60)

    # Si des checks échouent, on lève une erreur (Airflow verra le task en rouge)
    if scan.has_check_fails():
        print("\nDes tests de qualite ont echoue !")
        sys.exit(1)
    else:
        print("\nTous les tests de qualite sont passes.")


if __name__ == "__main__":
    run_checks()
