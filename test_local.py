"""
Test local du pipeline — SANS déploiement, SANS appels réseau réels.

On simule (monkeypatch) les appels Zoom et l'envoi SMTP, puis on injecte un
faux payload `meeting.ended`. Objectifs :
  1. Valider la transformation Pandas (dédoublonnage, agrégation, ratio).
  2. Générer un aperçu HTML (bilan_preview.html) à ouvrir dans le navigateur.
  3. Rejouer le flux complet de `main(request)` de bout en bout.

Lancement :  python test_local.py
"""

import json
import sys
from unittest import mock

# Console Windows : force l'UTF-8 pour afficher les emojis/accents sans planter.
try:
    sys.stdout.reconfigure(encoding="utf-8")
except (AttributeError, ValueError):
    pass

import main


# ---------------------------------------------------------------------------
# Fausses données participants (avec cas réels à tester)
# ---------------------------------------------------------------------------
# - alice : 2 sessions (déconnexion/reconnexion) -> doit être regroupée
# - bob   : 1 session complète
# - carol : PAS d'email -> dédoublonnage par nom, 2 sessions
# - dan   : présence partielle -> ratio faible
FAKE_PARTICIPANTS_DASHBOARD = [
    {"user_name": "Alice Martin", "email": "alice@oued.io",
     "join_time": "2026-07-17T09:00:00Z", "leave_time": "2026-07-17T09:20:00Z",
     "duration": 1200, "leave_reason": "left the meeting",
     "device": "Windows", "network_type": "Wired", "location": "Paris"},
    {"user_name": "Alice Martin", "email": "alice@oued.io",
     "join_time": "2026-07-17T09:25:00Z", "leave_time": "2026-07-17T10:00:00Z",
     "duration": 2100, "leave_reason": "host ended the meeting",
     "device": "Windows", "network_type": "WiFi", "location": "Paris"},
    {"user_name": "Bob Diallo", "email": "bob@oued.io",
     "join_time": "2026-07-17T09:02:00Z", "leave_time": "2026-07-17T10:00:00Z",
     "duration": 3480, "leave_reason": "host ended the meeting",
     "device": "Mac", "network_type": "WiFi", "location": "Dakar"},
    {"user_name": "Carol (invitée)", "email": "",
     "join_time": "2026-07-17T09:05:00Z", "leave_time": "2026-07-17T09:15:00Z",
     "duration": 600, "leave_reason": "network connection error",
     "device": "iPhone", "network_type": "Cellular", "location": "Abidjan"},
    {"user_name": "Carol (invitée)", "email": "",
     "join_time": "2026-07-17T09:40:00Z", "leave_time": "2026-07-17T09:58:00Z",
     "duration": 1080, "leave_reason": "left the meeting",
     "device": "iPhone", "network_type": "WiFi", "location": "Abidjan"},
    {"user_name": "Dan Koffi", "email": "dan@oued.io",
     "join_time": "2026-07-17T09:50:00Z", "leave_time": "2026-07-17T10:00:00Z",
     "duration": 600, "leave_reason": "host ended the meeting",
     "device": "Android", "network_type": "Cellular", "location": "Lomé"},
]

MEETING_TOTAL_SECONDS = 3600  # réunion d'1h


def _fake_request(body_dict):
    """Simule l'objet `request` de Flask/GCF (get_data + headers)."""
    raw = json.dumps(body_dict)
    return mock.Mock(
        get_data=lambda as_text=False: raw,
        headers={},  # pas de signature -> ignorée sans secret configuré
    )


def _captured_send_email(html_body, topic):
    """Remplace l'envoi SMTP : écrit l'aperçu HTML sur disque."""
    with open("bilan_preview.html", "w", encoding="utf-8") as fh:
        fh.write(html_body)
    print(f"  [email simulé] sujet = '📊 Bilan de la réunion : {topic}'")
    print("  [email simulé] aperçu écrit dans bilan_preview.html")


def test_transformation():
    """Vérifie le dédoublonnage / l'agrégation / le ratio sur les fausses données."""
    print("\n=== 1. Test de transform_data ===")
    rows = main._normalize_dashboard(FAKE_PARTICIPANTS_DASHBOARD)
    df, total = main.transform_data(rows, MEETING_TOTAL_SECONDS)

    assert len(df) == 4, f"Attendu 4 participants uniques, obtenu {len(df)}"
    alice = df[df["email"] == "alice@oued.io"].iloc[0]
    assert alice["sessions"] == 2, "Alice devrait avoir 2 sessions regroupées"
    assert abs(alice["total_seconds"] - 3300) < 1, "Durée cumulée d'Alice = 1200+2100"
    # Dernière déconnexion chronologique conservée
    assert alice["leave_reason"] == "host ended the meeting"
    carol = df[df["name"] == "Carol (invitée)"].iloc[0]
    assert carol["sessions"] == 2, "Carol (sans email) regroupée par nom"

    print(df[["name", "email", "sessions", "duree_min",
              "presence_ratio", "leave_reason", "device", "network_type"]]
          .to_string(index=False))
    print("  ✅ Dédoublonnage, agrégation et ratio OK")
    return df, total


def test_full_pipeline():
    """Rejoue main(request) de bout en bout avec le réseau et SMTP simulés."""
    print("\n=== 2. Test du flux complet main(request) ===")
    fake_event = {
        "event": "meeting.ended",
        "payload": {"object": {
            "uuid": "aBcD1234EfGh==",
            "id": "1234567890",
            "topic": "Réunion d'équipe Oued",
        }},
    }

    with mock.patch.object(main, "get_zoom_token", return_value="fake-token"), \
         mock.patch.object(main, "get_meeting_details",
                           return_value=("Réunion d'équipe Oued", MEETING_TOTAL_SECONDS)), \
         mock.patch.object(main, "extract_data",
                           return_value=(main._normalize_dashboard(FAKE_PARTICIPANTS_DASHBOARD),
                                         "dashboard")), \
         mock.patch.object(main, "send_email", side_effect=_captured_send_email):
        status_body, status_code = main.main(_fake_request(fake_event))[:2]

    assert status_code == 200, f"Attendu 200, obtenu {status_code}"
    print(f"  Réponse HTTP : {status_code} ('{status_body}')")
    print("  ✅ Flux complet OK")


def test_url_validation():
    """Vérifie la réponse au challenge de validation d'URL Zoom."""
    print("\n=== 3. Test du challenge endpoint.url_validation ===")
    import os
    os.environ["ZOOM_WEBHOOK_SECRET_TOKEN"] = "secret-de-test"
    body = {"event": "endpoint.url_validation",
            "payload": {"plainToken": "abc123"}}
    resp = main.main(_fake_request(body))
    data = json.loads(resp[0])
    assert data["plainToken"] == "abc123"
    assert "encryptedToken" in data and len(data["encryptedToken"]) == 64
    print(f"  Réponse : {data}")
    print("  ✅ Validation d'URL OK")
    del os.environ["ZOOM_WEBHOOK_SECRET_TOKEN"]


if __name__ == "__main__":
    test_transformation()
    test_full_pipeline()
    test_url_validation()
    print("\n🎉 Tous les tests locaux sont passés. "
          "Ouvre bilan_preview.html pour voir le tableau de bord.")
