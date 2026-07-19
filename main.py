"""
Pipeline ETL serverless : bilan de présence Zoom par email.

Déclencheur : Webhook Zoom `meeting.ended` sur une Google Cloud Function (HTTP).
Flux : validation webhook -> OAuth Server-to-Server -> extraction (Dashboard
puis fallback Report) -> transformation Pandas -> email HTML via SMTP.

Point de terminaison HTTP GCF : `main(request)`.
"""

import base64
import hashlib
import hmac
import json
import logging
import os
import urllib.parse
from datetime import datetime, timezone
from email.mime.multipart import MIMEMultipart
from email.mime.text import MIMEText

import pandas as pd
import requests
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger("zoom-attendance")

ZOOM_OAUTH_URL = "https://zoom.us/oauth/token"
ZOOM_API_BASE = "https://api.zoom.us/v2"
HTTP_TIMEOUT = 30  # secondes


# ---------------------------------------------------------------------------
# Session HTTP robuste (timeout + retries avec backoff sur 429/5xx)
# ---------------------------------------------------------------------------
def _build_session():
    """Retourne une `requests.Session` avec une politique de retry."""
    session = requests.Session()
    retry = Retry(
        total=4,
        backoff_factor=1.5,  # 0s, 1.5s, 3s, 6s...
        status_forcelist=(429, 500, 502, 503, 504),
        allowed_methods=frozenset(["GET", "POST"]),
        respect_retry_after_header=True,
    )
    adapter = HTTPAdapter(max_retries=retry)
    session.mount("https://", adapter)
    return session


SESSION = _build_session()


# ---------------------------------------------------------------------------
# Configuration / variables d'environnement
# ---------------------------------------------------------------------------
def _require_env(name):
    """Lit une variable d'env obligatoire, lève une erreur explicite sinon."""
    value = os.environ.get(name)
    if not value:
        raise RuntimeError(f"Variable d'environnement manquante : {name}")
    return value


def _encode_meeting_id(meeting_id):
    """
    Encode l'identifiant de réunion pour l'URL.

    Les UUID Zoom peuvent contenir des `/` ou commencer par `/`. Dans ce cas,
    l'API exige un DOUBLE URL-encodage. Les IDs numériques passent tels quels.
    """
    meeting_id = str(meeting_id)
    if meeting_id.startswith("/") or "//" in meeting_id:
        return urllib.parse.quote(urllib.parse.quote(meeting_id, safe=""), safe="")
    return urllib.parse.quote(meeting_id, safe="")


# ---------------------------------------------------------------------------
# 0. Sécurité du webhook (validation d'URL + signature)
# ---------------------------------------------------------------------------
def verify_zoom_signature(headers, raw_body):
    """
    Vérifie la signature `x-zm-signature` d'un événement Zoom.

    Message signé = "v0:{timestamp}:{raw_body}" ; HMAC-SHA256 avec le
    Webhook Secret Token. Renvoie True si valide (ou si aucun secret n'est
    configuré, pour rester tolérant en dev).
    """
    secret = os.environ.get("ZOOM_WEBHOOK_SECRET_TOKEN")
    if not secret:
        logger.warning("ZOOM_WEBHOOK_SECRET_TOKEN absent : vérification de signature ignorée.")
        return True

    signature = headers.get("x-zm-signature")
    timestamp = headers.get("x-zm-request-timestamp")
    if not signature or not timestamp:
        return False

    message = f"v0:{timestamp}:{raw_body}"
    expected = "v0=" + hmac.new(
        secret.encode("utf-8"), message.encode("utf-8"), hashlib.sha256
    ).hexdigest()
    return hmac.compare_digest(expected, signature)


def build_url_validation_response(plain_token):
    """
    Construit la réponse au challenge `endpoint.url_validation` de Zoom.

    Zoom envoie un `plainToken` ; on renvoie `plainToken` + son HMAC-SHA256.
    """
    secret = _require_env("ZOOM_WEBHOOK_SECRET_TOKEN")
    encrypted = hmac.new(
        secret.encode("utf-8"), plain_token.encode("utf-8"), hashlib.sha256
    ).hexdigest()
    return {"plainToken": plain_token, "encryptedToken": encrypted}


# ---------------------------------------------------------------------------
# 2. Authentification : Server-to-Server OAuth
# ---------------------------------------------------------------------------
def get_zoom_token():
    """
    Récupère un access token via Server-to-Server OAuth (account_credentials).

    Auth Basic avec Client ID / Client Secret ; l'Account ID passe en query.
    """
    account_id = _require_env("ZOOM_ACCOUNT_ID")
    client_id = _require_env("ZOOM_CLIENT_ID")
    client_secret = _require_env("ZOOM_CLIENT_SECRET")

    basic = base64.b64encode(f"{client_id}:{client_secret}".encode()).decode()
    try:
        resp = SESSION.post(
            ZOOM_OAUTH_URL,
            params={"grant_type": "account_credentials", "account_id": account_id},
            headers={"Authorization": f"Basic {basic}"},
            timeout=HTTP_TIMEOUT,
        )
        resp.raise_for_status()
    except requests.RequestException as exc:
        logger.error("Échec de l'obtention du token Zoom : %s", exc)
        raise
    return resp.json()["access_token"]


# ---------------------------------------------------------------------------
# 3. Extraction : Dashboard prioritaire, fallback Report
# ---------------------------------------------------------------------------
def _paginate(url, token, params):
    """Itère sur toutes les pages d'un endpoint paginé Zoom (next_page_token)."""
    headers = {"Authorization": f"Bearer {token}"}
    records = []
    next_token = None
    while True:
        page_params = dict(params)
        page_params["page_size"] = 300
        if next_token:
            page_params["next_page_token"] = next_token
        resp = SESSION.get(url, headers=headers, params=page_params, timeout=HTTP_TIMEOUT)
        resp.raise_for_status()
        payload = resp.json()
        records.extend(payload.get("participants", []))
        next_token = payload.get("next_page_token")
        if not next_token:
            break
    return records


def _normalize_dashboard(records):
    """Normalise les enregistrements Dashboard vers le schéma commun."""
    rows = []
    for r in records:
        rows.append(
            {
                "name": (r.get("user_name") or "").strip(),
                "email": (r.get("email") or "").strip(),
                "join_time": r.get("join_time"),
                "leave_time": r.get("leave_time"),
                "duration": r.get("duration"),  # secondes (souvent absent -> calculé)
                "leave_reason": r.get("leave_reason") or "N/A",
                "device": r.get("device") or "N/A",
                "network_type": r.get("network_type") or "N/A",
                "location": r.get("location") or "N/A",
            }
        )
    return rows


def _normalize_report(records):
    """Normalise les enregistrements Report vers le schéma commun."""
    rows = []
    for r in records:
        rows.append(
            {
                "name": (r.get("name") or "").strip(),
                "email": (r.get("user_email") or "").strip(),
                "join_time": r.get("join_time"),
                "leave_time": r.get("leave_time"),
                "duration": r.get("duration"),  # secondes
                "leave_reason": r.get("leave_reason") or "N/A",
                "device": "N/A",
                "network_type": "N/A",
                "location": "N/A",
            }
        )
    return rows


def extract_data(meeting_id, token):
    """
    Extrait la liste des participants, Dashboard prioritaire puis fallback Report.

    Le Dashboard (/metrics) garantit leave_reason / device / network mais exige
    un plan Business+ et une réunion assez récente. En cas d'échec (403/400,
    réunion trop ancienne, aucun participant), on bascule sur le Report (/report).
    Retourne (rows, source).
    """
    encoded_id = _encode_meeting_id(meeting_id)

    # --- Tentative 1 : Dashboard ---
    try:
        url = f"{ZOOM_API_BASE}/metrics/meetings/{encoded_id}/participants"
        records = _paginate(url, token, params={"type": "past"})
        if records:
            logger.info("Extraction Dashboard OK : %d participants.", len(records))
            return _normalize_dashboard(records), "dashboard"
        logger.info("Dashboard n'a renvoyé aucun participant, fallback Report.")
    except requests.RequestException as exc:
        logger.warning("Dashboard indisponible (%s), fallback Report.", exc)

    # --- Tentative 2 : Report ---
    try:
        url = f"{ZOOM_API_BASE}/report/meetings/{encoded_id}/participants"
        records = _paginate(url, token, params={})
        logger.info("Extraction Report OK : %d participants.", len(records))
        return _normalize_report(records), "report"
    except requests.RequestException as exc:
        logger.error("Échec de l'extraction Report : %s", exc)
        raise


def get_meeting_details(meeting_id, token):
    """
    Récupère le topic et la durée totale de la réunion via le Report meeting.

    La durée est en minutes côté API ; on la convertit en secondes.
    Renvoie (topic, duree_totale_secondes | None).
    """
    encoded_id = _encode_meeting_id(meeting_id)
    headers = {"Authorization": f"Bearer {token}"}
    try:
        resp = SESSION.get(
            f"{ZOOM_API_BASE}/report/meetings/{encoded_id}",
            headers=headers,
            timeout=HTTP_TIMEOUT,
        )
        resp.raise_for_status()
        data = resp.json()
        topic = data.get("topic")
        minutes = data.get("duration")
        total_seconds = int(minutes) * 60 if minutes else None
        return topic, total_seconds
    except requests.RequestException as exc:
        logger.warning("Détails de la réunion indisponibles : %s", exc)
        return None, None


# ---------------------------------------------------------------------------
# 4. Transformation (Pandas)
# ---------------------------------------------------------------------------
def _parse_dt(series):
    """Convertit une colonne ISO 8601 en datetime UTC (tolérant aux erreurs)."""
    return pd.to_datetime(series, utc=True, errors="coerce")


def transform_data(rows, meeting_total_seconds=None):
    """
    Nettoie et enrichit les données participants avec Pandas.

    - Dédoublonnage : regroupe les reconnexions par participant (email sinon nom).
    - Agrégation : durée totale sommée, première arrivée (min join_time),
      dernier départ (max leave_time).
    - Enrichissement : ratio de présence (% de la durée totale de la réunion).
    - Analyse : conserve la dernière raison de déconnexion / device / réseau.

    Retourne (DataFrame trié, duree_reunion_secondes).
    """
    if not rows:
        return pd.DataFrame(), meeting_total_seconds or 0

    df = pd.DataFrame(rows)
    df["join_time"] = _parse_dt(df["join_time"])
    df["leave_time"] = _parse_dt(df["leave_time"])

    # Durée par ligne : valeur API si dispo, sinon calcul leave - join.
    calc = (df["leave_time"] - df["join_time"]).dt.total_seconds()
    df["duration"] = pd.to_numeric(df["duration"], errors="coerce").fillna(calc)
    df["duration"] = df["duration"].clip(lower=0).fillna(0)

    # Clé d'identité : email si présent, sinon nom normalisé.
    df["participant_key"] = df["email"].str.lower().where(
        df["email"].str.len() > 0, df["name"].str.lower()
    )

    # Trie par arrivée pour que "last" = dernière déconnexion chronologique.
    df = df.sort_values("join_time")

    def _last_valid(series, default="N/A"):
        valides = series[(series.notna()) & (series != "N/A") & (series != "")]
        return valides.iloc[-1] if len(valides) else default

    grouped = df.groupby("participant_key", dropna=False).agg(
        name=("name", "first"),
        email=("email", "first"),
        join_time=("join_time", "min"),
        leave_time=("leave_time", "max"),
        total_seconds=("duration", "sum"),
        sessions=("duration", "size"),
        leave_reason=("leave_reason", _last_valid),
        device=("device", _last_valid),
        network_type=("network_type", _last_valid),
        location=("location", _last_valid),
    ).reset_index(drop=True)

    # Durée de référence de la réunion pour le ratio de présence.
    if not meeting_total_seconds:
        span = (df["leave_time"].max() - df["join_time"].min()).total_seconds()
        meeting_total_seconds = span if span and span > 0 else grouped["total_seconds"].max()

    if meeting_total_seconds and meeting_total_seconds > 0:
        grouped["presence_ratio"] = (
            grouped["total_seconds"] / meeting_total_seconds * 100
        ).clip(upper=100).round(1)
    else:
        grouped["presence_ratio"] = 0.0

    grouped["duree_min"] = (grouped["total_seconds"] / 60).round(1)
    grouped = grouped.sort_values("total_seconds", ascending=False).reset_index(drop=True)
    return grouped, meeting_total_seconds


# ---------------------------------------------------------------------------
# 5. Rendu HTML
# ---------------------------------------------------------------------------
def _fmt_time(dt):
    """Formate un datetime en HH:MM:SS UTC, tolérant au NaT."""
    if pd.isna(dt):
        return "—"
    return dt.strftime("%d/%m %H:%M:%S")


def render_html(df, topic, meeting_total_seconds, source):
    """Transforme le DataFrame en un tableau HTML stylisé (email-friendly)."""
    if df.empty:
        return f"<p>Aucun participant trouvé pour la réunion « {topic} ».</p>"

    duree_reunion = round((meeting_total_seconds or 0) / 60, 1)
    rows_html = []
    for _, r in df.iterrows():
        ratio = r["presence_ratio"]
        # Barre de présence colorée selon le ratio.
        if ratio >= 80:
            couleur = "#16a34a"
        elif ratio >= 50:
            couleur = "#d97706"
        else:
            couleur = "#dc2626"
        barre = (
            f'<div style="background:#e5e7eb;border-radius:4px;width:90px;height:14px;">'
            f'<div style="background:{couleur};width:{min(ratio,100):.0f}%;height:14px;'
            f'border-radius:4px;"></div></div>'
        )
        rows_html.append(
            f"""
            <tr>
              <td style="padding:8px 10px;border-bottom:1px solid #eee;">{r['name'] or '—'}</td>
              <td style="padding:8px 10px;border-bottom:1px solid #eee;color:#555;">{r['email'] or '—'}</td>
              <td style="padding:8px 10px;border-bottom:1px solid #eee;text-align:center;">{_fmt_time(r['join_time'])}</td>
              <td style="padding:8px 10px;border-bottom:1px solid #eee;text-align:center;">{_fmt_time(r['leave_time'])}</td>
              <td style="padding:8px 10px;border-bottom:1px solid #eee;text-align:right;">{r['duree_min']:.1f} min</td>
              <td style="padding:8px 10px;border-bottom:1px solid #eee;">
                <div style="display:flex;align-items:center;gap:6px;">{barre}<span>{ratio:.0f}%</span></div>
              </td>
              <td style="padding:8px 10px;border-bottom:1px solid #eee;text-align:center;">{int(r['sessions'])}</td>
              <td style="padding:8px 10px;border-bottom:1px solid #eee;color:#555;">{r['leave_reason']}</td>
              <td style="padding:8px 10px;border-bottom:1px solid #eee;color:#555;">{r['device']}</td>
              <td style="padding:8px 10px;border-bottom:1px solid #eee;color:#555;">{r['network_type']}</td>
            </tr>"""
        )

    generated = datetime.now(timezone.utc).strftime("%d/%m/%Y %H:%M UTC")
    source_label = "Dashboard (metrics)" if source == "dashboard" else "Report"
    return f"""
    <div style="font-family:-apple-system,Segoe UI,Roboto,Helvetica,Arial,sans-serif;color:#111;max-width:1000px;">
      <h2 style="margin-bottom:4px;">📊 Bilan de la réunion : {topic}</h2>
      <p style="color:#666;margin-top:0;">
        {len(df)} participant(s) &middot; durée réunion ~{duree_reunion:.0f} min
        &middot; source : {source_label} &middot; généré le {generated}
      </p>
      <table style="border-collapse:collapse;width:100%;font-size:13px;">
        <thead>
          <tr style="background:#111;color:#fff;text-align:left;">
            <th style="padding:10px;">Participant</th>
            <th style="padding:10px;">Email</th>
            <th style="padding:10px;text-align:center;">Arrivée</th>
            <th style="padding:10px;text-align:center;">Départ</th>
            <th style="padding:10px;text-align:right;">Durée</th>
            <th style="padding:10px;">Ratio de présence</th>
            <th style="padding:10px;text-align:center;">Sessions</th>
            <th style="padding:10px;">Dernière déconnexion</th>
            <th style="padding:10px;">Device</th>
            <th style="padding:10px;">Réseau</th>
          </tr>
        </thead>
        <tbody>{''.join(rows_html)}</tbody>
      </table>
      <p style="color:#999;font-size:11px;margin-top:14px;">
        « Sessions » = nombre de connexions distinctes (reconnexions incluses).
        Ratio de présence = durée cumulée du participant / durée totale de la réunion.
      </p>
    </div>"""


# ---------------------------------------------------------------------------
# 6. Notification email (SMTP)
# ---------------------------------------------------------------------------
import os
import requests

def send_email(html_content, topic):
    # Récupération de la clé depuis l'environnement Render
    api_key = os.environ.get("RESEND_API_KEY")
    url = "https://api.resend.com/emails"
    
    headers = {
        "Authorization": f"Bearer {api_key}",
        "Content-Type": "application/json"
    }
    
    # Sur l'offre gratuite, Resend impose d'utiliser cette adresse d'expédition
    payload = {
        "from": "onboarding@resend.dev",
        "to": [os.environ.get("EMAIL_TO")],
        "subject": f"📊 Bilan de la réunion : {topic}",
        "html": html_content
    }
    
    response = requests.post(url, json=payload, headers=headers)
    
    if response.status_code in (200, 201):
        print("✅ Email envoyé avec succès via l'API Resend !")
    else:
        print(f"❌ Erreur lors de l'envoi de l'email : {response.text}")
        response.raise_for_status()

# ---------------------------------------------------------------------------
# Orchestration ETL
# ---------------------------------------------------------------------------
def run_pipeline(meeting_id, fallback_topic=None):
    """Enchaîne extraction -> transformation -> rendu -> email pour un meeting."""
    token = get_zoom_token()
    topic, total_seconds = get_meeting_details(meeting_id, token)
    topic = topic or fallback_topic or "Réunion Zoom"

    rows, source = extract_data(meeting_id, token)
    df, meeting_total_seconds = transform_data(rows, total_seconds)
    html = render_html(df, topic, meeting_total_seconds, source)
    send_email(html, topic)
    return {"participants": int(len(df)), "source": source, "topic": topic}


# ---------------------------------------------------------------------------
# Point d'entrée Google Cloud Function (HTTP)
# ---------------------------------------------------------------------------
def main(request):
    """
    Entrée HTTP GCF déclenchée par le webhook Zoom.

    Renvoie TOUJOURS 200 après un événement traité (sinon Zoom réessaie en
    boucle et peut désactiver le webhook), tout en loggant les erreurs internes.
    """
    raw_body = request.get_data(as_text=True) or ""
    try:
        body = json.loads(raw_body) if raw_body else {}
    except json.JSONDecodeError:
        logger.error("Corps de requête non-JSON.")
        return ("Bad Request", 400)

    event = body.get("event")

    # 0a. Challenge de validation d'URL (configuration du webhook côté Zoom).
    if event == "endpoint.url_validation":
        plain_token = body.get("payload", {}).get("plainToken", "")
        return (json.dumps(build_url_validation_response(plain_token)),
                200, {"Content-Type": "application/json"})

    # 0b. Vérification de la signature des événements réels.
    if not verify_zoom_signature(request.headers, raw_body):
        logger.warning("Signature Zoom invalide, requête rejetée.")
        return ("Unauthorized", 401)

    if event != "meeting.ended":
        logger.info("Événement ignoré : %s", event)
        return ("Ignored", 200)

    # 1. Extraction du meetingId depuis le payload (UUID prioritaire).
    meeting_obj = body.get("payload", {}).get("object", {})
    meeting_uuid = meeting_obj.get("uuid")
    meeting_num_id = meeting_obj.get("id")
    meeting_id = meeting_uuid or meeting_num_id
    topic = meeting_obj.get("topic")

    if not meeting_id:
        logger.error("Aucun identifiant de réunion dans le payload.")
        return ("No meeting id", 200)

    try:
        result = run_pipeline(meeting_id, fallback_topic=topic)
        logger.info("Pipeline terminé : %s", result)
        return ("OK", 200)
    except Exception as exc:  # noqa: BLE001 - on absorbe pour ne pas faire réessayer Zoom
        logger.exception("Échec du pipeline pour meeting %s : %s", meeting_id, exc)
        return ("Erreur interne loggée", 200)
