# Guide — Pipeline ETL présences Zoom

## 1. Structure du payload `meeting.ended`

Zoom envoie un `POST` JSON à ta Cloud Function. Voici ce que la fonction lit :

```json
{
  "event": "meeting.ended",
  "event_ts": 1690000000000,
  "payload": {
    "account_id": "AbCdEfG",
    "object": {
      "id": "1234567890",
      "uuid": "aBcD1234EfGh==",          // <-- utilisé en priorité (récurrences)
      "host_id": "xxxxxx",
      "topic": "Réunion d'équipe Oued",   // <-- utilisé comme titre de secours
      "type": 2,
      "start_time": "2026-07-17T09:00:00Z",
      "end_time":   "2026-07-17T10:00:00Z",
      "duration": 60,
      "timezone": "Europe/Paris"
    }
  }
}
```

- On extrait `payload.object.uuid` (sinon `id`) → `meetingId`.
- `payload.object.topic` sert de titre si l'appel « détails réunion » échoue.
- ⚠️ L'`uuid` peut contenir `/` ou `+` : le code le **double-URL-encode** automatiquement.

### Événement de validation d'URL (à la config du webhook)
Quand tu enregistres l'endpoint dans Zoom, Zoom envoie d'abord :
```json
{ "event": "endpoint.url_validation",
  "payload": { "plainToken": "abc123" } }
```
La fonction répond `{ "plainToken": "...", "encryptedToken": "HMAC-SHA256(plainToken)" }`.

---

## 2. Variables d'environnement

| Variable | Rôle |
|---|---|
| `ZOOM_ACCOUNT_ID` | Server-to-Server OAuth : Account ID |
| `ZOOM_CLIENT_ID` | Client ID de l'app S2S |
| `ZOOM_CLIENT_SECRET` | Client Secret de l'app S2S |
| `ZOOM_WEBHOOK_SECRET_TOKEN` | Secret Token du webhook (validation + signature) |
| `SMTP_HOST` | ex: `smtp.gmail.com` |
| `SMTP_PORT` | ex: `587` (STARTTLS) |
| `SMTP_USER` | identifiant SMTP |
| `SMTP_PASSWORD` | mot de passe / app password |
| `EMAIL_FROM` | expéditeur (défaut = `SMTP_USER`) |
| `EMAIL_TO` | destinataire(s), séparés par des virgules |

> Ne mets JAMAIS ces valeurs dans le code. Sur GCF, utilise `--set-env-vars` ou, mieux, Secret Manager.

---

## 3. Scopes Zoom nécessaires (app Server-to-Server OAuth)

- `dashboard_meetings:read:admin` → endpoint **Dashboard** (`/metrics/...`, plan Business+)
- `report_meetings:read:admin` → endpoint **Report** (`/report/...`, fallback, dès le plan Pro)
- Active l'event **`meeting.ended`** dans la section *Feature > Event Subscriptions* et colle l'URL de la fonction.

---

## 4. Déploiement (Google Cloud Functions, 2e gén.)

```bash
gcloud functions deploy zoom-attendance \
  --gen2 \
  --runtime=python311 \
  --region=europe-west1 \
  --source=. \
  --entry-point=main \
  --trigger-http \
  --allow-unauthenticated \
  --memory=512Mi \
  --timeout=120s \
  --set-env-vars=ZOOM_ACCOUNT_ID=...,ZOOM_CLIENT_ID=...,ZOOM_CLIENT_SECRET=...,ZOOM_WEBHOOK_SECRET_TOKEN=...,SMTP_HOST=smtp.gmail.com,SMTP_PORT=587,SMTP_USER=...,SMTP_PASSWORD=...,EMAIL_FROM=...,EMAIL_TO=...
```

- `--allow-unauthenticated` est nécessaire pour que Zoom joigne l'URL ; la sécurité est
  assurée par la **vérification de signature HMAC** dans le code.
- Récupère l'URL renvoyée par le déploiement et colle-la dans Zoom (Event Subscription).
- Pandas peut demander un peu de mémoire → 512 Mi conseillé.

---

## 5. Logique Dashboard → Report (option « les deux »)

1. Appel **Dashboard** `/metrics/meetings/{id}/participants?type=past` → champs riches
   (`leave_reason`, `device`, `network_type`, `location`).
2. Si échec (403 plan insuffisant, 400 réunion trop ancienne, 0 participant) →
   fallback **Report** `/report/meetings/{id}/participants` (durée fiable, champs
   device/réseau à `N/A`).
3. Les deux sources sont normalisées vers un schéma commun avant la transformation Pandas.

---

## 6. Test local rapide

```bash
pip install -r requirements.txt functions-framework
functions-framework --target=main --debug   # sert sur http://localhost:8080
```
Puis envoie un faux payload `meeting.ended` avec `curl` (sans `ZOOM_WEBHOOK_SECRET_TOKEN`
la signature est ignorée en dev) pour valider le flux de bout en bout.
