SIMULATEUR BOURSIER X MODE DEFAUT

Version: clé en main
Statut: stable
Public: utilisateur final (aucune config technique obligatoire)

1) OBJECTIF

Ce fichier décrit une configuration prête à l’emploi du simulateur boursier:
- Utilisable immédiatement
- Stable sans API premium
- Sans modifier automatiquement les paramètres sensibles déjà en place
- Adapté à un usage quotidien (suivi portefeuille, transactions, analyse, assistant)

2) MODE PAR DEFAUT (STABLE)

Le mode par défaut s’applique uniquement sur action explicite (bouton), jamais automatiquement.

Valeurs appliquées:
- live_enabled = 1
- live_mode = polling
- refresh_seconds = 10
- realtime_symbols = SPY,QQQ,AAPL,MSFT,GLD,EEM
- snapshot_min_seconds = 10
- snapshot_min_delta = 1.0
- ws_stale_seconds = 20
- max_line_pct = 25.0
- max_sector_pct = 45.0
- max_zone_pct = 55.0
- alert_loss_pct = -7.0
- alert_drawdown_pct = -10.0
- alert_gain_pct = 10.0

Important:
- Les paramètres de base utilisateur (capital initial, exchange, devise de valorisation, méthode comptable) ne sont pas écrasés par ce mode.

3) POURQUOI CE MODE EST STABLE

- Flux principal en REST Yahoo (pas besoin de clé WebSocket)
- Rafraîchissement UI modéré (10s)
- Snapshots anti-bruit (intervalle + delta minimum)
- Garde-fous de risque préconfigurés
- Alertes activées avec seuils cohérents

4) FONCTIONS DISPONIBLES DANS LE SIMULATEUR

- Onglet Synthèse:
  - KPI portefeuille (capital, disponible, investi, performance)
  - Graphique d’évolution basé sur snapshots
  - Répartition par secteur
  - Répartition par zone géographique

- Onglet Sélection d’Actifs:
  - Univers par zone: USA / Europe / Asie / Pays émergent
  - Liste métaux précieux + terres rares
  - Tableau de prix unitaires live
  - Formulaire d’achat/vente
  - Tableau des positions ouvertes

- Onglet Marchés:
  - Cotations live ETF/Actions
  - Opportunités (momentum/volatilité)
  - Points de vigilance
  - Contexte géopolitique (flux news + score)

- Onglet Backtest & Ops:
  - Backtest buy&hold et SMA50
  - Replay snapshots
  - Alertes récentes
  - Logs techniques

- Onglet Assistant IA:
  - Recommandations locales structurées
  - Option OpenAI si clé disponible

5) SOURCES DE DONNEES

- Quotes REST: Yahoo Finance quote API
- Fallback historique: yfinance
- FX live: paires Yahoo (ex: USDEUR=X)
- WebSocket tick-by-tick optionnel: Polygon
- News géopolitiques: flux RSS Google News

6) PERSISTANCE

Base locale:
- data/portfolio_simulator.db

Tables principales:
- settings
- transactions
- snapshots
- alert_events
- backtest_runs
- app_logs

7) COHERENCE PORTFEUILLE / PRIX / COURBE

Le simulateur maintient la cohérence suivante:
- Un prix unitaire live mis à jour impacte la valorisation de ligne
- La somme des lignes + cash impacte la valeur totale du portefeuille
- La valeur totale alimente la création de snapshots
- Les snapshots alimentent la courbe d’évolution

8) SECURITE OPERATIONNELLE

Avant enregistrement d’un ordre:
- Vérification cash disponible (achat)
- Vérification quantité détenue (vente)
- Vérification limites ligne / secteur / zone
- Blocage si contrainte dépassée

9) ALERTES

Détection:
- Perte seuil
- Drawdown seuil
- Gain seuil
- Concentration excessive

Livraison:
- Webhook JSON (si URL configurée)
- Email SMTP (si configuration SMTP disponible)

10) VARIABLES D’ENVIRONNEMENT OPTIONNELLES

- POLYGON_API_KEY
- OPENAI_API_KEY
- PORTFOLIO_ALERT_WEBHOOK
- PORTFOLIO_ALERT_EMAIL
- SMTP_HOST
- SMTP_PORT
- SMTP_USER
- SMTP_PASS
- SMTP_FROM

11) DEMARRAGE RAPIDE

Commande:
streamlit run portfolio_simulator_app.py

Ensuite:
1. Ouvrir la sidebar
2. Cliquer “Appliquer le mode par défaut (stable)”
3. Vérifier les actifs live
4. Commencer les transactions

12) RESULTAT ATTENDU

Après activation du mode par défaut:
- Application utilisable immédiatement
- Mise à jour des prix en direct (REST)
- Valorisation portefeuille dynamique
- Courbe d’évolution alimentée automatiquement
- Base de données et logs actifs

Fin du fichier.
