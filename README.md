# EMSI — Activité Pratique N°4 – KAFKA et Spring Cloud

Réimplémentation **personnelle et indépendante** d’un laboratoire Kafka + Spring Cloud Stream, réalisée dans le cadre pédagogique EMSI.

**Dépôt GitHub cible :** `emsi-tp04-kafka-spring-cloud`

---

## Provenance (transparence)

Ce projet a été développé **indépendamment** pour couvrir les objectifs pédagogiques du TP04 EMSI (producteur, consommateur, Spring Cloud Stream, transformation / agrégation).

- Package Java : `ma.emsi.tp04.kafka` (aucun package `net.youssfi`)
- Les sources de ce dossier **ne sont pas** une copie du dépôt du professeur Mohamed YOUSSFI
- Un clone pédagogique local peut exister ailleurs sur le poste de travail à titre de **référence de cours** ; il n’est **ni importé ni modifié** par ce projet
- Référence pédagogique publique : [mohamedYoussfi/event-driven-kafka-spring-cloud-stream](https://github.com/mohamedYoussfi/event-driven-kafka-spring-cloud-stream) (inspiration de cours uniquement)

---

## Description technique

Application Spring Boot qui :

1. Publie des événements de visite vers Kafka (REST + `StreamBridge`)
2. Consomme et journalise ces événements (`Consumer`)
3. Génère un flux périodique (`Supplier`)
4. Agrège les visites par page sur une fenêtre de 5 s (Kafka Streams)
5. Expose un snapshot et un flux SSE de métriques
6. Sert un tableau de bord HTML minimal

## Objectif pédagogique

Comprendre et démontrer une chaîne event-driven locale avec Spring Cloud Stream et Kafka Streams, de façon reproductible sur Windows.

## Technologies

| Composant | Choix |
|-----------|--------|
| Java | 21 |
| Spring Boot | 3.4.2 |
| Spring Cloud | 2024.0.0 |
| Spring Cloud Stream | Kafka + Kafka Streams binders |
| Build | Maven |
| Broker local | Docker Compose (Confluent 7.6.1) |
| Port HTTP | **8088** |

## Architecture

```text
REST POST /api/visits ──StreamBridge──► visit-ingress ──► visitLogger
visitPulse (400 ms) ──► visit-pulse ──► visitWindowCount ──► visit-metrics
                                              │
                                   visit-count-store (fenêtre 5 s)
                                              │
                         GET /api/metrics/snapshot | SSE /api/metrics/live
                                              │
                                   static/dashboard.html
```

## Structure

```text
emsi-tp04-kafka-spring-cloud/
├── pom.xml
├── README.md
├── .gitignore
├── docker/compose.yml
└── src/
    ├── main/java/ma/emsi/tp04/kafka/...
    ├── main/resources/application.properties
    ├── main/resources/static/dashboard.html
    └── test/java/...
```

## Prérequis

- JDK 21
- Maven 3.9+
- Docker Desktop (pour le broker local)

## Installation

```powershell
cd emsi-tp04-kafka-spring-cloud
mvn -DskipTests package
```

## Démarrage Kafka (Docker)

Vérifier d’abord que le port **9092** est libre (ne pas démarrer si une autre instance Kafka tourne déjà).

```powershell
cd docker
docker compose -f compose.yml up -d
```

Broker attendu : `localhost:9092`.
Surcharge possible : `$env:KAFKA_BROKERS = "localhost:9092"`.

## Démarrage de l’application

```powershell
mvn spring-boot:run
```

- API : `http://localhost:8088`
- Dashboard : `http://localhost:8088/dashboard.html`

## Exemples

```http
POST http://localhost:8088/api/visits
Content-Type: application/json

{
  "pageId": "catalog",
  "visitorId": "student-1",
  "dwellMs": 420,
  "topic": "visit-ingress"
}
```

```http
POST http://localhost:8088/api/visits/demo
GET  http://localhost:8088/api/metrics/snapshot
```

## Tests

```powershell
# Tests unitaires (sans broker) — profil par défaut
mvn test

# Smoke TCP Kafka optionnel (n’envoie aucun message)
$env:RUN_KAFKA_SMOKE = "true"
mvn test -Pwith-kafka
```

Les tests d’intégration bout-en-bout (publish → consume → streams) nécessitent un broker démarré manuellement et ne sont **pas** exécutés automatiquement ici afin d’éviter toute perturbation d’environnements locaux.

## Gestion des erreurs

- Validation Bean Validation sur `PublishRequest`
- `RestExceptionHandler` (400 / 500)
- Journalisation défensive dans le consumer
- Snapshot métriques résilient si le state store Streams n’est pas encore prêt

## Limites connues

- Pas de sécurité (authn/authz) sur les endpoints
- Un seul broker Docker (lab local)
- Le dashboard est volontairement minimal
- Les tests automatisés couvrent le domaine / REST / publisher mock — pas un cluster Kafka complet

## Références pédagogiques

- EMSI — Activité Pratique N°4 – KAFKA et Spring Cloud
- Documentation Spring Cloud Stream / Kafka Streams
- Cours et supports associés (dont travaux illustratifs publics du professeur Mohamed YOUSSFI, utilisés comme référence pédagogique uniquement)
