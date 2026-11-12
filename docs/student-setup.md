# Mise en route TP5 (≈ 8 min)

1. `node -v` → v24.x. `docker info` doit répondre (Linux : groupe `docker`, puis reconnexion).
2. `npm ci` puis `npm run setup` (attend les health checks, applique topologie et migrations). Port occupé : `.env` (copie de `.env.example`).
3. `npm run smoke` : vert (les tests qui exigent Docker échouent explicitement si l'infrastructure est absente).
4. `npm run start`, puis `npm run status`. Ouvrez RabbitMQ Management : exchanges `business.*`, queues `*.events`, `*.events.retry`, `*.events.dlq`.
5. `npm run stop`, puis `npm run test:regression` : le socle TP1–TP4 est vert. Puis `npm run test:acceptance` : **notez ce qui est `TODO ATTENDU` avant de modifier quoi que ce soit**.

## Variables utiles — une ligne
| But | Bash | PowerShell |
|---|---|---|
| Instance nommée d'un consommateur (2e worker d'une même queue) | `WORKER_NAME=stats-b npm run dev:statistics` | `$env:WORKER_NAME="stats-b"; npm run dev:statistics` |
| Lancer `dev` sans les consommateurs | `DEV_EVENTS=0 npm run dev` | `$env:DEV_EVENTS="0"; npm run dev` |
| Commandes en gRPC (TP3) | `STOCK_BACKEND=grpc npm run dev` | `$env:STOCK_BACKEND="grpc"; npm run dev` |

PowerShell : nettoyez avec `Remove-Item Env:WORKER_NAME, Env:DEV_EVENTS -ErrorAction SilentlyContinue`.

## Relever l'état pour vos fiches
- `npm run lab:state` : *ready / unacked / consumers* des queues, tables SQL, faits enregistrés par Audit.
- SQL direct : `docker compose exec postgres psql -U lab -d infal73_tp5_lab -c "SELECT * FROM processed_events"`.
- Logs : `npm run logs -- statistics` (une ligne JSON par événement, horodatée, avec `eventId`).
- RabbitMQ Management : onglet *Queues*, colonnes *Ready / Unacked / Consumers* (rafraîchies toutes les 500 ms dans ce laboratoire ; lisez-les après stabilisation, et **avant** de redémarrer un consommateur si vous voulez photographier un backlog).
