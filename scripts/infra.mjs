// Pilote l'infrastructure de laboratoire : up | down | destroy | status
//   up      : docker compose up --wait (health checks) + topologie RabbitMQ + migrations + état initial
//   down    : arrête les conteneurs (les données de test sont conservées)
//   destroy : arrête ET supprime le volume PostgreSQL du laboratoire (uniquement le volume de ce projet Compose)
import { lab } from './lib/lab-config.mjs';
import { compose, requireDocker } from './lib/docker.mjs';
import { mgmtReachable } from './lib/mgmt.mjs';
import { applyTopology } from './topology.mjs';
import { migrate } from './migrate.mjs';

const command = process.argv[2] ?? 'up';

if (command === 'status') {
  console.log(`RabbitMQ Management ${lab.mgmtUrl} : ${(await mgmtReachable()) ? 'joignable' : 'INJOIGNABLE'}`);
  process.exit(0);
}

requireDocker();

if (command === 'up') {
  console.log('Démarrage de RabbitMQ et PostgreSQL (attente des health checks)...');
  const up = compose(['up', '-d', '--wait', '--wait-timeout', '180', 'rabbitmq', 'postgres'], { inherit: true });
  if (!up.ok) {
    console.error('✖ docker compose up a échoué (un port est-il déjà occupé ? voir RABBITMQ_AMQP_PORT / RABBITMQ_MGMT_PORT / POSTGRES_PORT).');
    process.exit(1);
  }
  await applyTopology();
  await migrate();
  console.log(`✔ prêt — Management : ${lab.mgmtUrl} (utilisateur « ${lab.rabbitUser} »), PostgreSQL : 127.0.0.1:${lab.pgPort}`);
} else if (command === 'down') {
  process.exit(compose(['down'], { inherit: true }).ok ? 0 : 1);
} else if (command === 'destroy') {
  process.exit(compose(['down', '--volumes'], { inherit: true }).ok ? 0 : 1);
} else {
  console.error(`commande inconnue : ${command} (up | down | destroy | status)`);
  process.exit(1);
}
