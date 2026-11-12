// T7 — arrêt / redémarrage MAÎTRISÉ du conteneur RabbitMQ du laboratoire (ne touche à aucun autre conteneur).
// Usage : node scripts/lab/broker.mjs stop | start | restart
// `start` attend la santé du conteneur puis la disponibilité de l'API Management. Les queues durables sont conservées.
import { compose, requireDocker } from '../lib/docker.mjs';
import { waitForManagement } from '../topology.mjs';

requireDocker();
const action = process.argv[2];
if (action === 'stop') {
  const result = compose(['stop', 'rabbitmq']);
  console.log(result.ok ? 'RabbitMQ arrêté' : `✖ ${result.stderr}`);
  process.exit(result.ok ? 0 : 1);
} else if (action === 'start' || action === 'restart') {
  const result = compose([action === 'restart' ? 'restart' : 'start', 'rabbitmq']);
  if (!result.ok) {
    console.error(`✖ ${result.stderr}`);
    process.exit(1);
  }
  await waitForManagement(120_000);
  console.log('RabbitMQ prêt (Management joignable)');
} else {
  console.error('usage : node scripts/lab/broker.mjs stop | start | restart');
  process.exit(1);
}
