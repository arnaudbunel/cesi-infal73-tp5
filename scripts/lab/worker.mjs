// Arrêt / reprise d'UN consommateur d'événements (T3). Usage : node scripts/lab/worker.mjs stop|start|status <notifications|statistics|audit>
// Raccourcis : npm run worker:stop -- notifications   /   npm run worker:start -- notifications
// Ne touche que les processus lancés par « npm run start » (pid dans .lab-state/pids). RabbitMQ n'est pas arrêté :
// la queue du consommateur arrêté conserve ses messages (backlog) tant qu'ils ne sont pas consommés.
import { run } from '../stack.mjs';

const [action, name] = process.argv.slice(2);
if (!['stop', 'start', 'status'].includes(action) || !['notifications', 'statistics', 'audit'].includes(name)) {
  console.error('usage : worker.mjs stop|start|status <notifications|statistics|audit>');
  process.exit(1);
}
await run(action, [name]);
