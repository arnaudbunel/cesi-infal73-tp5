import { spawnSync } from 'node:child_process';

/** Exécute `docker compose ...` à la racine du dépôt. Retourne { ok, stdout, stderr }. */
export function compose(args, { inherit = false } = {}) {
  const result = spawnSync('docker', ['compose', ...args], {
    cwd: new URL('../..', import.meta.url),
    encoding: 'utf8',
    stdio: inherit ? 'inherit' : 'pipe',
  });
  return { ok: result.status === 0, stdout: result.stdout ?? '', stderr: result.stderr ?? (result.error ? String(result.error) : '') };
}

/** Vérifie que Docker est joignable et explique l'erreur sinon (daemon éteint, droits, binaire absent). */
export function checkDocker() {
  const result = spawnSync('docker', ['info', '--format', '{{.ServerVersion}}'], { encoding: 'utf8' });
  if (result.error) return { ok: false, hint: "Docker n'est pas installé ou introuvable dans le PATH." };
  if (result.status === 0) return { ok: true, hint: '' };
  const err = `${result.stderr}`;
  if (/permission denied/i.test(err)) return { ok: false, hint: "Docker refuse l'accès au daemon : ajoutez votre utilisateur au groupe « docker » (Linux) puis reconnectez-vous." };
  if (/cannot connect|is the docker daemon running|error during connect/i.test(err)) return { ok: false, hint: 'Le daemon Docker ne répond pas : démarrez Docker Desktop / le service docker.' };
  return { ok: false, hint: `docker info a échoué : ${err.trim().split('\n')[0]}` };
}

export function requireDocker() {
  const status = checkDocker();
  if (!status.ok) {
    console.error(`✖ ${status.hint}`);
    process.exit(2);
  }
}
