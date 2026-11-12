// Charge `.env` (s'il existe) dans process.env SANS écraser les variables déjà exportées. Importé pour son effet de bord.
import { existsSync, readFileSync } from 'node:fs';

const file = new URL('../../.env', import.meta.url);
if (existsSync(file)) {
  for (const line of readFileSync(file, 'utf8').split(/\r?\n/)) {
    const match = /^\s*([A-Za-z_][A-Za-z0-9_]*)\s*=\s*(.*?)\s*$/.exec(line);
    if (!match || line.trimStart().startsWith('#')) continue;
    const [, key, raw] = match;
    if (process.env[key] === undefined) process.env[key] = raw.replace(/^(['"])(.*)\1$/, '$2');
  }
}
