// Migrations SQL versionnées : database/migrations/NNN_*.sql, appliquées dans l'ordre, une seule fois (table schema_migrations).
import { readdirSync, readFileSync } from 'node:fs';
import pg from 'pg';
import { lab } from './lib/lab-config.mjs';

export async function migrate({ log = console.log } = {}) {
  const dir = new URL('../database/migrations/', import.meta.url);
  const files = readdirSync(dir).filter((f) => /^\d+_.+\.sql$/.test(f)).sort();
  const client = new pg.Client({ connectionString: lab.databaseUrl });
  await client.connect();
  try {
    await client.query('CREATE TABLE IF NOT EXISTS schema_migrations (version text PRIMARY KEY, applied_at timestamptz NOT NULL DEFAULT now())');
    const applied = new Set((await client.query('SELECT version FROM schema_migrations')).rows.map((r) => r.version));
    for (const file of files) {
      if (applied.has(file)) continue;
      await client.query('BEGIN');
      try {
        await client.query(readFileSync(new URL(file, dir), 'utf8'));
        await client.query('INSERT INTO schema_migrations(version) VALUES ($1)', [file]);
        await client.query('COMMIT');
        log(`migration appliquée : ${file}`);
      } catch (error) {
        await client.query('ROLLBACK');
        throw error;
      }
    }
    log(`schéma à jour (${files.length} migration(s) connue(s))`);
  } finally {
    await client.end();
  }
}

if (process.argv[1]?.endsWith('migrate.mjs')) {
  migrate().catch((error) => {
    console.error(`✖ migration impossible (${lab.databaseUrl.replace(/:[^:@/]*@/, ':***@')}) : ${error.message}`);
    process.exit(1);
  });
}
