// Node 22 entrypoint for github-core-clone-01.
// Launches the statically compiled Go forge binary directly (no HTTP proxy layer)
// so Go's net/http handles registration, /done polling, and all four protocol surfaces natively.

import { spawn } from 'node:child_process';
import { existsSync } from 'node:fs';
import { dirname, join } from 'node:path';
import { fileURLToPath } from 'node:url';

const ROOT_DIR = dirname(fileURLToPath(import.meta.url));
const ENV_URL = (process.env.MAKEFUN_ENV_URL ?? '').trim();

if (ENV_URL === '' && !process.argv.includes('-standalone')) {
  process.stderr.write('[entry] MAKEFUN_ENV_URL is not set: there is no env to register with\n');
  process.exit(1);
}

function resolveBinary(): string {
  const override = process.env.FORGE_BIN;
  if (override && existsSync(override)) {
    return override;
  }
  if (process.platform === 'linux' && process.arch === 'x64') {
    return join(ROOT_DIR, 'forge-linux-amd64');
  }
  const localCandidates = [
    join(ROOT_DIR, `forge-${process.platform}-${process.arch}`),
    `/tmp/forge-${process.platform}-${process.arch}`,
    join(ROOT_DIR, 'forge-linux-amd64'),
  ];
  for (const candidate of localCandidates) {
    if (existsSync(candidate)) {
      return candidate;
    }
  }
  throw new Error(`no compiled forge binary found for ${process.platform}/${process.arch}`);
}

const binPath = resolveBinary();
const child = spawn(binPath, [], {
  stdio: 'inherit',
  env: process.env,
});

for (const sig of ['SIGTERM', 'SIGINT', 'SIGHUP'] as const) {
  process.on(sig, () => {
    if (!child.killed) {
      child.kill(sig);
    }
  });
}

child.on('error', (err) => {
  process.stderr.write(`[entry] failed to spawn ${binPath}: ${err.message}\n`);
  process.exit(1);
});

child.on('exit', (code, signal) => {
  if (signal) {
    process.exit(0);
  }
  process.exit(code ?? 1);
});
