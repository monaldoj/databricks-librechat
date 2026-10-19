/**
 * Embedded Ephemeral MongoDB Launcher for Databricks Apps
 *
 * Uses `mongodb-memory-server` (already installed in LibreChat's node_modules)
 * to run a standalone `mongod` process on 127.0.0.1:27017 backed by an
 * ephemeral directory (/tmp/librechat-mongodb) inside the Databricks App compute.
 *
 * From LibreChat's perspective, only MONGO_URI=mongodb://127.0.0.1:27017/LibreChat changes.
 */

const fs = require('node:fs');
const path = require('node:path');

const MONGO_HOST = process.env.MONGO_EMBEDDED_HOST || '127.0.0.1';
const MONGO_PORT = parseInt(process.env.MONGO_EMBEDDED_PORT || '27017', 10);
const MONGO_DB_NAME = process.env.MONGO_EMBEDDED_DB || 'LibreChat';
const MONGO_DB_PATH = process.env.MONGO_EMBEDDED_DATA_DIR || '/tmp/librechat-mongodb';
const MONGO_BIN_DIR = process.env.MONGOMS_DOWNLOAD_DIR || '/tmp/mongodb-binaries';

process.env.MONGOMS_DOWNLOAD_DIR = MONGO_BIN_DIR;

function resolveMongoMemoryServer() {
  const searchPaths = [
    process.cwd(),
    '/tmp/librechat',
    '/tmp/librechat/api',
    __dirname,
  ];
  for (const base of searchPaths) {
    try {
      const resolved = require.resolve('mongodb-memory-server', { paths: [base] });
      return require(resolved);
    } catch (_) {
      // try next path
    }
  }
  return require('mongodb-memory-server');
}

async function main() {
  fs.mkdirSync(MONGO_DB_PATH, { recursive: true });
  fs.mkdirSync(MONGO_BIN_DIR, { recursive: true });

  const { MongoMemoryServer } = resolveMongoMemoryServer();

  console.log(`[Embedded-MongoDB] Starting mongod on ${MONGO_HOST}:${MONGO_PORT} (dbPath=${MONGO_DB_PATH})...`);

  let mongod;
  try {
    mongod = await MongoMemoryServer.create({
      instance: {
        ip: MONGO_HOST,
        port: MONGO_PORT,
        dbName: MONGO_DB_NAME,
        dbPath: MONGO_DB_PATH,
        storageEngine: 'wiredTiger',
      },
      binary: {
        downloadDir: MONGO_BIN_DIR,
      },
    });
  } catch (err) {
    console.warn(`[Embedded-MongoDB] wiredTiger init warning (${err.message}); retrying with default storage engine...`);
    mongod = await MongoMemoryServer.create({
      instance: {
        ip: MONGO_HOST,
        port: MONGO_PORT,
        dbName: MONGO_DB_NAME,
        dbPath: MONGO_DB_PATH,
      },
      binary: {
        downloadDir: MONGO_BIN_DIR,
      },
    });
  }

  const uri = mongod.getUri(MONGO_DB_NAME);
  console.log(`[Embedded-MongoDB] Ready at ${uri}`);
  console.log(`MONGODB_READY=mongodb://${MONGO_HOST}:${MONGO_PORT}/${MONGO_DB_NAME}`);

  const shutdown = async (signal) => {
    console.log(`[Embedded-MongoDB] Received ${signal}, stopping mongod...`);
    try {
      await mongod.stop({ doCleanup: false });
    } catch (e) {
      console.error('[Embedded-MongoDB] Error stopping mongod:', e);
    }
    process.exit(0);
  };

  process.on('SIGINT', () => shutdown('SIGINT'));
  process.on('SIGTERM', () => shutdown('SIGTERM'));

  // Keep process alive
  setInterval(() => {}, 60_000);
}

main().catch((err) => {
  console.error('[Embedded-MongoDB] Fatal startup error:', err);
  process.exit(1);
});
