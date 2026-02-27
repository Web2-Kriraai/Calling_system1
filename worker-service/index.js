import { CallWorker } from './worker.js';

const worker = new CallWorker();

console.log('--- Worker Service Starting ---');
console.log('🚀 [Worker] Listening for jobs...');

// Graceful shutdown
const shutdown = async (signal) => {
    console.log(`${signal} received. Closing worker...`);
    await worker.worker.close();
    process.exit(0);
};

process.on('SIGTERM', () => shutdown('SIGTERM'));
process.on('SIGINT', () => shutdown('SIGINT'));
