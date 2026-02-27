import { Scheduler } from './scheduler.js';

const scheduler = new Scheduler();

async function main() {
    console.log('--- Scheduler Service Starting ---');

    // In a production environment, this might run periodically
    // For demonstration, we run it once and then exit or set an interval.
    const runLoop = async () => {
        try {
            await scheduler.run();
        } catch (err) {
            console.error('Fatal error in scheduler loop:', err);
        } finally {
            console.log('Waiting for next scan...');
            setTimeout(runLoop, 60000); // Scan every minute
        }
    };

    runLoop();
}

main().catch(console.error);

// Handle graceful shutdown
process.on('SIGTERM', async () => {
    console.log('SIGTERM received. Shutting down...');
    process.exit(0);
});
