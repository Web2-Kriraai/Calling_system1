import { createQueue } from './shared-lib/index.js';

async function clearQueue() {
    const queue = createQueue();
    console.log(`:broom: Attempting to obliterate queue: ${queue.name}...`);

    try {
        // obliterate({ force: true }) removes the queue and all its jobs (waiting, active, completed, failed, etc.)
        await queue.obliterate({ force: true });
        console.log(':white_check_mark: Queue obliterated successfully.');
    } catch (error) {
        console.error(':x: Failed to clear queue:', error.message);
    } finally {
        await queue.close();
        process.exit(0);
    }
}

clearQueue();