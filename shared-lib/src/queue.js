import { Queue, Worker } from 'bullmq';
import { getRedis } from './redis.js';
import { config } from './config.js';

export const queueName = config.queue.name;

export function createQueue() {
    return new Queue(queueName, {
        connection: getRedis(),
        defaultJobOptions: {
            attempts: 5,
            backoff: {
                type: 'exponential',
                delay: 5000,
            },
            removeOnComplete: true,
            removeOnFail: {
                age: 24 * 3600, // keep for 24 hours
            }
        }
    });
}
