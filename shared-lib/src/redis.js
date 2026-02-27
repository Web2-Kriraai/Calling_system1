import Redis from 'ioredis';
import { config } from './config.js';

let redis = null;

export function getRedis() {
    if (redis) return redis;

    redis = new Redis({
        host: config.redis.host,
        port: config.redis.port,
        password: config.redis.password,
        maxRetriesPerRequest: null, // Required by BullMQ
    });

    console.log('✅ Redis Connected');
    return redis;
}
