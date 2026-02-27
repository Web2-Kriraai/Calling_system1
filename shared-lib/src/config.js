import dotenv from 'dotenv';
import path from 'path';
import { fileURLToPath } from 'url';

const __dirname = path.dirname(fileURLToPath(import.meta.url));

// Determine which env file to load
const nodeEnv = process.env.NODE_ENV || 'development';
const envFile = nodeEnv === 'production' ? '.env.production' : '.env';
const envPath = path.resolve(__dirname, `../../${envFile}`);

console.log(`[Config] Loading environment from: ${envFile} (NODE_ENV: ${nodeEnv})`);

dotenv.config({ path: envPath });

// Fallback: If MONGODB_URI is still missing, try generic .env as last resort
if (!process.env.MONGODB_URI && envFile !== '.env') {
    dotenv.config({ path: path.resolve(__dirname, '../../.env') });
}

export const config = {
    mongodb: {
        uri: process.env.MONGODB_URI,
    },
    api: {
        callingKey: process.env.CALLING_API_KEY,
    },
    redis: {
        host: process.env.REDIS_HOST || 'localhost',
        port: parseInt(process.env.REDIS_PORT || '6379'),
        password: process.env.REDIS_PASSWORD || undefined,
    },
    queue: {
        name: 'outbound-calls',
    },
    concurrency: {
        globalMax: 5000,
    },
    webhooks: {
        basic: process.env.WEBHOOK_BASIC_URL,
        premium: process.env.WEBHOOK_PREMIUM_URL,
    },
    analysis: {
        apiUrl: process.env.ANALYSIS_API_URL,
    }
};
