import { MongoClient } from 'mongodb';
import { config } from './config.js';

let client = null;
let db = null;

export async function getDb() {
    if (db) return db;

    if (!config.mongodb.uri) {
        throw new Error('❌ MONGODB_URI is not defined in the environment. Please check your .env file.');
    }

    client = new MongoClient(config.mongodb.uri);
    await client.connect();
    db = client.db();
    console.log('✅ MongoDB Connected');
    return db;
}

export async function closeDb() {
    if (client) {
        await client.close();
        client = null;
        db = null;
    }
}
