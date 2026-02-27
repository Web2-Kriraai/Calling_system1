import { getRedis } from './redis.js';

/**
 * ConcurrencyGuard handles distributed concurrency limits using Redis.
 * It uses Lua scripts to ensure atomic operations.
 */
export class ConcurrencyGuard {
  constructor() {
    this.redis = getRedis();
  }

  /**
   * Attempts to acquire a concurrency slot for a specific campaign and user.
   * @param {string} campaignId 
   * @param {string} userId 
   * @param {number} campaignLimit 
   * @param {number} userLimit 
   * @returns {Promise<boolean>} True if slot acquired, false otherwise.
   */
  async acquireSlot(campaignId, userId, campaignLimit, userLimit) {
    const campaignKey = `concurrency:campaign:${campaignId}`;
    const userKey = `concurrency:user:${userId}`;

    const luaScript = `
      local campaignCount = tonumber(redis.call('GET', KEYS[1]) or '0')
      local userCount = tonumber(redis.call('GET', KEYS[2]) or '0')
      
      if campaignCount < tonumber(ARGV[1]) and userCount < tonumber(ARGV[2]) then
        redis.call('INCR', KEYS[1])
        redis.call('INCR', KEYS[2])
        redis.call('EXPIRE', KEYS[1], 300)
        redis.call('EXPIRE', KEYS[2], 300)
        return 1
      else
        return 0
      end
    `;

    const result = await this.redis.eval(luaScript, 2, campaignKey, userKey, campaignLimit, userLimit);
    return result === 1;
  }

  /**
   * Releases the concurrency slot.
   * @param {string} campaignId 
   * @param {string} userId 
   */
  async releaseSlot(campaignId, userId) {
    const campaignKey = `concurrency:campaign:${campaignId}`;
    const userKey = `concurrency:user:${userId}`;

    const luaScript = `
      redis.call('DECR', KEYS[1])
      redis.call('DECR', KEYS[2])
      
      -- Ensure values don't go below zero
      if tonumber(redis.call('GET', KEYS[1])) < 0 then redis.call('SET', KEYS[1], '0') end
      if tonumber(redis.call('GET', KEYS[2])) < 0 then redis.call('SET', KEYS[2], '0') end
      return 1
    `;

    await this.redis.eval(luaScript, 2, campaignKey, userKey);
  }
}

export const concurrencyGuard = new ConcurrencyGuard();
