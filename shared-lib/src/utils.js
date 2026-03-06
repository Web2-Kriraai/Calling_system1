import { DateTime } from 'luxon';

/**
 * Validates if the current time is within campaign business hours.
 * @param {Object} businessHours 
 * @returns {boolean}
 */
/**
 * Validates if the current time is within campaign business hours.
 * Supports timezone parsing and nested callingHours/businessHours structures.
 * @param {Object} campaignOrConfig Full campaign object or hours config
 * @returns {boolean}
 */
export function isWithinBusinessHours(campaignOrConfig) {
    if (!campaignOrConfig) return true;

    // 1. Extract config and timezone
    const config = campaignOrConfig.callingHours || campaignOrConfig.businessHours || campaignOrConfig;
    const timezoneStr = campaignOrConfig.timezone || 'Asia/Kolkata';

    // 2. Check if enabled (callingHours has an 'enabled' flag, businessHours usually doesn't)
    if (config.enabled === false) return true;

    // 3. Determine Zone
    let zone = 'Asia/Kolkata';
    if (timezoneStr) {
        if (timezoneStr.includes('Chennai') || timezoneStr.includes('Kolkata') || timezoneStr.includes('Mumbai')) {
            zone = 'Asia/Kolkata';
        } else {
            const match = timezoneStr.match(/UTC([+-]\d+:\d+)/);
            if (match) {
                zone = `UTC${match[1]}`;
            }
        }
    }

    const now = DateTime.now().setZone(zone);
    const schedule = config.schedule || config;

    // 4. Check Exceptions
    if (config.exceptions && Array.isArray(config.exceptions)) {
        const todayDate = now.toISODate(); // YYYY-MM-DD
        if (config.exceptions.some(ex => ex.date === todayDate)) {
            return false;
        }
    }

    // 5. Check Day Schedule
    const currentDay = now.weekdayLong.toLowerCase();
    const daySchedule = schedule[currentDay];

    if (!daySchedule || daySchedule.closed) return false;

    if (daySchedule.open && daySchedule.close) {
        const [startH, startM] = daySchedule.open.split(':').map(Number);
        const [endH, endM] = daySchedule.close.split(':').map(Number);

        const start = now.set({ hour: startH, minute: startM, second: 0, millisecond: 0 });
        const end = now.set({ hour: endH, minute: endM, second: 0, millisecond: 0 });

        return now >= start && now <= end;
    }

    return true;
}

/**
 * Calculates job priority based on contact and campaign metadata.
 * Lower number = Higher priority in BullMQ.
 * @param {Object} campaign 
 * @param {Object} contact 
 * @returns {number}
 */
export function calculatePriority(campaign, contact) {
    let priority = 100; // Default

    if (campaign.isUrgent) priority -= 50;
    if (contact.isVip) priority -= 20;
    if (contact.retryCount > 0) priority += contact.retryCount * 10;

    return Math.max(1, priority);
}
