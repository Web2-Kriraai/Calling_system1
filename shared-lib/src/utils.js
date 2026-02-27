import { DateTime } from 'luxon';

/**
 * Validates if the current time is within campaign business hours.
 * @param {Object} businessHours 
 * @returns {boolean}
 */
/**
 * Validates if the current time is within campaign business hours.
 * Handles the nested schedule format: businessHours: { monday: { open, close, closed }, ... }
 * @param {Object} businessHours 
 * @returns {boolean}
 */
export function isWithinBusinessHours(config) {
    if (!config) return true;

    // Prioritize callingHours structure
    const isEnabled = config.enabled !== false;
    if (!isEnabled) return true;

    const schedule = config.schedule || config;
    const now = DateTime.now().setZone('Asia/Kolkata');
    const currentDay = now.weekdayLong.toLowerCase();

    const daySchedule = schedule[currentDay];
    if (!daySchedule || daySchedule.closed) return false;

    if (daySchedule.open && daySchedule.close) {
        const [startH, startM] = daySchedule.open.split(':').map(Number);
        const [endH, endM] = daySchedule.close.split(':').map(Number);

        const start = now.set({ hour: startH, minute: startM, second: 0 });
        const end = now.set({ hour: endH, minute: endM, second: 0 });

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
