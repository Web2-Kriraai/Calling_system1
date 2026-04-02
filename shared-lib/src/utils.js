// import { DateTime } from 'luxon';

// /**
//  * Validates if the current time is within campaign business hours.
//  * @param {Object} businessHours 
//  * @returns {boolean}
//  */
// /**
//  * Validates if the current time is within campaign business hours.
//  * Supports timezone parsing and nested callingHours/businessHours structures.
//  * @param {Object} campaignOrConfig Full campaign object or hours config
//  * @returns {boolean}
//  */
// export function isWithinBusinessHours(campaignOrConfig) {
//     if (!campaignOrConfig) return true;

//     // 1. Extract config and timezone
//     const config = campaignOrConfig.callingHours || campaignOrConfig.businessHours || campaignOrConfig;
//     const timezoneStr = campaignOrConfig.timezone || 'Asia/Kolkata';

//     // 2. Check if enabled (callingHours has an 'enabled' flag, businessHours usually doesn't)
//     if (config.enabled === false) return true;

//     // 3. Determine Zone
//     let zone = 'Asia/Kolkata';
//     if (timezoneStr) {
//         if (timezoneStr.includes('Chennai') || timezoneStr.includes('Kolkata') || timezoneStr.includes('Mumbai')) {
//             zone = 'Asia/Kolkata';
//         } else {
//             const match = timezoneStr.match(/UTC([+-]\d+:\d+)/);
//             if (match) {
//                 zone = `UTC${match[1]}`;
//             }
//         }
//     }

//     const now = DateTime.now().setZone(zone);
//     const schedule = config.schedule || config;

//     // 4. Check Exceptions
//     if (config.exceptions && Array.isArray(config.exceptions)) {
//         const todayDate = now.toISODate(); // YYYY-MM-DD
//         if (config.exceptions.some(ex => ex.date === todayDate)) {
//             return false;
//         }
//     }

//     // 5. Check Day Schedule
//     const currentDay = now.weekdayLong.toLowerCase();
//     const daySchedule = schedule[currentDay];

//     if (!daySchedule || daySchedule.closed) return false;

//     if (daySchedule.open && daySchedule.close) {
//         const [startH, startM] = daySchedule.open.split(':').map(Number);
//         const [endH, endM] = daySchedule.close.split(':').map(Number);

//         const start = now.set({ hour: startH, minute: startM, second: 0, millisecond: 0 });
//         const end = now.set({ hour: endH, minute: endM, second: 0, millisecond: 0 });

//         return now >= start && now <= end;
//     }

//     return true;
// }

// /**
//  * Calculates job priority based on contact and campaign metadata.
//  * Lower number = Higher priority in BullMQ.
//  * @param {Object} campaign 
//  * @param {Object} contact 
//  * @returns {number}
//  */
// export function calculatePriority(campaign, contact) {
//     let priority = 100; // Default

//     if (campaign.isUrgent) priority -= 50;
//     if (contact.isVip) priority -= 20;
//     if (contact.retryCount > 0) priority += contact.retryCount * 10;

//     return Math.max(1, priority);
// }
// /**
//  * Parses a string time from IST into a UTC Date object.
//  * Handles formats like "2026-03-12 10:00 AM", "10:00 AM" (assumes today), or ISO strings.
//  * @param {string} timeStr 
//  * @returns {Date|null}
//  */
// export function parseISTTimeToDate(timeStr) {
//     if (!timeStr) return null;

//     try {
//         // First try luxon to parse it as IST
//         let dt;

//         // If it's just a time like "10:00 AM", prepend today's date in IST
//         if (/^\d{1,2}:\d{2}\s*(AM|PM)$/i.test(timeStr.trim())) {
//             const todayIST = DateTime.now().setZone('Asia/Kolkata').toISODate();
//             dt = DateTime.fromFormat(`${todayIST} ${timeStr.trim()}`, 'yyyy-MM-dd h:mm a', { zone: 'Asia/Kolkata' });
//         } else if (/^\d{4}-\d{2}-\d{2}\s+\d{1,2}:\d{2}\s*(AM|PM)$/i.test(timeStr.trim())) {
//             dt = DateTime.fromFormat(timeStr.trim(), 'yyyy-MM-dd h:mm a', { zone: 'Asia/Kolkata' });
//         } else if (/^\d{2}-\d{2}-\d{4}\s+\d{1,2}:\d{2}$/.test(timeStr.trim())) {
//             // New format: "11-03-2026 15:33"
//             dt = DateTime.fromFormat(timeStr.trim(), 'dd-MM-yyyy HH:mm', { zone: 'Asia/Kolkata' });
//         } else {
//             // Fallback to standard ISO parsing if possible
//             dt = DateTime.fromISO(timeStr, { zone: 'Asia/Kolkata' });
//         }

//         if (dt.isValid) {
//             return dt.toJSDate();
//         }
//     } catch (error) {
//         console.error(`❌ [Utils] Error parsing IST time "${timeStr}":`, error.message);
//     }

//     return null;
// }
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

    // 1. Next_Action / Follow-up Priority
    // If a follow-up has set a specific priority (High, Medium, Low), adjust base.
    if (contact.priority) {
        const p = String(contact.priority).toLowerCase();
        if (p === 'high') priority -= 40;
        else if (p === 'medium' || p === 'normal') priority -= 20;
        else if (p === 'low') priority += 20;
    }

    // 2. Campaign Level Urgency
    if (campaign.isUrgent) priority -= 50;

    // 3. Contact Level Importance
    if (contact.isVip) priority -= 20;

    // 4. Retry Penalty (lower priority for contacts that already failed multiple times)
    if (contact.retryCount > 0) priority += contact.retryCount * 10;

    return Math.max(1, priority);
}

/**
 * While campaign.status is `testing`, production activation is blocked until testCallStatus === 'passed'
 * (set by your test-call completion handler).
 */
export function isCampaignBlockedByTestCall(campaign) {
    if (!campaign || campaign.status !== 'testing') return false;
    return campaign.testCallStatus !== 'passed';
}

/**
 * Parses a string time from IST into a UTC Date object.
 * Handles formats like "2026-03-12 10:00 AM", "10:00 AM" (assumes today), or ISO strings.
 * @param {string} timeStr 
 * @returns {Date|null}
 */
export function parseISTTimeToDate(timeStr) {
    if (!timeStr) return null;

    try {
        // First try luxon to parse it as IST
        let dt;

        // If it's just a time like "10:00 AM", prepend today's date in IST
        if (/^\d{1,2}:\d{2}\s*(AM|PM)$/i.test(timeStr.trim())) {
            const todayIST = DateTime.now().setZone('Asia/Kolkata').toISODate();
            dt = DateTime.fromFormat(`${todayIST} ${timeStr.trim()}`, 'yyyy-MM-dd h:mm a', { zone: 'Asia/Kolkata' });
        } else if (/^\d{4}-\d{2}-\d{2}\s+\d{1,2}:\d{2}\s*(AM|PM)$/i.test(timeStr.trim())) {
            dt = DateTime.fromFormat(timeStr.trim(), 'yyyy-MM-dd h:mm a', { zone: 'Asia/Kolkata' });
        } else if (/^\d{2}-\d{2}-\d{4}\s+\d{1,2}:\d{2}$/.test(timeStr.trim())) {
            // New format: "11-03-2026 15:33"
            dt = DateTime.fromFormat(timeStr.trim(), 'dd-MM-yyyy HH:mm', { zone: 'Asia/Kolkata' });
        } else {
            // Fallback to standard ISO parsing if possible
            dt = DateTime.fromISO(timeStr, { zone: 'Asia/Kolkata' });
        }

        if (dt.isValid) {
            return dt.toJSDate();
        }
    } catch (error) {
        console.error(`❌ [Utils] Error parsing IST time "${timeStr}":`, error.message);
    }

    return null;
}