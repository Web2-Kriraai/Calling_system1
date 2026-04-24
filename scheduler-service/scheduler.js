// import { getDb, createQueue, calculatePriority, isWithinBusinessHours, parseISTTimeToDate } from 'shared-lib';
// import { ObjectId } from 'mongodb';
// import { DateTime } from 'luxon';

// export class Scheduler {
//     constructor() {
//         this.queue = createQueue();
//     }

//     /**
//      * Main loops that monitors campaigns and enqueues contacts.
//      * In a real production system, this could be triggered by a cron job or a dedicated loop.
//      */
//     async run() {
//         console.log('🚀 [Scheduler] Starting contact scanning loop...');
//         const db = await getDb();

//         // 0a. Activate immediateStart campaigns once their script is ready
//         await this.activateImmediateStartCampaigns(db);

//         // 0b. Activate scheduled campaigns whose time has arrived
//         await this.activateScheduledCampaigns(db);

//         // 0c. Process call analysis results for follow-ups
//         await this.processFollowUps(db);

//         // 1. Find active campaigns
//         const activeCampaigns = await db.collection('campaigns').find({
//             status: 'active',
//             archive: { $ne: true }
//         }).toArray();

//         console.log(`🔍 [Scheduler] Found ${activeCampaigns.length} active campaigns.`);

//         for (const campaign of activeCampaigns) {
//             await this.processCampaign(campaign, db);
//         }
//     }

//     /**
//      * Handles campaigns created with immediateStart: true.
//      * Instead of waiting for startTime, we poll the campaignscripts collection
//      * every scheduler loop (~30s). The moment a script is found, the campaign
//      * is validated and activated immediately.
//      */
//     async activateImmediateStartCampaigns(db) {
//         // Find all immediateStart campaigns that are still waiting to be activated
//         // We now also check for customizeSchedule: false as a synonym for immediateStart: true
//         const pendingCampaigns = await db.collection('campaigns').find({
//             $or: [
//                 { immediateStart: true },
//                 { customizeSchedule: false }
//             ],
//             status: { $nin: ['active', 'completed', 'paused', 'failed'] },
//             archive: { $ne: true }
//         }).toArray();

//         if (pendingCampaigns.length === 0) return;

//         console.log(`⚡ [Scheduler] Checking ${pendingCampaigns.length} immediateStart campaign(s) for script readiness...`);

//         for (const campaign of pendingCampaigns) {
//             const campaignId = campaign._id;

//             // Check if the campaign script has been generated yet
//             const script = await db.collection('campaign_scripts').findOne({
//                 $or: [
//                     { campaignId: campaignId },
//                     { campaignId: campaignId.toString() }
//                 ]
//             });

//             if (!script) {
//                 console.log(`⏳ [Scheduler] Script not ready yet for immediateStart campaign: ${campaign.campaignName} (${campaignId}). Will retry next loop.`);
//                 continue;
//             }

//             console.log(`✅ [Scheduler] Script found for campaign: ${campaign.campaignName} (${campaignId}). Proceeding to activate...`);

//             // Validate required fields (startDate/startTime not required for immediateStart)
//             const { isValid, missingFields } = this.validateCampaignConfig(campaign);
//             if (!isValid) {
//                 console.warn(`[Scheduler] immediateStart campaign ${campaign.campaignName} (${campaignId}) missing fields: ${missingFields.join(', ')}`);
//                 await db.collection('campaigns').updateOne(
//                     { _id: campaignId },
//                     {
//                         $set: {
//                             status: 'draft',
//                             error: `Missing required fields: ${missingFields.join(', ')}`,
//                             updatedAt: new Date()
//                         }
//                     }
//                 );
//                 continue;
//             }

//             // Check contacts exist
//             const contactExists = await db.collection('contactprocessings').findOne({ campaignId });
//             if (!contactExists) {
//                 console.warn(`[Scheduler] immediateStart campaign ${campaign.campaignName} (${campaignId}) has no contacts. Skipping activation.`);
//                 continue;
//             }

//             // Activate!
//             await db.collection('campaigns').updateOne(
//                 { _id: campaignId },
//                 {
//                     $set: {
//                         status: 'active',
//                         activatedAt: new Date(),
//                         updatedAt: new Date()
//                     }
//                 }
//             );
//             console.log(`🚀 [Scheduler] immediateStart campaign ACTIVATED: ${campaign.campaignName} (${campaignId})`);
//         }
//     }

//     /**
//      * Finds scheduled campaigns and activates them if start time has passed.
//      * Includes validations for campaign status and required configuration fields.
//      */
//     async activateScheduledCampaigns(db) {
//         const now = new Date();
//         const istTime = new Date(now.getTime() + (5.5 * 60 * 60 * 1000)); // Quick IST conversion
//         const currentDate = istTime.toISOString().split('T')[0];
//         const currentTime = istTime.toISOString().split('T')[1].split('.')[0];

//         // console.log(`🕒 [Scheduler] Checking for campaigns to activate or schedule... (Current IST: ${currentDate} ${currentTime})`);

//         // Find campaigns that are scheduled or active (to verify if they should still be active)
//         const candidates = await db.collection('campaigns').find({
//             status: { $in: ['active', 'scheduled', 'processing', 'draft', 'error', 'pending'] },
//             archive: { $ne: true }
//         }).toArray();

//         for (const campaign of candidates) {
//             const campaignId = campaign._id;

//             // Skip immediateStart campaigns here as they are handled by activateImmediateStartCampaigns
//             const isImmediate = campaign.immediateStart === true || campaign.customizeSchedule === false;
//             if (isImmediate) continue;

//             // 1. Check if the campaign script has been generated yet
//             const script = await db.collection('campaign_scripts').findOne({
//                 $or: [
//                     { campaignId: campaignId },
//                     { campaignId: campaignId.toString() }
//                 ]
//             });

//             if (!script) {
//                 // Keep status as 'processing' (or whatever it was) until script is ready
//                 continue;
//             }

//             // 2. Perform field validation
//             const { isValid, missingFields } = this.validateCampaignConfig(campaign);
//             if (!isValid) {
//                 console.warn(`[Scheduler] Campaign ${campaign.campaignName} (${campaignId}) missing fields: ${missingFields.join(', ')}`);
//                 await db.collection('campaigns').updateOne(
//                     { _id: campaignId },
//                     {
//                         $set: {
//                             status: 'draft',
//                             error: `Missing required fields: ${missingFields.join(', ')}`,
//                             updatedAt: new Date()
//                         }
//                     }
//                 );
//                 continue;
//             }

//             // 3. Check if it's time to activate
//             // If date is today, check time. If date is past, activate immediately.
//             const isTimeReached = campaign.startDate < currentDate ||
//                 (campaign.startDate === currentDate && campaign.startTime <= currentTime);

//             if (isTimeReached) {
//                 // Only activate if not already active
//                 if (campaign.status !== 'active') {
//                     // Check if contacts exist for this campaign
//                     const contactExists = await db.collection('contactprocessings').findOne({ campaignId: campaign._id });
//                     if (!contactExists) {
//                         console.warn(`[Scheduler] Campaign ${campaign.campaignName} (${campaign._id}) has no contacts. Skipping activation.`);
//                         continue;
//                     }

//                     console.log(`✨ [Scheduler] Activating campaign: ${campaign.campaignName} (${campaign._id})`);
//                     await db.collection('campaigns').updateOne(
//                         { _id: campaign._id },
//                         { $set: { status: 'active', activatedAt: new Date(), updatedAt: new Date() } }
//                     );
//                 }
//             } else if (campaign.status !== 'scheduled') {
//                 // Start time not reached or campaign was prematurely set to active -> Set to Scheduled
//                 console.log(`🕒 [Scheduler] Campaign ${campaign.campaignName} (${campaignId}) start time not reached. Setting status to scheduled.`);
//                 await db.collection('campaigns').updateOne(
//                     { _id: campaignId },
//                     { $set: { status: 'scheduled', updatedAt: new Date() } }
//                 );
//             }
//         }
//     }

//     /**
//      * Validates that all required fields for a campaign are present.
//      */
//     validateCampaignConfig(campaign) {
//         // immediateStart campaigns don't need startDate/startTime — they activate on script readiness
//         const isImmediate = campaign.immediateStart === true || campaign.customizeSchedule === false;
//         const requiredFields = [
//             ...(!isImmediate ? [
//                 { field: 'startDate', label: 'Start Date' },
//                 { field: 'startTime', label: 'Start Time' }
//             ] : []),
//             { field: 'concurrentCalls', label: 'Concurrent Calls' },
//             { field: 'createdBy', label: 'Created By' },
//             { field: 'agentName', label: 'Agent Name' },
//             { field: 'selectedVoice', label: 'Selected Voice' }
//         ];

//         // Check for business hours either in 'callingHours' or 'businessHours'
//         const hasBusinessHours = (campaign.callingHours && Object.keys(campaign.callingHours).length > 0) ||
//             (campaign.businessHours && Object.keys(campaign.businessHours).length > 0);

//         const missingFields = requiredFields
//             .filter(f => !campaign[f.field])
//             .map(f => f.label);

//         if (!hasBusinessHours) {
//             missingFields.push('Business Hours');
//         }

//         return {
//             isValid: missingFields.length === 0,
//             missingFields
//         };
//     }

//     /**
//      * Processes a single campaign by scanning pending contacts.
//      */
//     async processCampaign(campaign, db) {
//         // If campaign uses Google Sheet with auto-sync enabled, we ignore endDate/endTime for dialing.
//         // We fetch the sheet doc once here and reuse it for both the end-window guard and completion logic.
//         let sheetDoc = null;
//         let ignoreEndWindow = false;
//         if (campaign?.googleSheetsDataId) {
//             try {
//                 sheetDoc = await db.collection('googlesheetsdatas').findOne({
//                     _id: new ObjectId(campaign.googleSheetsDataId)
//                 });
//                 ignoreEndWindow = sheetDoc?.autoSyncEnabled === true;
//             } catch (err) {
//                 console.warn(`[Scheduler] Could not fetch GoogleSheetsData for campaign ${campaign._id}:`, err.message);
//             }
//         }

//         // End window guard: if endDate/endTime is passed and tillCallsComplete !== true, do not enqueue new calls.
//         // (Worker also enforces this as a hard safety net for already-enqueued jobs)
//         try {
//             if (!ignoreEndWindow && campaign?.tillCallsComplete !== true && campaign?.endDate) {
//                 const tz = campaign.timezone || 'Asia/Kolkata';
//                 let zone = 'Asia/Kolkata';
//                 if (typeof tz === 'string') {
//                     if (tz.includes('Chennai') || tz.includes('Kolkata') || tz.includes('Mumbai')) {
//                         zone = 'Asia/Kolkata';
//                     } else {
//                         const match = tz.match(/UTC([+-]\d+:\d+)/);
//                         zone = match ? `UTC${match[1]}` : tz;
//                     }
//                 }

//                 const endDateISO = (campaign.endDate instanceof Date)
//                     ? DateTime.fromJSDate(campaign.endDate).toISODate()
//                     : (typeof campaign.endDate === 'string' ? (campaign.endDate.match(/^(\d{4}-\d{2}-\d{2})/)?.[1] || null) : null);

//                 if (endDateISO) {
//                     // Default to end-of-day if endTime is missing/unparseable.
//                     let endDt = DateTime.fromISO(endDateISO, { zone }).endOf('day');

//                     if (typeof campaign.endTime === 'string' && campaign.endTime.trim()) {
//                         const t = campaign.endTime.trim();
//                         const formats = ['HH:mm:ss', 'HH:mm', 'h:mm a', 'h:mm:ss a'];
//                         for (const fmt of formats) {
//                             const parsed = DateTime.fromFormat(t, fmt, { zone: 'UTC' });
//                             if (parsed.isValid) {
//                                 endDt = DateTime.fromISO(endDateISO, { zone }).set({
//                                     hour: parsed.hour,
//                                     minute: parsed.minute,
//                                     second: parsed.second,
//                                     millisecond: 0
//                                 });
//                                 break;
//                             }
//                         }
//                     }

//                     const now = DateTime.now().setZone(endDt.zoneName);
//                     if (endDt.isValid && now > endDt) {
//                         console.log(`⏹️ [Scheduler] Campaign ${campaign.campaignName} (${campaign._id}) is past end window (${endDt.toISO()}). Skipping enqueue.`);
//                         return;
//                     }
//                 }
//             }
//         } catch (e) {
//             // Never block scheduler loop because of end-window parsing issues
//             console.warn(`⚠️ [Scheduler] End window check failed for campaign ${campaign?._id}:`, e.message);
//         }

//         if (!isWithinBusinessHours(campaign)) {
//             // console.log(`🕒 [Scheduler] Campaign ${campaign.campaignName} (${campaign._id}) is currently outside calling hours. Skipping.`);
//             return;
//         }

//         console.log(`📡 [Scheduler] Processing campaign: ${campaign.campaignName} (${campaign._id})`);

//         // Fetch the user's purchased concurrent call limit from the DB
//         // The limit is stored per-phone-number in user.phoneNumbers[].concurrentCalls
//         const campaignUser = await db.collection('users').findOne(
//             { email: campaign.createdBy },
//             { projection: { phoneNumbers: 1 } }
//         );

//         // Find the phone number entry that matches this campaign's selected phone
//         // campaign.phoneNumberId or campaign.phoneNumber references the purchased number
//         const campaignPhoneRef = campaign.phoneNumberId || campaign.phoneNumber || campaign.fromNumber;
//         let userConcurrentLimit = 10; // safe default

//         if (campaignUser?.phoneNumbers && Array.isArray(campaignUser.phoneNumbers)) {
//             const matchedPhone = campaignUser.phoneNumbers.find(p =>
//                 p.id === campaignPhoneRef ||
//                 p.number === campaignPhoneRef ||
//                 p._id?.toString() === campaignPhoneRef?.toString()
//             );

//             if (matchedPhone?.concurrentCalls) {
//                 userConcurrentLimit = matchedPhone.concurrentCalls;
//                 console.log(`📞 [Scheduler] Phone ${matchedPhone.number} has concurrentCalls limit: ${userConcurrentLimit}`);
//             } else {
//                 // Fallback: sum all phone concurrentCalls for this user (if no phoneRef on campaign)
//                 const totalLimit = campaignUser.phoneNumbers.reduce((sum, p) => sum + (p.concurrentCalls || 0), 0);
//                 userConcurrentLimit = totalLimit || campaign.concurrentCalls || 10;
//                 console.log(`📞 [Scheduler] No phone match found. Using total/fallback limit: ${userConcurrentLimit}`);
//             }
//         } else {
//             userConcurrentLimit = campaign.concurrentCalls || 10;
//             console.log(`📞 [Scheduler] No phoneNumbers on user. Falling back to campaign limit: ${userConcurrentLimit}`);
//         }

//         const now = new Date();
//         const contactCursor = db.collection('contactprocessings').find({
//             campaignId: campaign._id,
//             $or: [
//                 { status: { $in: ['pending', 'enqueued'] }, $or: [{ scheduledAt: { $exists: false } }, { scheduledAt: { $lte: now } }] },
//                 {
//                     status: 'retry',
//                     nextRetryAt: { $lte: now }
//                 }
//             ]
//         }).project({ _id: 1, phone: 1, mobileNumber: 1, userId: 1, isVip: 1, retryCount: 1 });

//         let batch = [];
//         const batchSize = 1000;

//         while (await contactCursor.hasNext()) {
//             const contact = await contactCursor.next();

//             batch.push({
//                 name: `job_${contact._id}`,
//                 data: {
//                     campaignId: campaign._id.toString(),
//                     contactId: contact._id.toString(),
//                     userId: campaign.createdBy,
//                     phone: contact.mobileNumber || contact.contactData?.Number || contact.phone, // Flexible phone mapping
//                     metadata: {
//                         businessHours: campaign.callingHours || campaign.businessHours,
//                         campaignLimit: campaign.concurrentCalls || 500,
//                         userLimit: userConcurrentLimit, // ← user's actual purchased limit (cross-campaign total)
//                         maxRetryAttempts: campaign.maxRetryAttempts || 3,
//                         retryDelayMinutes: campaign.retryDelayMinutes || 30,
//                         voiceTier: campaign.selectedVoice?.tier || 'premium',
//                         ignoreEndWindow
//                     }
//                 },
//                 opts: {
//                     priority: calculatePriority(campaign, contact),
//                     jobId: `call:${campaign._id}:${contact._id}` // Idempotency key
//                 }
//             });

//             if (batch.length >= batchSize) {
//                 await this.enqueueBatch(batch);
//                 batch = [];
//             }
//         }

//         if (batch.length > 0) {
//             await this.enqueueBatch(batch);
//         }

//         // 3. Final Step: Check if the campaign is now fully completed
//         // Count contacts that are NOT in a final state (completed or failed)
//         const pendingCount = await db.collection('contactprocessings').countDocuments({
//             campaignId: campaign._id,
//             status: { $nin: ['completed', 'failed'] }
//         });

//         if (pendingCount === 0) {
//             // If campaign uses Google Sheet with auto-sync, don't complete until autoSyncUntilDate
//             let canComplete = true;
//             if (campaign.googleSheetsDataId) {
//                 try {
//                     // sheetDoc may have been fetched earlier; if not, fetch here.
//                     if (!sheetDoc) {
//                         sheetDoc = await db.collection('googlesheetsdatas').findOne({
//                             _id: new ObjectId(campaign.googleSheetsDataId)
//                         });
//                     }
//                     if (sheetDoc?.autoSyncEnabled === true && sheetDoc?.autoSyncUntilDate) {
//                         const until = new Date(sheetDoc.autoSyncUntilDate);
//                         if (Date.now() < until.getTime()) {
//                             canComplete = false;
//                             console.log(`📅 [Scheduler] Campaign ${campaign.campaignName} (${campaign._id}) has auto-sync until ${until.toISOString()}. Not marking completed yet.`);
//                         }
//                     }
//                 } catch (err) {
//                     console.warn(`[Scheduler] Could not check GoogleSheetsData for campaign ${campaign._id}:`, err.message);
//                 }
//             }
//             if (canComplete) {
//                 console.log(`🏁 [Scheduler] Campaign ${campaign.campaignName} (${campaign._id}) is fully completed.`);
//                 await db.collection('campaigns').updateOne(
//                     { _id: campaign._id },
//                     {
//                         $set: {
//                             status: 'completed',
//                             completedAt: new Date(),
//                             updatedAt: new Date()
//                         }
//                     }
//                 );
//             }
//         }
//     }

//     async enqueueBatch(jobs) {
//         try {
//             await this.queue.addBulk(jobs);
//             console.log(`✅ [Scheduler] Enqueued batch of ${jobs.length} jobs.`);

//             // Update status to 'enqueued' to avoid double scheduling in next run
//             const db = await getDb();
//             const contactIds = jobs.map(j => new ObjectId(j.data.contactId));
//             await db.collection('contactprocessings').updateMany(
//                 { _id: { $in: contactIds } },
//                 { $set: { status: 'enqueued' } }
//             );
//         } catch (error) {
//             console.error('❌ [Scheduler] Error enqueuing batch:', error.message);
//         }
//     }

//     /**
//      * Scans call_analysis for results that indicate a follow-up call is needed.
//      */
//     async processFollowUps(db) {
//         try {
//             // Find analysis records from the last 6 hours that indicate a follow-up is needed and haven't been processed.
//             const sixHoursAgo = new Date(Date.now() - 6 * 60 * 60 * 1000);

//             const analysesToProcess = await db.collection('call_analysis').find({
//                 'analysis_data.Next_Action.Type': { $regex: /^call$/i },
//                 followUpProcessed: { $ne: true },
//                 created_at: { $gte: sixHoursAgo }
//             }).toArray();

//             if (analysesToProcess.length === 0) return;

//             console.log(`🧠 [Scheduler] Found ${analysesToProcess.length} recent follow-up analysis records to process.`);

//             for (const analysis of analysesToProcess) {
//                 try {
//                     const { contact_id, campaign_id, analysis_data } = analysis;

//                     if (!contact_id || !campaign_id) {
//                         console.warn(`⚠️ [Scheduler] Skipping follow-up for analysis ${analysis._id} due to missing IDs.`);
//                         await db.collection('call_analysis').updateOne({ _id: analysis._id }, { $set: { followUpProcessed: true, followUpError: 'missing_ids' } });
//                         continue;
//                     }

//                     // 1. Check if campaign has follow-ups enabled
//                     const campaign = await db.collection('campaigns').findOne({ _id: new ObjectId(campaign_id) });
//                     if (!campaign || campaign.followup !== true) {
//                         console.log(`ℹ️ [Scheduler] Follow-up NOT enabled for campaign ${campaign_id}. Marking analysis as processed.`);
//                         await db.collection('call_analysis').updateOne({ _id: analysis._id }, { $set: { followUpProcessed: true } });
//                         continue;
//                     }

//                     // 2. Determine scheduled time
//                     const nextAction = analysis_data?.Next_Action;
//                     let scheduledTime;
//                     const analysisTime = nextAction?.Scheduled_Time || analysis.time;

//                     if (analysisTime) {
//                         scheduledTime = parseISTTimeToDate(analysisTime);
//                         if (!scheduledTime) {
//                             console.warn(`⚠️ [Scheduler] Could not parse follow-up time "${analysisTime}" for analysis ${analysis._id}. Falling back to 30 mins.`);
//                             scheduledTime = new Date(Date.now() + 30 * 60 * 1000);
//                         }
//                     } else {
//                         console.log(`ℹ️ [Scheduler] No follow-up time in analysis ${analysis._id}. Defaulting to 30 minutes.`);
//                         scheduledTime = new Date(Date.now() + 30 * 60 * 1000);
//                     }

//                     // 3. Fetch the original contact to duplicate its data
//                     const originalContact = await db.collection('contactprocessings').findOne({ _id: new ObjectId(contact_id) });
//                     if (!originalContact) {
//                         console.warn(`⚠️ [Scheduler] Original contact ${contact_id} not found for follow-up analysis ${analysis._id}.`);
//                         await db.collection('call_analysis').updateOne({ _id: analysis._id }, { $set: { followUpProcessed: true, followUpError: 'contact_not_found' } });
//                         continue;
//                     }

//                     // 4. Create a new contact processing entry for the follow-up
//                     const { _id, ...contactDataWithoutId } = originalContact;
//                     const followUpEntry = {
//                         ...contactDataWithoutId,
//                         status: 'pending',
//                         callReceiveStatus: 0,
//                         retryCount: 0,
//                         callAttempts: [],
//                         scheduledAt: scheduledTime,
//                         parentId: _id,
//                         isFollowUp: true,
//                         createdAt: new Date(),
//                         updatedAt: new Date()
//                     };

//                     await db.collection('contactprocessings').insertOne(followUpEntry);

//                     // 5. Mark analysis as processed
//                     await db.collection('call_analysis').updateOne(
//                         { _id: analysis._id },
//                         { $set: { followUpProcessed: true, followUpScheduledAt: scheduledTime } }
//                     );

//                     console.log(`✅ [Scheduler] Scheduled follow-up for contact ${contact_id} at ${scheduledTime.toISOString()} based on analysis ${analysis._id}.`);
//                 } catch (err) {
//                     console.error(`❌ [Scheduler] Error processing analysis record ${analysis._id}:`, err.message);
//                 }
//             }
//         } catch (error) {
//             console.error('❌ [Scheduler] processFollowUps failed:', error.message);
//         }
//     }
// }

import { getDb, createQueue, calculatePriority, isWithinBusinessHours, parseISTTimeToDate, isCampaignBlockedByTestCall } from 'shared-lib';
import { ObjectId } from 'mongodb';
import { DateTime } from 'luxon';

export class Scheduler {
    constructor() {
        this.queue = createQueue();
        this.isFollowUpEngineStarted = false;
        this.campaignParallelism = Math.max(1, parseInt(process.env.SCHEDULER_CAMPAIGN_PARALLELISM || '4', 10));
        this.queueBackpressureThreshold = Math.max(1000, parseInt(process.env.SCHEDULER_QUEUE_BACKPRESSURE_THRESHOLD || '50000', 10));
        this.queueResumeThreshold = Math.max(500, parseInt(process.env.SCHEDULER_QUEUE_RESUME_THRESHOLD || '30000', 10));
    }

    /**
     * Main loops that monitors campaigns and enqueues contacts.
     * In a real production system, this could be triggered by a cron job or a dedicated loop.
     */
    async run() {
        console.log('🚀 [Scheduler] Starting contact scanning loop...');
        const db = await getDb();

        // 0. Global expiry sweep: expire campaigns across non-terminal statuses
        // when end window has passed and tillCallsComplete !== true.
        await this.expirePastEndWindowCampaigns(db);

        // 0a. Activate immediateStart campaigns once their script is ready
        await this.activateImmediateStartCampaigns(db);

        // 0b. Activate scheduled campaigns whose time has arrived
        await this.activateScheduledCampaigns(db);

        // 0c. Process call analysis results for follow-ups (Hybrid Scale Model)
        if (!this.isFollowUpEngineStarted) {
            await this.initializeFollowUpEngine(db);
            this.isFollowUpEngineStarted = true;
        } else {
            // Periodic Reconciliation (The watcher handles real-time)
            await this.processFollowUps(db);
        }

        // 1. Find active or processing campaigns
        const activeCampaigns = await db.collection('campaigns').find({
            status: { $in: ['active', 'processing'] },
            archive: { $ne: true }
        }).toArray();

        console.log(`🔍 [Scheduler] Found ${activeCampaigns.length} active campaigns.`);
        await this.processCampaignsWithConcurrency(activeCampaigns, db);
    }

    async expirePastEndWindowCampaigns(db) {
        const candidates = await db.collection('campaigns').find({
            status: { $nin: ['expired', 'completed', 'failed'] },
            archive: { $ne: true },
            tillCallsComplete: { $ne: true },
            endDate: { $exists: true, $ne: null }
        }).toArray();

        if (candidates.length === 0) return;

        let expiredCount = 0;
        for (const campaign of candidates) {
            const expired = await this.expireCampaignIfPastEndWindow({
                campaign,
                db,
                context: 'global expiry sweep'
            });
            if (expired) expiredCount += 1;
        }

        if (expiredCount > 0) {
            console.log(`⌛ [Scheduler] Global expiry sweep marked ${expiredCount} campaign(s) as expired.`);
        }
    }

    async isQueueBackpressured() {
        const counts = await this.queue.getJobCounts('waiting', 'active', 'delayed', 'prioritized');
        const totalQueued =
            (counts.waiting || 0) +
            (counts.active || 0) +
            (counts.delayed || 0) +
            (counts.prioritized || 0);
        const threshold = this.queueBackpressureThreshold;
        const resumeAt = Math.min(threshold - 1, this.queueResumeThreshold);
        return {
            pressured: totalQueued >= threshold,
            totalQueued,
            threshold,
            resumeAt,
        };
    }

    async processCampaignsWithConcurrency(campaigns, db) {
        if (!Array.isArray(campaigns) || campaigns.length === 0) return;
        let idx = 0;
        const workers = Array.from({ length: Math.min(this.campaignParallelism, campaigns.length) }, () => (async () => {
            while (idx < campaigns.length) {
                const currentIdx = idx++;
                const campaign = campaigns[currentIdx];
                if (!campaign) continue;
                await this.processCampaign(campaign, db);
            }
        })());
        await Promise.all(workers);
    }

    /**
     * Handles campaigns created with immediateStart: true.
     * Instead of waiting for startTime, we poll the campaignscripts collection
     * every scheduler loop (~30s). The moment a script is found, the campaign
     * is validated and activated immediately.
     */
    async activateImmediateStartCampaigns(db) {
        // Find all immediateStart campaigns that are still waiting to be activated
        // We now also check for customizeSchedule: false as a synonym for immediateStart: true
        const pendingCampaigns = await db.collection('campaigns').find({
            $or: [
                { immediateStart: true },
                { customizeSchedule: false }
            ],
            status: { $nin: ['active', 'completed', 'paused', 'failed', 'expired'] },
            archive: { $ne: true }
        }).toArray();

        if (pendingCampaigns.length === 0) return;

        console.log(`⚡ [Scheduler] Checking ${pendingCampaigns.length} immediateStart campaign(s) for script readiness...`);

        for (const campaign of pendingCampaigns) {
            const campaignId = campaign._id;
            const isExpiredByWindow = await this.expireCampaignIfPastEndWindow({ campaign, db, context: 'immediateStart activation' });
            if (isExpiredByWindow) {
                continue;
            }

            // Check if the campaign script has been generated yet
            const script = await db.collection('campaign_scripts').findOne({
                $or: [
                    { campaignId: campaignId },
                    { campaignId: campaignId.toString() }
                ]
            });

            if (!script) {
                console.log(`⏳ [Scheduler] Script not ready yet for immediateStart campaign: ${campaign.campaignName} (${campaignId}). Will retry next loop.`);
                continue;
            }

            console.log(`✅ [Scheduler] Script found for campaign: ${campaign.campaignName} (${campaignId}). Proceeding to activate...`);

            // Validate required fields (startDate/startTime not required for immediateStart)
            const { isValid, missingFields } = this.validateCampaignConfig(campaign);
            if (!isValid) {
                console.warn(`[Scheduler] immediateStart campaign ${campaign.campaignName} (${campaignId}) missing fields: ${missingFields.join(', ')}`);
                await db.collection('campaigns').updateOne(
                    { _id: campaignId },
                    {
                        $set: {
                            status: 'draft',
                            error: `Missing required fields: ${missingFields.join(', ')}`,
                            updatedAt: new Date()
                        }
                    }
                );
                continue;
            }

            // Check contacts exist
            const contactExists = await db.collection('contactprocessings').findOne({ campaignId });
            if (!contactExists) {
                console.warn(`[Scheduler] immediateStart campaign ${campaign.campaignName} (${campaignId}) has no contacts. Skipping activation.`);
                continue;
            }

            if (isCampaignBlockedByTestCall(campaign)) {
                console.log(`🧪 [Scheduler] immediateStart campaign ${campaign.campaignName} (${campaignId}) is testing; waiting for testCallStatus=passed.`);
                continue;
            }

            // Activate!
            await db.collection('campaigns').updateOne(
                { _id: campaignId },
                {
                    $set: {
                        status: 'active',
                        activatedAt: new Date(),
                        updatedAt: new Date()
                    }
                }
            );
            console.log(`🚀 [Scheduler] immediateStart campaign ACTIVATED: ${campaign.campaignName} (${campaignId})`);
        }
    }

    /**
     * Finds scheduled campaigns and activates them if start time has passed.
     * Includes validations for campaign status and required configuration fields.
     */
    async activateScheduledCampaigns(db) {
        const now = new Date();
        const istTime = new Date(now.getTime() + (5.5 * 60 * 60 * 1000)); // Quick IST conversion
        const currentDate = istTime.toISOString().split('T')[0];
        const currentTime = istTime.toISOString().split('T')[1].split('.')[0];

        // console.log(`🕒 [Scheduler] Checking for campaigns to activate or schedule... (Current IST: ${currentDate} ${currentTime})`);

        // Find campaigns that are scheduled or active (to verify if they should still be active)
        const candidates = await db.collection('campaigns').find({
            status: { $in: ['active', 'scheduled', 'processing', 'draft', 'error', 'pending', 'testing'] },
            archive: { $ne: true }
        }).toArray();

        for (const campaign of candidates) {
            const campaignId = campaign._id;

            // Skip immediateStart campaigns here as they are handled by activateImmediateStartCampaigns
            const isImmediate = campaign.immediateStart === true || campaign.customizeSchedule === false;
            if (isImmediate) continue;
            const isExpiredByWindow = await this.expireCampaignIfPastEndWindow({ campaign, db, context: 'scheduled activation' });
            if (isExpiredByWindow) {
                continue;
            }

            // 1. Check if the campaign script has been generated yet
            const script = await db.collection('campaign_scripts').findOne({
                $or: [
                    { campaignId: campaignId },
                    { campaignId: campaignId.toString() }
                ]
            });

            if (!script) {
                // Keep status as 'processing' (or whatever it was) until script is ready
                continue;
            }

            // 2. Perform field validation
            const { isValid, missingFields } = this.validateCampaignConfig(campaign);
            if (!isValid) {
                console.warn(`[Scheduler] Campaign ${campaign.campaignName} (${campaignId}) missing fields: ${missingFields.join(', ')}`);
                await db.collection('campaigns').updateOne(
                    { _id: campaignId },
                    {
                        $set: {
                            status: 'draft',
                            error: `Missing required fields: ${missingFields.join(', ')}`,
                            updatedAt: new Date()
                        }
                    }
                );
                continue;
            }

            if (isCampaignBlockedByTestCall(campaign)) {
                console.log(`🧪 [Scheduler] Campaign ${campaign.campaignName} (${campaignId}) is testing; waiting for testCallStatus=passed.`);
                continue;
            }

            // 3. Check if it's time to activate
            // If date is today, check time. If date is past, activate immediately.
            const isTimeReached = campaign.startDate < currentDate ||
                (campaign.startDate === currentDate && campaign.startTime <= currentTime);

            if (isTimeReached) {
                // Only activate if not already active
                if (campaign.status !== 'active') {
                    // Check if contacts exist for this campaign
                    const contactExists = await db.collection('contactprocessings').findOne({ campaignId: campaign._id });
                    if (!contactExists) {
                        console.warn(`[Scheduler] Campaign ${campaign.campaignName} (${campaign._id}) has no contacts. Skipping activation.`);
                        continue;
                    }

                    console.log(`✨ [Scheduler] Activating campaign: ${campaign.campaignName} (${campaign._id})`);
                    await db.collection('campaigns').updateOne(
                        { _id: campaign._id },
                        { $set: { status: 'active', activatedAt: new Date(), updatedAt: new Date() } }
                    );
                }
            } else if (campaign.status !== 'scheduled') {
                // Start time not reached or campaign was prematurely set to active -> Set to Scheduled
                console.log(`🕒 [Scheduler] Campaign ${campaign.campaignName} (${campaignId}) start time not reached. Setting status to scheduled.`);
                await db.collection('campaigns').updateOne(
                    { _id: campaignId },
                    { $set: { status: 'scheduled', updatedAt: new Date() } }
                );
            }
        }
    }

    /**
     * Validates that all required fields for a campaign are present.
     */
    validateCampaignConfig(campaign) {
        // immediateStart campaigns don't need startDate/startTime — they activate on script readiness
        const isImmediate = campaign.immediateStart === true || campaign.customizeSchedule === false;
        const requiredFields = [
            ...(!isImmediate ? [
                { field: 'startDate', label: 'Start Date' },
                { field: 'startTime', label: 'Start Time' }
            ] : []),
            { field: 'concurrentCalls', label: 'Concurrent Calls' },
            { field: 'createdBy', label: 'Created By' },
            { field: 'agentName', label: 'Agent Name' },
            { field: 'selectedVoice', label: 'Selected Voice' }
        ];

        // Check for business hours either in 'callingHours' or 'businessHours'
        const hasBusinessHours = (campaign.callingHours && Object.keys(campaign.callingHours).length > 0) ||
            (campaign.businessHours && Object.keys(campaign.businessHours).length > 0);

        const missingFields = requiredFields
            .filter(f => !campaign[f.field])
            .map(f => f.label);

        if (!hasBusinessHours) {
            missingFields.push('Business Hours');
        }

        return {
            isValid: missingFields.length === 0,
            missingFields
        };
    }

    getCampaignEndDateTime(campaign) {
        if (!campaign?.endDate) return null;
        const tz = campaign.timezone || 'Asia/Kolkata';
        let zone = 'Asia/Kolkata';
        if (typeof tz === 'string') {
            if (tz.includes('Chennai') || tz.includes('Kolkata') || tz.includes('Mumbai')) {
                zone = 'Asia/Kolkata';
            } else {
                const match = tz.match(/UTC([+-]\d+:\d+)/);
                zone = match ? `UTC${match[1]}` : tz;
            }
        }

        const endDateISO = (campaign.endDate instanceof Date)
            ? DateTime.fromJSDate(campaign.endDate).toISODate()
            : (typeof campaign.endDate === 'string' ? (campaign.endDate.match(/^(\d{4}-\d{2}-\d{2})/)?.[1] || null) : null);
        if (!endDateISO) return null;

        let endDt = DateTime.fromISO(endDateISO, { zone }).endOf('day');
        if (typeof campaign.endTime === 'string' && campaign.endTime.trim()) {
            const t = campaign.endTime.trim();
            const formats = ['HH:mm:ss', 'HH:mm', 'h:mm a', 'h:mm:ss a'];
            for (const fmt of formats) {
                const parsed = DateTime.fromFormat(t, fmt, { zone: 'UTC' });
                if (parsed.isValid) {
                    endDt = DateTime.fromISO(endDateISO, { zone }).set({
                        hour: parsed.hour,
                        minute: parsed.minute,
                        second: parsed.second,
                        millisecond: 0
                    });
                    break;
                }
            }
        }
        return endDt.isValid ? endDt : null;
    }

    async expireCampaignIfPastEndWindow({ campaign, db, context = 'scheduler', ignoreEndWindow }) {
        if (!campaign || campaign.tillCallsComplete === true) return false;
        if (campaign.status === 'draft') return false;

        let shouldIgnoreEndWindow = ignoreEndWindow === true;
        if (ignoreEndWindow !== true && campaign.googleSheetsDataId) {
            try {
                const sheetDoc = await db.collection('googlesheetsdatas').findOne({
                    _id: new ObjectId(campaign.googleSheetsDataId)
                });
                shouldIgnoreEndWindow = sheetDoc?.autoSyncEnabled === true;
            } catch (err) {
                console.warn(`[Scheduler] Could not fetch GoogleSheetsData for campaign ${campaign._id}:`, err.message);
            }
        }
        if (shouldIgnoreEndWindow) return false;

        const endDt = this.getCampaignEndDateTime(campaign);
        if (!endDt) return false;

        const now = DateTime.now().setZone(endDt.zoneName);
        if (now <= endDt) return false;

        const updated = await db.collection('campaigns').updateOne(
            { _id: campaign._id, status: { $ne: 'expired' } },
            {
                $set: {
                    status: 'expired',
                    expiredAt: new Date(),
                    updatedAt: new Date()
                }
            }
        );
        if (updated.modifiedCount > 0) {
            console.log(`⌛ [Scheduler] Campaign ${campaign.campaignName} (${campaign._id}) marked expired in ${context}; end window was ${endDt.toISO()}.`);
        }
        return true;
    }

    /**
     * Processes a single campaign by scanning pending contacts.
     */
    async processCampaign(campaign, db) {
        // If campaign uses Google Sheet with auto-sync enabled, we ignore endDate/endTime for dialing.
        // We fetch the sheet doc once here and reuse it for both the end-window guard and completion logic.
        let sheetDoc = null;
        let ignoreEndWindow = false;
        if (campaign?.googleSheetsDataId) {
            try {
                sheetDoc = await db.collection('googlesheetsdatas').findOne({
                    _id: new ObjectId(campaign.googleSheetsDataId)
                });
                ignoreEndWindow = sheetDoc?.autoSyncEnabled === true;
            } catch (err) {
                console.warn(`[Scheduler] Could not fetch GoogleSheetsData for campaign ${campaign._id}:`, err.message);
            }
        }
        // End window guard: if endDate/endTime is passed and tillCallsComplete !== true, mark campaign expired and stop scheduling.
        const isExpiredByWindow = await this.expireCampaignIfPastEndWindow({
            campaign,
            db,
            context: 'campaign processing',
            ignoreEndWindow
        });
        if (isExpiredByWindow) {
            return;
        }

        if (!isWithinBusinessHours(campaign)) {
            // console.log(`🕒 [Scheduler] Campaign ${campaign.campaignName} (${campaign._id}) is currently outside calling hours. Skipping.`);
            return;
        }

        console.log(`📡 [Scheduler] Processing campaign: ${campaign.campaignName} (${campaign._id})`);

        // Fetch the user's purchased concurrent call limit from the DB
        // The limit is stored per-phone-number in user.phoneNumbers[].concurrentCalls
        const campaignUser = await db.collection('users').findOne(
            { email: campaign.createdBy },
            { projection: { phoneNumbers: 1 } }
        );

        // Find the phone number entry that matches this campaign's selected phone
        // campaign.phoneNumberId or campaign.phoneNumber references the purchased number
        const campaignPhoneRef = campaign.phoneNumberId || campaign.phoneNumber || campaign.fromNumber;
        let userConcurrentLimit = 10; // safe default

        if (campaignUser?.phoneNumbers && Array.isArray(campaignUser.phoneNumbers)) {
            const matchedPhone = campaignUser.phoneNumbers.find(p =>
                p.id === campaignPhoneRef ||
                p.number === campaignPhoneRef ||
                p._id?.toString() === campaignPhoneRef?.toString()
            );

            if (matchedPhone?.concurrentCalls) {
                userConcurrentLimit = matchedPhone.concurrentCalls;
                console.log(`📞 [Scheduler] Phone ${matchedPhone.number} has concurrentCalls limit: ${userConcurrentLimit}`);
            } else {
                // Fallback: sum all phone concurrentCalls for this user (if no phoneRef on campaign)
                const totalLimit = campaignUser.phoneNumbers.reduce((sum, p) => sum + (p.concurrentCalls || 0), 0);
                userConcurrentLimit = totalLimit || campaign.concurrentCalls || 10;
                console.log(`📞 [Scheduler] No phone match found. Using total/fallback limit: ${userConcurrentLimit}`);
            }
        } else {
            userConcurrentLimit = campaign.concurrentCalls || 10;
            console.log(`📞 [Scheduler] No phoneNumbers on user. Falling back to campaign limit: ${userConcurrentLimit}`);
        }

        const now = new Date();
        const contactQuery = {
            campaignId: campaign._id,
            $or: [
                { status: { $in: ['pending', 'enqueued'] }, $or: [{ scheduledAt: { $exists: false } }, { scheduledAt: { $lte: now } }] },
                {
                    status: 'retry',
                    nextRetryAt: { $lte: now }
                }
            ]
        };

        const contactCursor = db.collection('contactprocessings').find(contactQuery)
            .project({ _id: 1, phone: 1, mobileNumber: 1, userId: 1, isVip: 1, retryCount: 1, isFollowUp: 1 });

        let batch = [];
        const batchSize = 1000;

        while (await contactCursor.hasNext()) {
            if (batch.length === 0) {
                const backpressure = await this.isQueueBackpressured();
                if (backpressure.pressured) {
                    console.warn(
                        `⏸️ [Scheduler] Stopping campaign enqueue due to backpressure ` +
                        `(campaign=${campaign._id}, queue=${backpressure.totalQueued}, threshold=${backpressure.threshold})`
                    );
                    break;
                }
            }
            const contact = await contactCursor.next();

            batch.push({
                name: `job_${contact._id}`,
                data: {
                    campaignId: campaign._id.toString(),
                    contactId: contact._id.toString(),
                    userId: campaign.createdBy,
                    phone: contact.mobileNumber || contact.contactData?.Number || contact.phone, // Flexible phone mapping
                    metadata: {
                        businessHours: campaign.callingHours || campaign.businessHours,
                        campaignLimit: campaign.concurrentCalls || 500,
                        userLimit: userConcurrentLimit, // ← user's actual purchased limit (cross-campaign total)
                        maxRetryAttempts: campaign.maxRetryAttempts || 3,
                        retryDelayMinutes: campaign.retryDelayMinutes || 30,
                        voiceTier: campaign.selectedVoice?.tier || 'premium',
                        ignoreEndWindow
                    }
                },
                opts: {
                    priority: calculatePriority(campaign, contact),
                    jobId: `call:${campaign._id}:${contact._id}` // Idempotency key
                }
            });

            if (batch.length >= batchSize) {
                await this.enqueueBatch(batch);
                batch = [];
            }
        }

        if (batch.length > 0) {
            await this.enqueueBatch(batch);
        }

        // 3. Final Step: Check if the campaign is now fully completed
        // Count contacts that are NOT in a final state (completed or failed)
        const pendingCount = await db.collection('contactprocessings').countDocuments({
            campaignId: campaign._id,
            status: { $nin: ['completed', 'failed'] }
        });

        // PRODUCTION GUARD: Only block completion for analyses that can schedule follow-ups.
        // Non-call next actions (e.g. "None") are intentionally excluded so campaigns can finish.
        const pendingAnalyses = await db.collection('call_analysis').countDocuments({
            campaign_id: campaign._id.toString(),
            followUpProcessed: { $ne: true },
            'analysis_data.Next_Action.Type': { $regex: /^call$/i }
        });

        if (pendingCount === 0 && pendingAnalyses === 0) {
            // If campaign uses Google Sheet with auto-sync, don't complete until autoSyncUntilDate
            let canComplete = true;
            if (campaign.googleSheetsDataId) {
                try {
                    // sheetDoc may have been fetched earlier; if not, fetch here.
                    if (!sheetDoc) {
                        sheetDoc = await db.collection('googlesheetsdatas').findOne({
                            _id: new ObjectId(campaign.googleSheetsDataId)
                        });
                    }
                    if (sheetDoc?.autoSyncEnabled === true && sheetDoc?.autoSyncUntilDate) {
                        const until = new Date(sheetDoc.autoSyncUntilDate);
                        if (Date.now() < until.getTime()) {
                            canComplete = false;
                            console.log(`📅 [Scheduler] Campaign ${campaign.campaignName} (${campaign._id}) has auto-sync until ${until.toISOString()}. Not marking completed yet.`);
                        }
                    }
                } catch (err) {
                    console.warn(`[Scheduler] Could not check GoogleSheetsData for campaign ${campaign._id}:`, err.message);
                }
            }
            if (canComplete) {
                console.log(`🏁 [Scheduler] Campaign ${campaign.campaignName} (${campaign._id}) is fully completed.`);
                await db.collection('campaigns').updateOne(
                    { _id: campaign._id },
                    {
                        $set: {
                            status: 'completed',
                            completedAt: new Date(),
                            updatedAt: new Date()
                        }
                    }
                );
            }
        }
    }

    async enqueueBatch(jobs) {
        try {
            const backpressure = await this.isQueueBackpressured();
            if (backpressure.pressured) {
                console.warn(
                    `⏸️ [Scheduler] Backpressure active. Skipping batch enqueue (${jobs.length} jobs). ` +
                    `queue=${backpressure.totalQueued}, threshold=${backpressure.threshold}, resumeAt=${backpressure.resumeAt}`
                );
                return;
            }
            await this.queue.addBulk(jobs);
            console.log(`✅ [Scheduler] Enqueued batch of ${jobs.length} jobs.`);

            // Update status to 'enqueued' to avoid double scheduling in next run
            const db = await getDb();
            const contactIds = jobs.map(j => new ObjectId(j.data.contactId));
            await db.collection('contactprocessings').updateMany(
                { _id: { $in: contactIds } },
                { $set: { status: 'enqueued' } }
            );
        } catch (error) {
            console.error('❌ [Scheduler] Error enqueuing batch:', error.message);
        }
    }

    /**
     * Entry point for follow-up management.
     * For 10cr+ records, we use a hybrid model:
     * 1. A real-time MongoDB Change Stream (Instant)
     * 2. A background polling loop (Reconciliation)
     */
    async initializeFollowUpEngine(db) {
        console.log('🛠️ [Scheduler] Initializing High-Scale Follow-up Engine...');

        // 1. Kick off real-time watcher (Production Ready)
        this.startAnalysisWatcher(db);

        // 2. Perform initial reconciliation poll to catch any records from while we were down
        await this.processFollowUps(db);
    }

    /**
     * REAL-TIME: Listens for new call_analysis records using MongoDB Change Streams.
     * This provides 0-latency follow-ups without continuously polling a 100M record table.
     */
    async startAnalysisWatcher(db) {
        try {
            console.log('👀 [Scheduler] Starting Real-time analysis watcher...');

            const pipeline = [
                {
                    $match: {
                        operationType: 'insert',
                        'fullDocument.analysis_data.Next_Action.Type': { $regex: /^call$/i },
                        'fullDocument.followUpProcessed': { $ne: true }
                    }
                }
            ];

            const changeStream = db.collection('call_analysis').watch(pipeline, { fullDocument: 'updateLookup' });

            changeStream.on('change', async (change) => {
                const analysis = change.fullDocument;
                if (!analysis) return;

                // Process immediately in real-time
                await this.processBatchOfAnalyses([analysis], db);
            });

            changeStream.on('error', (err) => {
                console.error('❌ [Scheduler] Change Stream error. Restarting in 5s...', err.message);
                setTimeout(() => this.startAnalysisWatcher(db), 5000);
            });

        } catch (error) {
            console.warn('⚠️ [Scheduler] Could not start Change Stream (Is this a Replica Set?). Falling back to polling only.', error.message);
        }
    }

    /**
     * RECONCILIATION: Background polling loop to catch any records missed by the watcher.
     * Optimized for 10cr+ scale using high-speed bulkWrite.
     */
    async processFollowUps(db) {
        try {
            const BATCH_SIZE = 1000; // Larger batch for reconciliation
            const MAX_TOTAL_PROCESS = 20000; // Higher cap for "10cr" scale bursts
            let totalProcessed = 0;

            console.log(`🚀 [Scheduler] Scanning for follow-up candidates (All unprocessed)...`);

            while (totalProcessed < MAX_TOTAL_PROCESS) {
                // Fetch using indexed fields only for speed
                const analysesToProcess = await db.collection('call_analysis').find({
                    'analysis_data.Next_Action.Type': { $regex: /^call$/i },
                    followUpProcessed: { $ne: true }
                })
                    .sort({ created_at: 1 }) // Process oldest first
                    .limit(BATCH_SIZE)
                    .toArray();

                if (analysesToProcess.length === 0) {
                    if (totalProcessed > 0) console.log(`🏁 [Scheduler] Reconciliation catch-up complete.`);
                    break;
                }

                await this.processBatchOfAnalyses(analysesToProcess, db);

                totalProcessed += analysesToProcess.length;
                console.log(`🔹 [Scheduler] Reconciliation Progress: ${totalProcessed} processed.`);
            }
        } catch (error) {
            console.error('❌ [Scheduler] processFollowUps failed:', error.stack);
        }
    }

    /**
     * CORE ENGINE: Processes a batch of analysis records and updates contacts in bulk.
     * Used by both the Real-time Watcher and the Polling Reconciler.
     */
    async processBatchOfAnalyses(analyses, db) {
        if (!analyses || analyses.length === 0) return;

        try {
            // 1. Atomic Claim (Prevents double-processing in high-scale clusters)
            const analysisIds = analyses.map(a => a._id);
            await db.collection('call_analysis').updateMany(
                { _id: { $in: analysisIds } },
                { $set: { followUpProcessed: 'in-progress', updatedAt: new Date() } }
            );

            // 2. Pre-fetch campaigns (Batch optimization)
            const campaignIds = [...new Set(analyses.map(a => a.campaign_id).filter(Boolean))];
            const campaigns = await db.collection('campaigns').find({
                _id: { $in: campaignIds.map(id => (typeof id === 'string' ? new ObjectId(id) : id)) }
            }).toArray();
            const campaignMap = new Map(campaigns.map(c => [c._id.toString(), c]));
            const contactIdToObjectId = (value) => {
                try {
                    return typeof value === 'string' ? new ObjectId(value) : value;
                } catch {
                    return null;
                }
            };
            const contactObjectIds = analyses
                .map(a => contactIdToObjectId(a.contact_id))
                .filter(Boolean);
            const contacts = contactObjectIds.length > 0
                ? await db.collection('contactprocessings').find(
                    { _id: { $in: contactObjectIds } },
                    { projection: { _id: 1, status: 1, callReceiveStatus: 1, nextRetryAt: 1, isFollowUp: 1 } }
                ).toArray()
                : [];
            const contactMap = new Map(contacts.map(c => [String(c._id), c]));

            const contactBulkUpdates = [];
            const contactPolicyAuditUpdates = [];
            const analysisFinalUpdates = [];

            for (const analysis of analyses) {
                const { contact_id, campaign_id, analysis_data } = analysis;
                const campaign = campaignMap.get(campaign_id?.toString());
                const contactObjId = contactIdToObjectId(contact_id);
                const contactSnapshot = contactObjId ? contactMap.get(String(contactObjId)) : null;

                // Skip if campaign is missing or follow-up disabled
                if (!campaign || campaign.followup !== true) {
                    analysisFinalUpdates.push({
                        updateOne: {
                            filter: { _id: analysis._id },
                            update: { $set: { followUpProcessed: true, followUpError: !campaign ? 'campaign_not_found' : 'followup_disabled', updatedAt: new Date() } }
                        }
                    });
                    continue;
                }

                if (isCampaignBlockedByTestCall(campaign)) {
                    analysisFinalUpdates.push({
                        updateOne: {
                            filter: { _id: analysis._id },
                            update: { $set: { followUpProcessed: true, followUpError: 'campaign_testing', updatedAt: new Date() } }
                        }
                    });
                    continue;
                }

                if (!contactObjId) {
                    analysisFinalUpdates.push({
                        updateOne: {
                            filter: { _id: analysis._id },
                            update: { $set: { followUpProcessed: true, followUpError: 'invalid_contact_id', updatedAt: new Date() } }
                        }
                    });
                    continue;
                }

                if (!contactSnapshot) {
                    analysisFinalUpdates.push({
                        updateOne: {
                            filter: { _id: analysis._id },
                            update: { $set: { followUpProcessed: true, followUpError: 'contact_not_found', updatedAt: new Date() } }
                        }
                    });
                    continue;
                }

                // retry_wins policy: follow-up scheduling only transitions a truly completed call.
                // This prevents follow-up writes from overriding retry/processing states.
                if (String(contactSnapshot.status) !== 'completed') {
                    contactPolicyAuditUpdates.push({
                        updateOne: {
                            filter: { _id: contactObjId },
                            update: {
                                $push: {
                                    statusHistory: {
                                        fromStatus: String(contactSnapshot.status || ''),
                                        toStatus: String(contactSnapshot.status || ''),
                                        reason: 'follow-up-skipped-retry-wins',
                                        policy: 'retry_wins',
                                        analysisId: analysis._id,
                                        note: 'Skipped follow-up because contact is not in completed state',
                                        timestamp: new Date()
                                    }
                                }
                            }
                        }
                    });
                    analysisFinalUpdates.push({
                        updateOne: {
                            filter: { _id: analysis._id },
                            update: {
                                $set: {
                                    followUpProcessed: true,
                                    followUpError: 'retry_wins_state_not_completed',
                                    followUpSkippedBecauseStatus: String(contactSnapshot.status || ''),
                                    updatedAt: new Date()
                                }
                            }
                        }
                    });
                    continue;
                }
                if (Number(contactSnapshot.callReceiveStatus) !== 3) {
                    contactPolicyAuditUpdates.push({
                        updateOne: {
                            filter: { _id: contactObjId },
                            update: {
                                $push: {
                                    statusHistory: {
                                        fromStatus: String(contactSnapshot.status || ''),
                                        toStatus: String(contactSnapshot.status || ''),
                                        reason: 'follow-up-skipped-retry-wins',
                                        policy: 'retry_wins',
                                        analysisId: analysis._id,
                                        note: `Skipped follow-up because callReceiveStatus=${Number(contactSnapshot.callReceiveStatus || 0)} (expected 3)`,
                                        timestamp: new Date()
                                    }
                                }
                            }
                        }
                    });
                    analysisFinalUpdates.push({
                        updateOne: {
                            filter: { _id: analysis._id },
                            update: {
                                $set: {
                                    followUpProcessed: true,
                                    followUpError: 'retry_wins_call_not_completed',
                                    followUpSkippedBecauseReceiveStatus: Number(contactSnapshot.callReceiveStatus || 0),
                                    updatedAt: new Date()
                                }
                            }
                        }
                    });
                    continue;
                }

                // Analyze timing
                const nextAction = analysis_data?.Next_Action;
                let scheduledTime = null;
                const rawTime = nextAction?.Scheduled_Time;
                if (rawTime) scheduledTime = parseISTTimeToDate(rawTime);
                if (!scheduledTime) {
                    scheduledTime = new Date(Date.now() + (campaign.retryDelayMinutes || 30) * 60 * 1000);
                }

                // Push Contact Update
                contactBulkUpdates.push({
                    updateOne: {
                        filter: { _id: contactObjId, status: 'completed', callReceiveStatus: 3 },
                        update: {
                            $set: {
                                status: 'pending',
                                scheduledAt: scheduledTime,
                                priority: nextAction?.Priority || 'Medium',
                                callReceiveStatus: 0,
                                nextRetryAt: null,
                                retryCount: 0,
                                isFollowUp: true,
                                lastFollowUpAt: new Date(),
                                updatedAt: new Date()
                            },
                            $push: {
                                statusHistory: {
                                    fromStatus: 'completed',
                                    toStatus: 'pending',
                                    reason: 'follow-up-analysis',
                                    analysisId: analysis._id,
                                    timestamp: new Date()
                                }
                            }
                        }
                    }
                });

                // Push Analysis Completion
                analysisFinalUpdates.push({
                    updateOne: {
                        filter: { _id: analysis._id },
                        update: {
                            $set: {
                                followUpProcessed: true,
                                followUpScheduledAt: scheduledTime,
                                followUpContactId: contact_id,
                                updatedAt: new Date()
                            }
                        }
                    }
                });
            }

            // 2b. Re-activate completed campaigns if they now have pending follow-ups
            const completedCampaignIdsToRestore = campaigns
                .filter(c => c.status === 'completed')
                .map(c => c._id);
            if (completedCampaignIdsToRestore.length > 0) {
                console.log(`🔋 [Scheduler] Re-activating ${completedCampaignIdsToRestore.length} campaigns for new follow-up tasks...`);
                await db.collection('campaigns').updateMany(
                    { _id: { $in: completedCampaignIdsToRestore } },
                    { $set: { status: 'active', updatedAt: new Date() } }
                );
            }

            // 3. High-Speed Execution
            if (contactBulkUpdates.length > 0) {
                await db.collection('contactprocessings').bulkWrite(contactBulkUpdates, { ordered: false });
            }
            if (contactPolicyAuditUpdates.length > 0) {
                await db.collection('contactprocessings').bulkWrite(contactPolicyAuditUpdates, { ordered: false });
            }
            if (analysisFinalUpdates.length > 0) {
                await db.collection('call_analysis').bulkWrite(analysisFinalUpdates, { ordered: false });
            }

        } catch (error) {
            console.error('❌ [Scheduler] processBatchOfAnalyses failed:', error.message);
        }
    }
}