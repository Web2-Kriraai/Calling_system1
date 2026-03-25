import { getDb, createQueue, calculatePriority, isWithinBusinessHours, parseISTTimeToDate } from 'shared-lib';
import { ObjectId } from 'mongodb';
import { DateTime } from 'luxon';

export class Scheduler {
    constructor() {
        this.queue = createQueue();
    }

    /**
     * Main loops that monitors campaigns and enqueues contacts.
     * In a real production system, this could be triggered by a cron job or a dedicated loop.
     */
    async run() {
        console.log('🚀 [Scheduler] Starting contact scanning loop...');
        const db = await getDb();

        // 0a. Activate immediateStart campaigns once their script is ready
        await this.activateImmediateStartCampaigns(db);

        // 0b. Activate scheduled campaigns whose time has arrived
        await this.activateScheduledCampaigns(db);

        // 0c. Process call analysis results for follow-ups
        await this.processFollowUps(db);

        // 1. Find active campaigns
        const activeCampaigns = await db.collection('campaigns').find({
            status: 'active',
            archive: { $ne: true }
        }).toArray();

        console.log(`🔍 [Scheduler] Found ${activeCampaigns.length} active campaigns.`);

        for (const campaign of activeCampaigns) {
            await this.processCampaign(campaign, db);
        }
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
            status: { $nin: ['active', 'completed', 'paused', 'failed'] },
            archive: { $ne: true }
        }).toArray();

        if (pendingCampaigns.length === 0) return;

        console.log(`⚡ [Scheduler] Checking ${pendingCampaigns.length} immediateStart campaign(s) for script readiness...`);

        for (const campaign of pendingCampaigns) {
            const campaignId = campaign._id;

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
            status: { $in: ['active', 'scheduled', 'processing', 'draft', 'error', 'pending'] },
            archive: { $ne: true }
        }).toArray();

        for (const campaign of candidates) {
            const campaignId = campaign._id;

            // Skip immediateStart campaigns here as they are handled by activateImmediateStartCampaigns
            const isImmediate = campaign.immediateStart === true || campaign.customizeSchedule === false;
            if (isImmediate) continue;

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

        // End window guard: if endDate/endTime is passed and tillCallsComplete !== true, do not enqueue new calls.
        // (Worker also enforces this as a hard safety net for already-enqueued jobs)
        try {
            if (!ignoreEndWindow && campaign?.tillCallsComplete !== true && campaign?.endDate) {
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

                if (endDateISO) {
                    // Default to end-of-day if endTime is missing/unparseable.
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

                    const now = DateTime.now().setZone(endDt.zoneName);
                    if (endDt.isValid && now > endDt) {
                        console.log(`⏹️ [Scheduler] Campaign ${campaign.campaignName} (${campaign._id}) is past end window (${endDt.toISO()}). Skipping enqueue.`);
                        return;
                    }
                }
            }
        } catch (e) {
            // Never block scheduler loop because of end-window parsing issues
            console.warn(`⚠️ [Scheduler] End window check failed for campaign ${campaign?._id}:`, e.message);
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
        const contactCursor = db.collection('contactprocessings').find({
            campaignId: campaign._id,
            $or: [
                { status: { $in: ['pending', 'enqueued'] }, $or: [{ scheduledAt: { $exists: false } }, { scheduledAt: { $lte: now } }] },
                {
                    status: 'retry',
                    nextRetryAt: { $lte: now }
                }
            ]
        }).project({ _id: 1, phone: 1, mobileNumber: 1, userId: 1, isVip: 1, retryCount: 1 });

        let batch = [];
        const batchSize = 1000;

        while (await contactCursor.hasNext()) {
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

        if (pendingCount === 0) {
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
     * Scans call_analysis for results that indicate a follow-up call is needed.
     */
    async processFollowUps(db) {
        try {
            // Find analysis records from the last 6 hours that indicate a follow-up is needed and haven't been processed.
            const sixHoursAgo = new Date(Date.now() - 6 * 60 * 60 * 1000);

            const analysesToProcess = await db.collection('call_analysis').find({
                'analysis_data.Next_Action.Type': { $regex: /^call$/i },
                followUpProcessed: { $ne: true },
                created_at: { $gte: sixHoursAgo }
            }).toArray();

            if (analysesToProcess.length === 0) return;

            console.log(`🧠 [Scheduler] Found ${analysesToProcess.length} recent follow-up analysis records to process.`);

            for (const analysis of analysesToProcess) {
                try {
                    const { contact_id, campaign_id, analysis_data } = analysis;

                    if (!contact_id || !campaign_id) {
                        console.warn(`⚠️ [Scheduler] Skipping follow-up for analysis ${analysis._id} due to missing IDs.`);
                        await db.collection('call_analysis').updateOne({ _id: analysis._id }, { $set: { followUpProcessed: true, followUpError: 'missing_ids' } });
                        continue;
                    }

                    // 1. Check if campaign has follow-ups enabled
                    const campaign = await db.collection('campaigns').findOne({ _id: new ObjectId(campaign_id) });
                    if (!campaign || campaign.followup !== true) {
                        console.log(`ℹ️ [Scheduler] Follow-up NOT enabled for campaign ${campaign_id}. Marking analysis as processed.`);
                        await db.collection('call_analysis').updateOne({ _id: analysis._id }, { $set: { followUpProcessed: true } });
                        continue;
                    }

                    // 2. Determine scheduled time
                    const nextAction = analysis_data?.Next_Action;
                    let scheduledTime;
                    const analysisTime = nextAction?.Scheduled_Time || analysis.time;

                    if (analysisTime) {
                        scheduledTime = parseISTTimeToDate(analysisTime);
                        if (!scheduledTime) {
                            console.warn(`⚠️ [Scheduler] Could not parse follow-up time "${analysisTime}" for analysis ${analysis._id}. Falling back to 30 mins.`);
                            scheduledTime = new Date(Date.now() + 30 * 60 * 1000);
                        }
                    } else {
                        console.log(`ℹ️ [Scheduler] No follow-up time in analysis ${analysis._id}. Defaulting to 30 minutes.`);
                        scheduledTime = new Date(Date.now() + 30 * 60 * 1000);
                    }

                    // 3. Fetch the original contact to duplicate its data
                    const originalContact = await db.collection('contactprocessings').findOne({ _id: new ObjectId(contact_id) });
                    if (!originalContact) {
                        console.warn(`⚠️ [Scheduler] Original contact ${contact_id} not found for follow-up analysis ${analysis._id}.`);
                        await db.collection('call_analysis').updateOne({ _id: analysis._id }, { $set: { followUpProcessed: true, followUpError: 'contact_not_found' } });
                        continue;
                    }

                    // 4. Create a new contact processing entry for the follow-up
                    const { _id, ...contactDataWithoutId } = originalContact;
                    const followUpEntry = {
                        ...contactDataWithoutId,
                        status: 'pending',
                        callReceiveStatus: 0,
                        retryCount: 0,
                        callAttempts: [],
                        scheduledAt: scheduledTime,
                        parentId: _id,
                        isFollowUp: true,
                        createdAt: new Date(),
                        updatedAt: new Date()
                    };

                    await db.collection('contactprocessings').insertOne(followUpEntry);

                    // 5. Mark analysis as processed
                    await db.collection('call_analysis').updateOne(
                        { _id: analysis._id },
                        { $set: { followUpProcessed: true, followUpScheduledAt: scheduledTime } }
                    );

                    console.log(`✅ [Scheduler] Scheduled follow-up for contact ${contact_id} at ${scheduledTime.toISOString()} based on analysis ${analysis._id}.`);
                } catch (err) {
                    console.error(`❌ [Scheduler] Error processing analysis record ${analysis._id}:`, err.message);
                }
            }
        } catch (error) {
            console.error('❌ [Scheduler] processFollowUps failed:', error.message);
        }
    }
}