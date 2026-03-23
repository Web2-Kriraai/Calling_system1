import { Worker } from 'bullmq';
import {
    getRedis,
    getDb,
    concurrencyGuard,
    isWithinBusinessHours,
    queueName,
    postCallQueueName,
    createPostCallQueue,
    config
} from 'shared-lib';
import { ObjectId } from 'mongodb';

const POST_CALL_DELAY_MS = parseInt(process.env.POST_CALL_DELAY_MS || '2500', 10);
const POST_CALL_WORKER_CONCURRENCY = parseInt(process.env.POST_CALL_WORKER_CONCURRENCY || '500', 10);
const ANALYSIS_API_MAX_ATTEMPTS = parseInt(process.env.ANALYSIS_API_MAX_ATTEMPTS || '5', 10);
const ANALYSIS_API_RETRY_MS = parseInt(process.env.ANALYSIS_API_RETRY_MS || '3000', 10);
const ANALYSIS_API_INITIAL_DELAY_MS = parseInt(process.env.ANALYSIS_API_INITIAL_DELAY_MS || '2500', 10);
const CALL_CREDIT_MAX_ATTEMPTS = parseInt(process.env.CALL_CREDIT_MAX_ATTEMPTS || '5', 10);
const CALL_CREDIT_RETRY_MS = parseInt(process.env.CALL_CREDIT_RETRY_MS || '3000', 10);

/**
 * Billable duration (seconds) from CallLog events. Prefers call_answered + call_hangup timestamps, then cdr_push.Duration when ANSWER.
 */
function computeDurationFromEvents(events) {
    if (!events || !Array.isArray(events)) return 0;
    const hangup = events.find(e => e.event_type === 'call_hangup');
    const answered = events.find(e => e.event_type === 'call_answered');
    const cdrPush = events.find(e => e.event_type === 'cdr_push');

    if (hangup?.data?.end_time && answered?.data?.answer_time) {
        try {
            const end = new Date(hangup.data.end_time);
            const start = new Date(answered.data.answer_time);
            return Math.max(0, Math.floor((end - start) / 1000));
        } catch {
            /* fall through */
        }
    }
    if (cdrPush?.data?.CallStatus === 'ANSWER') {
        const d = parseInt(cdrPush.data.Duration, 10);
        if (!Number.isNaN(d) && d >= 0) return d;
    }
    return 0;
}

export class CallWorker {
    constructor() {
        this.postCallQueue = createPostCallQueue();

        this.worker = new Worker(queueName, this.processJob.bind(this), {
            connection: getRedis(),
            concurrency: 2000, // Increased to allow more jobs to "wait" for campaign slots
            lockDuration: 600000, // 10 minutes to match max call duration + polling
            limiter: {
                max: 5000,
                duration: 1000,
            }
        });

        this.worker.on('failed', (job, err) => {
            if (err.message !== 'OUTSIDE_BUSINESS_HOURS' && err.message !== 'CONCURRENCY_LIMIT_REACHED') {
                console.error(`❌ [Worker] Job ${job.id} failed:`, err.message);
            }
        });

        // Dedicated worker for post-call (billing, CallLog, analysis) — frees main workers immediately after call completes
        this.postCallWorker = new Worker(postCallQueueName, this.processPostCallJob.bind(this), {
            connection: getRedis(),
            concurrency: POST_CALL_WORKER_CONCURRENCY,
            lockDuration: 180000, // 3 min: CallLog poll + analysis HTTP
            limiter: {
                max: 2000,
                duration: 1000,
            }
        });

        this.postCallWorker.on('failed', (job, err) => {
            console.error(`❌ [PostCallWorker] Job ${job?.id} failed:`, err.message);
        });
    }

    /**
     * Enqueue post-call processing with delay so provider/webhook can persist CallLog before we run.
     */
    async enqueuePostCallJob(jobData, result) {
        const { campaignId, contactId } = jobData;
        const api = result?.apiResponse || {};
        const apiCallId = api.call?.id || api.call_id || api.id || api.callId;
        // BullMQ / Redis: custom jobId must not contain ":" — use hyphens only
        const jobId =
            apiCallId != null && !String(apiCallId).startsWith('call_')
                ? `post-call-${campaignId}-${contactId}-${String(apiCallId)}`
                : `post-call-${campaignId}-${contactId}-${Date.now()}`;

        try {
            await this.postCallQueue.add(
                'post-call',
                { jobData, apiResponse: api },
                { jobId, delay: POST_CALL_DELAY_MS }
            );
            console.log(`📬 [Worker] Post-call job queued (${jobId}, delay ${POST_CALL_DELAY_MS}ms)`);
        } catch (err) {
            // Duplicate jobId while job still in queue — safe to ignore
            if (String(err.message || '').toLowerCase().includes('already') || err.name === 'JobIdDuplicateError') {
                console.log(`ℹ️ [Worker] Post-call job already exists for ${jobId}, skipping duplicate enqueue`);
                return;
            }
            console.error(`❌ [Worker] Failed to enqueue post-call job:`, err.message);
            throw err;
        }
    }

    async processPostCallJob(job) {
        const { jobData, apiResponse } = job.data || {};
        if (!jobData || !jobData.contactId) {
            console.warn(`⚠️ [PostCallWorker] Invalid post-call job payload, skipping`);
            return;
        }
        const metadata = jobData.metadata || {};
        await this.triggerPostCallActions(jobData, { apiResponse }, metadata, null);
    }

    async close() {
        await Promise.all([
            this.worker.close(),
            this.postCallWorker.close(),
            this.postCallQueue.close()
        ]);
    }

    /**
     * Persist operational errors to MongoDB errorlogs collection.
     * Uses native driver shape compatible with the provided ErrorLog mongoose schema.
     */
    async logErrorToDb({
        errorType = 'system_error',
        errorCategory = 'unknown',
        severity = 'error',
        errorMessage,
        errorStack,
        errorCode,
        userId,
        userEmail,
        campaignId,
        campaignName,
        contactId,
        callId,
        metadata
    }) {
        try {
            const db = await getDb();

            const toObjectIdOrNull = (value) => {
                try {
                    return value ? new ObjectId(String(value)) : null;
                } catch {
                    return null;
                }
            };

            const doc = {
                timestamp: new Date(),
                errorType,
                errorCategory,
                severity,
                errorMessage: String(errorMessage || 'Unknown error'),
                errorStack: errorStack ? String(errorStack) : undefined,
                errorCode: errorCode ? String(errorCode) : undefined,
                userId: userId ? String(userId) : undefined,
                userEmail: userEmail ? String(userEmail) : undefined,
                campaignId: toObjectIdOrNull(campaignId),
                campaignName: campaignName ? String(campaignName) : undefined,
                contactId: toObjectIdOrNull(contactId),
                callId: callId ? String(callId) : undefined,
                metadata,
                resolved: false
            };

            await db.collection('errorlogs').insertOne(doc);
        } catch (logErr) {
            // Never throw from error logger; avoid cascading failures.
            console.error('⚠️ [Worker] Failed to write errorlogs record:', logErr.message);
        }
    }

    async processJob(job) {
        const { campaignId, contactId, userId, metadata } = job.data;
        const contactObjId = new ObjectId(contactId);
        const { campaignLimit, userLimit, businessHours } = metadata;

        console.log(`👷 [Worker] Processing job ${job.id} for contact ${contactId}`);

        const db = await getDb();

        // 0. Fetch latest campaign data to check status (Ensures we respect Paused/Stopped campaigns immediately)
        const campaign = await db.collection('campaigns').findOne({ _id: new ObjectId(campaignId) });

        if (!campaign || campaign.status !== 'active' || campaign.archive === true) {
            const reason = !campaign ? 'Campaign not found' :
                campaign.archive === true ? 'Campaign is archived' :
                    `Campaign status is ${campaign.status}`;

            console.log(`⏸️ [Worker] Job ${job.id} for campaign ${campaignId} ignored: ${reason}.`);
            // Just return. The job is marked as "completed" in BullMQ, but since we didn't update MongoDB status, 
            // the Scheduler will pick up this contact again in the next loop when the campaign becomes active and not archived.
            return;
        }

        // 0.1 Fetch latest contact data and sync with BullMQ job data
        const contact = await db.collection('contactprocessings').findOne({ _id: contactObjId });

        // Ensure BullMQ job data stays in sync with real-time DB state
        await job.updateData({
            ...job.data,
            callReceiveStatus: contact?.callReceiveStatus,
            dbStatus: contact?.status
        });

        // 1. Validate Calling Hours
        if (!isWithinBusinessHours(campaign)) {
            console.log(`🕒 [Worker] Job ${job.id} for campaign ${campaignId} ignored: Outside calling hours.`);
            // Throwing an error forces BullMQ to retry the job according to its own backoff settings,
            // without needing manual state (moveToDelayed) which causes "Missing Lock" errors.
            throw new Error('OUTSIDE_BUSINESS_HOURS');
        }

        // 1.1 Validate Business Hours (Legacy - shared-lib might be using businessHours as fallback)
        // Note: The isWithinBusinessHours check above handles both callingHours and businessHours.

        // 2. Acquire Distributed Concurrency Slot (Wait for availability)
        let hasSlot = false;
        const checkInterval = 500; // Reduced to 500ms for faster acquisition
        const startTime = Date.now();
        const maxWaitTime = 3600000; // 1 hour safety timeout

        while (!hasSlot) {
            hasSlot = await concurrencyGuard.acquireSlot(
                campaignId,
                userId,
                campaignLimit || 500,
                userLimit || 100
            );

            if (!hasSlot) {
                if (Date.now() - startTime > maxWaitTime) {
                    throw new Error('CONCURRENCY_WAIT_TIMEOUT');
                }

                // Check if campaign is still active while waiting
                const currentCampaign = await db.collection('campaigns').findOne({ _id: new ObjectId(campaignId) });
                if (!currentCampaign || currentCampaign.status !== 'active' || currentCampaign.archive === true) {
                    console.log(`📡 [Worker] Job ${job.id} stopped waiting: Campaign no longer active.`);
                    return;
                }

                // Wait before next check
                await new Promise(resolve => setTimeout(resolve, checkInterval));
            }
        }

        let shouldReleaseSlot = true; // Declare here so it is accessible in finally{}
        let currentlyHoldingSlot = false;
        let attemptStartedAt = 0; // Set right after executeCall; used for nextRetryAt so delay is from attempt start, not poll end

        try {
            // 3. Update Status to 'processing'
            await db.collection('contactprocessings').updateOne(
                { _id: contactObjId },
                { $set: { status: 'processing', lastAttemptAt: new Date() } }
            );

            // 4. Fetch latest contact data again to ensure consistent retry logic after slot acquisition
            const currentContact = await db.collection('contactprocessings').findOne({ _id: contactObjId });
            if (!currentContact) throw new Error(`Contact ${contactId} not found`);
            if (['completed', 'failed'].includes(currentContact.status)) {
                console.log(`⏩ [Worker] Contact ${contactId} was handled while waiting for slot.`);
                return;
            }

            // 5. Execute API Call
            const result = await this.executeCall(job.data);
            attemptStartedAt = Date.now(); // Capture once; used for nextRetryAt and callAttempts timestamp (retry delay from attempt start)
            currentlyHoldingSlot = true; // Still holding the slot from the initiation acquisition

            // 6. Polling Logic: Wait for the API to register the call (Status 1, 2, or 3)
            console.log(`⏳ [Worker] Waiting for call registration for ${contactId}...`);
            let updatedContact = await this.pollStatus(contactId, [1, 2, 3], 120000, 1000); // Increased to 120s timeout for high concurrency
            let callStatus = parseInt(updatedContact?.callReceiveStatus) || 0;

            // 7. State-Based Slot Management
            if (callStatus === 1) {
                // INITIATED: User wants the slot FREE while waiting for conversation to start
                console.log(`📡 [Worker] Call ${contactId} is INITIATED. Releasing slot and waiting for transition to Running (2)...`);
                await concurrencyGuard.releaseSlot(campaignId, userId);
                currentlyHoldingSlot = false;
                shouldReleaseSlot = false;

                // Wait for status to become 2 (Running) or 3 (Completed)
                updatedContact = await this.pollStatus(contactId, [2, 3], 180000, 2000); // Increased to 180s for carrier/webhook delays
                callStatus = parseInt(updatedContact?.callReceiveStatus) || 0;

                if (callStatus === 2) {
                    console.log(`📡 [Worker] Call ${contactId} started RUNNING. Re-acquiring slot...`);
                    // Re-acquire slot for the duration of the conversation
                    let reAcquired = false;
                    while (!reAcquired) {
                        reAcquired = await concurrencyGuard.acquireSlot(
                            campaignId, userId, campaignLimit || 500, userLimit || 100
                        );
                        if (!reAcquired) {
                            const currentCampaign = await db.collection('campaigns').findOne({ _id: new ObjectId(campaignId) });
                            if (!currentCampaign || currentCampaign.status !== 'active' || currentCampaign.archive === true) {
                                console.log(`📡 [Worker] Campaign stopped. Dropping contact ${contactId}.`);
                                return;
                            }

                            // Check if call completed while waiting for the slot (prevents job from stalling)
                            const checkContact = await db.collection('contactprocessings').findOne({ _id: contactObjId });
                            const checkStatus = parseInt(checkContact?.callReceiveStatus) || 0;
                            if (checkStatus === 3 || checkStatus === 0) {
                                console.log(`📡 [Worker] Call ${contactId} status became ${checkStatus} while waiting for slot! Breaking re-acquire loop.`);
                                callStatus = checkStatus;
                                break;
                            }

                            await new Promise(resolve => setTimeout(resolve, 2000));
                        }
                    }

                    if (reAcquired) {
                        currentlyHoldingSlot = true;
                        shouldReleaseSlot = true;

                        // Now wait for completion
                        console.log(`📡 [Worker] Slot held for ${contactId}. Waiting for completion...`);
                        updatedContact = await this.pollStatus(contactId, [3], 600000, 2000);
                        callStatus = parseInt(updatedContact?.callReceiveStatus) || 0;
                    }
                }
            } else if (callStatus === 2) {
                // RUNNING immediately: Keep the slot and wait for completion
                console.log(`📡 [Worker] Call ${contactId} is already RUNNING. Keeping slot and waiting for completion...`);
                updatedContact = await this.pollStatus(contactId, [3], 600000, 2000);
                callStatus = parseInt(updatedContact?.callReceiveStatus) || 0;
            } else if (callStatus === 3) {
                // COMPLETED immediately: Release slot in the next step
                console.log(`📡 [Worker] Call ${contactId} COMPLETED immediately.`);
            }

            // Sync BullMQ job data with final status
            await job.updateData({
                ...job.data,
                callReceiveStatus: callStatus,
                dbStatus: updatedContact?.status
            });

            console.log(`🔍 [Worker] Final Status for ${contactId}: DB=${callStatus}`);

            const maxRetries = metadata.maxRetryAttempts || 3;
            const retryDelayMinutes = metadata.retryDelayMinutes || 30;
            const currentRetryCount = currentContact.retryCount || 0;
            const currentAttempts = (currentContact.callAttempts?.length || 0) + 1;

            if (callStatus === 3) {
                // COMPLETED: Success, release slot.
                console.log(`✅ [Worker] Call ${contactId} COMPLETED (confirmed by DB). Releasing slot.`);

                // Update contact status to completed and log successful attempt
                await db.collection('contactprocessings').updateOne(
                    { _id: contactObjId },
                    {
                        $set: {
                            status: 'completed',
                            callReceiveStatus: callStatus,
                            updatedAt: new Date()
                        },
                        $push: {
                            callAttempts: {
                                attempt: currentAttempts,
                                timestamp: new Date(),
                                status: 'completed',
                                message: 'Success'
                            }
                        }
                    }
                );

                // RELEASE SLOT if holding
                if (currentlyHoldingSlot) {
                    await concurrencyGuard.releaseSlot(campaignId, userId);
                    currentlyHoldingSlot = false;
                }
                shouldReleaseSlot = false;

                // Post-call on dedicated queue (frees slot fast). Fallback inline if Redis/queue fails.
                try {
                    await this.enqueuePostCallJob(job.data, result);
                } catch (triggerError) {
                    console.error(`⚠️ [Worker] Post-call enqueue failed, running inline:`, triggerError.message);
                    try {
                        await new Promise(r => setTimeout(r, POST_CALL_DELAY_MS));
                        await this.triggerPostCallActions(job.data, result, metadata, updatedContact || currentContact);
                    } catch (inlineErr) {
                        console.error(`⚠️ [Worker] Inline post-call failed:`, inlineErr.message);
                    }
                }
            } else {
                // FAILED (0) or NOT RECEIVED (1): Persist immediately; nextRetryAt from attempt start so delay is not inflated by polling
                const isRetryable = currentRetryCount + 1 < maxRetries;
                const nextStatus = isRetryable ? 'retry' : 'failed';
                const baseTime = attemptStartedAt > 0 ? attemptStartedAt : Date.now();
                const nextRetryAt = isRetryable ? new Date(baseTime + retryDelayMinutes * 60000) : null;
                const attemptTimestamp = new Date(attemptStartedAt > 0 ? attemptStartedAt : Date.now());

                await db.collection('contactprocessings').updateOne(
                    { _id: contactObjId },
                    {
                        $set: {
                            status: nextStatus,
                            callReceiveStatus: callStatus,
                            nextRetryAt,
                            updatedAt: new Date()
                        },
                        $inc: { retryCount: 1 },
                        $push: {
                            callAttempts: {
                                attempt: currentAttempts,
                                timestamp: attemptTimestamp,
                                status: nextStatus,
                                message: callStatus === 1 ? 'Not Received' : 'API Attempt Failed'
                            }
                        }
                    }
                );

                // RELEASE SLOT if holding
                if (currentlyHoldingSlot) {
                    await concurrencyGuard.releaseSlot(campaignId, userId);
                    currentlyHoldingSlot = false;
                }
                shouldReleaseSlot = false;

                console.log(`🔁 [Worker] Call ${contactId} FAILED/RETRY (Status: ${callStatus}). Releasing slot.`);
            }

            return result;
        } catch (error) {
            console.error(`❌ [Worker] Execution error for job ${job.id}:`, error.message);

            // Fetch state for system-level error retry
            const db = await getDb();
            const contact = await db.collection('contactprocessings').findOne({ _id: contactObjId });
            const campaignForError = await db.collection('campaigns').findOne({ _id: new ObjectId(campaignId) });
            const maxRetries = metadata.maxRetryAttempts || 3;
            const retryDelayMinutes = metadata.retryDelayMinutes || 30;
            const currentRetryCount = contact?.retryCount || 0;
            const currentAttempts = (contact?.callAttempts?.length || 0) + 1;

            const isRetryable = currentRetryCount + 1 < maxRetries;
            const status = isRetryable ? 'retry' : 'failed';
            const nextRetryAt = isRetryable ? new Date(Date.now() + retryDelayMinutes * 60000) : null;

            await db.collection('contactprocessings').updateOne(
                { _id: contactObjId },
                {
                    $set: {
                        status,
                        lastError: error.message,
                        nextRetryAt,
                        updatedAt: new Date()
                    },
                    $inc: { retryCount: 1 },
                    $push: {
                        callAttempts: {
                            attempt: currentAttempts,
                            timestamp: new Date(),
                            status,
                            message: `System Error: ${error.message}`
                        }
                    }
                }
            );

            console.log(`✅ [Worker] Error handled for contact ${contactId}. Next status: ${status}.`);

            await this.logErrorToDb({
                errorType: 'call_failure',
                errorCategory: 'worker_execution',
                severity: 'error',
                errorMessage: error.message,
                errorStack: error.stack,
                errorCode: String(error.message || '').includes('fetch failed') ? 'FETCH_FAILED' : undefined,
                userId: userId,
                userEmail: campaignForError?.createdBy,
                campaignId,
                campaignName: campaignForError?.campaignName || campaignForError?.name,
                contactId,
                metadata: {
                    jobId: job.id,
                    callReceiveStatus: contact?.callReceiveStatus,
                    retryStatus: status,
                    retryCountAfterUpdate: (contact?.retryCount || 0) + 1
                }
            });

            return { success: false, error: error.message };
        } finally {
            // 6. Release Concurrency Slot (Conditional - only if we had failed unexpectedly before status loop)
            if (typeof shouldReleaseSlot === 'undefined' || shouldReleaseSlot === true) {
                // If we get here, it means we didn't release the initial slot or re-acquired one and crashed
                await concurrencyGuard.releaseSlot(campaignId, userId);
            }
        }
    }
    async executeCall(data) {
        const db = await getDb();
        const campaign = await db.collection('campaigns').findOne({ _id: new ObjectId(data.campaignId) });

        const tier = campaign?.selectedVoice?.tier || 'premium';

        let url;
        if (tier === 'basic') {
            url = process.env.CALL_API_BASIC_URL || 'http://72.60.221.48:8000/api/v1/calls/initiate-campaign-call';
        } else {
            url = process.env.CALL_API_PREMIUM_URL || 'http://72.60.221.48:8000/api/v1/calls/initiate-campaign-call';
        }

        const payload = {
            campaign_id: data.campaignId,
            contact_id: data.contactId
        };

        console.log(`📞 [Worker] Making ${tier} call to ${data.phone || data.contactId} via API (${url})...`);

        const response = await fetch(url, {
            method: 'POST',
            headers: {
                'Content-Type': 'application/json',
                'X-API-Key': config.api.callingKey
            },
            body: JSON.stringify(payload)
        });

        if (!response.ok) {
            const errorText = await response.text();
            throw new Error(`API_CALL_FAILED: ${response.status} - ${errorText}`);
        }

        const result = await response.json();

        console.log(`✅ [Worker] API call successful:`, result);
        return {
            success: true,
            apiResponse: result,
            timestamp: new Date().toISOString()
        };
    }

    /**
     * Polls the database for a specific status update.
     * @param {string} contactId 
     * @param {Array} targetStatuses 
     * @param {number} timeoutMs 
     * @returns {Object|null} The updated contact document or last fetched version on timeout
     */
    async pollStatus(contactId, targetStatuses, timeoutMs = 60000, intervalMs = 2000) {
        const db = await getDb();
        const contactObjId = new ObjectId(contactId);
        const startTime = Date.now();

        while (Date.now() - startTime < timeoutMs) {
            const contact = await db.collection('contactprocessings').findOne({ _id: contactObjId });
            const status = parseInt(contact?.callReceiveStatus) || 0;

            if (targetStatuses.includes(status)) {
                return contact;
            }

            // Wait before next poll
            await new Promise(resolve => setTimeout(resolve, intervalMs));
        }

        console.warn(`🕒 [Worker] Polling timeout for ${contactId} waiting for statuses: [${targetStatuses}]`);
        return await db.collection('contactprocessings').findOne({ _id: contactObjId });
    }

    async triggerPostCallActions(jobData, result, metadata, contact) {
        const { campaignId, contactId } = jobData;
        const apiResponse = result.apiResponse || {};
        const db = await getDb();

        // 1. Fetch Verified Duration from CallLogs
        let duration = 0;
        // Prefer deriving callId from CallLogs (matched by lead_id/contact), not from API response.
        let callId = null;
        let leadId = apiResponse.call?.lead_id || apiResponse.lead_id || null;
        const apiCallId = null;
        let billingAttemptsUsed = 1;

        try {
            for (let creditAttempt = 1; creditAttempt <= CALL_CREDIT_MAX_ATTEMPTS; creditAttempt++) {
                billingAttemptsUsed = creditAttempt;
                duration = 0;

                if (creditAttempt > 1) {
                    console.log(
                        `🔄 [Worker] Credit billing attempt ${creditAttempt}/${CALL_CREDIT_MAX_ATTEMPTS} for call ${apiCallId || contactId} — duration was 0, waiting ${CALL_CREDIT_RETRY_MS}ms for CallLog/CDR events...`
                    );
                    await new Promise(r => setTimeout(r, CALL_CREDIT_RETRY_MS));
                }

                let callLog = null;

                if (creditAttempt === 1) {
                    const maxPollTime = 20000;
                    const pollInterval = 1000;
                    const pollStart = Date.now();
                    let pollRound = 0;

                    while (Date.now() - pollStart < maxPollTime) {
                        pollRound += 1;
                        if (leadId != null) {
                            callLog = await db.collection('CallLogs').findOne({ lead_id: leadId });
                        }
                        if (!callLog && callId && !String(callId).startsWith('call_')) {
                            callLog = await db.collection('CallLogs').findOne({ call_id: callId });
                        }

                        if (!callLog) {
                            callLog = await db.collection('CallLogs').findOne(
                                {
                                    $or: [
                                        { contact_id: contactId },
                                        { contact_id: new ObjectId(contactId) }
                                    ]
                                },
                                { sort: { createdAt: -1 } }
                            );
                        }

                        if (callLog) break;

                        if (Math.floor((Date.now() - pollStart) / 1000) % 3 === 0) {
                            console.log(
                                `⏳ [Worker] Still waiting for CallLog for ${contactId}... (${Math.round((Date.now() - pollStart) / 1000)}s, poll #${pollRound})`
                            );
                        }

                        await new Promise(resolve => setTimeout(resolve, pollInterval));
                    }
                } else {
                    if (leadId != null) {
                        callLog = await db.collection('CallLogs').findOne({ lead_id: leadId });
                    }
                    if (!callLog && apiCallId != null) {
                        callLog = await db.collection('CallLogs').findOne({ call_id: apiCallId });
                    }
                    if (!callLog && callId && !String(callId).startsWith('call_')) {
                        callLog = await db.collection('CallLogs').findOne({ call_id: callId });
                    }
                    if (!callLog) {
                        callLog = await db.collection('CallLogs').findOne(
                            {
                                $or: [
                                    { contact_id: contactId },
                                    { contact_id: new ObjectId(contactId) }
                                ]
                            },
                            { sort: { createdAt: -1 } }
                        );
                    }
                }

                if (callLog) {
                    const isCurrentCall =
                        (leadId != null && String(callLog.lead_id) === String(leadId)) ||
                        (apiCallId != null && String(callLog.call_id) === String(apiCallId)) ||
                        (leadId == null && apiCallId == null);
                    if (isCurrentCall) {
                        if (callLog.call_id) {
                            callId = callLog.call_id;
                        }
                        if (leadId == null) leadId = callLog.lead_id || leadId;
                        const events = callLog.call_data?.events || [];
                        duration = computeDurationFromEvents(events);
                        if (duration > 0) {
                            console.log(`📄 [Worker] Billable duration from events (attempt ${creditAttempt}/${CALL_CREDIT_MAX_ATTEMPTS}): ${duration}s`);
                        } else if (events?.length) {
                            console.log(
                                `📄 [Worker] Duration still 0 after parsing ${events.length} event(s) (attempt ${creditAttempt}/${CALL_CREDIT_MAX_ATTEMPTS}) — CDR/hangup may arrive late`
                            );
                        }
                    } else {
                        duration = (apiResponse.call?.duration || apiResponse.duration || 0) / 1000;
                    }
                } else {
                    duration = (apiResponse.call?.duration || apiResponse.duration || 0) / 1000;
                    if (creditAttempt === 1) {
                        console.warn(`⚠️ [Worker] No CallLog found for ${contactId} after 20s wait. Using API duration fallback.`);
                    }
                }

                if (duration > 0) break;
                if (creditAttempt >= CALL_CREDIT_MAX_ATTEMPTS) break;
                if (!apiCallId && leadId == null) break;
            }
        } catch (logError) {
            console.error(`❌ [Worker] Error fetching CallLog:`, logError.message);
            duration = (apiResponse.call?.duration || apiResponse.duration || 0) / 1000;
        }

        if (apiCallId != null) {
            callId = apiCallId;
        }
        if (leadId == null && apiResponse.call?.lead_id != null) {
            leadId = apiResponse.call.lead_id;
        }
        // Fallback Call ID if still missing
        callId = callId || `call_${Date.now()}`;

        // Resolve target CallLog document (prefer lead_id, then call_id, then latest by contact)
        // NOTE: lead_id/call_id may be stored as string or number, so use type-safe $or filters.
        let callLogUpdateFilter = null;
        if (leadId != null) {
            callLogUpdateFilter = {
                $or: [
                    { lead_id: leadId },
                    { lead_id: String(leadId) }
                ]
            };
        } else if (callId && !String(callId).startsWith('call_')) {
            callLogUpdateFilter = {
                $or: [
                    { call_id: callId },
                    { call_id: String(callId) }
                ]
            };
        } else {
            const latestByContact = await db.collection('CallLogs').findOne(
                { $or: [{ contact_id: contactId }, { contact_id: new ObjectId(contactId) }] },
                { sort: { createdAt: -1 }, projection: { _id: 1 } }
            );
            if (latestByContact) callLogUpdateFilter = { _id: latestByContact._id };
        }

        console.log(
            `💰 [Worker] Processing credit deduction for Call ID: ${callId}, Lead ID: ${leadId ?? 'N/A'} (Duration: ${duration}s, billing attempts: ${billingAttemptsUsed}/${CALL_CREDIT_MAX_ATTEMPTS})...`
        );

        // Idempotency: BullMQ retries must not double-deduct for same lead_id/call_id
        if (leadId != null || (callId && !String(callId).startsWith('call_'))) {
            const dedupeOr = [];
            if (leadId != null) {
                dedupeOr.push({ 'reference.leadId': leadId });
                dedupeOr.push({ 'reference.leadId': String(leadId) });
            }
            if (callId && !String(callId).startsWith('call_')) {
                dedupeOr.push({ 'reference.callId': callId });
                dedupeOr.push({ 'reference.callId': String(callId) });
            }
            const alreadyBilled = await db.collection('credittransactions').findOne({
                type: 'call_deduction',
                $or: dedupeOr
            });
            if (alreadyBilled) {
                console.log(`ℹ️ [Worker] Credit already recorded for call ${callId} / lead ${leadId ?? 'N/A'}, skipping duplicate post-call billing.`);
                return;
            }
        }

        try {
            // 1. Fetch Campaign and User
            const campaign = await db.collection('campaigns').findOne({ _id: new ObjectId(campaignId) });
            if (!campaign) throw new Error(`Campaign ${campaignId} not found`);

            // Find user associated with the campaign
            let user = await db.collection('users').findOne({ email: campaign.createdBy });
            if (!user) {
                user = await db.collection('users').findOne({ _id: new ObjectId(campaign.userId || campaign.createdBy?.id) });
            }
            if (!user) throw new Error(`User for campaign ${campaignId} not found`);

            // 2. Determine Plan Tier and Rate
            let currentTier = 'A';
            let ratePerMinute = 0.08;

            try {
                if (user.creditPlan && user.creditPlan.currentTier) {
                    currentTier = user.creditPlan.currentTier;
                }

                // Map UI tier codes to package identifiers
                const tierToId = { 'A': 'starter', 'B': 'professional', 'C': 'enterprise', 'D': 'premium' };
                const targetPackageId = tierToId[currentTier] || 'starter';

                const pkg = await db.collection('creditpackages').findOne({ packageId: targetPackageId });

                // Determine if we should use Basic Model Price vs Standard Price
                const isBasicVoice = campaign.selectedVoice?.tier === 'basic';

                if (pkg) {
                    if (isBasicVoice && typeof pkg.basicModelPrice === 'number') {
                        ratePerMinute = pkg.basicModelPrice;
                        console.log(`🎙️ [Worker] Using Basic Voice Rate: ${ratePerMinute} for ${targetPackageId}`);
                    } else if (typeof pkg.pricePerMinute === 'number') {
                        ratePerMinute = pkg.pricePerMinute;
                        console.log(`🎙️ [Worker] Using Standard Voice Rate: ${ratePerMinute} for ${targetPackageId}`);
                    } else {
                        // Fallback within package
                        ratePerMinute = isBasicVoice ? 0.05 : 0.08;
                    }
                } else {
                    // Global Fallbacks if package is missing
                    const fallbackPremium = { 'A': 0.08, 'B': 0.075, 'C': 0.07, 'D': 0.065 };
                    const fallbackBasic = { 'A': 0.055, 'B': 0.05, 'C': 0.045, 'D': 0.04 };

                    ratePerMinute = isBasicVoice ?
                        (fallbackBasic[currentTier] || 0.05) :
                        (fallbackPremium[currentTier] || 0.08);

                    console.log(`🎙️ [Worker] Package not found. Using fallback ${isBasicVoice ? 'Basic' : 'Premium'} rate: ${ratePerMinute}`);
                }
            } catch (pricingError) {
                console.warn(`[Worker] Pricing lookup error, using fallback 0.08:`, pricingError.message);
                ratePerMinute = 0.08;
            }

            // 3. Billing Brackets and Cost Calculation
            const durationInSeconds = duration; // Already in seconds from logic above

            const fullMinutes = Math.floor(durationInSeconds / 60);
            const remainingSeconds = durationInSeconds % 60;

            const FALLBACK_BRACKETS = [
                { fromSecond: 1, toSecond: 15, percentOfRatePerMinute: 25 },
                { fromSecond: 16, toSecond: 30, percentOfRatePerMinute: 50 },
                { fromSecond: 31, toSecond: 45, percentOfRatePerMinute: 75 },
                { fromSecond: 46, toSecond: 60, percentOfRatePerMinute: 100 }
            ];

            let billingBrackets = FALLBACK_BRACKETS;
            const bracketSetting = await db.collection('systemsettings').findOne({ key: 'callBillingBracketsV1' });
            if (bracketSetting?.value && Array.isArray(bracketSetting.value)) {
                billingBrackets = bracketSetting.value;
            }

            let partialMinuteFraction = 0;
            if (remainingSeconds > 0) {
                const matchedBracket = billingBrackets.find(b => remainingSeconds >= b.fromSecond && remainingSeconds <= b.toSecond);
                const bracket = matchedBracket || billingBrackets[billingBrackets.length - 1];
                partialMinuteFraction = (bracket?.percentOfRatePerMinute ?? 100) / 100;
            }

            const cost = parseFloat(((fullMinutes * ratePerMinute) + (partialMinuteFraction * ratePerMinute)).toFixed(6));

            // 4. Atomic Credit Deduction
            if (user.credits < cost) {
                console.warn(`⚠️ [Worker] Insufficient credits for ${user.email}. Cost: ${cost}, Balance: ${user.credits}`);
                if (callLogUpdateFilter) {
                    const logUpdate = await db.collection('CallLogs').updateOne(
                        callLogUpdateFilter,
                        {
                            $set: {
                                creditsDeducted: false,
                                creditDeductionError: 'insufficient_credits',
                                processedAt: new Date(),
                                updatedAt: new Date()
                            }
                        }
                    );
                    if (logUpdate.matchedCount === 0) {
                        console.warn(`⚠️ [Worker] No CallLog matched while marking insufficient_credits (callId=${callId}, leadId=${leadId ?? 'N/A'})`);
                    }
                }
                return;
            }

            const deductionResult = await db.collection('users').updateOne(
                { _id: user._id, credits: { $gte: cost } },
                {
                    $inc: { credits: -cost },
                    $set: { updatedAt: new Date() }
                }
            );

            if (deductionResult.modifiedCount === 0 && cost > 0) {
                throw new Error('Credit deduction failed (likely race condition or insufficient funds)');
            }

            // 5. Log Transaction
            await db.collection('credittransactions').insertOne({
                userId: user._id,
                userEmail: user.email,
                type: 'call_deduction',
                amount: -cost,
                balanceAfter: parseFloat(((user.credits || 0) - cost).toFixed(6)),
                description: `Call Usage`,
                reference: {
                    campaignId: campaign._id,
                    campaignName: campaign.name || campaign.campaignName,
                    callDuration: durationInSeconds,
                    callId: callId,
                    leadId: leadId
                },
                createdAt: new Date(),
                updatedAt: new Date()
            });

            // 6. Update Analytics (Using 'analytics' collection based on DB check)
            const today = new Date().toISOString().split('T')[0];
            await db.collection('analytics').updateOne(
                {
                    userId: user.email, // Analytics uses email or ID? DB check showed 'niya@gmail.com'
                    campaignId: campaign._id.toString(),
                    date: today
                },
                {
                    $inc: {
                        totalMinutes: parseFloat((durationInSeconds / 60).toFixed(4)),
                        totalCalls: 1,
                        connectedCalls: duration > 0 ? 1 : 0
                    },
                    $set: { updatedAt: new Date() }
                },
                { upsert: true }
            );

            // 7. Update Call Log (by call_id so we update the current attempt, not a previous one for same contact)
            if (callLogUpdateFilter) {
                const logUpdate = await db.collection('CallLogs').updateOne(
                    callLogUpdateFilter,
                    {
                        $set: {
                            creditsDeducted: true,
                            creditsDeductedAmount: cost,
                            updatedAt: new Date()
                        }
                    }
                );
                if (logUpdate.matchedCount === 0) {
                    console.warn(`⚠️ [Worker] No CallLog matched for creditsDeducted update (callId=${callId}, leadId=${leadId ?? 'N/A'})`);
                }
            }

            console.log(
                `✅ [Worker] Post-call actions completed for ${callId}. Cost: ${cost} (billing attempts used: ${billingAttemptsUsed}/${CALL_CREDIT_MAX_ATTEMPTS})`
            );

            // 8. Trigger Call Analysis API (retries: indexing often lags a few seconds after call end)
            if (callId && !String(callId).startsWith('call_')) {
                try {
                    // Give transcript/turns pipeline a brief head start before first analysis request.
                    if (ANALYSIS_API_INITIAL_DELAY_MS > 0) {
                        await new Promise(r => setTimeout(r, ANALYSIS_API_INITIAL_DELAY_MS));
                    }

                    // ANALYSIS_API_URL can be base (http://host:5000) or prefix (http://host:5000/analyze/call)
                    const rawAnalysis = (config.analysis?.apiUrl || 'http://72.60.221.48:5000').replace(/\/$/, '');
                    const analysisUrl = rawAnalysis.includes('/analyze/call')
                        ? `${rawAnalysis}/${encodeURIComponent(String(callId))}`
                        : `${rawAnalysis}/analyze/call/${encodeURIComponent(String(callId))}`;

                    for (let attempt = 1; attempt <= ANALYSIS_API_MAX_ATTEMPTS; attempt++) {
                        console.log(`🧠 [Worker] Triggering Analysis API for Call ID: ${callId} (attempt ${attempt}/${ANALYSIS_API_MAX_ATTEMPTS})...`);
                        const analysisResponse = await fetch(analysisUrl, {
                            method: 'POST',
                            headers: { 'Content-Type': 'application/json' }
                        });

                        if (analysisResponse.ok) {
                            console.log(`✅ [Worker] Analysis API triggered successfully for ${callId}`);
                            break;
                        }

                        const errText = await analysisResponse.text();
                        const notReady =
                            analysisResponse.status === 404 ||
                            analysisResponse.status === 503 ||
                            analysisResponse.status === 502 ||
                            /not\s*found/i.test(errText);

                        if (notReady && attempt < ANALYSIS_API_MAX_ATTEMPTS) {
                            console.warn(`⚠️ [Worker] Analysis API not ready for ${callId} (${analysisResponse.status}), retry in ${ANALYSIS_API_RETRY_MS}ms...`);
                            await new Promise(r => setTimeout(r, ANALYSIS_API_RETRY_MS));
                            continue;
                        }

                        console.warn(`⚠️ [Worker] Analysis API failed for ${callId}:`, errText);
                        break;
                    }
                } catch (analysisError) {
                    console.error(`❌ [Worker] Error triggering Analysis API:`, analysisError.message);
                }
            }

        } catch (error) {
            console.error(`❌ [Worker] triggerPostCallActions CRITICAL ERROR:`, error.message);
            await this.logErrorToDb({
                errorType: 'system_error',
                errorCategory: 'post_call_actions',
                severity: 'critical',
                errorMessage: error.message,
                errorStack: error.stack,
                campaignId,
                contactId,
                callId,
                metadata: { stage: 'triggerPostCallActions' }
            });
        }
    }
}
