import { Worker } from 'bullmq';
import {
    getRedis,
    getDb,
    concurrencyGuard,
    isWithinBusinessHours,
    queueName,
    config
} from 'shared-lib';
import { ObjectId } from 'mongodb';

export class CallWorker {
    constructor() {
        this.worker = new Worker(queueName, this.processJob.bind(this), {
            connection: getRedis(),
            concurrency: 20,
            lockDuration: 300000, // 5 minutes to prevent "Missing Lock" during long API calls
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
    }

    async processJob(job) {
        const { campaignId, contactId, userId, metadata } = job.data;
        const contactObjId = new ObjectId(contactId);
        const { campaignLimit, userLimit, businessHours } = metadata;

        console.log(`👷 [Worker] Processing job ${job.id} for contact ${contactId}`);

        const db = await getDb();

        // 0. Fetch latest campaign data to check status (Ensures we respect Paused/Stopped campaigns immediately)
        const campaign = await db.collection('campaigns').findOne({ _id: new ObjectId(campaignId) });
        if (!campaign || campaign.status !== 'active') {
            const reason = !campaign ? 'Campaign not found' : `Campaign status is ${campaign.status}`;
            console.log(`⏸️ [Worker] Job ${job.id} for campaign ${campaignId} ignored: ${reason}.`);
            // Just return. The job is marked as "completed" in BullMQ, but since we didn't update MongoDB status, 
            // the Scheduler will pick up this contact again in the next loop when the campaign becomes active.
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

        if (!contact || ['completed', 'failed'].includes(contact.status)) {
            console.log(`⏩ [Worker] Skipping contact ${contactId}: Status is already ${contact?.status || 'unknown'}`);
            return;
        }

        // 1. Validate Business Hours

        // 2. Acquire Distributed Concurrency Slot
        const hasSlot = await concurrencyGuard.acquireSlot(
            campaignId,
            userId,
            campaignLimit || 500,
            userLimit || 100
        );

        if (!hasSlot) {
            console.log(`🚦 [Worker] Limit reached for ${campaignId}. Re-queuing naturally.`);
            // Throwing an error forces BullMQ to retry the job according to its own backoff settings, 
            // without needing manual state (moveToDelayed) which causes "Missing Lock" errors.
            throw new Error('CONCURRENCY_LIMIT_REACHED');
        }

        let shouldReleaseSlot = true; // Declare here so it is accessible in finally{}

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

            // Re-fetch contact to get the latest callReceiveStatus from MongoDB
            const updatedContact = await db.collection('contactprocessings').findOne({ _id: contactObjId });

            // Sync BullMQ job data again after API call to reflect latest status
            await job.updateData({
                ...job.data,
                callReceiveStatus: updatedContact?.callReceiveStatus,
                dbStatus: updatedContact?.status
            });

            const apiResponse = result?.apiResponse || {};

            // Check status from both DB and API Response (API is more real-time immediately after execution)
            const dbStatus = parseInt(updatedContact?.callReceiveStatus) || 0;
            const apiStatus = parseInt(apiResponse.callreceivestatus || apiResponse.call?.status) || 0;

            // Use the "most advanced" status (3 > 2 > 1 > 0)
            const callStatus = Math.max(dbStatus, apiStatus);

            console.log(`🔍 [Worker] Status Check for ${contactId}: DB=${dbStatus}, API=${apiStatus} -> Final=${callStatus}`);

            const maxRetries = metadata.maxRetryAttempts || 3;
            const retryDelayMinutes = metadata.retryDelayMinutes || 30;
            const currentRetryCount = currentContact.retryCount || 0;
            const currentAttempts = (currentContact.callAttempts?.length || 0) + 1;

            shouldReleaseSlot = true;

            if (callStatus === 2) {
                // RUNNING: Do NOT release slot yet. Wait for webhook.
                shouldReleaseSlot = false;
                await db.collection('contactprocessings').updateOne(
                    { _id: contactObjId },
                    {
                        $set: {
                            status: 'running',
                            callReceiveStatus: 2,
                            updatedAt: new Date()
                        },
                        $push: {
                            callAttempts: {
                                attempt: currentAttempts,
                                timestamp: new Date(),
                                status: 'running',
                                message: 'Call is currently active'
                            }
                        }
                    }
                );
                console.log(`📡 [Worker] Call ${contactId} is RUNNING. Holding slot.`);
            } else if (callStatus === 3) {
                // COMPLETED: Success, release slot.
                await db.collection('contactprocessings').updateOne(
                    { _id: contactObjId },
                    {
                        $set: {
                            status: 'completed',
                            callReceiveStatus: 3,
                            completedAt: new Date(),
                            updatedAt: new Date()
                        },
                        $push: {
                            callAttempts: {
                                attempt: currentAttempts,
                                timestamp: new Date(),
                                status: 'completed',
                                message: 'Processed'
                            }
                        }
                    }
                );
                console.log(`✅ [Worker] Call ${contactId} COMPLETED. Releasing slot.`);

                // Trigger post-call actions (billing/analysis)
                try {
                    await this.triggerPostCallActions(job.data, result, metadata, currentContact);
                } catch (triggerError) {
                    console.error(`⚠️ [Worker] Post-call actions failed:`, triggerError.message);
                }
            } else {
                // FAILED (0) or NOT RECEIVED (1): Retry if possible, release slot.
                const isRetryable = currentRetryCount + 1 < maxRetries;
                const nextStatus = isRetryable ? 'retry' : 'failed';
                const nextRetryAt = isRetryable ? new Date(Date.now() + retryDelayMinutes * 60000) : null;

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
                                timestamp: new Date(),
                                status: nextStatus,
                                message: callStatus === 1 ? 'Not Received' : 'API Attempt Failed'
                            }
                        }
                    }
                );
                console.log(`🔁 [Worker] Call ${contactId} FAILED/RETRY (Status: ${callStatus}). Releasing slot.`);
            }

            return result;
        } catch (error) {
            console.error(`❌ [Worker] Execution error for job ${job.id}:`, error.message);

            // Fetch state for system-level error retry
            const db = await getDb();
            const contact = await db.collection('contactprocessings').findOne({ _id: contactObjId });
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
            return { success: false, error: error.message };
        } finally {
            // 6. Release Concurrency Slot (Conditional)
            if (typeof shouldReleaseSlot === 'undefined' || shouldReleaseSlot === true) {
                await concurrencyGuard.releaseSlot(campaignId, userId);
            }
        }
    }

    async executeCall(data) {
        const url = 'http://72.60.221.48:8000/api/v1/calls/initiate-campaign-call';
        const payload = {
            campaign_id: data.campaignId,
            contact_id: data.contactId
        };

        console.log(`📞 [Worker] Making call to ${data.phone} via API...`);

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

    async triggerPostCallActions(jobData, result, metadata, contact) {
        const { campaignId, contactId } = jobData;
        const apiResponse = result.apiResponse || {};

        // Robust ID extraction
        const callId = apiResponse.call?.id || apiResponse.call_id || apiResponse.id || apiResponse.callId || `call_${Date.now()}`;

        // Robust duration extraction (ms)
        const duration = apiResponse.call?.duration || apiResponse.duration || 0;

        const db = await getDb();

        console.log(`💰 [Worker] Processing credit deduction for Call ID: ${callId} (Duration: ${duration}ms)...`);

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
                if (pkg && typeof pkg.pricePerMinute === 'number') {
                    ratePerMinute = pkg.pricePerMinute;
                } else {
                    const fallbackRates = { 'A': 0.08, 'B': 0.075, 'C': 0.07, 'D': 0.065 };
                    ratePerMinute = fallbackRates[currentTier] || 0.08;
                }
            } catch (pricingError) {
                console.warn(`[Worker] Pricing lookup error, using fallback 0.08:`, pricingError.message);
                ratePerMinute = 0.08;
            }

            // 3. Billing Brackets and Cost Calculation
            const durationInSeconds = duration / 1000;
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
                await db.collection('CallLogs').updateOne(
                    { call_id: callId },
                    {
                        $set: {
                            creditsDeducted: false,
                            creditDeductionError: 'insufficient_credits',
                            processedAt: new Date(),
                            updatedAt: new Date()
                        }
                    },
                    { upsert: true }
                );
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
                description: `Call Usage - ${Math.round(durationInSeconds)}s (${(durationInSeconds / 60).toFixed(2)}m)`,
                reference: {
                    campaignId: campaign._id,
                    campaignName: campaign.name || campaign.campaignName,
                    callDuration: durationInSeconds,
                    contactPhone: contact?.mobileNumber || contact?.phone || jobData.phone,
                    contactName: contact?.contactData?.Name || contact?.name,
                    callId: callId
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

            // 7. Update Call Log
            await db.collection('CallLogs').updateOne(
                { call_id: callId },
                {
                    $set: {
                        creditsDeducted: true,
                        creditsDeductedAmount: cost,
                        duration: durationInSeconds,
                        campaign_id: campaign._id,
                        contact_id: new ObjectId(contactId),
                        processedAt: new Date(),
                        updatedAt: new Date()
                    }
                },
                { upsert: true }
            );

            console.log(`✅ [Worker] Post-call actions completed for ${callId}. Cost: ${cost}`);

        } catch (error) {
            console.error(`❌ [Worker] triggerPostCallActions CRITICAL ERROR:`, error.message);
        }

        // 8. Trigger Analysis API (Async)
        const analysisUrl = config.analysis?.apiUrl;
        if (analysisUrl && callId && !callId.startsWith('call_')) {
            console.log(`📊 [Worker] Dispatching analysis for ${callId}...`);
            fetch(`${analysisUrl}/${callId}`, { method: 'GET' })
                .catch(err => console.error(`❌ [Worker] Analysis API error:`, err.message));
        }
    }
}
