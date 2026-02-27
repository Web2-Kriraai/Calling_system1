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
            concurrency: 100,
            lockDuration: 60000, // 60 seconds
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
            console.log(`⏸️ [Worker] Job ${job.id} for campaign ${campaignId} ignored: ${reason}`);

            if (campaign?.status === 'paused') {
                // If paused, move back to delayed to wait for resumption
                await job.moveToDelayed(Date.now() + 60000, job.token);
            }
            return;
        }

        // 0.1 Fetch latest contact data to ensure it's not already handled
        const contact = await db.collection('contactprocessings').findOne({ _id: contactObjId });
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
            console.log(`🚦 [Worker] Concurrency limit reached for ${campaignId} or ${userId}. Re-queuing.`);
            await job.moveToDelayed(Date.now() + 30000, job.token);
            return; // Return instead of throw to avoid lock conflict
        }

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

            // PRODUCTION FIX: Correctly map both your API's 'success' status and the legacy status codes
            const apiResponse = result.apiResponse || {};
            const isApiSuccess = apiResponse.status === 'success' || apiResponse.callreceivestatus === 3;
            const isApiNotReceived = apiResponse.callreceivestatus === 1 || apiResponse.status === 'not_received';

            const maxRetries = metadata.maxRetryAttempts || 3;
            const retryDelayMinutes = metadata.retryDelayMinutes || 30;
            const currentRetryCount = currentContact.retryCount || 0;
            const currentAttempts = (currentContact.callAttempts?.length || 0) + 1;

            // 6. Update Database based on API response
            if (isApiSuccess) {
                // Success
                await db.collection('contactprocessings').updateOne(
                    { _id: contactObjId },
                    {
                        $set: {
                            status: 'completed',
                            result,
                            completedAt: new Date(),
                            updatedAt: new Date()
                        },
                        $push: {
                            callAttempts: {
                                attempt: currentAttempts,
                                timestamp: new Date(),
                                status: 'success',
                                message: 'call completed successfully',
                                response: result.apiResponse
                            }
                        }
                    }
                );
                console.log(`✅ [Worker] Successfully completed job ${job.id}`);

                // 7. Trigger Webhooks and Analysis
                try {
                    await this.triggerPostCallActions(job.data, result, metadata);
                } catch (triggerError) {
                    console.error(`⚠️ [Worker] Post-call actions failed for ${job.id}:`, triggerError.message);
                }
            } else if (isApiNotReceived) {
                // Handle business-level retry logic
                const isRetryable = currentRetryCount + 1 < maxRetries;
                const nextStatus = isRetryable ? 'retry' : 'failed';
                const nextRetryAt = isRetryable ? new Date(Date.now() + retryDelayMinutes * 60000) : null;

                await db.collection('contactprocessings').updateOne(
                    { _id: contactObjId },
                    {
                        $set: {
                            status: nextStatus,
                            lastError: 'call not received',
                            nextRetryAt,
                            updatedAt: new Date()
                        },
                        $inc: { retryCount: 1 },
                        $push: {
                            callAttempts: {
                                attempt: currentAttempts,
                                timestamp: new Date(),
                                status: nextStatus,
                                message: 'call not received',
                                response: result.apiResponse
                            }
                        }
                    }
                );
                console.log(`⚠️  [Worker] Retry required for job ${job.id}. Next status: ${nextStatus}. Delay: ${retryDelayMinutes}m`);

                // Return normally to remove from current queue. The scheduler will pick it up after the delay.
                return result;
            } else {
                // Unexpected status code handling
                console.warn(`❓ [Worker] Unexpected API response for job ${job.id}:`, apiResponse);
                await db.collection('contactprocessings').updateOne(
                    { _id: contactObjId },
                    {
                        $set: { status: 'completed', result, updatedAt: new Date() },
                        $push: {
                            callAttempts: {
                                attempt: currentAttempts,
                                timestamp: new Date(),
                                status: 'completed',
                                message: `Processed with unknown status: ${apiStatus}`,
                                response: result.apiResponse
                            }
                        }
                    }
                );
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
                            message: `System Error: ${error.message}`,
                            response: null
                        }
                    }
                }
            );

            // PRODUCTION FIX: We NEVER throw here for business-handled failures.
            // Throwing makes BullMQ retry immediately (ignoring retryDelayMinutes).
            // We've already updated MongoDB with the correct status (retry or failed).
            console.log(`✅ [Worker] Error handled for contact ${contactId}. Next status: ${status}.`);
            return { success: false, error: error.message };
        } finally {
            // 6. Release Concurrency Slot
            await concurrencyGuard.releaseSlot(campaignId, userId);
        }
    }

    async executeCall(data) {
        const url = 'https://imelda-vindictive-semicolloquially.ngrok-free.dev/api/v1/calls/initiate-campaign-call';
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

    async triggerPostCallActions(jobData, result, metadata) {
        const { campaignId, contactId } = jobData;
        const apiResponse = result.apiResponse || {};
        const callId = apiResponse.call_id || apiResponse.id || `call_${Date.now()}`;
        const tier = metadata.voiceTier || 'premium';

        // Determine Webhook URL
        const webhookUrl = tier === 'basic' ? config.webhooks.basic : config.webhooks.premium;

        if (!webhookUrl) {
            console.warn(`⚠️ [Worker] No webhook URL configured for tier: ${tier}`);
        } else {
            console.log(`🔗 [Worker] Triggering ${tier} webhook for call ${callId}...`);

            const webhookPayload = {
                call_id: callId,
                campaign_id: campaignId,
                contact_id: contactId,
                status: 'completed',
                call_data: {
                    events: [
                        {
                            event_type: 'call.completed',
                            data: {
                                data: {
                                    duration: apiResponse.duration || 0 // Use duration from response if available
                                }
                            }
                        }
                    ]
                }
            };

            fetch(webhookUrl, {
                method: 'POST',
                headers: { 'Content-Type': 'application/json' },
                body: JSON.stringify(webhookPayload)
            }).catch(err => console.error(`❌ [Worker] Webhook failed:`, err.message));
        }

        // Trigger Analysis API
        const analysisUrl = config.analysis.apiUrl;
        if (analysisUrl) {
            console.log(`📊 [Worker] Triggering analysis for call ${callId}...`);
            fetch(`${analysisUrl}/${callId}`, {
                method: 'GET' // or POST depending on your API, user said "call URL"
            }).catch(err => console.error(`❌ [Worker] Analysis API failed:`, err.message));
        }
    }
}
