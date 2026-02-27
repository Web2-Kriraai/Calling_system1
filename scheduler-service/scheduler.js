import { getDb, createQueue, calculatePriority } from 'shared-lib';
import { ObjectId } from 'mongodb';

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

        // 0. Activate scheduled campaigns whose time has arrived
        await this.activateScheduledCampaigns(db);

        // 1. Find active campaigns
        const activeCampaigns = await db.collection('campaigns').find({
            status: 'active'
        }).toArray();

        console.log(`🔍 [Scheduler] Found ${activeCampaigns.length} active campaigns.`);

        for (const campaign of activeCampaigns) {
            await this.processCampaign(campaign, db);
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

        console.log(`🕒 [Scheduler] Checking for campaigns to activate... (Current IST: ${currentDate} ${currentTime})`);

        // Only find campaigns that are NOT active, completed, paused, or failed
        const scheduledCampaigns = await db.collection('campaigns').find({
            status: { $nin: ['active', 'completed', 'paused', 'failed', 'invalid_configuration'] },
            startDate: { $lte: currentDate }
        }).toArray();

        for (const campaign of scheduledCampaigns) {
            // If date is today, check time. If date is past, activate immediately.
            const isTimeReached = campaign.startDate < currentDate ||
                (campaign.startDate === currentDate && campaign.startTime <= currentTime);

            if (isTimeReached) {
                // Perform field validation before activation
                const { isValid, missingFields } = this.validateCampaignConfig(campaign);

                if (!isValid) {
                    console.warn(`⚠️ [Scheduler] Campaign ${campaign.campaignName} (${campaign._id}) missing fields: ${missingFields.join(', ')}`);
                    await db.collection('campaigns').updateOne(
                        { _id: campaign._id },
                        {
                            $set: {
                                status: 'invalid_configuration',
                                error: `Missing required fields: ${missingFields.join(', ')}`,
                                updatedAt: new Date()
                            }
                        }
                    );
                    continue;
                }

                console.log(`✨ [Scheduler] Activating campaign: ${campaign.campaignName} (${campaign._id})`);
                await db.collection('campaigns').updateOne(
                    { _id: campaign._id },
                    { $set: { status: 'active', activatedAt: new Date(), updatedAt: new Date() } }
                );
            }
        }
    }

    /**
     * Validates that all required fields for a campaign are present.
     */
    validateCampaignConfig(campaign) {
        const requiredFields = [
            { field: 'startDate', label: 'Start Date' },
            { field: 'startTime', label: 'Start Time' },
            { field: 'concurrentCalls', label: 'Concurrent Calls' },
            { field: 'createdBy', label: 'Created By' }
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
        console.log(`📡 [Scheduler] Processing campaign: ${campaign.campaignName} (${campaign._id})`);

        const now = new Date();
        const contactCursor = db.collection('contactprocessings').find({
            campaignId: campaign._id,
            $or: [
                { status: { $in: ['pending', 'enqueued'] } },
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
                        userLimit: 100, // Example user-level limit
                        maxRetryAttempts: campaign.maxRetryAttempts || 3,
                        retryDelayMinutes: campaign.retryDelayMinutes || 30,
                        voiceTier: campaign.selectedVoice?.tier || 'premium'
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
                { $set: { status: 'enqueued', enqueuedAt: new Date() } }
            );
        } catch (error) {
            console.error('❌ [Scheduler] Error enqueuing batch:', error.message);
        }
    }
}
