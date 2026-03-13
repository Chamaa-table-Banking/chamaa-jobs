import axios from 'axios';
import dotenv from 'dotenv';
import { RedisQueues, connectRedis } from '../functions/queue.js';

dotenv.config();

const depositQueue = 'queue:shortcode:worker';
const STK_STATUS_API = `${process.env.gateway}/api/v1/payments/stk/response/status`
const PAYMENT_IN_API = `${process.env.mpesa_ms_url}/payments/in`;
const WALLET_CREDIT_API = `${process.env.mpesa_ms_url}/wallet/`
const POLL_INTERVAL_MS = parseInt(process.env.POLL_INTERVAL_MS || '1000', 10); // Default 15 seconds
console.log('Worker configuration:', {
    STK_STATUS_API,
    PAYMENT_IN_API,
    WALLET_CREDIT_API,
    POLL_INTERVAL_MS,
});
connectRedis();

/**
 * Parses custom date format YYYYMMDDHHMMSS to ISO string
 * @param {string} dateString - Date string in format YYYYMMDDHHMMSS
 * @returns {string} ISO date string
 */
const parseCustomDate = (dateString) => {
    const parsed = new Date(
        dateString.replace(/^(\d{4})(\d{2})(\d{2})(\d{2})(\d{2})(\d{2})$/, '$1-$2-$3T$4:$5:$6')
    );
    return parsed.toISOString();
};

/**
 * Processes a single queue item
 * @param {Object} data - Data from Redis queue
 */
const processQueueItem = async (data) => {
    const chamaDetails = data.chama_details;
    const stkData = data.stkpush;

    // Fetch transaction status from STK API
    const transactionResponse = await axios.post(STK_STATUS_API, {
        MerchantRequestID: stkData.data.MerchantRequestID,
    });
    //   console.log(transactionResponse.data.data.data)

    // Response is an array with a single stringified JSON object
    const responseData = Array.isArray(transactionResponse.data.data.data)
        ? JSON.parse(transactionResponse.data.data.data[0])
        : transactionResponse.data.data.data;
    // console.log(responseData)
    // Extract transaction ID and date from response
    const callbackItems = responseData.stkCallback.CallbackMetadata.Item;

    const transactionId = callbackItems.find((item) => item.Name == 'MpesaReceiptNumber')?.Value;
    const transactionDate = callbackItems.find((item) => item.Name == 'TransactionDate')?.Value;
    // Build payment payload
    const payload = {
        business_shortcode:
            chamaDetails.active_payment_method === 'till'
                ? chamaDetails.till_number
                : chamaDetails.paybill,
        transaction_id: transactionId,
        amount: data.Amount,
        chamaa_id: chamaDetails.id,
        user_id: data.user_id,
        cycle_id: null,
        date: parseCustomDate(`${transactionDate}`),
        actualized: true,
        type: 'credit',
    };
    const wallet_payload = {
        "is_debit": false,
        "is_credit": true,
        "transaction_id":transactionId,
        "amount": +data.Amount,
        "chamaa_id": chamaDetails.id,
        "user_id": data.user_id,
        "cycle_id": "null",
        "date": parseCustomDate(`${transactionDate}`)
    }

    // Send payment to payment API
    const paymentResponse = await axios.post(PAYMENT_IN_API, payload);
    await axios.post(WALLET_CREDIT_API, wallet_payload);

    console.log('Payment processed successfully:', {
        paymentId: paymentResponse.data.data.id,
        amount: payload.amount,
        transactionId: payload.transaction_id,
    });

    return paymentResponse.data;
};

/**
 * Polls the queue and processes items
 */
const pollQueue = async () => {
    try {
        const queueLength = await RedisQueues.queueLength(depositQueue);

        if (queueLength > 0) {
            console.log(`Processing ${queueLength} item(s) from queue...`);

            while (queueLength > 0) {
                let d
                try {
                    const data = await RedisQueues.popRightFromQueue(depositQueue);
                    d = data
                    if (!data) break;

                    await processQueueItem(data);
                } catch (itemError) {
                    console.error('Error processing queue item:', itemError.message);
                    await RedisQueues.addToQueue(depositQueue, d)
                    console.log('Returned item to queue')
                    // Continue with next item instead of breaking
                }
            }
        }
    } catch (error) {
        console.error('Error polling queue:', error.message);
    }
};

/**
 * Starts the queue worker with configurable interval
 */
const startWorker = () => {
    console.log(`Starting queue worker with ${POLL_INTERVAL_MS}ms interval...`);

    // Run immediately on start
    pollQueue();

    // Then run at configured interval
    setInterval(pollQueue, POLL_INTERVAL_MS);
};
startWorker();
pollQueue();
