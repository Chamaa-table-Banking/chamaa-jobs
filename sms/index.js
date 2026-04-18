import axios from 'axios';
import dotenv from 'dotenv';
import AfricasTalking from 'africastalking';
import { RedisQueues, connectRedis } from '../functions/queue.js';
import RedisSets from '../functions/sets.js'
import { Pool } from 'pg'
dotenv.config();
const user_info_api = `${process.env.gateway}/api/v1/user/one/`;
const chamaa_info_api = `${process.env.gateway}/api/v1/chamaa/getbyid/`;
const cycle_info_api = `${process.env.gateway}/api/v1/cycles/`;
const POLL_INTERVAL_MS = parseInt(1500); // Default 15 seconds
const sms_notification_queue = "queue:sms:notifications";
const sms_response_queue = "queue:sms:responses";
const sms_notification_queue_failed = "queue:sms:notifications:failed";
const today = new Date().toISOString().split('T')[0];
console.log('Worker configuration:', {
    user_info_api,
    chamaa_info_api,
    cycle_info_api,
    POLL_INTERVAL_MS,
});
const pool = new Pool({
    host: 'ep-flat-bar-aba7x6f8-pooler.eu-west-2.aws.neon.tech',
    user: 'neondb_owner',
    password: 'npg_zqjXJ8nsE3Fd',
    database: 'neondb',
    port: '5432',
    ssl: {
        rejectUnauthorized: false,
        sslmode: 'require',
    }
});
const client = await pool.connect();
console.log('Connected to PostgreSQL database');
connectRedis();
const apikey = process.env.SMS_API_KEY;
const credentials = {
    apiKey: apikey,         // use your sandbox app API key for development in the test environment
    username: process.env.username,      // use 'sandbox' for development in the test environment
};
const sms = AfricasTalking(credentials);
const sendSMS = async (phoneNumber, message, chamaa_id, user_id, type) => {
    try {
        const options = {
            to: [phoneNumber],
            message: message,
        };
        console.log(credentials);
        console.log('Sending SMS with options:', options);
        let response = await sms.SMS.send(options);
        response.Recipients[0].message = message;
        response.Recipients[0].chamaa_id = chamaa_id;
        response.Recipients[0].user_id = user_id;
        response.Recipients[0].type = type;

        console.log('SMS sent successfully:', response);
    
        
        await RedisQueues.addToQueue(sms_response_queue, JSON.stringify(response));
    } catch (error) {
        console.error('Error sending SMS:', error);
        // Add the notification to the failed queue
        await RedisQueues.addToQueue(sms_notification_queue_failed, JSON.stringify(notification));
    }
};
const getChamaaInfo = async (chamaaId) => {
    try {
        const response = await axios.get(`${chamaa_info_api}${chamaaId}`);
        return response.data.data;
    } catch (error) {
        console.error('Error fetching chamaa info:', error);
        return null;
    }
}
const getCycleInfo = async (cycleId) => {
    try {
        const response = await axios.get(`${cycle_info_api}${cycleId}`);
        return response.data.data;
    } catch (error) {
        console.error('Error fetching cycle info:', error);
        return null;
    }
}

const getUserInfo = async (userId) => {
    try {
        const response = await axios.get(`${user_info_api}${userId}`);
        return response.data.data;
    } catch (error) {
        console.error('Error fetching user info:', error);
        return null;
    }
}
const insertTOSmsDatabase = async (sms) => {
    try {
            const query = `
            INSERT INTO sms
            (cost, receipient, messageid, status, message, status_code,type, chamaa_id, user_id)
            VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9)
            `;
            const values = [
            sms.cost,
            sms.receipient,
            sms.messageId,
            sms.status,
            sms.message,
            sms.statusCode,
            sms.type || 'response',
            sms.chamaa_id,
            sms.user_id
            ];

        await client.query(query, values);
        console.log('SMS response data inserted into database successfully');
    } catch (error) {
        console.error('Error inserting SMS response data into database:', error);
    }
}
const constructSMSMessage = (type, data, cycleInfo, chamaaInfo, userInfo) => {
   
    switch (type) {
        case 'financial_reminder':
                switch (data.type) {
                    case 'contribution_due':
                        return `Hello! ${data.userName}, we hope you're doing well. This is a reminder that your contribution of ${data.amount} for cycle ${data.name} in ${data.chamaa_name} is due on ${data.dueDate}. Please make sure to contribute on time to avoid any penalties. Thank you!`;
                    case 'cycle_completion':
                        return `Congratulations! Cycle ${data.name} has been completed successfully. Your contribution of ${data.amount} has been received. Thank you for being a part of this cycle!`;
                    case 'insufficient_balance':
                        return `Dear ${data.userName}, we noticed that your balance is insufficient to make the required contribution of ${data.amount} for cycle ${data.name} in ${data.chamaa_name}. Please top up your account to avoid any penalties. Thank you!`;
                    default:
                        return `You have a new notification: ${type} for cycle ${data.name} in ${data.chamaa_name}. Please check your account for more details.`;
                
                }   
        default:
            return `You have a new notification: ${type} for cycle ${data.name} in ${data.chamaa_name}. Please check your account for more details.`; 
    }
}

const processSMSNotifications = async () => {
    let notification = null;
    try {
        let queueLength = await RedisQueues.queueLength(sms_notification_queue);
        console.log(`Checking for SMS notifications. Queue length: ${queueLength}`);
        
        while (queueLength > 0) {
            const notificationData = await RedisQueues.popRightFromQueue(sms_notification_queue)
            console.log('Popped SMS notification from queue:', notificationData);
            if (!notificationData == null) {
                notification = JSON.parse(notificationData);
                console.log('Processing SMS notification:', notification);

                const promises = [];
                promises.push(
                    getChamaaInfo(notification.chamaa_id),
                    getCycleInfo(notification.cycle_id),//notification.cycle_id is currently hardcoded for testing purposes, replace with the actual cycle_id when ready to test with real data
                    getUserInfo(notification.user_id)
                );
                const [chamaaInfo, cycleInfo, userInfo] = await Promise.all(promises);
                if (chamaaInfo &&userInfo) {
                    let message = constructSMSMessage({
                        type: notification.type, data: notification, cycleInfo, chamaaInfo, userInfo
                    });
                    message.type = notification.type;
                    await sendSMS(userInfo.phone_number, message, notification.chamaa_id, notification.user_id, notification.type);
                } else {
                    console.error('Failed to fetch necessary information for SMS notification:', {
                        chamaaInfo,
                        cycleInfo,
                        userInfo
                    });
                 await RedisQueues.addToQueue(sms_notification_queue_failed, JSON.stringify(notification));


                }

            }
        }
    } catch (error) {
        console.error('Error processing SMS notification job:', error);
        await RedisQueues.addToQueue(sms_notification_queue, JSON.stringify(notification));

    }
}

// setInterval(processSMSNotifications, POLL_INTERVAL_MS);
const processSMSResponses = async () => {
    try {
        let queueLength = await RedisQueues.queueLength(sms_response_queue);
        console.log(`Checking for SMS responses. Queue length: ${queueLength}`);
        while (queueLength > 0) {
            const responseData = await RedisQueues.popRightFromQueue(sms_response_queue)
            console.log('Popped SMS response from queue:', responseData);
            if (responseData) {
                const response = JSON.parse(responseData);
                const recipient = response.SMSMessageData.Recipients[0];
                const payload = {
                    cost: +recipient.cost.split(' ')[1],// sample KES 1.6000 
                    receipient: recipient.number,
                    messageId: recipient.messageId,
                    status: recipient.status,
                    message: response.message,
                    statusCode: recipient.statusCode,
                    type: recipient.type,
                    chamaa_id: recipient.chamaa_id,
                    user_id: recipient.user_id,

                }
                await insertTOSmsDatabase(payload);
            }
        }
    } catch (error) {
        console.error('Error processing SMS response job:', error);
    }
}
// processSMSNotifications();
processSMSResponses();

// processSMSNotifications();
// console.log(await RedisQueues.peek(sms_response_queue));

setInterval(processSMSNotifications, POLL_INTERVAL_MS);
setInterval(processSMSResponses, POLL_INTERVAL_MS * 2);