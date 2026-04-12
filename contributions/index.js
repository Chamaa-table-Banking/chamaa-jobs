import axios from 'axios';
import dotenv from 'dotenv';
import { RedisQueues, connectRedis } from '../functions/queue.js';
import { getPostgresPool } from '../dataStore/index.js';
import { Pool } from 'pg'
dotenv.config();
const business_logic_contribute_api = `${process.env.gateway}/api/v1/payments/contribute`;
const WALLET_CREDIT_API = `${process.env.mpesa_ms_url}/wallet/`
const POLL_INTERVAL_MS = parseInt(1500); // Default 15 seconds
const contributionQueue = "queue:contributions:jobs";
const contributionRetryQueue = "queue:contributions:retry";
const sms_notification_queue = "queue:sms:notifications";
console.log('Worker configuration:', {
    business_logic_contribute_api,
    WALLET_CREDIT_API,
    POLL_INTERVAL_MS,
});
const pool = new Pool({
    host: 'ep-flat-bar-aba7x6f8-pooler.eu-west-2.aws.neon.tech',
    user: 'neondb_owner',
    password:'npg_zqjXJ8nsE3Fd',
    database: 'neondb',
    port:'5432',
    ssl: {
        rejectUnauthorized: false,
        sslmode: 'require',
    }
});
const client = await pool.connect();
connectRedis();
const jobData = async () => {
    try {
        const sql = `SELECT c.chamaa_id,
       c.id     as cycle_id,
       c.amount_per_member,
       b.amount as available_balance,
       b.user_id as user_id_wallet,
            cu.is_active
        FROM cycles c
                join (select id,
                            cycle_id,
                            user_id,
                            is_active
                    from cycles_users) cu on cu.cycle_id = c.id
                join(select amount,
                            user_id,chamaa_id
                    from wallet_balances) b on CAST(cu.user_id AS VARCHAR) = b.user_id
        where NOW() > c.start_date
        AND NOW() < c.end_date and b.chamaa_id = CAST(c.chamaa_id AS VARCHAR)`
        const { rows } = await client.query(sql);
        return rows;
    } catch (e) {
        console.log('Error processing queue item: t', e);
    }
}
const allMembers_of_active_cycle = async (cycle_id) => {
    try {
        const sql = `SELECT c.chamaa_id,
                        c.id     as cycle_id,
                            cu.user_id
                    FROM cycles c
                            join (select id,
                                        cycle_id,
                                        user_id,
                                        is_active
                                from cycles_users) cu on cu.cycle_id = c.id

                    join (select username,id,phone,email from users) u
                    on cast(u.id as VARCHAR) = CAST(cu.user_id AS VARCHAR)
                    where NOW() > c.start_date
                    AND NOW() < c.end_date and cu.is_active =true and c.id = '${cycle_id}';`
        const { rows } = await client.query(sql);
        return rows;
    }
    catch (e) {
        console.log('Error processing queue item: t', e);
    }
}
const confirm_if_a_user_has_enough_balance = async (user_id, chamaa_id, expected_amount) => {
    try {
        const sql = `select sum(amount) from wallet_balances where chamaa_id ='${chamaa_id}' and user_id ='${user_id}';`
        const { rows } = await client.query(sql);
        return {
            hasEnoughBalance: parseFloat(rows[0].sum) >= parseFloat(expected_amount),
            availableBalance: rows[0].sum
        }
    }
    catch (e) {
        console.log('Error processing queue item: t', e);
    }
}
const get_next_in_line_contributor = async (cycle_id) => {
    try {
        const sql = `
        SELECT id, user_id
        FROM next_in_line
        WHERE cycle_id = '${cycle_id}'
        AND is_paid_in_full = false
        AND date >= CURRENT_DATE
        AND date < CURRENT_DATE + INTERVAL '1 day'
        ORDER BY date DESC;
        `
        const { rows } = await client.query(sql);
        return rows.length > 0 ? rows[0] : false;
    }
    catch (e) {
        console.log('Error processing queue item: t', e);
    }
}

const confirm_if_all_members_have_enough_balance = async (members, expected_amount) => {
    //expected amount multiplied by the number of members is the total expected amount for the cycle contribution
    const total_expected_amount = expected_amount * members.length;
    let available_balance = 0;
    let insufficient_balance_members = [];

    for (const member of members) {
        const { user_id, chamaa_id } = member;
        const hasEnoughBalance = await confirm_if_a_user_has_enough_balance(user_id, chamaa_id, expected_amount);
        if (!hasEnoughBalance.hasEnoughBalance) {
            //send notification to the user about insufficient balance, 
            insufficient_balance_members.push({
                user_id,
                chamaa_id,
                available_balance: hasEnoughBalance.availableBalance,
                expected_amount
            });
        }
        else{
            available_balance += parseFloat(expected_amount);
        }
    }
    return {
        hasEnoughBalance: available_balance >= total_expected_amount,
        totalAvailableBalance: available_balance,
        totalExpectedAmount: total_expected_amount,
        insufficientBalanceMembers: insufficient_balance_members
    }
}
const filter_out_unique_clycles = (contributions) => {
    const uniqueCycles = new Set();
    return contributions.filter((contribution) => {
        if (uniqueCycles.has(contribution.cycle_id)) {
            return false;
        }
        uniqueCycles.add(contribution.cycle_id);
        return true;
    });
};

setInterval(async () => {
    try {
        const contributions = await jobData();
        let createJobs = false;
        const uniqueCycles = filter_out_unique_clycles(contributions);
        const cycleStatuses = await Promise.all(uniqueCycles.map(async (cycle) => {
            const members = await allMembers_of_active_cycle(cycle.cycle_id);
            const balanceCheck = await confirm_if_all_members_have_enough_balance(members, cycle.amount_per_member);
            //check if the next in line exists, 
            const next_in_line_id = await get_next_in_line_contributor(cycle.cycle_id);
            if(next_in_line_id){
                const next_in_line =await get_next_in_line_contributor(cycle.cycle_id);
                createJobs = true;
                return {
                cycle_id: cycle.cycle_id,
                chamaa_id: cycle.chamaa_id,
                next_in_line_id: next_in_line.id,
                user_id: next_in_line.user_id,
                number_of_members: members.length,
                amount_per_member: cycle.amount_per_member,
                hasEnoughBalance: balanceCheck.hasEnoughBalance,
                totalAvailableBalance: balanceCheck.totalAvailableBalance,
                totalExpectedAmount: balanceCheck.totalExpectedAmount,
                insufficientBalanceMembers: balanceCheck.insufficientBalanceMembers
            }
            }
            //next in line has to be today, otherwise drop the jobs
            
        }));
        if(createJobs){
        for (const status of cycleStatuses) {
                if (status.hasEnoughBalance) {
                    await RedisQueues.addToQueue(contributionQueue, status)
                    console.log(`Cycle ${status.cycle_id} in chamaa ${status.chamaa_id} has enough balance. Total Available: ${status.totalAvailableBalance}, Total Expected: ${status.totalExpectedAmount}`);
                }
                if (status.insufficientBalanceMembers.length > 0) {
                    for (const member of status.insufficientBalanceMembers) {
                        //send notification to the user about insufficient balance, 
                        await RedisQueues.addToQueue(sms_notification_queue, {
                            user_id: member.user_id,
                            chamaa_id: member.chamaa_id,
                            available_balance: member.available_balance,
                            expected_amount: member.expected_amount
                        });
                        console.log(`User ${member.user_id} in chamaa ${member.chamaa_id} has insufficient balance. Available: ${member.available_balance}, Expected: ${member.expected_amount}`);
                    }
                }
        }}
        else{
            console.log('No cycles are due for contribution today or next in line contributor is not due for contribution today');
        }
        console.log('Cycle statuses:', cycleStatuses.length > 0 ? JSON.stringify(cycleStatuses, null, 2) : 'No active cycles found');
    } catch (error) {
        console.error('Error fetching contributions:', error.message);
    }
}, 0);

/* 
**pop contribution jobs from the contributionQueue and 
** 1. Call the business logic api and the mpesa create wallet entry api
** 2. Send notifications to the users about the contribution status (success or failure)
** 3. Call the business logic api to credit the next in line member and debit the rest of the members in the cycle
**/

const processContributionJobs = async () => {
    try{
        let queueLength = await RedisQueues.queueLength(contributionQueue);
        console.log(`Processing contribution jobs. Queue length: ${queueLength}`);
        while (queueLength > 0) {
            const job = await RedisQueues.popLeftFromQueue(contributionQueue);
            if (job) {
                console.log('Processing job:', job);
                try{
                    // Call the business logic API to process the contribution
                    console.log('Calling business logic API with payload:', business_logic_contribute_api)
                  const response =await axios.post(business_logic_contribute_api, {
                        user_id: job.user_id,
                        cycle_id: job.cycle_id,
                        amount: job.amount_per_member,
                        next_in_line_id: job.next_in_line_id,
                    },
                    {
                        headers: {
                            'Content-Type': 'application/json'
                        }
                    },
                    
                );
                    console.log('Business logic API response:', response.data);
                    // Here you can also call the mpesa create wallet entry API if needed
                    console.log('Creating wallet entry with payload:', WALLET_CREDIT_API)
                    const wallet_payload = {
                        "is_debit": true,
                        "is_credit": false,
                        "transaction_id": `contribution-${job.cycle_id}-${Date.now()}`,
                        "amount": job.amount_per_member,
                        "chamaa_id": job.chamaa_id,
                        "user_id": job.user_id,
                        "cycle_id": job.cycle_id,
                        "transaction_type": "contribution",
                        "date": new Date()
                    }
                    const walletResponse = await axios.post(WALLET_CREDIT_API, wallet_payload,   {
                        headers: {
                            'Content-Type': 'application/json'
                        }
                    },);
                    console.log('Wallet API response:', walletResponse.data);

                    // and send notifications to users about the contribution status

                }
                catch(error){
                    console.log(error);
                    console.error('Error processing contribution job:', error.message);
                    // Optionally, you can re-add the job to the queue for retrying later
                    await RedisQueues.addToQueue(contributionRetryQueue, job);
                }
            }
            queueLength = await RedisQueues.queueLength(contributionQueue);
        }
    }catch (error) {}
};

setInterval(processContributionJobs, parseInt(POLL_INTERVAL_MS)+500);

//Note:
//consider the next in-line contribution date while selecting the active cycles, this will ensure that we are only processing the contributions that are due for contribution and not the ones that are not yet due.