import { createClient } from 'redis';

const client = createClient({
    username: 'default',
    password: '5tEfpjGaSU1wMwLkilHuoOd6yUgqouUR',
    socket: {
        host: 'redis-15649.c17.us-east-1-4.ec2.cloud.redislabs.com',
        port: 15649
    }
});

client.on('error', err => console.log('Redis Client Error', err));

export const connectRedis = async () => {
    await client.connect();
};


export default client;

