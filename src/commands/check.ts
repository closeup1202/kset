import { program } from './program.js';
import {execSync} from 'child_process';
import net from 'net';

function checkJava(): void {
    try {
        const output = execSync('java -version 2>&1').toString();
        const match = output.match(/version "(\d+)/);
        if (match) {
            const major = parseInt(match[1]);
            if (major >= 11) {
                console.log(`✅ Java ${major} detected`);
            } else {
                console.log(`❌ Java ${major} detected — Kafka requires Java 11 or higher`);
            }
        }
    } catch {
        console.log('❌ Java does not exist');
    }
}

function checkPort(port: number): Promise<void> {
    return new Promise((resolve) => {
        const server = net.createServer();
        server.listen(port, () => {
            server.close(() => {
                console.log(`✅ Port ${port} available`);
                resolve();
            });
        })
        server.on('error', () => {
            console.log(`❌ Port ${port} Already in progress`);
            resolve();
        });
    })
}

program
    .command('check')
    .description('Check system requirements for Kafka installation')
    .action(async () => {
        console.log('Started check environment...');
        checkJava();
        await checkPort(9092);
        console.log("\nCompleted check environment.");
    });