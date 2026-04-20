import {program} from './program.js';
import {execSync} from 'child_process';
import net from 'net';

function checkJava(): void {
    try {
        const output = execSync('java -version 2>&1').toString();
        const match = output.match(/version "(\d+)/);
        if (match) {
            const major = parseInt(match[1]);
            if (major >= 17) {
                console.log(`✅ Java ${major} detected (recommended)`);
            } else if (major >= 11) {
                console.log(`✅ Java ${major} detected (minimum requirement met, Java 17+ recommended)`);
            } else {
                console.log(`❌ Java ${major} detected — Kafka requires Java 11 or higher`);
            }
        }
    } catch {
        console.log('❌ Java not found — please install Java 17+');
    }
}

function checkDocker(): void {
    try {
        const output = execSync('docker --version 2>&1').toString().trim();
        console.log(`✅ ${output}`);
    } catch {
        console.log('⚠️  Docker not found — required for Docker environment');
    }
}

function checkDockerCompose(): void {
    try {
        const output = execSync('docker compose version 2>&1').toString().trim();
        console.log(`✅ ${output}`);
    } catch {
        console.log('⚠️  Docker Compose not found — required for Docker environment');
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
        });
        server.on('error', () => {
            console.log(`❌ Port ${port} already in use`);
            resolve();
        });
    });
}

program
    .command('check')
    .description('Check system requirements for Kafka installation')
    .option('-p, --port <number>', 'port to check', '9092')
    .action(async (options) => {
        const port = parseInt(options.port);
        console.log('\n🔍 Checking system requirements...\n');
        checkJava();
        checkDocker();
        checkDockerCompose();
        await checkPort(port);
        console.log('\n✅ Check completed.');
    });