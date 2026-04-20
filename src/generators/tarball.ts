import {KsetConfig} from '../wizard/initWizard.js';
import {execSync, spawn} from 'child_process';
import {mkdirSync, readFileSync, writeFileSync} from 'fs';
import {join, resolve} from 'path';
import net from 'net';

const TARBALL_URL = (version: string) =>
    `https://downloads.apache.org/kafka/${version}/kafka_2.13-${version}.tgz`;

const KAFKA_DIR = (version: string) =>
    `kafka_2.13-${version}`;

function getPropertiesPath(kafkaDir: string, mode: 'kraft' | 'zookeeper', version: string): string {
    const [major] = version.split('.').map(Number);
    if (mode === 'kraft' && major < 4) {
        return join(kafkaDir, 'config', 'kraft', 'server.properties');
    }
    return join(kafkaDir, 'config', 'server.properties');
}

function applyZookeeperConfig(config: KsetConfig, kafkaDir: string): void {
    const propertiesPath = join(kafkaDir, 'config', 'server.properties');
    const content = readFileSync(propertiesPath, 'utf-8');

    const updated = content
        .replace(/^#listeners=PLAINTEXT:\/\/:9092$/m, `listeners=PLAINTEXT://:${config.port}`)
        .replace(/^#advertised\.listeners=PLAINTEXT:\/\/your\.host\.name:9092$/m, `advertised.listeners=PLAINTEXT://localhost:${config.port}`)
        .replace(/^log\.dirs=\/tmp\/kafka-logs$/m, `log.dirs=${config.logPath}`)
        .replace(/^offsets\.topic\.replication\.factor=1$/m, `offsets.topic.replication.factor=${config.replicationFactor}`)
        .replace(/^transaction\.state\.log\.replication\.factor=1$/m, `transaction.state.log.replication.factor=${config.replicationFactor}`);

    writeFileSync(propertiesPath, updated);
}

function applyKraftConfig(config: KsetConfig, kafkaDir: string): void {
    const propertiesPath = getPropertiesPath(kafkaDir, config.mode, config.version);
    const content = readFileSync(propertiesPath, 'utf-8');
    const [major] = config.version.split('.').map(Number);

    let updated: string;

    if (major >= 4) {
        updated = content
            .replace(/^listeners=PLAINTEXT:\/\/:[0-9]+,CONTROLLER:\/\/:[0-9]+$/m, `listeners=PLAINTEXT://:${config.port},CONTROLLER://:9093`)
            .replace(/^advertised\.listeners=PLAINTEXT:\/\/localhost:[0-9]+,CONTROLLER:\/\/localhost:[0-9]+$/m, `advertised.listeners=PLAINTEXT://localhost:${config.port},CONTROLLER://localhost:9093`)
            .replace(/^log\.dirs=.*$/m, `log.dirs=${config.logPath}`)
            .replace(/^offsets\.topic\.replication\.factor=1$/m, `offsets.topic.replication.factor=${config.replicationFactor}`)
            .replace(/^transaction\.state\.log\.replication\.factor=1$/m, `transaction.state.log.replication.factor=${config.replicationFactor}`);
    } else {
        updated = content
            .replace(/^listeners=PLAINTEXT:\/\/:9092,CONTROLLER:\/\/:9093$/m, `listeners=PLAINTEXT://:${config.port},CONTROLLER://:9093`)
            .replace(/^advertised\.listeners=PLAINTEXT:\/\/localhost:[0-9]+(,CONTROLLER:\/\/localhost:[0-9]+)?$/m, `advertised.listeners=PLAINTEXT://localhost:${config.port},CONTROLLER://localhost:9093`)
            .replace(/^log\.dirs=\/tmp\/kraft-combined-logs$/m, `log.dirs=${config.logPath}`)
            .replace(/^offsets\.topic\.replication\.factor=1$/m, `offsets.topic.replication.factor=${config.replicationFactor}`)
            .replace(/^transaction\.state\.log\.replication\.factor=1$/m, `transaction.state.log.replication.factor=${config.replicationFactor}`);
    }

    writeFileSync(propertiesPath, updated);
}

function waitForZookeeper(port: number, retries = 20, interval = 2000): Promise<void> {
    return new Promise((resolve, reject) => {
        let attempts = 0;
        const check = () => {
            const socket = net.createConnection(port, 'localhost');
            socket.on('connect', () => {
                socket.destroy();
                resolve();
            });
            socket.on('error', () => {
                socket.destroy();
                attempts++;
                if (attempts >= retries) {
                    reject(new Error(`Zookeeper is not responding on port ${port}`));
                } else {
                    setTimeout(check, interval);
                }
            });
        };
        check();
    });
}

function waitForKafka(port: number, retries = 15, interval = 3000): Promise<void> {
    return new Promise((resolve, reject) => {
        let attempts = 0;
        const check = () => {
            const socket = net.createConnection(port, 'localhost');
            socket.setTimeout(2000);
            socket.on('connect', () => {
                socket.destroy();
                // wait for Kafka to fully initialize even after port opens
                setTimeout(resolve, 3000);
            });
            socket.on('error', () => {
                socket.destroy();
                attempts++;
                if (attempts >= retries) {
                    reject(new Error(`Kafka is not responding on port ${port}`));
                } else {
                    setTimeout(check, interval);
                }
            });
            socket.on('timeout', () => {
                socket.destroy();
                attempts++;
                if (attempts >= retries) {
                    reject(new Error(`Kafka is not responding on port ${port}`));
                } else {
                    setTimeout(check, interval);
                }
            });
        };
        check();
    });
}

export async function generateTarball(config: KsetConfig): Promise<void> {
    const installPath = resolve(config.installPath ?? './kafka');
    const tarballUrl = TARBALL_URL(config.version);
    const tarballFilename = `kafka_2.13-${config.version}.tgz`;
    const tarballPath = join(installPath, tarballFilename);
    const kafkaDir = join(installPath, KAFKA_DIR(config.version));

    mkdirSync(installPath, {recursive: true});

    console.log(`\n📥 Downloading Kafka ${config.version}...`);
    execSync(`curl -L --progress-bar "${tarballUrl}" -o "${tarballPath}"`, {stdio: 'inherit'});

    console.log(`\n📦 Extracting archive...`);
    execSync(`tar -xzf "${tarballPath}" -C "${installPath}"`, {stdio: 'inherit'});
    execSync(`rm "${tarballPath}"`);

    console.log(`\n⚙️  Applying configuration...`);
    if (config.mode === 'kraft') {
        applyKraftConfig(config, kafkaDir);
    } else {
        applyZookeeperConfig(config, kafkaDir);
    }

    if (config.mode === 'kraft') {
        console.log(`\n🔧 Initializing KRaft storage...`);
        const kafkaStorageSh = join(kafkaDir, 'bin', 'kafka-storage.sh');
        const propertiesPath = getPropertiesPath(kafkaDir, config.mode, config.version);
        const [major] = config.version.split('.').map(Number);

        if (major >= 4) {
            const uuid = execSync(`${kafkaStorageSh} random-uuid`).toString().trim();
            execSync(`${kafkaStorageSh} format --standalone -t ${uuid} -c "${propertiesPath}"`, {stdio: 'inherit'});
        } else {
            const uuid = execSync(`${kafkaStorageSh} random-uuid`).toString().trim();
            execSync(`${kafkaStorageSh} format -t ${uuid} -c "${propertiesPath}"`, {stdio: 'inherit'});
        }
    }

    console.log(`\n✅ Kafka ${config.version} installed successfully!`);
    console.log(`📁 Installation path: ${kafkaDir}`);

    const configPath = join(kafkaDir, 'config', 'server.properties');
    const startScript = join(kafkaDir, 'bin', 'kafka-server-start.sh');

    if (config.createTopic && config.topicName && config.partitions) {
        console.log(`\n📌 Starting Kafka briefly to create the topic...`);
        const kafkaStartSh = join(kafkaDir, 'bin', 'kafka-server-start.sh');
        const kafkaStopSh = join(kafkaDir, 'bin', 'kafka-server-stop.sh');
        const kafkaTopicsSh = join(kafkaDir, 'bin', 'kafka-topics.sh');
        const propertiesPath = getPropertiesPath(kafkaDir, config.mode, config.version);

        if (config.mode === 'zookeeper') {
            const zookeeperStartSh = join(kafkaDir, 'bin', 'zookeeper-server-start.sh');
            const zookeeperPropertiesPath = join(kafkaDir, 'config', 'zookeeper.properties');

            const zookeeperProcess = spawn(zookeeperStartSh, [zookeeperPropertiesPath], {
                detached: true,
                stdio: 'ignore',
            });
            zookeeperProcess.unref();

            console.log(`⏳ Waiting for Zookeeper to start...`);
            await waitForZookeeper(2181);
        }

        const kafkaProcess = spawn(kafkaStartSh, [propertiesPath], {
            detached: true,
            stdio: 'ignore',
        });
        kafkaProcess.unref();

        console.log(`⏳ Waiting for Kafka to start...`);
        await waitForKafka(config.port);

        console.log(`📌 Creating topic...`);
        execSync(
            `${kafkaTopicsSh} --create \
        --topic ${config.topicName} \
        --partitions ${config.partitions} \
        --replication-factor ${config.replicationFactor} \
        --bootstrap-server localhost:${config.port}`,
            {stdio: 'inherit'}
        );
        console.log(`✅ Topic "${config.topicName}" created successfully!`);

        console.log(`\n🛑 Stopping Kafka...`);
        execSync(`${kafkaStopSh}`, {stdio: 'inherit'});

        if (config.mode === 'zookeeper') {
            const zookeeperStopSh = join(kafkaDir, 'bin', 'zookeeper-server-stop.sh');
            console.log(`🛑 Stopping Zookeeper...`);
            execSync(`${zookeeperStopSh}`, {stdio: 'inherit'});
        }
    }

    console.log(`\n👉 To start:`);
    if (config.mode === 'zookeeper') {
        const zookeeperStartSh = join(kafkaDir, 'bin', 'zookeeper-server-start.sh');
        const zookeeperPropertiesPath = join(kafkaDir, 'config', 'zookeeper.properties');
        console.log(`\n   1. Start Zookeeper first:`);
        console.log(`      ${zookeeperStartSh} ${zookeeperPropertiesPath}`);
        console.log(`\n   2. Start Kafka:`);
        console.log(`      ${startScript} ${configPath}`);
    } else {
        console.log(`   ${startScript} ${configPath}`);
    }
}