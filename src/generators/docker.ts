import {KsetConfig} from '../wizard/initWizard.js';
import {mkdirSync, readFileSync, writeFileSync} from 'fs';
import {dirname, join} from 'path';
import {fileURLToPath} from 'url';

const __dirname = dirname(fileURLToPath(import.meta.url));

const KRAFT_PROPERTIES_URL = (version: string) => {
    const [major] = version.split('.').map(Number);
    if (major >= 4) {
        return `https://raw.githubusercontent.com/apache/kafka/${version}/config/server.properties`;
    }
    return `https://raw.githubusercontent.com/apache/kafka/${version}/config/kraft/server.properties`;
};

const ZOOKEEPER_PROPERTIES_URL = (version: string) =>
    `https://raw.githubusercontent.com/apache/kafka/${version}/config/server.properties`;

function applyZookeeperConfig(content: string, config: KsetConfig): string {
    return content
        .replace(/^#listeners=PLAINTEXT:\/\/:9092$/m, `listeners=PLAINTEXT://0.0.0.0:${config.port}`)
        .replace(/^#advertised\.listeners=PLAINTEXT:\/\/your\.host\.name:9092$/m, `advertised.listeners=PLAINTEXT://localhost:${config.port}`)
        .replace(/^log\.dirs=\/tmp\/kafka-logs$/m, `log.dirs=/var/kafka/logs`)
        .replace(/^offsets\.topic\.replication\.factor=1$/m, `offsets.topic.replication.factor=${config.replicationFactor}`)
        .replace(/^transaction\.state\.log\.replication\.factor=1$/m, `transaction.state.log.replication.factor=${config.replicationFactor}`);
}

function applyKraftConfigSingle(content: string, config: KsetConfig, major: number): string {
    if (major >= 4) {
        return content
            .replace(/^listeners=PLAINTEXT:\/\/:[0-9]+,CONTROLLER:\/\/:[0-9]+$/m, `listeners=PLAINTEXT://0.0.0.0:${config.port},CONTROLLER://0.0.0.0:9093`)
            .replace(/^advertised\.listeners=PLAINTEXT:\/\/localhost:[0-9]+,CONTROLLER:\/\/localhost:[0-9]+$/m, `advertised.listeners=PLAINTEXT://localhost:${config.port},CONTROLLER://localhost:9093`)
            .replace(/^log\.dirs=.*$/m, `log.dirs=/var/kafka/logs`)
            .replace(/^offsets\.topic\.replication\.factor=1$/m, `offsets.topic.replication.factor=${config.replicationFactor}`)
            .replace(/^transaction\.state\.log\.replication\.factor=1$/m, `transaction.state.log.replication.factor=${config.replicationFactor}`);
    } else {
        return content
            .replace(/^listeners=PLAINTEXT:\/\/:9092,CONTROLLER:\/\/:9093$/m, `listeners=PLAINTEXT://0.0.0.0:${config.port},CONTROLLER://0.0.0.0:9093`)
            .replace(/^advertised\.listeners=PLAINTEXT:\/\/localhost:[0-9]+(,CONTROLLER:\/\/localhost:[0-9]+)?$/m, `advertised.listeners=PLAINTEXT://localhost:${config.port},CONTROLLER://localhost:9093`)
            .replace(/^log\.dirs=\/tmp\/kraft-combined-logs$/m, `log.dirs=/var/kafka/logs`)
            .replace(/^offsets\.topic\.replication\.factor=1$/m, `offsets.topic.replication.factor=${config.replicationFactor}`)
            .replace(/^transaction\.state\.log\.replication\.factor=1$/m, `transaction.state.log.replication.factor=${config.replicationFactor}`);
    }
}

function generateDockerCompose(config: KsetConfig, clusterId?: string): string {
    const templatePath = join(__dirname, '..', 'templates', `docker-compose-${config.brokerCount}.yml`);
    let template = readFileSync(templatePath, 'utf-8');

    template = template
        .replace(/\{\{VERSION\}\}/g, config.version)
        .replace(/\{\{PORT\}\}/g, String(config.port))
        .replace(/\{\{REPLICATION_FACTOR\}\}/g, String(config.replicationFactor));

    if (config.brokerCount > 1 && clusterId) {
        template = template.replace(/\{\{CLUSTER_ID\}\}/g, clusterId);
        for (let i = 2; i <= config.brokerCount; i++) {
            template = template.replace(new RegExp(`\\{\\{PORT${i}\\}\\}`, 'g'), String(config.port + i - 1));
        }
    }

    return template;
}

export async function generateDocker(config: KsetConfig): Promise<void> {
    const [major] = config.version.split('.').map(Number);
    const outputDir = process.cwd();
    mkdirSync(outputDir, {recursive: true});

    let clusterId: string | undefined;

    if (config.brokerCount > 1) {
        // 멀티 브로커: 환경변수 방식 사용, server.properties 불필요
        const uuid = crypto.randomUUID().replace(/-/g, '');
        const buffer = Buffer.from(uuid, 'hex');
        clusterId = buffer.toString('base64url').substring(0, 22);
    } else {
        // 단일 브로커: server.properties 생성
        const url = config.mode === 'kraft'
            ? KRAFT_PROPERTIES_URL(config.version)
            : ZOOKEEPER_PROPERTIES_URL(config.version);

        console.log('\n📥 공식 server.properties 다운로드 중...');
        const response = await fetch(url);
        if (!response.ok) {
            throw new Error(`server.properties 다운로드 실패: ${response.status}`);
        }

        const baseContent = await response.text();
        if (config.mode === 'kraft') {
            writeFileSync(join(outputDir, 'server.properties'), applyKraftConfigSingle(baseContent, config, major));
        } else {
            writeFileSync(join(outputDir, 'server.properties'), applyZookeeperConfig(baseContent, config));
        }
    }

    writeFileSync(join(outputDir, 'docker-compose.yml'), generateDockerCompose(config, clusterId));

    console.log(`\n✅ Docker 설정 파일 생성 완료!`);
    console.log(`📁 생성 경로: ${outputDir}`);
    console.log(`   - docker-compose.yml`);
    if (config.brokerCount === 1) {
        console.log(`   - server.properties`);
    }
    console.log(`\n👉 시작하려면:`);
    console.log(`   docker compose up -d`);

    if (config.createTopic && config.topicName && config.partitions) {
        const targetService = config.brokerCount > 1 ? 'kafka-1' : 'kafka';
        console.log(`\n📌 토픽 생성 명령어:`);
        console.log(`   docker compose exec ${targetService} /opt/kafka/bin/kafka-topics.sh --create \\`);
        console.log(`     --topic ${config.topicName} \\`);
        console.log(`     --partitions ${config.partitions} \\`);
        console.log(`     --replication-factor ${config.replicationFactor} \\`);
        console.log(`     --bootstrap-server localhost:${config.port}`);
    }
}