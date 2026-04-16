import { KsetConfig } from '../wizard/initWizard.js';
import { execSync } from 'child_process';
import { existsSync, mkdirSync } from 'fs';
import { join } from 'path';

const TARBALL_URL = (version: string) =>
    `https://downloads.apache.org/kafka/${version}/kafka_2.13-${version}.tgz`;

const TARBALL_FILENAME = (version: string) =>
    `kafka_2.13-${version}.tgz`;

const KAFKA_DIR = (version: string) =>
    `kafka_2.13-${version}`;

function applyZookeeperConfig(config: KsetConfig, kafkaDir: string): void {
    const propertiesPath = join(kafkaDir, 'config', 'server.properties');
    const content = execSync(`cat ${propertiesPath}`).toString();

    const updated = content
        .replace(/^#listeners=PLAINTEXT:\/\/:9092$/m, `listeners=PLAINTEXT://:${config.port}`)
        .replace(/^#advertised\.listeners=PLAINTEXT:\/\/your\.host\.name:9092$/m, `advertised.listeners=PLAINTEXT://localhost:${config.port}`)
        .replace(/^log\.dirs=\/tmp\/kafka-logs$/m, `log.dirs=${config.logPath}`)
        .replace(/^offsets\.topic\.replication\.factor=1$/m, `offsets.topic.replication.factor=${config.replicationFactor}`)
        .replace(/^transaction\.state\.log\.replication\.factor=1$/m, `transaction.state.log.replication.factor=${config.replicationFactor}`);

    execSync(`cat > ${propertiesPath} << 'KSET_EOF'\n${updated}\nKSET_EOF`);
}

function applyKraftConfig(config: KsetConfig, kafkaDir: string): void {
    const propertiesPath = join(kafkaDir, 'config', 'kraft', 'server.properties');
    const content = execSync(`cat ${propertiesPath}`).toString();

    const updated = content
        .replace(/^listeners=PLAINTEXT:\/\/:9092,CONTROLLER:\/\/:9093$/m, `listeners=PLAINTEXT://:${config.port},CONTROLLER://:9093`)
        .replace(/^advertised\.listeners=PLAINTEXT:\/\/localhost:9092$/m, `advertised.listeners=PLAINTEXT://localhost:${config.port}`)
        .replace(/^log\.dirs=\/tmp\/kraft-combined-logs$/m, `log.dirs=${config.logPath}`)
        .replace(/^offsets\.topic\.replication\.factor=1$/m, `offsets.topic.replication.factor=${config.replicationFactor}`)
        .replace(/^transaction\.state\.log\.replication\.factor=1$/m, `transaction.state.log.replication.factor=${config.replicationFactor}`);

    execSync(`cat > ${propertiesPath} << 'KSET_EOF'\n${updated}\nKSET_EOF`);
}

export async function generateTarball(config: KsetConfig): Promise<void> {
    const installPath = config.installPath;
    const tarballFilename = TARBALL_FILENAME(config.version);
    const tarballUrl = TARBALL_URL(config.version);
    const kafkaDir = join(installPath, KAFKA_DIR(config.version));

    // 1. installPath 생성
    mkdirSync(installPath, { recursive: true });

    // 2. tarball 다운로드
    console.log(`\n📥 Kafka ${config.version} 다운로드 중...`);
    const tarballPath = join(installPath, tarballFilename);
    execSync(`curl -L --progress-bar "${tarballUrl}" -o "${tarballPath}"`, { stdio: 'inherit' });

    // 3. 압축 해제
    console.log(`\n📦 압축 해제 중...`);
    execSync(`tar -xzf "${tarballPath}" -C "${installPath}"`, { stdio: 'inherit' });
    execSync(`rm "${tarballPath}"`);

    // 4. server.properties 치환
    console.log(`\n⚙️  설정 파일 적용 중...`);
    if (config.mode === 'kraft') {
        applyKraftConfig(config, kafkaDir);
    } else {
        applyZookeeperConfig(config, kafkaDir);
    }

    // 5. KRaft 모드면 storage format
    if (config.mode === 'kraft') {
        console.log(`\n🔧 KRaft storage 초기화 중...`);
        const uuid = execSync(`${join(kafkaDir, 'bin', 'kafka-storage.sh')} random-uuid`).toString().trim();
        execSync(
            `${join(kafkaDir, 'bin', 'kafka-storage.sh')} format -t ${uuid} -c "${join(kafkaDir, 'config', 'kraft', 'server.properties')}"`,
            { stdio: 'inherit' }
        );
    }

    console.log(`\n✅ Kafka ${config.version} 설치 완료!`);
    console.log(`📁 설치 경로: ${kafkaDir}`);
    console.log(`\n👉 시작하려면: ${join(kafkaDir, 'bin', 'kafka-server-start.sh')} ${join(kafkaDir, 'config', config.mode === 'kraft' ? 'kraft/server.properties' : 'server.properties')}`);
}