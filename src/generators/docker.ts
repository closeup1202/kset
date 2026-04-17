import {KsetConfig} from '../wizard/initWizard.js';
import {mkdirSync, writeFileSync} from 'fs';
import {join} from 'path';

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

function applyKraftConfig(content: string, config: KsetConfig, major: number): string {
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

function generateDockerCompose(config: KsetConfig): string {
    const [major] = config.version.split('.').map(Number);

    if (config.mode === 'zookeeper') {
        return `services:
  zookeeper:
    image: apache/kafka:${config.version}
    command: /opt/kafka/bin/zookeeper-server-start.sh /opt/kafka/config/zookeeper.properties
    ports:
      - "2181:2181"

  kafka:
    image: apache/kafka:${config.version}
    user: root
    depends_on:
      - zookeeper
    ports:
      - "${config.port}:${config.port}"
    volumes:
      - ./server.properties:/opt/kafka/config/server.properties
      - kafka-logs:/var/kafka/logs
    command: /opt/kafka/bin/kafka-server-start.sh /opt/kafka/config/server.properties

volumes:
  kafka-logs: 
`;
    }

    return `services:
  kafka:
    image: apache/kafka:${config.version}
    user: root
    ports:
      - "${config.port}:${config.port}"
      - "9093:9093"
    volumes:
      - ./server.properties:/opt/kafka/config/${major >= 4 ? '' : 'kraft/'}server.properties
      - kafka-logs:/var/kafka/logs
    command: >
      bash -c "
        /opt/kafka/bin/kafka-storage.sh format ${major >= 4 ? '--standalone' : ''} -t $$(/opt/kafka/bin/kafka-storage.sh random-uuid) -c /opt/kafka/config/${major >= 4 ? '' : 'kraft/'}server.properties &&
        /opt/kafka/bin/kafka-server-start.sh /opt/kafka/config/${major >= 4 ? '' : 'kraft/'}server.properties
      "
volumes:
  kafka-logs: 
`;
}

export async function generateDocker(config: KsetConfig): Promise<void> {
    const [major] = config.version.split('.').map(Number);
    const url = config.mode === 'kraft'
        ? KRAFT_PROPERTIES_URL(config.version)
        : ZOOKEEPER_PROPERTIES_URL(config.version);

    console.log('\n📥 공식 server.properties 다운로드 중...');
    const response = await fetch(url);
    if (!response.ok) {
        throw new Error(`server.properties 다운로드 실패: ${response.status}`);
    }

    let content = await response.text();
    content = config.mode === 'kraft'
        ? applyKraftConfig(content, config, major)
        : applyZookeeperConfig(content, config);

    const outputDir = process.cwd();
    mkdirSync(outputDir, {recursive: true});

    writeFileSync(join(outputDir, 'server.properties'), content);
    writeFileSync(join(outputDir, 'docker-compose.yml'), generateDockerCompose(config));

    console.log(`\n✅ Docker 설정 파일 생성 완료!`);
    console.log(`📁 생성 경로: ${outputDir}`);
    console.log(`   - server.properties`);
    console.log(`   - docker-compose.yml`);
    console.log(`\n👉 시작하려면:`);
    console.log(`   docker compose up -d`);

    if (config.createTopic && config.topicName && config.partitions) {
        console.log(`\n📌 토픽 생성 명령어:`);
        console.log(`   docker compose exec kafka /opt/kafka/bin/kafka-topics.sh --create \\`);
        console.log(`     --topic ${config.topicName} \\`);
        console.log(`     --partitions ${config.partitions} \\`);
        console.log(`     --replication-factor ${config.replicationFactor} \\`);
        console.log(`     --bootstrap-server localhost:${config.port}`);
    }
}