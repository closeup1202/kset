import {confirm, input, select} from '@inquirer/prompts';

export interface KsetConfig {
    environment: 'local' | 'docker' | 'systemd';
    version: string;
    mode: 'kraft' | 'zookeeper';
    brokerCount: number;
    replicationFactor: number;
    port: number;
    createTopic: boolean;
    topicName?: string;
    partitions?: number;
    installPath?: string;
    logPath: string;
}

async function fetchKafkaVersions(): Promise<string[]> {
    const response = await fetch('https://downloads.apache.org/kafka/');
    const html = await response.text();
    const matches = html.match(/href="(\d+\.\d+\.\d+)\/"/g) ?? [];
    return matches
        .map((m) => m.replace(/href="|\/"/g, ''))
        .reverse();
}

function supportsKRaft(version: string): boolean {
    const [major, minor] = version.split('.').map(Number);
    return major > 3 || (major === 3 && minor >= 3);
}

function kraftOnly(version: string): boolean {
    const [major] = version.split('.').map(Number);
    return major >= 4;
}

export async function runInitWizard(): Promise<KsetConfig> {
    console.log('📡 Kafka 버전 목록 불러오는 중...');
    const kafkaVersions = await fetchKafkaVersions();

    const environment = await select({
        message: '설치 환경을 선택하세요',
        choices: [
            {name: '로컬 직접 설치 (tarball)', value: 'local' as const},
            {name: 'Docker / docker-compose', value: 'docker' as const},
            {name: '운영 서버 (Linux systemd)', value: 'systemd' as const},
        ],
    });

    const version = await select({
        message: 'Kafka 버전을 선택하세요',
        choices: kafkaVersions.map((v) => ({name: v, value: v})),
    });

    const mode = kraftOnly(version)
        ? 'kraft' as const
        : await select({
            message: '모드를 선택하세요',
            choices: supportsKRaft(version)
                ? [
                    {name: 'KRaft (권장)', value: 'kraft' as const},
                    {name: 'Zookeeper', value: 'zookeeper' as const},
                ]
                : [{name: 'Zookeeper (이 버전은 KRaft를 지원하지 않아요)', value: 'zookeeper' as const}],
        });

    if (kraftOnly(version)) {
        console.log('ℹ️  Kafka 4.0 이상은 KRaft 모드만 지원해요');
    }

    const brokerCount = await select({
        message: '브로커 수를 선택하세요',
        choices: [
            {name: '1', value: 1},
            {name: '3', value: 3},
            {name: '5', value: 5},
        ],
    });

    const replicationFactorInput = await input({
        message: `Replication factor를 입력하세요 (하나 이상, ${brokerCount} 이하)`,
        default: '1',
        validate: (value) => {
            const num = parseInt(value);
            return (num >= 1 && num <= brokerCount) || `하나 이상, ${brokerCount} 이하로 입력해주세요`;
        },
    });

    const replicationFactor = parseInt(replicationFactorInput);

    const portInput = await input({
        message: '리스너 포트를 입력하세요',
        default: '9092',
        validate: (value) => {
            const num = parseInt(value);
            return (!isNaN(num) && num > 0 && num < 65536) || '유효한 포트 번호를 입력해주세요';
        },
    });
    const port = parseInt(portInput);

    const createTopic = await confirm({
        message: '초기 토픽을 생성할까요?',
        default: false,
    });

    let topicName, partitions;

    if (createTopic) {
        topicName = await input({
            message: '토픽 이름을 입력하세요',
            default: 'my-topic',
        });

        const partitionsInput = await input({
            message: '파티션 수를 입력하세요',
            default: '3',
        });
        partitions = parseInt(partitionsInput);
    }

    let installPath: string | undefined;

    if (environment === 'local' || environment === 'systemd') {
        installPath = await input({
            message: '설치 경로를 입력하세요',
            default: './kafka',
        });
    }

    const logPath = environment === 'docker'
        ? 'kafka-logs'
        : await input({
            message: '로그 파일 경로를 입력하세요',
            default: '/tmp/kafka-logs',
        });

    return {
        environment,
        version,
        mode,
        brokerCount,
        replicationFactor,
        port,
        createTopic,
        topicName,
        partitions,
        installPath,
        logPath
    };
}