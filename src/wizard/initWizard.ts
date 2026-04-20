import {confirm, input, select} from '@inquirer/prompts';

export interface KsetConfig {
    environment: 'local' | 'docker';
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
    console.log('📡 Fetching Kafka version list...');
    const kafkaVersions = await fetchKafkaVersions();

    const environment = await select({
        message: 'Select installation environment',
        choices: [
            {name: 'Local direct installation (tarball)', value: 'local' as const},
            {name: 'Docker / docker-compose', value: 'docker' as const}
        ],
    });

    const version = await select({
        message: 'Select Kafka version',
        choices: kafkaVersions.map((v) => ({name: v, value: v})),
    });

    const mode = kraftOnly(version)
        ? 'kraft' as const
        : await select({
            message: 'Select mode',
            choices: supportsKRaft(version)
                ? [
                    {name: 'KRaft (recommended)', value: 'kraft' as const},
                    {name: 'Zookeeper', value: 'zookeeper' as const},
                ]
                : [{name: 'Zookeeper (this version does not support KRaft)', value: 'zookeeper' as const}],
        });

    if (kraftOnly(version)) {
        console.log('ℹ️  Kafka 4.0 and above only support KRaft mode');
    }

    const brokerCount = await select({
        message: 'Select broker count',
        choices: [
            {name: '1', value: 1},
            {name: '3', value: 3},
            {name: '5', value: 5},
        ],
    });

    const replicationFactorInput = await input({
        message: `Enter replication factor (between 1 and ${brokerCount})`,
        default: '1',
        validate: (value) => {
            const num = parseInt(value);
            return (num >= 1 && num <= brokerCount) || `Please enter a value between 1 and ${brokerCount}`;
        },
    });

    const replicationFactor = parseInt(replicationFactorInput);

    const portInput = await input({
        message: 'Enter listener port',
        default: '9092',
        validate: (value) => {
            const num = parseInt(value);
            return (!isNaN(num) && num > 0 && num < 65536) || 'Please enter a valid port number';
        },
    });
    const port = parseInt(portInput);

    const createTopic = await confirm({
        message: 'Create an initial topic?',
        default: false,
    });

    let topicName, partitions;

    if (createTopic) {
        topicName = await input({
            message: 'Enter topic name',
            default: 'my-topic',
        });

        const partitionsInput = await input({
            message: 'Enter partition count',
            default: '3',
        });
        partitions = parseInt(partitionsInput);
    }

    let installPath: string | undefined;

    if (environment === 'local') {
        installPath = await input({
            message: 'Enter installation path',
            default: './kafka',
        });
    }

    const logPath = environment === 'docker'
        ? 'kafka-logs'
        : await input({
            message: 'Enter log file path',
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