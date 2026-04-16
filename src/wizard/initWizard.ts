import inquirer from 'inquirer';

export interface KsetConfig {
    environment: 'local' | 'docker' | 'systemd';
    version: string;
    mode: 'kraft' | 'zookeeper';
    brokerCount: number;
    port: number;
    createTopic: boolean;
    topicName?: string;
    partitions?: number;
    replicationFactor?: number;
}

const KAFKA_VERSIONS = ['3.7.0', '3.6.2', '3.5.1', '3.4.0', '3.3.0', '3.2.3', '3.1.2'];

function supportsKRaft(version: string): boolean {
    const [major, minor] = version.split('.').map(Number);
    return major > 3 || (major === 3 && minor >= 3);
}

export async function runInitWizard() : Promise<KsetConfig> {
    const {environment} = await inquirer.prompt([
        {
            type: 'list',
            name: 'environment',
            message: 'Choice your install environment',
            choices: [
                { name: '로컬 직접 설치 (tarball)', value: 'local' },
                { name: 'Docker / docker-compose', value: 'docker' },
                { name: '운영 서버 (Linux systemd)', value: 'systemd' },
            ]
        }
    ]);

    const { version } = await inquirer.prompt([
        {
            type: 'list',
            name: 'version',
            message: 'Kafka 버전을 선택하세요',
            choices: KAFKA_VERSIONS,
        },
    ]);

    const { mode } = await inquirer.prompt([
        {
            type: 'list',
            name: 'mode',
            message: '모드를 선택하세요',
            choices: supportsKRaft(version)
                ? [
                    { name: 'KRaft (recommended)', value: 'kraft' },
                    { name: 'Zookeeper', value: 'zookeeper' },
                ]
                : [{ name: 'Zookeeper (This version dose not supported KRaft)', value: 'zookeeper' }],
        },
    ]);

    const { brokerCount } = await inquirer.prompt([
        {
            type: 'list',
            name: 'brokerCount',
            message: '브로커 수를 선택하세요',
            choices: [
                { name: '1 (개발용)', value: 1 },
                { name: '3 (운영 권장)', value: 3 },
            ],
        },
    ]);

    const { port } = await inquirer.prompt([
        {
            type: 'input',
            name: 'port',
            message: '리스너 포트를 입력하세요',
            default: '9092',
            validate: (input: string) => {
                const num = parseInt(input);
                return (!isNaN(num) && num > 0 && num < 65536) || '유효한 포트 번호를 입력해주세요';
            },
            filter: (input: string) => parseInt(input),
        },
    ]);

    const { createTopic } = await inquirer.prompt([
        {
            type: 'confirm',
            name: 'createTopic',
            message: '초기 토픽을 생성할까요?',
            default: false,
        },
    ]);

    let topicName, partitions, replicationFactor;

    if (createTopic) {
        const topicAnswers = await inquirer.prompt([
            {
                type: 'input',
                name: 'topicName',
                message: '토픽 이름을 입력하세요',
                default: 'my-topic',
            },
            {
                type: 'input',
                name: 'partitions',
                message: '파티션 수를 입력하세요',
                default: '3',
                filter: (input: string) => parseInt(input),
            },
            {
                type: 'input',
                name: 'replicationFactor',
                message: 'Replication factor를 입력하세요',
                default: '1',
                filter: (input: string) => parseInt(input),
            },
        ]);

        topicName = topicAnswers.topicName;
        partitions = topicAnswers.partitions;
        replicationFactor = topicAnswers.replicationFactor;
    }

    return {
        environment,
        version,
        mode,
        brokerCount,
        port,
        createTopic,
        topicName,
        partitions,
        replicationFactor,
    };
}