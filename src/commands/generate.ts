import {program} from './program.js';
import {generateTarball} from '../generators/tarball.js';
import {generateDocker} from '../generators/docker.js';
import {KsetConfig} from '../wizard/initWizard.js';

program
    .command('generate')
    .description('Generate Kafka config files without interactive wizard')
    .requiredOption('--env <environment>', 'Installation environment (local | docker)')
    .requiredOption('--kafka-version <version>', 'Kafka version (e.g. 4.2.0)')
    .option('--broker <number>', 'Number of brokers', '1')
    .option('--port <number>', 'Listener port', '9092')
    .option('--replication <number>', 'Replication factor', '1')
    .option('--mode <mode>', 'Kafka mode (kraft | zookeeper)', 'kraft')
    .action(async (options) => {
        const env = options.env as 'local' | 'docker';
        const brokerCount = parseInt(options.broker);
        const port = parseInt(options.port);
        const replicationFactor = parseInt(options.replication);
        const [major] = options.kafkaVersion.split('.').map(Number);

        // 유효성 검사
        if (!['local', 'docker'].includes(env)) {
            console.error('❌ --env must be local or docker');
            process.exit(1);
        }

        if (!['kraft', 'zookeeper'].includes(options.mode)) {
            console.error('❌ --mode must be kraft or zookeeper');
            process.exit(1);
        }

        if (![1, 3, 5].includes(brokerCount)) {
            console.error('❌ --broker must be 1, 3, or 5');
            process.exit(1);
        }

        if (replicationFactor > brokerCount) {
            console.error(`❌ --replication cannot exceed --broker (${brokerCount})`);
            process.exit(1);
        }

        // 4.x는 KRaft 강제
        const mode = major >= 4 ? 'kraft' : options.mode as 'kraft' | 'zookeeper';
        if (major >= 4 && options.mode === 'zookeeper') {
            console.log('ℹ️  Kafka 4.0+ only supports KRaft mode. Switching to KRaft.');
        }

        const config: KsetConfig = {
            environment: env,
            version: options.kafkaVersion,
            mode,
            brokerCount,
            replicationFactor,
            port,
            createTopic: false,
            installPath: env === 'local' ? './kafka' : undefined,
            logPath: env === 'docker' ? 'kafka-logs' : '/tmp/kafka-logs',
        };

        console.log('\n📋 설정:\n');
        console.log(JSON.stringify(config, null, 2));
        console.log('');

        if (env === 'local') {
            await generateTarball(config);
        } else if (env === 'docker') {
            await generateDocker(config);
        }
    });