import {program} from './program.js';
import {runInitWizard} from '../wizard/initWizard.js';
import {generateTarball} from '../generators/tarball.js';
import {generateDocker} from '../generators/docker.js';

program
    .command('init')
    .description('Initialize Kafka setup interactively')
    .action(async () => {
        console.log('\n🚀 Starting kset...\n');
        const config = await runInitWizard();
        console.log('\n📋 Selected configuration:\n');
        console.log(JSON.stringify(config, null, 2));
        console.log('\n✅ Configuration complete! Proceeding to file generation.');

        if (config.environment === 'local') {
            await generateTarball(config);
        } else if (config.environment === 'docker') {
            await generateDocker(config);
        }
    });