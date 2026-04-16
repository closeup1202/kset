import {program} from './program.js';
import {runInitWizard} from '../wizard/initWizard.js';
import {generateTarball} from '../generators/tarball.js';

program
    .command('init')
    .description('Initialize Kafka setup interactively')
    .action(async () => {
        console.log('\n🚀 kset 시작합니다...\n');
        const config = await runInitWizard();
        console.log('\n📋 선택한 설정:\n');
        console.log(JSON.stringify(config, null, 2));
        console.log('\n✅ 설정 완료! 곧 파일 생성 단계로 이어집니다.');

        if (config.environment === 'local') {
            await generateTarball(config);
        }
    });