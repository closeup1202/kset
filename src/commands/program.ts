import {Command} from 'commander';

export const program = new Command();

program
    .name('kset')
    .description('Apache Kafka setup CLI')
    .version('0.1.1');
