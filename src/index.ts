#!/usr/bin/env node

import {program} from './commands/program.js';
import './commands/check.js';
import './commands/init.js';
import './commands/generate.js';

program.parse(process.argv);