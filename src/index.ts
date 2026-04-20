#!/usr/bin/env node

import {program} from './commands/program.js';
import './commands/check.js';
import './commands/init.js';

program.parse(process.argv);