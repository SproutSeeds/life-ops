#!/usr/bin/env node

import { runCmailCli } from "../src/index.js";

const exitCode = await runCmailCli(process.argv.slice(2));
process.exitCode = exitCode;
