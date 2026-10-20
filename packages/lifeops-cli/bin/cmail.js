#!/usr/bin/env node

import { runCmailCli } from "../src/index.js";
import { runCloudCmail } from "../src/cmail-cloud.js";

const args = process.argv.slice(2);
const exitCode = (await runCloudCmail(args)) ?? await runCmailCli(args);
process.exitCode = exitCode;
