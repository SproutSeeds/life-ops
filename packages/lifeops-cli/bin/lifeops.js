#!/usr/bin/env node

import { runCli } from "../src/index.js";
import { runCloudCmail } from "../src/cmail-cloud.js";

const args = process.argv.slice(2);
const exitCode = (args[0] === "cmail" ? await runCloudCmail(args.slice(1)) : null) ?? await runCli(args);
process.exitCode = exitCode;
