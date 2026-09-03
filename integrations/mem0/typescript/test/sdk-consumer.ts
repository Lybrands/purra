import { Memory } from "mem0ai/oss";
import { Mem0Memory, type Mem0Client } from "purra-mem0";

declare const sdk: Memory;
const client: Mem0Client = sdk;
new Mem0Memory({ client, scope: { user: "u", project: "p" }, journalPath: "/host/journal.db" });
