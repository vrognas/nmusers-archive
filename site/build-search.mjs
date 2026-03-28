/**
 * Build the Orama search index from exported search data.
 *
 * Creates two files:
 *   - search-index.json: Orama index (subject + author, compact)
 *   - search-snippets.json: body snippets keyed by URL (for display)
 *
 * Usage: node site/build-search.mjs [--output site/output]
 */

import { readFileSync, writeFileSync } from "fs";
import { create, insertMultiple } from "@orama/orama";
import { persist } from "@orama/plugin-data-persistence";

const outputDir = process.argv.includes("--output")
  ? process.argv[process.argv.indexOf("--output") + 1]
  : "site/output";

console.log("Loading search data...");
const docs = JSON.parse(readFileSync(`${outputDir}/search-data.json`, "utf-8"));
console.log(`Loaded ${docs.length} documents`);

// Build snippets map (url → body snippet) for client-side display
const snippets = {};
for (const doc of docs) {
  snippets[doc.url] = doc.body;
}
const snippetsPath = `${outputDir}/search-snippets.json`;
writeFileSync(snippetsPath, JSON.stringify(snippets));
const snippetsMB = (Buffer.byteLength(JSON.stringify(snippets)) / 1024 / 1024).toFixed(1);
console.log(`Wrote ${snippetsPath} (${snippetsMB} MB)`);

// Build Orama index (subject + from_name only — fast and compact)
console.log("Creating Orama index...");
const db = await create({
  schema: {
    subject: "string",
    from_name: "string",
    category: "enum",
    year: "number",
    date: "string",
    url: "string",
  },
});

// Strip body from docs before inserting
const indexDocs = docs.map(({ body, ...rest }) => rest);

console.log("Inserting documents...");
await insertMultiple(db, indexDocs, 500);

console.log("Persisting index...");
const indexData = await persist(db, "json");
const indexPath = `${outputDir}/search-index.json`;
writeFileSync(indexPath, JSON.stringify(indexData));
const indexMB = (Buffer.byteLength(JSON.stringify(indexData)) / 1024 / 1024).toFixed(1);
console.log(`Wrote ${indexPath} (${indexMB} MB)`);
