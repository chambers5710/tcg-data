#!/usr/bin/env node
/**
 * Mirror printed card art from images.pokemontcg.io into Cloudflare R2.
 * Keys: {cardId}/small.png and {cardId}/large.png
 *
 * Fill data/.env from .env.example, create the bucket, then:
 *   node sync-images.mjs
 *
 * Resume-safe: HEAD skips objects already in the bucket. Do not run this from the Worker.
 * Progress: data/sync-images-report.txt is overwritten as it goes (bounded, gitignored).
 */

import crypto from "node:crypto";
import fs from "node:fs";
import path from "node:path";
import { fileURLToPath } from "node:url";

const ROOT = path.dirname(fileURLToPath(import.meta.url));
const CARDS = path.join(ROOT, "cards", "en");
const REPORT = path.join(ROOT, "sync-images-report.txt");
const RECENT = 12;
const FAIL_KEEP = 40;
const REGION = "auto";
const SERVICE = "s3";
const UA = "tcg-data-image-mirror/1";

loadEnv(path.join(ROOT, ".env"));

const accountId = required("CLOUDFLARE_ACCOUNT_ID");
const bucket = required("R2_BUCKET");
const accessKey = required("R2_ACCESS_KEY_ID");
const secretKey = required("R2_SECRET_ACCESS_KEY");
const jurisdiction = (process.env.R2_JURISDICTION || "").trim();
const concurrency = Math.max(1, Number(process.env.CONCURRENCY || 4));
const host = jurisdiction
	? `${accountId}.${jurisdiction}.r2.cloudflarestorage.com`
	: `${accountId}.r2.cloudflarestorage.com`;

const jobs = [];
for (const file of fs.readdirSync(CARDS).filter((name) => name.endsWith(".json")).sort()) {
	const cards = JSON.parse(fs.readFileSync(path.join(CARDS, file), "utf8"));
	for (const card of cards) {
		if (!card?.id) continue;
		const images = card.images || {};
		if (images.small) jobs.push({ id: card.id, kind: "small", url: images.small });
		if (images.large) jobs.push({ id: card.id, kind: "large", url: images.large });
	}
}

console.log(`${jobs.length} objects from ${CARDS} → r2://${bucket} (concurrency ${concurrency})`);
console.log(`report ${REPORT}`);

const started = Date.now();
let done = 0;
let skipped = 0;
let uploaded = 0;
let failed = 0;
let lastFlush = 0;
let closed = false;
const recent = [];
const failures = [];
let crash = "";

writeReport("running");

process.on("SIGINT", () => stop("interrupted", 130));
process.on("SIGTERM", () => stop("interrupted", 143));
process.on("uncaughtException", (err) => {
	crash = stack(err);
	stop("crashed", 1);
});
process.on("unhandledRejection", (err) => {
	crash = stack(err);
	stop("crashed", 1);
});

try {
	await pool(jobs, concurrency, async (job) => {
		const key = `${job.id}/${job.kind}.png`;
		try {
			const existing = await r2("HEAD", key);
			if (existing.status === 200) {
				skipped += 1;
				tick("skip", key);
				return;
			}
			if (existing.status !== 404) {
				throw new Error(`HEAD ${key} → ${existing.status}`);
			}
			const body = await download(job.url);
			const put = await r2("PUT", key, body, "image/png");
			if (put.status !== 200 && put.status !== 204) {
				throw new Error(`PUT ${key} → ${put.status} ${put.text.slice(0, 200)}`);
			}
			uploaded += 1;
			tick("put", key);
		} catch (err) {
			failed += 1;
			noteFail(key, err);
			tick("fail", key);
		}
	});
} catch (err) {
	crash = stack(err);
	writeReport("crashed");
	process.exit(1);
}

writeReport(failed ? "finished with errors" : "finished");
console.log(`done=${done} uploaded=${uploaded} skipped=${skipped} failed=${failed}`);
console.log(`report ${REPORT}`);
if (failed) {
	for (const line of failures.slice(-20)) console.error(line);
	process.exit(1);
}

function tick(kind, key) {
	done += 1;
	recent.push(`${kind} ${key}`);
	if (recent.length > RECENT) recent.shift();
	const now = Date.now();
	const pulse = kind === "fail" || done % 25 === 0 || now - lastFlush > 5000;
	if (pulse) writeReport("running");
	if (done % 50 === 0 || kind === "fail") {
		console.log(`${done}/${jobs.length} ${kind} ${key} (up ${uploaded} skip ${skipped} fail ${failed})`);
	}
}

function noteFail(key, err) {
	failures.push(`${iso(new Date())} ${key}  ${err.message || err}`);
	if (failures.length > FAIL_KEEP) failures.shift();
}

function writeReport(state) {
	lastFlush = Date.now();
	const elapsed = Math.max(1, (Date.now() - started) / 1000);
	const rate = done / elapsed;
	const left = Math.max(0, jobs.length - done);
	const eta = rate > 0 ? formatSecs(left / rate) : "—";
	const lines = [
		`sync-images  ${state}`,
		`updated      ${iso(new Date())}`,
		`started      ${iso(new Date(started))}`,
		`elapsed      ${formatSecs(elapsed)}`,
		`bucket       ${bucket}`,
		`concurrency  ${concurrency}`,
		`progress     ${done} / ${jobs.length}  (${pct(done, jobs.length)})`,
		`uploaded     ${uploaded}`,
		`skipped      ${skipped}`,
		`failed       ${failed}`,
		`rate         ${rate.toFixed(1)} /s`,
		`eta          ${eta}`,
		"",
		"recent",
		...(recent.length ? recent.map((row) => `  ${row}`) : ["  (none yet)"]),
		"",
		`failures (last ${FAIL_KEEP}; oldest dropped)`,
		...(failures.length ? failures.map((row) => `  ${row}`) : ["  (none)"]),
	];
	if (crash) {
		lines.push("", "crash", ...crash.split("\n").slice(0, 16).map((row) => `  ${row}`));
	}
	lines.push("");
	fs.writeFileSync(REPORT, lines.join("\n"));
}

function stop(state, code) {
	if (closed) return;
	closed = true;
	writeReport(state);
	console.error(`${state}; see ${REPORT}`);
	process.exit(code);
}

function stack(err) {
	return (err && err.stack) || String(err);
}

function iso(date) {
	return date.toISOString();
}

function pct(part, whole) {
	if (!whole) return "0%";
	return `${((100 * part) / whole).toFixed(1)}%`;
}

function formatSecs(secs) {
	const n = Math.round(Number(secs) || 0);
	const h = Math.floor(n / 3600);
	const m = Math.floor((n % 3600) / 60);
	const s = n % 60;
	if (h) return `${h}h ${m}m ${s}s`;
	if (m) return `${m}m ${s}s`;
	return `${s}s`;
}

async function download(url) {
	const res = await retry(() =>
		fetch(url, { headers: { "user-agent": UA, accept: "image/png,image/*" } })
	);
	if (!res.ok) throw new Error(`GET ${url} → ${res.status}`);
	return Buffer.from(await res.arrayBuffer());
}

async function r2(method, key, body, contentType) {
	const payload = body ?? Buffer.alloc(0);
	const payloadHash = sha256Hex(payload);
	const now = new Date();
	const amzDate = compactUtc(now);
	const dateStamp = amzDate.slice(0, 8);
	const encodedKey = key.split("/").map(rfc3986).join("/");
	const canonicalUri = `/${rfc3986(bucket)}/${encodedKey}`;
	const headers = {
		host,
		"x-amz-content-sha256": payloadHash,
		"x-amz-date": amzDate,
	};
	if (contentType) headers["content-type"] = contentType;
	const signed = Object.keys(headers).sort();
	const canonicalHeaders = signed.map((name) => `${name}:${headers[name]}\n`).join("");
	const signedHeaders = signed.join(";");
	const canonicalRequest = [
		method,
		canonicalUri,
		"",
		canonicalHeaders,
		signedHeaders,
		payloadHash,
	].join("\n");
	const credentialScope = `${dateStamp}/${REGION}/${SERVICE}/aws4_request`;
	const stringToSign = [
		"AWS4-HMAC-SHA256",
		amzDate,
		credentialScope,
		sha256Hex(canonicalRequest),
	].join("\n");
	const signingKey = hmac(
		hmac(hmac(hmac(`AWS4${secretKey}`, dateStamp), REGION), SERVICE),
		"aws4_request"
	);
	const signature = hmac(signingKey, stringToSign).toString("hex");
	headers.authorization = `AWS4-HMAC-SHA256 Credential=${accessKey}/${credentialScope}, SignedHeaders=${signedHeaders}, Signature=${signature}`;

	return retry(async () => {
		const res = await fetch(`https://${host}${canonicalUri}`, {
			method,
			headers,
			body: method === "PUT" ? payload : undefined,
		});
		const text = method === "HEAD" ? "" : await res.text();
		if (res.status >= 500 || res.status === 429) {
			const err = new Error(`${method} ${key} → ${res.status}`);
			err.retryable = true;
			throw err;
		}
		return { status: res.status, text };
	});
}

async function retry(fn) {
	let last;
	for (let attempt = 0; attempt < 4; attempt += 1) {
		try {
			return await fn();
		} catch (err) {
			last = err;
			if (!err.retryable && !isNet(err)) throw err;
			await sleep(1000 * 2 ** attempt);
		}
	}
	throw last;
}

function pool(items, size, worker) {
	let i = 0;
	const runners = Array.from({ length: size }, async () => {
		while (i < items.length) {
			const item = items[i];
			i += 1;
			await worker(item);
		}
	});
	return Promise.all(runners);
}

function loadEnv(file) {
	if (!fs.existsSync(file)) {
		console.error(`Missing ${file}. Copy .env.example to .env and fill R2 credentials.`);
		process.exit(1);
	}
	for (const raw of fs.readFileSync(file, "utf8").split("\n")) {
		const line = raw.trim();
		if (!line || line.startsWith("#")) continue;
		const eq = line.indexOf("=");
		if (eq < 1) continue;
		const key = line.slice(0, eq).trim();
		let value = line.slice(eq + 1).trim();
		if (
			(value.startsWith('"') && value.endsWith('"')) ||
			(value.startsWith("'") && value.endsWith("'"))
		) {
			value = value.slice(1, -1);
		}
		if (process.env[key] === undefined) process.env[key] = value;
	}
}

function required(name) {
	const value = (process.env[name] || "").trim();
	if (!value) {
		console.error(`Set ${name} in .env`);
		process.exit(1);
	}
	return value;
}

function rfc3986(value) {
	return encodeURIComponent(value).replace(/[!'()*]/g, (c) => `%${c.charCodeAt(0).toString(16).toUpperCase()}`);
}

function compactUtc(date) {
	return date.toISOString().replace(/[-:]/g, "").replace(/\.\d+Z$/, "Z");
}

function sha256Hex(data) {
	return crypto.createHash("sha256").update(data).digest("hex");
}

function hmac(key, value) {
	return crypto.createHmac("sha256", key).update(value).digest();
}

function isNet(err) {
	return err?.cause?.code || err?.code === "ECONNRESET" || err?.code === "ETIMEDOUT";
}

function sleep(ms) {
	return new Promise((resolve) => setTimeout(resolve, ms));
}
