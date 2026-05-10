import express from "express";
import axios from "axios";
import multer from "multer";
import { nanoid } from "nanoid";
import { Queue, Worker } from "bullmq";
import Redis from "ioredis";
import client from "prom-client";
import dotenv from "dotenv";

dotenv.config();

const app = express();
app.use(express.json({ limit: "5mb" }));

const upload = multer({
  storage: multer.memoryStorage(),
  limits: { fileSize: 10 * 1024 * 1024 }
});

const PORT = Number(process.env.PORT || 8787);
const HOST = process.env.HOST || "127.0.0.1";
const PAGE_URL = process.env.SEEDANCE_PAGE_URL || "https://veoaifree.com/seedance-2-0-video-generator-free/";
const AJAX_URL = process.env.SEEDANCE_AJAX_URL || "https://veoaifree.com/wp-admin/admin-ajax.php";
const POLL_INTERVAL_MS = Number(process.env.POLL_INTERVAL_MS || 10000);
const MAX_POLL_ATTEMPTS = Number(process.env.MAX_POLL_ATTEMPTS || 18);
const REQUEST_TIMEOUT_MS = Number(process.env.REQUEST_TIMEOUT_MS || 30000);
const REDIS_URL = process.env.REDIS_URL || "redis://127.0.0.1:6379";
const QUEUE_NAME = process.env.QUEUE_NAME || "seedance-video-jobs";
const JOB_TTL_SECONDS = Number(process.env.JOB_TTL_SECONDS || 86400);
const API_KEY = String(process.env.API_KEY || "").trim();
const RATE_LIMIT_PER_MINUTE = Number(process.env.RATE_LIMIT_PER_MINUTE || 20);

const http = axios.create({ timeout: REQUEST_TIMEOUT_MS });
const redis = new Redis(REDIS_URL, { maxRetriesPerRequest: null });
const redisSub = new Redis(REDIS_URL, { maxRetriesPerRequest: null });
const queue = new Queue(QUEUE_NAME, { connection: redis });

let nonceCache = { value: "", fetchedAt: 0 };
const NONCE_TTL_MS = 5 * 60 * 1000;

const modelInfo = {
  id: "seedance-2.0-web-proxy",
  object: "model",
  created: 0,
  owned_by: "proxy",
  permission: []
};

const register = new client.Registry();
client.collectDefaultMetrics({ register });

const requestCounter = new client.Counter({
  name: "seedance_proxy_requests_total",
  help: "Total HTTP requests",
  labelNames: ["method", "route", "status"]
});
const upstreamCounter = new client.Counter({
  name: "seedance_proxy_upstream_calls_total",
  help: "Total upstream calls",
  labelNames: ["action_type", "result"]
});
const jobCounter = new client.Counter({
  name: "seedance_proxy_jobs_total",
  help: "Total jobs by type and final status",
  labelNames: ["kind", "status"]
});
const requestDuration = new client.Histogram({
  name: "seedance_proxy_request_duration_seconds",
  help: "HTTP request latency",
  labelNames: ["method", "route", "status"],
  buckets: [0.01, 0.05, 0.1, 0.2, 0.5, 1, 2, 5, 10]
});

register.registerMetric(requestCounter);
register.registerMetric(upstreamCounter);
register.registerMetric(jobCounter);
register.registerMetric(requestDuration);

function nowIso() {
  return new Date().toISOString();
}

function sleep(ms) {
  return new Promise((resolve) => setTimeout(resolve, ms));
}

function parseNonce(html) {
  const m = html.match(/var ajax_object = \{"ajax_url":"[^"]+","nonce":"([^"]+)"\}/);
  return m?.[1] || "";
}

function jobKey(id) {
  return `seedance:job:${id}`;
}

function rateKey(ip, minuteKey) {
  return `seedance:ratelimit:${ip}:${minuteKey}`;
}

function getClientIp(req) {
  const xff = String(req.headers["x-forwarded-for"] || "").split(",")[0].trim();
  return xff || req.ip || "unknown";
}

async function setJobState(id, patch) {
  const key = jobKey(id);
  await redis.set(key, JSON.stringify(patch), "EX", JOB_TTL_SECONDS);
}

async function getJobState(id) {
  const raw = await redis.get(jobKey(id));
  if (!raw) return null;
  return JSON.parse(raw);
}

function normalizeVideoUrl(v) {
  const s = String(v || "").trim();
  if (!s) return "";
  return s.replace("videos/", "video/");
}

function makeGenerationResponse(job) {
  return {
    id: job.id,
    object: "video.generation",
    created: job.created,
    model: job.model,
    status: job.status,
    error: job.error || null,
    output: job.output || []
  };
}

function makeResponsesStyle(job) {
  return {
    id: `resp_${job.id}`,
    object: "response",
    created_at: job.created,
    status: job.status,
    model: job.model,
    error: job.error || null,
    output: (job.output || []).map((item, idx) => ({
      type: "output_video",
      id: `outvid_${job.id}_${idx + 1}`,
      url: item.url,
      mime_type: item.mime_type || "video/mp4"
    }))
  };
}

function extractPrompt(body) {
  const direct = String(body?.prompt || "").trim();
  if (direct) return direct;

  const input = body?.input;
  if (typeof input === "string") return input.trim();
  if (Array.isArray(input)) {
    for (const item of input) {
      if (typeof item === "string" && item.trim()) return item.trim();
      const text = String(item?.text || item?.content || "").trim();
      if (text) return text;
      if (Array.isArray(item?.content)) {
        for (const c of item.content) {
          const t = String(c?.text || c?.content || "").trim();
          if (t) return t;
        }
      }
    }
  }
  return "";
}

function normalizeAspectRatio(body, fallback = "VIDEO_ASPECT_RATIO_LANDSCAPE") {
  const ar = String(body?.aspect_ratio || "").trim();
  if (ar) return ar;
  const size = String(body?.size || body?.aspectRatio || "").trim().toLowerCase();
  if (size === "16:9" || size === "1280x720" || size === "landscape") return "VIDEO_ASPECT_RATIO_LANDSCAPE";
  if (size === "9:16" || size === "720x1280" || size === "portrait") return "VIDEO_ASPECT_RATIO_PORTRAIT";
  if (size === "1:1" || size === "1024x1024" || size === "square") return "VIDEO_ASPECT_RATIO_SQUARE";
  return fallback;
}

async function getNonce(force = false) {
  const fresh = Date.now() - nonceCache.fetchedAt < NONCE_TTL_MS;
  if (!force && nonceCache.value && fresh) return nonceCache.value;

  const page = await http.get(PAGE_URL, {
    headers: {
      "User-Agent": "Mozilla/5.0 (compatible; seedance2-proxy/1.0)",
      "Accept": "text/html,application/xhtml+xml"
    }
  });

  const nonce = parseNonce(String(page.data || ""));
  if (!nonce) throw new Error("Failed to extract upstream nonce");

  nonceCache = { value: nonce, fetchedAt: Date.now() };
  return nonce;
}

async function postAjax(data, actionType, isFormData = false) {
  try {
    if (isFormData) {
      const res = await http.post(AJAX_URL, data, {
        headers: {
          ...data.getHeaders(),
          "User-Agent": "Mozilla/5.0 (compatible; seedance2-proxy/1.0)",
          "Origin": "https://veoaifree.com",
          "Referer": PAGE_URL
        }
      });
      upstreamCounter.labels(actionType, "ok").inc();
      return res.data;
    }

    const body = new URLSearchParams(data).toString();
    const res = await http.post(AJAX_URL, body, {
      headers: {
        "Content-Type": "application/x-www-form-urlencoded; charset=UTF-8",
        "User-Agent": "Mozilla/5.0 (compatible; seedance2-proxy/1.0)",
        "Origin": "https://veoaifree.com",
        "Referer": PAGE_URL
      }
    });
    upstreamCounter.labels(actionType, "ok").inc();
    return res.data;
  } catch (e) {
    upstreamCounter.labels(actionType, "err").inc();
    throw e;
  }
}

async function markFailed(job, code, message) {
  job.status = "failed";
  job.error = { code, message };
  job.updated = nowIso();
  await setJobState(job.id, job);
  jobCounter.labels(job.kind, "failed").inc();
}

async function processTextToVideo(job) {
  let nonce = await getNonce(false);

  let sceneData;
  try {
    sceneData = await postAjax({
      action: "veo_video_generator",
      nonce,
      prompt: job.prompt,
      totalVariations: String(job.n),
      aspectRatio: job.aspectRatio,
      actionType: "full-video-generate"
    }, "full-video-generate");
  } catch {
    nonce = await getNonce(true);
    sceneData = await postAjax({
      action: "veo_video_generator",
      nonce,
      prompt: job.prompt,
      totalVariations: String(job.n),
      aspectRatio: job.aspectRatio,
      actionType: "full-video-generate"
    }, "full-video-generate");
  }

  const sceneText = String(sceneData || "").trim();
  if (!sceneText) {
    await markFailed(job, "upstream_empty_scene", "Upstream returned empty scene data");
    return;
  }

  for (let i = 1; i <= MAX_POLL_ATTEMPTS; i++) {
    await sleep(POLL_INTERVAL_MS);

    const out = await postAjax({
      action: "veo_video_generator",
      nonce,
      sceneData: sceneText,
      actionType: "final-video-results"
    }, "final-video-results");

    const text = String(out || "").trim();
    const lower = text.toLowerCase();

    if (!text) continue;
    if (lower.includes("rate limit") || lower.includes("error") || lower.includes("retry")) {
      await markFailed(job, "upstream_error", text.slice(0, 400));
      return;
    }
    if (text.startsWith("http://") || text.startsWith("https://")) {
      job.status = "succeeded";
      job.output = [{ url: normalizeVideoUrl(text), mime_type: "video/mp4" }];
      job.updated = nowIso();
      await setJobState(job.id, job);
      jobCounter.labels(job.kind, "succeeded").inc();
      return;
    }

    job.meta.lastPollRaw = text.slice(0, 300);
    job.meta.pollAttempts = i;
    job.updated = nowIso();
    await setJobState(job.id, job);
  }

  await markFailed(job, "upstream_timeout", "Timed out while polling final video result");
}

async function processImageToVideo(job) {
  const FormData = (await import("form-data")).default;
  let nonce = await getNonce(false);

  const submit = async (n) => {
    const form = new FormData();
    form.append("action", "veo_video_generator");
    form.append("nonce", n);
    form.append("prompt", job.prompt);
    form.append("totalVariations", String(job.n));
    form.append("aspectRatio", job.aspectRatio);
    form.append("actionType", "img-to-video-start");
    form.append("img1", Buffer.from(job.imageBase64, "base64"), {
      filename: job.imageFilename || "image.png",
      contentType: job.imageMimeType || "image/png"
    });
    return postAjax(form, "img-to-video-start", true);
  };

  let sceneData;
  try {
    sceneData = await submit(nonce);
  } catch {
    nonce = await getNonce(true);
    sceneData = await submit(nonce);
  }

  const sceneText = String(sceneData || "").trim();
  if (!sceneText) {
    await markFailed(job, "upstream_empty_scene", "Upstream returned empty scene data");
    return;
  }

  for (let i = 1; i <= MAX_POLL_ATTEMPTS; i++) {
    await sleep(POLL_INTERVAL_MS);
    const out = await postAjax({
      action: "veo_video_generator",
      nonce,
      sceneData: sceneText,
      actionType: "final-video-results"
    }, "final-video-results");

    const text = String(out || "").trim();
    const lower = text.toLowerCase();

    if (!text) continue;
    if (lower.includes("rate limit") || lower.includes("error") || lower.includes("retry")) {
      await markFailed(job, "upstream_error", text.slice(0, 400));
      return;
    }
    if (text.startsWith("http://") || text.startsWith("https://")) {
      job.status = "succeeded";
      job.output = [{ url: normalizeVideoUrl(text), mime_type: "video/mp4" }];
      job.updated = nowIso();
      await setJobState(job.id, job);
      jobCounter.labels(job.kind, "succeeded").inc();
      return;
    }

    job.meta.lastPollRaw = text.slice(0, 300);
    job.meta.pollAttempts = i;
    job.updated = nowIso();
    await setJobState(job.id, job);
  }

  await markFailed(job, "upstream_timeout", "Timed out while polling final video result");
}

async function authAndRateLimit(req, res, next) {
  if (API_KEY) {
    const auth = String(req.headers.authorization || "");
    if (!auth.startsWith("Bearer ")) {
      return res.status(401).json({ error: { code: "unauthorized", message: "Missing Bearer token" } });
    }
    const token = auth.slice(7).trim();
    if (token !== API_KEY) {
      return res.status(401).json({ error: { code: "unauthorized", message: "Invalid API key" } });
    }
  }

  const ip = getClientIp(req);
  const minuteKey = Math.floor(Date.now() / 60000);
  const key = rateKey(ip, minuteKey);
  const count = await redis.incr(key);
  if (count === 1) await redis.expire(key, 70);

  if (count > RATE_LIMIT_PER_MINUTE) {
    return res.status(429).json({ error: { code: "rate_limited", message: `Rate limit exceeded (${RATE_LIMIT_PER_MINUTE}/min)` } });
  }

  next();
}

const worker = new Worker(
  QUEUE_NAME,
  async (bullJob) => {
    const job = await getJobState(bullJob.id);
    if (!job) return;

    try {
      job.status = "processing";
      job.updated = nowIso();
      await setJobState(job.id, job);

      if (job.kind === "image_to_video") {
        await processImageToVideo(job);
      } else {
        await processTextToVideo(job);
      }
    } catch (err) {
      await markFailed(job, "proxy_exception", err?.message || "Unknown proxy exception");
    }
  },
  {
    connection: redisSub,
    concurrency: 2
  }
);

worker.on("failed", async (bullJob, err) => {
  if (!bullJob?.id) return;
  const job = await getJobState(bullJob.id);
  if (!job) return;
  await markFailed(job, "worker_failed", err?.message || "Bull worker failed");
});

app.use((req, res, next) => {
  const end = requestDuration.startTimer();
  res.on("finish", () => {
    const route = req.route?.path || req.path || "unknown";
    const status = String(res.statusCode);
    requestCounter.labels(req.method, route, status).inc();
    end({ method: req.method, route, status });
  });
  next();
});

app.get("/metrics", async (_req, res) => {
  res.set("Content-Type", register.contentType);
  res.end(await register.metrics());
});

app.get("/health", async (_req, res) => {
  const waiting = await queue.getWaitingCount();
  const active = await queue.getActiveCount();
  res.json({ ok: true, ts: nowIso(), queue: { waiting, active } });
});

app.get("/v1/models", authAndRateLimit, (_req, res) => {
  res.json({ object: "list", data: [modelInfo] });
});

async function handleCreateTextVideo(req, res) {
  const prompt = extractPrompt(req.body);
  const aspectRatio = normalizeAspectRatio(req.body);
  const nRaw = Number(req.body?.n ?? req.body?.num_outputs ?? 1);
  const n = Number.isFinite(nRaw) ? Math.max(1, Math.min(1, Math.floor(nRaw))) : 1;

  if (!prompt || prompt.length < 15) {
    return res.status(400).json({
      error: { code: "invalid_prompt", message: "prompt is required and must be at least 15 characters" }
    });
  }

  const id = `vidgen_${nanoid(12)}`;
  const created = Math.floor(Date.now() / 1000);
  const job = {
    id,
    kind: "text_to_video",
    created,
    updated: nowIso(),
    status: "queued",
    model: modelInfo.id,
    prompt,
    n,
    aspectRatio,
    output: [],
    error: null,
    meta: { pollAttempts: 0, lastPollRaw: "" }
  };

  await setJobState(id, job);
  await queue.add("generate", { id }, { jobId: id, removeOnComplete: true, removeOnFail: true });

  const responseFormat = typeof req.body?.response_format === "string" ? req.body.response_format : req.body?.response_format?.type;
  const format = String(req.query.format || responseFormat || "generation");
  if (format === "responses") return res.status(202).json(makeResponsesStyle(job));
  return res.status(202).json(makeGenerationResponse(job));
}

app.post("/v1/videos/generations", authAndRateLimit, handleCreateTextVideo);
app.post("/v1/video/generations", authAndRateLimit, handleCreateTextVideo);

async function handleCreateImageVideo(req, res) {
  const file = req.file;
  const prompt = extractPrompt(req.body);
  const aspectRatio = normalizeAspectRatio(req.body);

  if (!file) {
    return res.status(400).json({ error: { code: "invalid_image", message: "image file is required (form field: image)" } });
  }
  if (!prompt || prompt.length < 3) {
    return res.status(400).json({ error: { code: "invalid_prompt", message: "prompt is required" } });
  }

  const id = `vidgen_${nanoid(12)}`;
  const created = Math.floor(Date.now() / 1000);
  const job = {
    id,
    kind: "image_to_video",
    created,
    updated: nowIso(),
    status: "queued",
    model: modelInfo.id,
    prompt,
    n: 1,
    aspectRatio,
    imageBase64: file.buffer.toString("base64"),
    imageFilename: file.originalname || "image.png",
    imageMimeType: file.mimetype || "image/png",
    output: [],
    error: null,
    meta: { pollAttempts: 0, lastPollRaw: "" }
  };

  await setJobState(id, job);
  await queue.add("generate", { id }, { jobId: id, removeOnComplete: true, removeOnFail: true });

  const responseFormat = typeof req.body?.response_format === "string" ? req.body.response_format : req.body?.response_format?.type;
  const format = String(req.query.format || responseFormat || "generation");
  if (format === "responses") return res.status(202).json(makeResponsesStyle(job));
  return res.status(202).json(makeGenerationResponse(job));
}

app.post("/v1/videos/image-to-video", authAndRateLimit, upload.single("image"), handleCreateImageVideo);
app.post("/v1/video/image-to-video", authAndRateLimit, upload.single("image"), handleCreateImageVideo);

app.get("/v1/videos/generations/:id", authAndRateLimit, async (req, res) => {
  const job = await getJobState(req.params.id);
  if (!job) {
    return res.status(404).json({ error: { code: "not_found", message: "generation id not found" } });
  }

  const format = String(req.query.format || "generation");
  if (format === "responses") return res.json(makeResponsesStyle(job));
  return res.json(makeGenerationResponse(job));
});

app.listen(PORT, HOST, () => {
  console.log(`seedance2 2api proxy listening on ${HOST}:${PORT}`);
});
