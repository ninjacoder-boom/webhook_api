require("dotenv").config();
const express = require("express");
const axios = require("axios");

const app = express();
app.use(express.json({ limit: "1mb" }));

const VERIFY_TOKEN = process.env.VERIFY_TOKEN || "myWebhookToken123";

const SFMC = {
  clientId: process.env.SFMC_CLIENT_ID,
  clientSecret: process.env.SFMC_CLIENT_SECRET,
  subdomain: process.env.SFMC_SUBDOMAIN,
  deExternalKey: process.env.SFMC_DE_KEY || "FD_Flow"
};

const isDev = process.env.NODE_ENV !== "production";

function log(level, message, meta = "") {
  if (!isDev && level === "DEBUG") return;
  const timestamp = new Date().toISOString();
  console.log(`[${timestamp}] [${level}] ${message} ${meta}`);
}

let tokenCache = { value: null, expiresAt: null };

async function getSFMCToken() {
  const now = Date.now();

  if (tokenCache.value && now < tokenCache.expiresAt - 60000) {
    return tokenCache.value;
  }

  try {
    const response = await axios.post(
      `https://${SFMC.subdomain}.auth.marketingcloudapis.com/v2/token`,
      {
        grant_type: "client_credentials",
        client_id: SFMC.clientId,
        client_secret: SFMC.clientSecret
      }
    );

    tokenCache.value = response.data.access_token;
    tokenCache.expiresAt = now + response.data.expires_in * 1000;

    log("INFO", "New SFMC token fetched");
    return tokenCache.value;
  } catch (err) {
    log("ERROR", "SFMC token fetch failed:", err.response?.data || err.message);
    throw err;
  }
}

async function saveFlowDataToDE({ from, profileName, amount, tenure, messageId, timestamp }) {
  try {
    const token = await getSFMCToken();

    const payload = [{
      keys: { MessageId: messageId },
      values: {
        PhoneNumber: from,
        ProfileName: profileName,
        Amount: amount,
        Tenure: tenure,
        MessageId: messageId,
        Timestamp: timestamp
      }
    }];

    await axios.post(
      `https://${SFMC.subdomain}.rest.marketingcloudapis.com/hub/v1/dataevents/key:${SFMC.deExternalKey}/rowset`,
      payload,
      {
        headers: {
          Authorization: `Bearer ${token}`,
          "Content-Type": "application/json"
        }
      }
    );

    log("INFO", `Saved to DE | Phone: ${from} | Name: ${profileName} | Amount: ${amount} | Tenure: ${tenure} | MsgID: ${messageId} | Time: ${timestamp}`);
  } catch (err) {
    if (err.response?.status === 401) {
      tokenCache = { value: null, expiresAt: null };
    }
    log("ERROR", "Failed to save Flow data to DE:", err.response?.data || err.message);
  }
}

app.get("/webhook", (req, res) => {
  const mode = req.query["hub.mode"];
  const token = req.query["hub.verify_token"];
  const challenge = req.query["hub.challenge"];

  if (mode === "subscribe" && token === VERIFY_TOKEN) {
    log("INFO", "Webhook verified");
    return res.status(200).send(challenge);
  }

  return res.sendStatus(403);
});

app.post("/webhook", async (req, res) => {
  const body = req.body;

  if (body.object !== "whatsapp_business_account") {
    return res.status(404).json({ status: "error", message: "Not a whatsapp_business_account event" });
  }

  const collectedFlowData = [];

  try {
    const entries = body.entry || [];

    for (const entry of entries) {
      const changes = entry.changes || [];

      for (const change of changes) {
        const messages = change.value?.messages || [];
        const contacts = change.value?.contacts || [];

        for (const message of messages) {
          const from = message.from;
          const messageId = message.id;
          const timestamp = new Date(message.timestamp * 1000).toISOString();

          

          const contact = contacts.find(c => c.wa_id === from);
          const profileName = contact?.profile?.name || "";

          if (
            message.type === "interactive" &&
            message.interactive?.type === "nfm_reply"
          ) {
            const flowResponse = message.interactive.nfm_reply?.response_json;

            if (!flowResponse) {
              log("DEBUG", "No response_json in flow reply, skipping");
              continue;
            }

            let flowData;
            try {
              flowData = typeof flowResponse === "string"
                ? JSON.parse(flowResponse)
                : flowResponse;
            } catch (parseErr) {
              log("ERROR", "Failed to parse flow response_json");
              continue;
            }

            const amount = flowData?.amount ?? null;
            const tenure = flowData?.tenure ?? null;

            if (amount === null && tenure === null) {
              log("DEBUG", "Flow response missing amount and tenure, skipping");
              continue;
            }

            // Log parsed flow data
            log("INFO", `Flow Data Received → Phone: ${from} | Name: ${profileName} | Amount: ${amount} | Tenure: ${tenure} | MsgID: ${messageId} | Time: ${timestamp}`);

            // Collect for response
            collectedFlowData.push({
              phone: from,
              profileName,
              amount,
              tenure,
              messageId,
              timestamp
            });

            // Save to SFMC (non-blocking for response)
            saveFlowDataToDE({ from, profileName, amount, tenure, messageId, timestamp });
          }
        }
      }
    }

  } catch (err) {
    log("ERROR", "Webhook processing failed:", err.message);
    return res.status(500).json({ status: "error", message: err.message });
  }

  // Return flow data in response
  return res.status(200).json({
    status: "ok",
    received: collectedFlowData.length,
    flowData: collectedFlowData
  });
});

const PORT = process.env.PORT || 3000;
app.listen(PORT, () => {
  log("INFO", `Server running on port ${PORT}`);
});











/*
const express = require("express");
const axios = require("axios");

const app = express();
app.use(express.json({ limit: "1mb" }));

const VERIFY_TOKEN = process.env.VERIFY_TOKEN || "myWebhookToken123";

const SFMC = {
  clientId: process.env.SFMC_CLIENT_ID,
  clientSecret: process.env.SFMC_CLIENT_SECRET,
  subdomain: process.env.SFMC_SUBDOMAIN,
  deExternalKey: process.env.SFMC_DE_KEY || "WebhookDE"
};

const isDev = process.env.NODE_ENV !== "production";

function log(level, message, meta = "") {
  if (!isDev && level === "DEBUG") return;
  const timestamp = new Date().toISOString();
  console.log(`[${timestamp}] [${level}] ${message} ${meta}`);
}

let tokenCache = { value: null, expiresAt: null };

async function getSFMCToken() {
  const now = Date.now();

  if (tokenCache.value && now < tokenCache.expiresAt - 60000) {
    return tokenCache.value;
  }

  try {
    const response = await axios.post(
      `https://${SFMC.subdomain}.auth.marketingcloudapis.com/v2/token`,
      {
        grant_type: "client_credentials",
        client_id: SFMC.clientId,
        client_secret: SFMC.clientSecret
      }
    );

    tokenCache.value = response.data.access_token;
    tokenCache.expiresAt = now + response.data.expires_in * 1000;

    log("INFO", "New SFMC token fetched");
    return tokenCache.value;
  } catch (err) {
    log("ERROR", "SFMC token fetch failed");
    throw err;
  }
}

// ── CHANGE 1: Added profileName to signature ──
async function saveToDE({ from, profileName, eventType, content, messageId, timestamp }) {
  try {
    const token = await getSFMCToken();

    const payload = [{
      keys: { MessageId: messageId },
      values: {
        PhoneNumber: from,
        ProfileName: profileName,  // ── CHANGE 2: Added ProfileName to payload ──
        EventType: eventType,
        Content: content,
        MessageId: messageId,
        Timestamp: timestamp
      }
    }];

    await axios.post(
      `https://${SFMC.subdomain}.rest.marketingcloudapis.com/hub/v1/dataevents/key:${SFMC.deExternalKey}/rowset`,
      payload,
      {
        headers: {
          Authorization: `Bearer ${token}`,
          "Content-Type": "application/json"
        }
      }
    );

    log("INFO", `Saved to DE | ${eventType} | ${from} | ${profileName}`);
  } catch (err) {
    if (err.response?.status === 401) {
      tokenCache = { value: null, expiresAt: null };
    }
    log("ERROR", "Failed to save to DE");
  }
}

app.get("/webhook", (req, res) => {
  const mode = req.query["hub.mode"];
  const token = req.query["hub.verify_token"];
  const challenge = req.query["hub.challenge"];

  if (mode === "subscribe" && token === VERIFY_TOKEN) {
    log("INFO", "Webhook verified");
    return res.status(200).send(challenge);
  }

  return res.sendStatus(403);
});

app.post("/webhook", async (req, res) => {
  const body = req.body;

  if (body.object !== "whatsapp_business_account") {
    return res.sendStatus(404);
  }

  res.sendStatus(200);

  try {
    const entries = body.entry || [];

    for (const entry of entries) {
      const changes = entry.changes || [];

      for (const change of changes) {
        const messages = change.value?.messages || [];
        const contacts = change.value?.contacts || [];  // ── CHANGE 3: Extract contacts ──

        for (const message of messages) {
          const from = message.from;
          const messageId = message.id;
          const timestamp = new Date(message.timestamp * 1000).toISOString();

          // ── CHANGE 4: Extract profileName from contacts ──
          const contact = contacts.find(c => c.wa_id === from);
          const profileName = contact?.profile?.name || "";

          if (message.type === "text") {
            await saveToDE({
              from,
              profileName,        // ── CHANGE 5 ──
              eventType: "text",
              content: message.text?.body || "",
              messageId,
              timestamp
            });
          }

          else if (message.type === "reaction") {
            await saveToDE({
              from,
              profileName,        // ── CHANGE 5 ──
              eventType: "reaction",
              content: message.reaction?.emoji || "",
              messageId: message.reaction?.message_id || messageId,
              timestamp
            });
          }
        }
      }
    }

  } catch (err) {
    log("ERROR", "Webhook processing failed");
  }
});

const PORT = process.env.PORT || 3000;
app.listen(PORT, () => {
  log("INFO", `Server running on port ${PORT}`);
});

*/








/*
const express = require("express");
const axios = require("axios");

const app = express();
app.use(express.json({ limit: "1mb" })); // Prevent huge payloads

// ─────────────────────────────────────────
// CONFIG
// ─────────────────────────────────────────
const VERIFY_TOKEN = process.env.VERIFY_TOKEN || "myWebhookToken123";

const SFMC = {
  clientId: process.env.SFMC_CLIENT_ID,
  clientSecret: process.env.SFMC_CLIENT_SECRET,
  subdomain: process.env.SFMC_SUBDOMAIN,
  deExternalKey: process.env.SFMC_DE_KEY || "WebhookDE"
};

const isDev = process.env.NODE_ENV !== "production";

// ─────────────────────────────────────────
// LIGHTWEIGHT LOGGER (Production Safe)
// ─────────────────────────────────────────
function log(level, message, meta = "") {
  if (!isDev && level === "DEBUG") return;

  const timestamp = new Date().toISOString();
  console.log(`[${timestamp}] [${level}] ${message} ${meta}`);
}

// ─────────────────────────────────────────
// TOKEN CACHE
// ─────────────────────────────────────────
let tokenCache = { value: null, expiresAt: null };

async function getSFMCToken() {
  const now = Date.now();

  if (tokenCache.value && now < tokenCache.expiresAt - 60000) {
    return tokenCache.value;
  }


  try {
    const response = await axios.post(
      `https://${SFMC.subdomain}.auth.marketingcloudapis.com/v2/token`,
      {
        grant_type: "client_credentials",
        client_id: SFMC.clientId,
        client_secret: SFMC.clientSecret
      }
    );

    tokenCache.value = response.data.access_token;
    tokenCache.expiresAt = now + response.data.expires_in * 1000;

    log("INFO", "New SFMC token fetched");
    return tokenCache.value;
  } catch (err) {
    log("ERROR", "SFMC token fetch failed");
    throw err;
  }
}

// ─────────────────────────────────────────
// SAVE TO DATA EXTENSION
// ─────────────────────────────────────────
async function saveToDE({ from, eventType, content, messageId, timestamp }) {
  try {
    const token = await getSFMCToken();

    const payload = [{
      keys: { MessageId: messageId },
      values: {
        PhoneNumber: from,
        EventType: eventType,
        Content: content,
        MessageId: messageId,
        Timestamp: timestamp
      }
    }];

    await axios.post(
      `https://${SFMC.subdomain}.rest.marketingcloudapis.com/hub/v1/dataevents/key:${SFMC.deExternalKey}/rowset`,
      payload,
      {
        headers: {
          Authorization: `Bearer ${token}`,
          "Content-Type": "application/json"
        }
      }
    );

    log("INFO", `Saved to DE | ${eventType} | ${from}`);
  } catch (err) {
    if (err.response?.status === 401) {
      tokenCache = { value: null, expiresAt: null };
    }
    log("ERROR", "Failed to save to DE");
  }
}

// ─────────────────────────────────────────
// WEBHOOK VERIFICATION
// ─────────────────────────────────────────
app.get("/webhook", (req, res) => {
  const mode = req.query["hub.mode"];
  const token = req.query["hub.verify_token"];
  const challenge = req.query["hub.challenge"];

  if (mode === "subscribe" && token === VERIFY_TOKEN) {
    log("INFO", "Webhook verified");
    return res.status(200).send(challenge);
  }

  return res.sendStatus(403);
});

// ─────────────────────────────────────────
// WEBHOOK RECEIVER
// ─────────────────────────────────────────
app.post("/webhook", async (req, res) => {
  const body = req.body;

  if (body.object !== "whatsapp_business_account") {
    return res.sendStatus(404);
  }

  // Respond immediately to avoid retries
  res.sendStatus(200);

  try {
    const entries = body.entry || [];

    for (const entry of entries) {
      const changes = entry.changes || [];

      for (const change of changes) {
        const messages = change.value?.messages || [];

        for (const message of messages) {
          const from = message.from;
          const messageId = message.id;
          const timestamp = new Date(message.timestamp * 1000).toISOString();

          if (message.type === "text") {
            await saveToDE({
              from,
              eventType: "text",
              content: message.text?.body || "",
              messageId,
              timestamp
            });
          }

          else if (message.type === "reaction") {
            await saveToDE({
              from,
              eventType: "reaction",
              content: message.reaction?.emoji || "",
              messageId: message.reaction?.message_id || messageId,
              timestamp
            });
          }
        }
      }
    }

  } catch (err) {
    log("ERROR", "Webhook processing failed");
  }
});

// ─────────────────────────────────────────
// START SERVER
// ─────────────────────────────────────────
const PORT = process.env.PORT || 3000;
app.listen(PORT, () => {
  log("INFO", `Server running on port ${PORT}`);
});

*/

