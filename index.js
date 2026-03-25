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
  deExternalKey: process.env.SFMC_DE_KEY || "FD_Flow",
  fdAmountDeKey: process.env.SFMC_FD_AMOUNT_DE_KEY || "Internal_bhav",
  fdConfirmationDeKey: "Internal_bhav_FDconfirmation"   // ← NEW DE
};

const isDev = process.env.NODE_ENV !== "production";

function log(level, message, meta) {
  if (!isDev && level === "DEBUG") return;
  const timestamp = new Date().toISOString();
  let metaStr = "";
  if (meta !== undefined && meta !== null && meta !== "") {
    metaStr = typeof meta === "object" ? JSON.stringify(meta, null, 2) : String(meta);
  }
  console.log(`[${timestamp}] [${level}] ${message}${metaStr ? " " + metaStr : ""}`);
}

// ─── ROI map (tenure → annual rate) ──────────────────────────────────────────
const TENURE_ROI_MAP = {
  months12: 0.06,   // 6%
  months24: 0.065,  // 6.5%
  months36: 0.07,   // 7%
  months48: 0.075   // 7.5%
};

/**
 * Convert a decimal ROI to a whole-number percentage string.
 *   0.06  → "6"
 *   0.065 → "6.5"
 *   0.07  → "7"
 *   0.075 → "7.5"
 */
function roiToPercent(roi) {
  const pct = parseFloat((roi * 100).toPrecision(10));
  return String(pct);
}

/**
 * Calculate maturity amount using simple interest:
 *   Maturity = Principal + (Principal × ROI × Years)
 */
function calculateMaturity(principal, tenure) {
  const roi = TENURE_ROI_MAP[tenure];
  if (!roi || !principal) return null;

  const years = parseInt(tenure.replace("months", ""), 10) / 12;
  const maturityAmount = Math.round(principal + principal * roi * years);

  return {
    roi,                           // decimal kept for internal math
    roiDisplay: roiToPercent(roi), // "6", "6.5", "7", "7.5" → stored in DEs
    years,
    maturityAmount
  };
}

// ─── Token cache ──────────────────────────────────────────────────────────────
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

/**
 * Fetch FD record from Internal_bhav DE by Subscriber_Key (phone number).
 */
async function fetchFDRecordFromDE(phone) {
  try {
    const token = await getSFMCToken();

    const url = `https://${SFMC.subdomain}.rest.marketingcloudapis.com/data/v1/customobjectdata/key/${SFMC.fdAmountDeKey}/rowset?$filter=Subscriber_Key%20eq%20'${phone}'`;

    log("INFO", `Fetching FD record from Internal_bhav for Subscriber_Key: ${phone}`);

    const response = await axios.get(url, {
      headers: { Authorization: `Bearer ${token}` }
    });

    const items = response.data?.items || [];

    if (items.length === 0) {
      log("WARN", `No record found in ${SFMC.fdAmountDeKey} for Subscriber_Key: ${phone}`);
      return null;
    }

    const merged = {
      ...(items[0].keys   || {}),
      ...(items[0].values || {})
    };

    log("INFO", `FD record fetched for ${phone}:`, merged);
    return merged;

  } catch (err) {
    if (err.response?.status === 401) tokenCache = { value: null, expiresAt: null };
    log("ERROR", `Failed to fetch FD record for ${phone}:`, {
      status: err.response?.status,
      data: err.response?.data,
      message: err.message
    });
    return null;
  }
}

/**
 * Upsert Tenure, ROI and Maturity_Amount back to Internal_bhav DE.
 * ROI is stored as a whole-number percentage string ("6", "6.5", "7", "7.5").
 */
async function updateInternalBhavDE({ phone, tenure, roiDisplay, maturityAmount }) {
  try {
    const token = await getSFMCToken();

    const payload = [
      {
        keys: {
          Subscriber_Key: phone,
          Mobile: phone
        },
        values: {
          Tenure:          tenure,
          ROI:             roiDisplay,          // "6", "6.5", "7", "7.5"
          Maturity_Amount: String(maturityAmount)
        }
      }
    ];

    await axios.post(
      `https://${SFMC.subdomain}.rest.marketingcloudapis.com/hub/v1/dataevents/key:${SFMC.fdAmountDeKey}/rowset`,
      payload,
      { headers: { Authorization: `Bearer ${token}`, "Content-Type": "application/json" } }
    );

    log("INFO", `Internal_bhav updated | Phone: ${phone} | Tenure: ${tenure} | ROI: ${roiDisplay}% | Maturity_Amount: ${maturityAmount}`);
  } catch (err) {
    if (err.response?.status === 401) tokenCache = { value: null, expiresAt: null };
    log("ERROR", "Failed to update Internal_bhav DE:", {
      status: err.response?.status,
      data: err.response?.data,
      message: err.message
    });
  }
}

/**
 * Update calculated fields in Internal_bhav_FDconfirmation DE.
 *
 * Subscriber_Key, Mobile, and Name already exist in the DE — this call
 * only updates FD_Amount, Tenure, ROI, and Maturity_Amount against the
 * existing record matched by Subscriber_Key + Mobile (both = phone).
 */
async function upsertFDConfirmationDE({
  phone, fdAmount, tenure, roiDisplay, maturityAmount
}) {
  try {
    const token = await getSFMCToken();

    const payload = [
      {
        keys: {
          Subscriber_Key: phone,   // matches existing record
          Mobile:         phone    // matches existing record
        },
        values: {
          // Name is intentionally omitted — already exists, do not overwrite
          FD_Amount:       fdAmount       != null ? String(fdAmount)       : "",
          Tenure:          tenure         || "",
          ROI:             roiDisplay     || "",   // "6", "6.5", "7", "7.5"
          Maturity_Amount: maturityAmount != null ? String(maturityAmount) : ""
        }
      }
    ];

    await axios.post(
      `https://${SFMC.subdomain}.rest.marketingcloudapis.com/hub/v1/dataevents/key:${SFMC.fdConfirmationDeKey}/rowset`,
      payload,
      { headers: { Authorization: `Bearer ${token}`, "Content-Type": "application/json" } }
    );

    log("INFO", `Internal_bhav_FDconfirmation updated | Phone: ${phone} | FD_Amount: ${fdAmount} | Tenure: ${tenure} | ROI: ${roiDisplay}% | Maturity: ${maturityAmount}`);
  } catch (err) {
    if (err.response?.status === 401) tokenCache = { value: null, expiresAt: null };
    log("ERROR", "Failed to update Internal_bhav_FDconfirmation DE:", {
      status: err.response?.status,
      data: err.response?.data,
      message: err.message
    });
  }
}

/**
 * Save full flow result to FD_Flow DE.
 */
async function saveFlowDataToDE({
  from, profileName, amountType, partialAmount,
  finalAmount, tenure, roiDisplay, maturityAmount, messageId, timestamp
}) {
  try {
    const token = await getSFMCToken();

    const payload = [
      {
        keys: { MessageId: messageId },
        values: {
          PhoneNumber:     from,
          ProfileName:     profileName,
          AmountType:      amountType,
          PartialAmount:   partialAmount,
          FinalAmount:     finalAmount,
          Tenure:          tenure,
          ROI:             roiDisplay != null ? roiDisplay : null,   // "6", "6.5" …
          Maturity_Amount: maturityAmount != null ? String(maturityAmount) : null,
          MessageId:       messageId,
          Timestamp:       timestamp
        }
      }
    ];

    await axios.post(
      `https://${SFMC.subdomain}.rest.marketingcloudapis.com/hub/v1/dataevents/key:${SFMC.deExternalKey}/rowset`,
      payload,
      { headers: { Authorization: `Bearer ${token}`, "Content-Type": "application/json" } }
    );

    log("INFO", `Saved to FD_Flow DE | Phone: ${from} | AmountType: ${amountType} | FinalAmount: ${finalAmount} | Tenure: ${tenure} | ROI: ${roiDisplay}% | Maturity: ${maturityAmount} | MsgID: ${messageId}`);
  } catch (err) {
    if (err.response?.status === 401) tokenCache = { value: null, expiresAt: null };
    log("ERROR", "Failed to save to FD_Flow DE:", {
      status: err.response?.status,
      data: err.response?.data,
      message: err.message
    });
  }
}

// ─── Webhook verify ───────────────────────────────────────────────────────────
app.get("/webhook", (req, res) => {
  const mode      = req.query["hub.mode"];
  const token     = req.query["hub.verify_token"];
  const challenge = req.query["hub.challenge"];

  if (mode === "subscribe" && token === VERIFY_TOKEN) {
    log("INFO", "Webhook verified");
    return res.status(200).send(challenge);
  }
  return res.sendStatus(403);
});

// ─── Webhook POST ─────────────────────────────────────────────────────────────
app.post("/webhook", async (req, res) => {
  const body = req.body;

  if (body.object !== "whatsapp_business_account") {
    return res.status(404).json({ status: "error", message: "Not a whatsapp_business_account event" });
  }

  const collectedFlowData = [];

  try {
    for (const entry of body.entry || []) {
      for (const change of entry.changes || []) {
        const messages = change.value?.messages || [];
        const contacts = change.value?.contacts || [];

        for (const message of messages) {
          const from        = message.from;
          const messageId   = message.id;
          const timestamp   = new Date(message.timestamp * 1000).toISOString();
          const contact     = contacts.find((c) => c.wa_id === from);
          const profileName = contact?.profile?.name || "";

          if (message.type === "interactive" && message.interactive?.type === "nfm_reply") {
            const flowResponse = message.interactive.nfm_reply?.response_json;

            if (!flowResponse) {
              log("DEBUG", "No response_json in flow reply, skipping");
              continue;
            }

            let flowData;
            try {
              flowData = typeof flowResponse === "string" ? JSON.parse(flowResponse) : flowResponse;
            } catch {
              log("ERROR", "Failed to parse flow response_json");
              continue;
            }

            const amountType    = flowData?.amount_type    ?? null;
            const partialAmount = flowData?.partial_amount ?? null;
            const tenure        = flowData?.tenure         ?? null;

            if (amountType === null && tenure === null) {
              log("DEBUG", "Flow response missing amount_type and tenure, skipping");
              continue;
            }

            // ── Step 1: Resolve finalAmount ───────────────────────────────────
            let finalAmount = null;
            let fdName      = profileName;   // fallback to WhatsApp profile name

            if (amountType === "full") {
              log("INFO", `amount_type=full — fetching FD record from Internal_bhav for: ${from}`);
              const fdRecord = await fetchFDRecordFromDE(from);
              finalAmount = fdRecord?.fd_amount ?? null;
              if (fdRecord?.name) fdName = fdRecord.name;   // prefer DE name if present
            } else {
              finalAmount = partialAmount;
            }

            // ── Step 2: Calculate ROI + Maturity Amount ───────────────────────
            let roi            = null;
            let roiDisplay     = null;   // "6", "6.5", "7", "7.5"
            let maturityAmount = null;

            if (tenure && finalAmount) {
              const calc = calculateMaturity(parseFloat(finalAmount), tenure);
              if (calc) {
                roi            = calc.roi;
                roiDisplay     = calc.roiDisplay;
                maturityAmount = calc.maturityAmount;
                log("INFO", `Maturity calculated | Tenure: ${tenure} | Principal: ${finalAmount} | ROI: ${roiDisplay}% | Maturity: ${maturityAmount}`);
              } else {
                log("WARN", `Could not calculate maturity — unknown tenure: ${tenure}`);
              }
            }

            // ── Step 3: Log full flow result ──────────────────────────────────
            log("INFO", `Flow Data → Phone: ${from} | Name: ${profileName} | AmountType: ${amountType} | PartialAmount: ${partialAmount} | FinalAmount: ${finalAmount} | Tenure: ${tenure} | ROI: ${roiDisplay}% | Maturity: ${maturityAmount} | MsgID: ${messageId} | Time: ${timestamp}`);

            const flowEntry = {
              phone:         from,
              profileName,
              amountType,
              partialAmount,
              finalAmount,
              tenure,
              roiDisplay,
              maturityAmount,
              messageId,
              timestamp
            };

            collectedFlowData.push(flowEntry);

            // ── Step 4: Save to FD_Flow DE (non-blocking) ─────────────────────
            saveFlowDataToDE({ from, profileName, amountType, partialAmount, finalAmount, tenure, roiDisplay, maturityAmount, messageId, timestamp });

            // ── Step 5: Upsert Tenure + ROI + Maturity back to Internal_bhav ──
            if (tenure && roiDisplay !== null && maturityAmount !== null) {
              updateInternalBhavDE({ phone: from, tenure, roiDisplay, maturityAmount });
            }

            // ── Step 6: Update FD_Amount, Tenure, ROI, Maturity_Amount in Internal_bhav_FDconfirmation ─
            // Subscriber_Key, Mobile, Name already exist — only the 4 calculated fields are written
            if (roiDisplay !== null && maturityAmount !== null) {
              upsertFDConfirmationDE({
                phone:         from,
                fdAmount:      finalAmount,
                tenure,
                roiDisplay,
                maturityAmount
              });
            }
          }
        }
      }
    }
  } catch (err) {
    log("ERROR", "Webhook processing failed:", err.message);
    return res.status(500).json({ status: "error", message: err.message });
  }

  return res.status(200).json({
    status: "ok",
    received: collectedFlowData.length,
    flowData: collectedFlowData
  });
});

const PORT = process.env.PORT || 3000;
app.listen(PORT, () => log("INFO", `Server running on port ${PORT}`));












// require("dotenv").config();
// const express = require("express");
// const axios = require("axios");

// const app = express();
// app.use(express.json({ limit: "1mb" }));

// const VERIFY_TOKEN = process.env.VERIFY_TOKEN || "myWebhookToken123";

// const SFMC = {
//   clientId: process.env.SFMC_CLIENT_ID,
//   clientSecret: process.env.SFMC_CLIENT_SECRET,
//   subdomain: process.env.SFMC_SUBDOMAIN,
//   deExternalKey: process.env.SFMC_DE_KEY || "FD_Flow",
//   fdAmountDeKey: process.env.SFMC_FD_AMOUNT_DE_KEY || "Internal_bhav",
//   fdConfirmationDeKey: "Internal_bhav_FDconfirmation"   // ← NEW DE
// };

// const isDev = process.env.NODE_ENV !== "production";

// function log(level, message, meta) {
//   if (!isDev && level === "DEBUG") return;
//   const timestamp = new Date().toISOString();
//   let metaStr = "";
//   if (meta !== undefined && meta !== null && meta !== "") {
//     metaStr = typeof meta === "object" ? JSON.stringify(meta, null, 2) : String(meta);
//   }
//   console.log(`[${timestamp}] [${level}] ${message}${metaStr ? " " + metaStr : ""}`);
// }

// // ─── ROI map (tenure → annual rate) ──────────────────────────────────────────
// const TENURE_ROI_MAP = {
//   months12: 0.06,   // 6%
//   months24: 0.065,  // 6.5%
//   months36: 0.07,   // 7%
//   months48: 0.075   // 7.5%
// };

// /**
//  * Convert a decimal ROI to a whole-number percentage string.
//  *   0.06  → "6"
//  *   0.065 → "6.5"
//  *   0.07  → "7"
//  *   0.075 → "7.5"
//  */
// function roiToPercent(roi) {
//   const pct = parseFloat((roi * 100).toPrecision(10));
//   return String(pct);
// }

// /**
//  * Calculate maturity amount using simple interest:
//  *   Maturity = Principal + (Principal × ROI × Years)
//  */
// function calculateMaturity(principal, tenure) {
//   const roi = TENURE_ROI_MAP[tenure];
//   if (!roi || !principal) return null;

//   const years = parseInt(tenure.replace("months", ""), 10) / 12;
//   const maturityAmount = Math.round(principal + principal * roi * years);

//   return {
//     roi,                           // decimal kept for internal math
//     roiDisplay: roiToPercent(roi), // "6", "6.5", "7", "7.5" → stored in DEs
//     years,
//     maturityAmount
//   };
// }

// // ─── Token cache ──────────────────────────────────────────────────────────────
// let tokenCache = { value: null, expiresAt: null };

// async function getSFMCToken() {
//   const now = Date.now();
//   if (tokenCache.value && now < tokenCache.expiresAt - 60000) {
//     return tokenCache.value;
//   }
//   try {
//     const response = await axios.post(
//       `https://${SFMC.subdomain}.auth.marketingcloudapis.com/v2/token`,
//       {
//         grant_type: "client_credentials",
//         client_id: SFMC.clientId,
//         client_secret: SFMC.clientSecret
//       }
//     );
//     tokenCache.value = response.data.access_token;
//     tokenCache.expiresAt = now + response.data.expires_in * 1000;
//     log("INFO", "New SFMC token fetched");
//     return tokenCache.value;
//   } catch (err) {
//     log("ERROR", "SFMC token fetch failed:", err.response?.data || err.message);
//     throw err;
//   }
// }

// /**
//  * Fetch FD record from Internal_bhav DE by Subscriber_Key (phone number).
//  */
// async function fetchFDRecordFromDE(phone) {
//   try {
//     const token = await getSFMCToken();

//     const url = `https://${SFMC.subdomain}.rest.marketingcloudapis.com/data/v1/customobjectdata/key/${SFMC.fdAmountDeKey}/rowset?$filter=Subscriber_Key%20eq%20'${phone}'`;

//     log("INFO", `Fetching FD record from Internal_bhav for Subscriber_Key: ${phone}`);

//     const response = await axios.get(url, {
//       headers: { Authorization: `Bearer ${token}` }
//     });

//     const items = response.data?.items || [];

//     if (items.length === 0) {
//       log("WARN", `No record found in ${SFMC.fdAmountDeKey} for Subscriber_Key: ${phone}`);
//       return null;
//     }

//     const merged = {
//       ...(items[0].keys   || {}),
//       ...(items[0].values || {})
//     };

//     log("INFO", `FD record fetched for ${phone}:`, merged);
//     return merged;

//   } catch (err) {
//     if (err.response?.status === 401) tokenCache = { value: null, expiresAt: null };
//     log("ERROR", `Failed to fetch FD record for ${phone}:`, {
//       status: err.response?.status,
//       data: err.response?.data,
//       message: err.message
//     });
//     return null;
//   }
// }

// /**
//  * Upsert Tenure, ROI and Maturity_Amount back to Internal_bhav DE.
//  * ROI is stored as a whole-number percentage string ("6", "6.5", "7", "7.5").
//  */
// async function updateInternalBhavDE({ phone, tenure, roiDisplay, maturityAmount }) {
//   try {
//     const token = await getSFMCToken();

//     const payload = [
//       {
//         keys: {
//           Subscriber_Key: phone,
//           Mobile: phone
//         },
//         values: {
//           Tenure:          tenure,
//           ROI:             roiDisplay,          // "6", "6.5", "7", "7.5"
//           Maturity_Amount: String(maturityAmount)
//         }
//       }
//     ];

//     await axios.post(
//       `https://${SFMC.subdomain}.rest.marketingcloudapis.com/hub/v1/dataevents/key:${SFMC.fdAmountDeKey}/rowset`,
//       payload,
//       { headers: { Authorization: `Bearer ${token}`, "Content-Type": "application/json" } }
//     );

//     log("INFO", `Internal_bhav updated | Phone: ${phone} | Tenure: ${tenure} | ROI: ${roiDisplay}% | Maturity_Amount: ${maturityAmount}`);
//   } catch (err) {
//     if (err.response?.status === 401) tokenCache = { value: null, expiresAt: null };
//     log("ERROR", "Failed to update Internal_bhav DE:", {
//       status: err.response?.status,
//       data: err.response?.data,
//       message: err.message
//     });
//   }
// }

// /**
//  * Upsert calculated data into Internal_bhav_FDconfirmation DE.
//  *
//  * Fields (matching DE schema):
//  *   Subscriber_Key (PK), Mobile (PK), Name, FD_Amount, Tenure, ROI, Maturity_Amount
//  */
// async function upsertFDConfirmationDE({
//   phone, name, fdAmount, tenure, roiDisplay, maturityAmount
// }) {
//   try {
//     const token = await getSFMCToken();

//     const payload = [
//       {
//         keys: {
//           Subscriber_Key: phone,
//           Mobile:         phone
//         },
//         values: {
//           Name:            name       || "",
//           FD_Amount:       fdAmount   != null ? String(fdAmount)       : "",
//           Tenure:          tenure     || "",
//           ROI:             roiDisplay || "",          // "6", "6.5", "7", "7.5"
//           Maturity_Amount: maturityAmount != null ? String(maturityAmount) : ""
//         }
//       }
//     ];

//     await axios.post(
//       `https://${SFMC.subdomain}.rest.marketingcloudapis.com/hub/v1/dataevents/key:${SFMC.fdConfirmationDeKey}/rowset`,
//       payload,
//       { headers: { Authorization: `Bearer ${token}`, "Content-Type": "application/json" } }
//     );

//     log("INFO", `Internal_bhav_FDconfirmation upserted | Phone: ${phone} | Name: ${name} | FD_Amount: ${fdAmount} | Tenure: ${tenure} | ROI: ${roiDisplay}% | Maturity: ${maturityAmount}`);
//   } catch (err) {
//     if (err.response?.status === 401) tokenCache = { value: null, expiresAt: null };
//     log("ERROR", "Failed to upsert Internal_bhav_FDconfirmation DE:", {
//       status: err.response?.status,
//       data: err.response?.data,
//       message: err.message
//     });
//   }
// }

// /**
//  * Save full flow result to FD_Flow DE.
//  */
// async function saveFlowDataToDE({
//   from, profileName, amountType, partialAmount,
//   finalAmount, tenure, roiDisplay, maturityAmount, messageId, timestamp
// }) {
//   try {
//     const token = await getSFMCToken();

//     const payload = [
//       {
//         keys: { MessageId: messageId },
//         values: {
//           PhoneNumber:     from,
//           ProfileName:     profileName,
//           AmountType:      amountType,
//           PartialAmount:   partialAmount,
//           FinalAmount:     finalAmount,
//           Tenure:          tenure,
//           ROI:             roiDisplay != null ? roiDisplay : null,   // "6", "6.5" …
//           Maturity_Amount: maturityAmount != null ? String(maturityAmount) : null,
//           MessageId:       messageId,
//           Timestamp:       timestamp
//         }
//       }
//     ];

//     await axios.post(
//       `https://${SFMC.subdomain}.rest.marketingcloudapis.com/hub/v1/dataevents/key:${SFMC.deExternalKey}/rowset`,
//       payload,
//       { headers: { Authorization: `Bearer ${token}`, "Content-Type": "application/json" } }
//     );

//     log("INFO", `Saved to FD_Flow DE | Phone: ${from} | AmountType: ${amountType} | FinalAmount: ${finalAmount} | Tenure: ${tenure} | ROI: ${roiDisplay}% | Maturity: ${maturityAmount} | MsgID: ${messageId}`);
//   } catch (err) {
//     if (err.response?.status === 401) tokenCache = { value: null, expiresAt: null };
//     log("ERROR", "Failed to save to FD_Flow DE:", {
//       status: err.response?.status,
//       data: err.response?.data,
//       message: err.message
//     });
//   }
// }

// // ─── Webhook verify ───────────────────────────────────────────────────────────
// app.get("/webhook", (req, res) => {
//   const mode      = req.query["hub.mode"];
//   const token     = req.query["hub.verify_token"];
//   const challenge = req.query["hub.challenge"];

//   if (mode === "subscribe" && token === VERIFY_TOKEN) {
//     log("INFO", "Webhook verified");
//     return res.status(200).send(challenge);
//   }
//   return res.sendStatus(403);
// });

// // ─── Webhook POST ─────────────────────────────────────────────────────────────
// app.post("/webhook", async (req, res) => {
//   const body = req.body;

//   if (body.object !== "whatsapp_business_account") {
//     return res.status(404).json({ status: "error", message: "Not a whatsapp_business_account event" });
//   }

//   const collectedFlowData = [];

//   try {
//     for (const entry of body.entry || []) {
//       for (const change of entry.changes || []) {
//         const messages = change.value?.messages || [];
//         const contacts = change.value?.contacts || [];

//         for (const message of messages) {
//           const from        = message.from;
//           const messageId   = message.id;
//           const timestamp   = new Date(message.timestamp * 1000).toISOString();
//           const contact     = contacts.find((c) => c.wa_id === from);
//           const profileName = contact?.profile?.name || "";

//           if (message.type === "interactive" && message.interactive?.type === "nfm_reply") {
//             const flowResponse = message.interactive.nfm_reply?.response_json;

//             if (!flowResponse) {
//               log("DEBUG", "No response_json in flow reply, skipping");
//               continue;
//             }

//             let flowData;
//             try {
//               flowData = typeof flowResponse === "string" ? JSON.parse(flowResponse) : flowResponse;
//             } catch {
//               log("ERROR", "Failed to parse flow response_json");
//               continue;
//             }

//             const amountType    = flowData?.amount_type    ?? null;
//             const partialAmount = flowData?.partial_amount ?? null;
//             const tenure        = flowData?.tenure         ?? null;

//             if (amountType === null && tenure === null) {
//               log("DEBUG", "Flow response missing amount_type and tenure, skipping");
//               continue;
//             }

//             // ── Step 1: Resolve finalAmount ───────────────────────────────────
//             let finalAmount = null;
//             let fdName      = profileName;   // fallback to WhatsApp profile name

//             if (amountType === "full") {
//               log("INFO", `amount_type=full — fetching FD record from Internal_bhav for: ${from}`);
//               const fdRecord = await fetchFDRecordFromDE(from);
//               finalAmount = fdRecord?.fd_amount ?? null;
//               if (fdRecord?.name) fdName = fdRecord.name;   // prefer DE name if present
//             } else {
//               finalAmount = partialAmount;
//             }

//             // ── Step 2: Calculate ROI + Maturity Amount ───────────────────────
//             let roi            = null;
//             let roiDisplay     = null;   // "6", "6.5", "7", "7.5"
//             let maturityAmount = null;

//             if (tenure && finalAmount) {
//               const calc = calculateMaturity(parseFloat(finalAmount), tenure);
//               if (calc) {
//                 roi            = calc.roi;
//                 roiDisplay     = calc.roiDisplay;
//                 maturityAmount = calc.maturityAmount;
//                 log("INFO", `Maturity calculated | Tenure: ${tenure} | Principal: ${finalAmount} | ROI: ${roiDisplay}% | Maturity: ${maturityAmount}`);
//               } else {
//                 log("WARN", `Could not calculate maturity — unknown tenure: ${tenure}`);
//               }
//             }

//             // ── Step 3: Log full flow result ──────────────────────────────────
//             log("INFO", `Flow Data → Phone: ${from} | Name: ${profileName} | AmountType: ${amountType} | PartialAmount: ${partialAmount} | FinalAmount: ${finalAmount} | Tenure: ${tenure} | ROI: ${roiDisplay}% | Maturity: ${maturityAmount} | MsgID: ${messageId} | Time: ${timestamp}`);

//             const flowEntry = {
//               phone:         from,
//               profileName,
//               amountType,
//               partialAmount,
//               finalAmount,
//               tenure,
//               roiDisplay,
//               maturityAmount,
//               messageId,
//               timestamp
//             };

//             collectedFlowData.push(flowEntry);

//             // ── Step 4: Save to FD_Flow DE (non-blocking) ─────────────────────
//             saveFlowDataToDE({ from, profileName, amountType, partialAmount, finalAmount, tenure, roiDisplay, maturityAmount, messageId, timestamp });

//             // ── Step 5: Upsert Tenure + ROI + Maturity back to Internal_bhav ──
//             if (tenure && roiDisplay !== null && maturityAmount !== null) {
//               updateInternalBhavDE({ phone: from, tenure, roiDisplay, maturityAmount });
//             }

//             // ── Step 6: Upsert calculated data to Internal_bhav_FDconfirmation ─
//             if (roiDisplay !== null && maturityAmount !== null) {
//               upsertFDConfirmationDE({
//                 phone:         from,
//                 name:          fdName,
//                 fdAmount:      finalAmount,
//                 tenure,
//                 roiDisplay,
//                 maturityAmount
//               });
//             }
//           }
//         }
//       }
//     }
//   } catch (err) {
//     log("ERROR", "Webhook processing failed:", err.message);
//     return res.status(500).json({ status: "error", message: err.message });
//   }

//   return res.status(200).json({
//     status: "ok",
//     received: collectedFlowData.length,
//     flowData: collectedFlowData
//   });
// });

// const PORT = process.env.PORT || 3000;
// app.listen(PORT, () => log("INFO", `Server running on port ${PORT}`));












// require("dotenv").config();
// const express = require("express");
// const axios = require("axios");

// const app = express();
// app.use(express.json({ limit: "1mb" }));

// const VERIFY_TOKEN = process.env.VERIFY_TOKEN || "myWebhookToken123";

// const SFMC = {
//   clientId: process.env.SFMC_CLIENT_ID,
//   clientSecret: process.env.SFMC_CLIENT_SECRET,
//   subdomain: process.env.SFMC_SUBDOMAIN,
//   deExternalKey: process.env.SFMC_DE_KEY || "FD_Flow",
//   fdAmountDeKey: process.env.SFMC_FD_AMOUNT_DE_KEY || "Internal_bhav"
// };

// const isDev = process.env.NODE_ENV !== "production";

// function log(level, message, meta) {
//   if (!isDev && level === "DEBUG") return;
//   const timestamp = new Date().toISOString();
//   let metaStr = "";
//   if (meta !== undefined && meta !== null && meta !== "") {
//     metaStr = typeof meta === "object" ? JSON.stringify(meta, null, 2) : String(meta);
//   }
//   console.log(`[${timestamp}] [${level}] ${message}${metaStr ? " " + metaStr : ""}`);
// }

// // ─── ROI map (tenure → annual rate) ──────────────────────────────────────────
// const TENURE_ROI_MAP = {
//   months12: 0.06,   // 6%
//   months24: 0.065,  // 6.5%
//   months36: 0.07,   // 7%
//   months48: 0.075   // 7.5%
// };

// /**
//  * Calculate maturity amount using simple interest:
//  *   Maturity = Principal + (Principal × ROI × Years)
//  *
//  * @param {number} principal  - FD amount
//  * @param {string} tenure     - e.g. "months48"
//  * @returns {{ roi: number, years: number, maturityAmount: number } | null}
//  */
// function calculateMaturity(principal, tenure) {
//   const roi = TENURE_ROI_MAP[tenure];
//   if (!roi || !principal) return null;

//   const years = parseInt(tenure.replace("months", ""), 10) / 12;
//   const maturityAmount = Math.round(principal + principal * roi * years);

//   return { roi, years, maturityAmount };
// }

// // ─── Token cache ──────────────────────────────────────────────────────────────
// let tokenCache = { value: null, expiresAt: null };

// async function getSFMCToken() {
//   const now = Date.now();
//   if (tokenCache.value && now < tokenCache.expiresAt - 60000) {
//     return tokenCache.value;
//   }
//   try {
//     const response = await axios.post(
//       `https://${SFMC.subdomain}.auth.marketingcloudapis.com/v2/token`,
//       {
//         grant_type: "client_credentials",
//         client_id: SFMC.clientId,
//         client_secret: SFMC.clientSecret
//       }
//     );
//     tokenCache.value = response.data.access_token;
//     tokenCache.expiresAt = now + response.data.expires_in * 1000;
//     log("INFO", "New SFMC token fetched");
//     return tokenCache.value;
//   } catch (err) {
//     log("ERROR", "SFMC token fetch failed:", err.response?.data || err.message);
//     throw err;
//   }
// }

// /**
//  * Fetch FD record from Internal_bhav DE by Subscriber_Key (phone number).
//  *
//  * SFMC response structure:
//  *   item.keys   → { subscriber_key, mobile }       ← primary key fields (lowercase)
//  *   item.values → { name, fd_amount, tenure, ... } ← all other fields  (lowercase)
//  *
//  * Returns full merged record or null if not found.
//  */
// async function fetchFDRecordFromDE(phone) {
//   try {
//     const token = await getSFMCToken();

//     const url = `https://${SFMC.subdomain}.rest.marketingcloudapis.com/data/v1/customobjectdata/key/${SFMC.fdAmountDeKey}/rowset?$filter=Subscriber_Key%20eq%20'${phone}'`;

//     log("INFO", `Fetching FD record from Internal_bhav for Subscriber_Key: ${phone}`);

//     const response = await axios.get(url, {
//       headers: { Authorization: `Bearer ${token}` }
//     });

//     const items = response.data?.items || [];

//     if (items.length === 0) {
//       log("WARN", `No record found in ${SFMC.fdAmountDeKey} for Subscriber_Key: ${phone}`);
//       return null;
//     }

//     // Merge keys + values — SFMC returns all field names in lowercase
//     const merged = {
//       ...(items[0].keys   || {}),
//       ...(items[0].values || {})
//     };

//     log("INFO", `FD record fetched for ${phone}:`, merged);
//     return merged;

//   } catch (err) {
//     if (err.response?.status === 401) tokenCache = { value: null, expiresAt: null };
//     log("ERROR", `Failed to fetch FD record for ${phone}:`, {
//       status: err.response?.status,
//       data: err.response?.data,
//       message: err.message
//     });
//     return null;
//   }
// }

// /**
//  * Upsert Tenure, ROI and Maturity_Amount back to Internal_bhav DE.
//  * Uses hub/v1/dataevents rowset which does INSERT or UPDATE (upsert) by primary key.
//  * Primary keys: Subscriber_Key + Mobile (both = phone)
//  */
// async function updateInternalBhavDE({ phone, tenure, roi, maturityAmount }) {
//   try {
//     const token = await getSFMCToken();

//     const payload = [
//       {
//         keys: {
//           Subscriber_Key: phone,
//           Mobile: phone
//         },
//         values: {
//           Tenure:          tenure,
//           ROI:             String(roi),
//           Maturity_Amount: String(maturityAmount)
//         }
//       }
//     ];

//     await axios.post(
//       `https://${SFMC.subdomain}.rest.marketingcloudapis.com/hub/v1/dataevents/key:${SFMC.fdAmountDeKey}/rowset`,
//       payload,
//       { headers: { Authorization: `Bearer ${token}`, "Content-Type": "application/json" } }
//     );

//     log("INFO", `Internal_bhav updated | Phone: ${phone} | Tenure: ${tenure} | ROI: ${roi} | Maturity_Amount: ${maturityAmount}`);
//   } catch (err) {
//     if (err.response?.status === 401) tokenCache = { value: null, expiresAt: null };
//     log("ERROR", "Failed to update Internal_bhav DE:", {
//       status: err.response?.status,
//       data: err.response?.data,
//       message: err.message
//     });
//   }
// }

// /**
//  * Save full flow result to FD_Flow DE.
//  */
// async function saveFlowDataToDE({
//   from, profileName, amountType, partialAmount,
//   finalAmount, tenure, roi, maturityAmount, messageId, timestamp
// }) {
//   try {
//     const token = await getSFMCToken();

//     const payload = [
//       {
//         keys: { MessageId: messageId },
//         values: {
//           PhoneNumber:     from,
//           ProfileName:     profileName,
//           AmountType:      amountType,
//           PartialAmount:   partialAmount,
//           FinalAmount:     finalAmount,
//           Tenure:          tenure,
//           ROI:             roi !== null ? String(roi) : null,
//           Maturity_Amount: maturityAmount !== null ? String(maturityAmount) : null,
//           MessageId:       messageId,
//           Timestamp:       timestamp
//         }
//       }
//     ];

//     await axios.post(
//       `https://${SFMC.subdomain}.rest.marketingcloudapis.com/hub/v1/dataevents/key:${SFMC.deExternalKey}/rowset`,
//       payload,
//       { headers: { Authorization: `Bearer ${token}`, "Content-Type": "application/json" } }
//     );

//     log("INFO", `Saved to FD_Flow DE | Phone: ${from} | AmountType: ${amountType} | FinalAmount: ${finalAmount} | Tenure: ${tenure} | ROI: ${roi} | Maturity: ${maturityAmount} | MsgID: ${messageId}`);
//   } catch (err) {
//     if (err.response?.status === 401) tokenCache = { value: null, expiresAt: null };
//     log("ERROR", "Failed to save to FD_Flow DE:", {
//       status: err.response?.status,
//       data: err.response?.data,
//       message: err.message
//     });
//   }
// }

// // ─── Webhook verify ───────────────────────────────────────────────────────────
// app.get("/webhook", (req, res) => {
//   const mode      = req.query["hub.mode"];
//   const token     = req.query["hub.verify_token"];
//   const challenge = req.query["hub.challenge"];

//   if (mode === "subscribe" && token === VERIFY_TOKEN) {
//     log("INFO", "Webhook verified");
//     return res.status(200).send(challenge);
//   }
//   return res.sendStatus(403);
// });

// // ─── Webhook POST ─────────────────────────────────────────────────────────────
// app.post("/webhook", async (req, res) => {
//   const body = req.body;

//   if (body.object !== "whatsapp_business_account") {
//     return res.status(404).json({ status: "error", message: "Not a whatsapp_business_account event" });
//   }

//   const collectedFlowData = [];

//   try {
//     for (const entry of body.entry || []) {
//       for (const change of entry.changes || []) {
//         const messages = change.value?.messages || [];
//         const contacts = change.value?.contacts || [];

//         for (const message of messages) {
//           const from        = message.from;
//           const messageId   = message.id;
//           const timestamp   = new Date(message.timestamp * 1000).toISOString();
//           const contact     = contacts.find((c) => c.wa_id === from);
//           const profileName = contact?.profile?.name || "";

//           if (message.type === "interactive" && message.interactive?.type === "nfm_reply") {
//             const flowResponse = message.interactive.nfm_reply?.response_json;

//             if (!flowResponse) {
//               log("DEBUG", "No response_json in flow reply, skipping");
//               continue;
//             }

//             let flowData;
//             try {
//               flowData = typeof flowResponse === "string" ? JSON.parse(flowResponse) : flowResponse;
//             } catch {
//               log("ERROR", "Failed to parse flow response_json");
//               continue;
//             }

//             const amountType    = flowData?.amount_type    ?? null;
//             const partialAmount = flowData?.partial_amount ?? null;
//             const tenure        = flowData?.tenure         ?? null;

//             if (amountType === null && tenure === null) {
//               log("DEBUG", "Flow response missing amount_type and tenure, skipping");
//               continue;
//             }

//             // ── Step 1: Resolve finalAmount ───────────────────────────────────
//             let finalAmount = null;

//             if (amountType === "full") {
//               log("INFO", `amount_type=full — fetching FD record from Internal_bhav for: ${from}`);
//               const fdRecord = await fetchFDRecordFromDE(from);
//               finalAmount = fdRecord?.fd_amount ?? null;
//             } else {
//               finalAmount = partialAmount;
//             }

//             // ── Step 2: Calculate ROI + Maturity Amount ───────────────────────
//             let roi            = null;
//             let maturityAmount = null;

//             if (tenure && finalAmount) {
//               const calc = calculateMaturity(parseFloat(finalAmount), tenure);
//               if (calc) {
//                 roi            = calc.roi;
//                 maturityAmount = calc.maturityAmount;
//                 log("INFO", `Maturity calculated | Tenure: ${tenure} | Principal: ${finalAmount} | ROI: ${roi} | Maturity: ${maturityAmount}`);
//               } else {
//                 log("WARN", `Could not calculate maturity — unknown tenure: ${tenure}`);
//               }
//             }

//             // ── Step 3: Log full flow result ──────────────────────────────────
//             log("INFO", `Flow Data → Phone: ${from} | Name: ${profileName} | AmountType: ${amountType} | PartialAmount: ${partialAmount} | FinalAmount: ${finalAmount} | Tenure: ${tenure} | ROI: ${roi} | Maturity: ${maturityAmount} | MsgID: ${messageId} | Time: ${timestamp}`);

//             const flowEntry = {
//               phone:         from,
//               profileName,
//               amountType,
//               partialAmount,
//               finalAmount,
//               tenure,
//               roi,
//               maturityAmount,
//               messageId,
//               timestamp
//             };

//             collectedFlowData.push(flowEntry);

//             // ── Step 4: Save to FD_Flow DE (non-blocking) ─────────────────────
//             saveFlowDataToDE({ from, profileName, amountType, partialAmount, finalAmount, tenure, roi, maturityAmount, messageId, timestamp });

//             // ── Step 5: Upsert Tenure + ROI + Maturity back to Internal_bhav ──
//             if (tenure && roi !== null && maturityAmount !== null) {
//               updateInternalBhavDE({ phone: from, tenure, roi, maturityAmount });
//             }
//           }
//         }
//       }
//     }
//   } catch (err) {
//     log("ERROR", "Webhook processing failed:", err.message);
//     return res.status(500).json({ status: "error", message: err.message });
//   }

//   return res.status(200).json({
//     status: "ok",
//     received: collectedFlowData.length,
//     flowData: collectedFlowData
//   });
// });

// const PORT = process.env.PORT || 3000;
// app.listen(PORT, () => log("INFO", `Server running on port ${PORT}`));







// require("dotenv").config();
// const express = require("express");
// const axios = require("axios");

// const app = express();
// app.use(express.json({ limit: "1mb" }));

// const VERIFY_TOKEN = process.env.VERIFY_TOKEN || "myWebhookToken123";

// const SFMC = {
//   clientId: process.env.SFMC_CLIENT_ID,
//   clientSecret: process.env.SFMC_CLIENT_SECRET,
//   subdomain: process.env.SFMC_SUBDOMAIN,
//   deExternalKey: process.env.SFMC_DE_KEY || "FD_Flow"
// };

// const isDev = process.env.NODE_ENV !== "production";

// function log(level, message, meta = "") {
//   if (!isDev && level === "DEBUG") return;
//   const timestamp = new Date().toISOString();
//   console.log(`[${timestamp}] [${level}] ${message} ${meta}`);
// }

// let tokenCache = { value: null, expiresAt: null };

// async function getSFMCToken() {
//   const now = Date.now();

//   if (tokenCache.value && now < tokenCache.expiresAt - 60000) {
//     return tokenCache.value;
//   }

//   try {
//     const response = await axios.post(
//       `https://${SFMC.subdomain}.auth.marketingcloudapis.com/v2/token`,
//       {
//         grant_type: "client_credentials",
//         client_id: SFMC.clientId,
//         client_secret: SFMC.clientSecret
//       }
//     );

//     tokenCache.value = response.data.access_token;
//     tokenCache.expiresAt = now + response.data.expires_in * 1000;

//     log("INFO", "New SFMC token fetched");
//     return tokenCache.value;
//   } catch (err) {
//     log("ERROR", "SFMC token fetch failed:", err.response?.data || err.message);
//     throw err;
//   }
// }

// async function saveFlowDataToDE({
//   from,
//   profileName,
//   amountType,
//   partialAmount,
//   tenure,
//   messageId,
//   timestamp
// }) {
//   try {
//     const token = await getSFMCToken();

//     const payload = [
//       {
//         keys: { MessageId: messageId },
//         values: {
//           PhoneNumber: from,
//           ProfileName: profileName,
//           AmountType: amountType,
//           PartialAmount: partialAmount,
//           Tenure: tenure,
//           MessageId: messageId,
//           Timestamp: timestamp
//         }
//       }
//     ];

//     await axios.post(
//       `https://${SFMC.subdomain}.rest.marketingcloudapis.com/hub/v1/dataevents/key:${SFMC.deExternalKey}/rowset`,
//       payload,
//       {
//         headers: {
//           Authorization: `Bearer ${token}`,
//           "Content-Type": "application/json"
//         }
//       }
//     );

//     log(
//       "INFO",
//       `Saved to DE | Phone: ${from} | Name: ${profileName} | AmountType: ${amountType} | PartialAmount: ${partialAmount} | Tenure: ${tenure} | MsgID: ${messageId} | Time: ${timestamp}`
//     );
//   } catch (err) {
//     if (err.response?.status === 401) {
//       tokenCache = { value: null, expiresAt: null };
//     }
//     log("ERROR", "Failed to save Flow data to DE:", err.response?.data || err.message);
//   }
// }

// app.get("/webhook", (req, res) => {
//   const mode = req.query["hub.mode"];
//   const token = req.query["hub.verify_token"];
//   const challenge = req.query["hub.challenge"];

//   if (mode === "subscribe" && token === VERIFY_TOKEN) {
//     log("INFO", "Webhook verified");
//     return res.status(200).send(challenge);
//   }

//   return res.sendStatus(403);
// });

// app.post("/webhook", async (req, res) => {
//   const body = req.body;

//   if (body.object !== "whatsapp_business_account") {
//     return res
//       .status(404)
//       .json({ status: "error", message: "Not a whatsapp_business_account event" });
//   }

//   const collectedFlowData = [];

//   try {
//     const entries = body.entry || [];

//     for (const entry of entries) {
//       const changes = entry.changes || [];

//       for (const change of changes) {
//         const messages = change.value?.messages || [];
//         const contacts = change.value?.contacts || [];

//         for (const message of messages) {
//           const from = message.from;
//           const messageId = message.id;
//           const timestamp = new Date(message.timestamp * 1000).toISOString();

//           const contact = contacts.find((c) => c.wa_id === from);
//           const profileName = contact?.profile?.name || "";

//           if (
//             message.type === "interactive" &&
//             message.interactive?.type === "nfm_reply"
//           ) {
//             const flowResponse = message.interactive.nfm_reply?.response_json;

//             if (!flowResponse) {
//               log("DEBUG", "No response_json in flow reply, skipping");
//               continue;
//             }

//             let flowData;
//             try {
//               flowData =
//                 typeof flowResponse === "string"
//                   ? JSON.parse(flowResponse)
//                   : flowResponse;
//             } catch (parseErr) {
//               log("ERROR", "Failed to parse flow response_json");
//               continue;
//             }

//             // Extract all fields from the Flow JSON payload
//             const amountType = flowData?.amount_type ?? null;
//             const partialAmount = flowData?.partial_amount ?? null;
//             const tenure = flowData?.tenure ?? null;

//             if (amountType === null && tenure === null) {
//               log("DEBUG", "Flow response missing amount_type and tenure, skipping");
//               continue;
//             }

//             log(
//               "INFO",
//               `Flow Data Received → Phone: ${from} | Name: ${profileName} | AmountType: ${amountType} | PartialAmount: ${partialAmount} | Tenure: ${tenure} | MsgID: ${messageId} | Time: ${timestamp}`
//             );

//             collectedFlowData.push({
//               phone: from,
//               profileName,
//               amountType,
//               partialAmount,
//               tenure,
//               messageId,
//               timestamp
//             });

//             // Save to SFMC (non-blocking)
//             saveFlowDataToDE({
//               from,
//               profileName,
//               amountType,
//               partialAmount,
//               tenure,
//               messageId,
//               timestamp
//             });
//           }
//         }
//       }
//     }
//   } catch (err) {
//     log("ERROR", "Webhook processing failed:", err.message);
//     return res.status(500).json({ status: "error", message: err.message });
//   }

//   return res.status(200).json({
//     status: "ok",
//     received: collectedFlowData.length,
//     flowData: collectedFlowData
//   });
// });

// const PORT = process.env.PORT || 3000;
// app.listen(PORT, () => {
//   log("INFO", `Server running on port ${PORT}`);
// });








// require("dotenv").config();
// const express = require("express");
// const axios = require("axios");

// const app = express();
// app.use(express.json({ limit: "1mb" }));

// const VERIFY_TOKEN = process.env.VERIFY_TOKEN || "myWebhookToken123";

// const SFMC = {
//   clientId: process.env.SFMC_CLIENT_ID,
//   clientSecret: process.env.SFMC_CLIENT_SECRET,
//   subdomain: process.env.SFMC_SUBDOMAIN,
//   deExternalKey: process.env.SFMC_DE_KEY || "FD_Flow"
// };

// const isDev = process.env.NODE_ENV !== "production";

// function log(level, message, meta = "") {
//   if (!isDev && level === "DEBUG") return;
//   const timestamp = new Date().toISOString();
//   console.log(`[${timestamp}] [${level}] ${message} ${meta}`);
// }

// let tokenCache = { value: null, expiresAt: null };

// async function getSFMCToken() {
//   const now = Date.now();

//   if (tokenCache.value && now < tokenCache.expiresAt - 60000) {
//     return tokenCache.value;
//   }

//   try {
//     const response = await axios.post(
//       `https://${SFMC.subdomain}.auth.marketingcloudapis.com/v2/token`,
//       {
//         grant_type: "client_credentials",
//         client_id: SFMC.clientId,
//         client_secret: SFMC.clientSecret
//       }
//     );

//     tokenCache.value = response.data.access_token;
//     tokenCache.expiresAt = now + response.data.expires_in * 1000;

//     log("INFO", "New SFMC token fetched");
//     return tokenCache.value;
//   } catch (err) {
//     log("ERROR", "SFMC token fetch failed:", err.response?.data || err.message);
//     throw err;
//   }
// }

// async function saveFlowDataToDE({ from, profileName, amount, tenure, messageId, timestamp }) {
//   try {
//     const token = await getSFMCToken();

//     const payload = [{
//       keys: { MessageId: messageId },
//       values: {
//         PhoneNumber: from,
//         ProfileName: profileName,
//         Amount: amount,
//         Tenure: tenure,
//         MessageId: messageId,
//         Timestamp: timestamp
//       }
//     }];

//     await axios.post(
//       `https://${SFMC.subdomain}.rest.marketingcloudapis.com/hub/v1/dataevents/key:${SFMC.deExternalKey}/rowset`,
//       payload,
//       {
//         headers: {
//           Authorization: `Bearer ${token}`,
//           "Content-Type": "application/json"
//         }
//       }
//     );

//     log("INFO", `Saved to DE | Phone: ${from} | Name: ${profileName} | Amount: ${amount} | Tenure: ${tenure} | MsgID: ${messageId} | Time: ${timestamp}`);
//   } catch (err) {
//     if (err.response?.status === 401) {
//       tokenCache = { value: null, expiresAt: null };
//     }
//     log("ERROR", "Failed to save Flow data to DE:", err.response?.data || err.message);
//   }
// }

// app.get("/webhook", (req, res) => {
//   const mode = req.query["hub.mode"];
//   const token = req.query["hub.verify_token"];
//   const challenge = req.query["hub.challenge"];

//   if (mode === "subscribe" && token === VERIFY_TOKEN) {
//     log("INFO", "Webhook verified");
//     return res.status(200).send(challenge);
//   }

//   return res.sendStatus(403);
// });

// app.post("/webhook", async (req, res) => {
//   const body = req.body;

//   if (body.object !== "whatsapp_business_account") {
//     return res.status(404).json({ status: "error", message: "Not a whatsapp_business_account event" });
//   }

//   const collectedFlowData = [];

//   try {
//     const entries = body.entry || [];

//     for (const entry of entries) {
//       const changes = entry.changes || [];

//       for (const change of changes) {
//         const messages = change.value?.messages || [];
//         const contacts = change.value?.contacts || [];

//         for (const message of messages) {
//           const from = message.from;
//           const messageId = message.id;
//           const timestamp = new Date(message.timestamp * 1000).toISOString();

          

//           const contact = contacts.find(c => c.wa_id === from);
//           const profileName = contact?.profile?.name || "";

//           if (
//             message.type === "interactive" &&
//             message.interactive?.type === "nfm_reply"
//           ) {
//             const flowResponse = message.interactive.nfm_reply?.response_json;

//             if (!flowResponse) {
//               log("DEBUG", "No response_json in flow reply, skipping");
//               continue;
//             }

//             let flowData;
//             try {
//               flowData = typeof flowResponse === "string"
//                 ? JSON.parse(flowResponse)
//                 : flowResponse;
//             } catch (parseErr) {
//               log("ERROR", "Failed to parse flow response_json");
//               continue;
//             }

//             const amount = flowData?.amount ?? null;
//             const tenure = flowData?.tenure ?? null;

//             if (amount === null && tenure === null) {
//               log("DEBUG", "Flow response missing amount and tenure, skipping");
//               continue;
//             }

//             // Log parsed flow data
//             log("INFO", `Flow Data Received → Phone: ${from} | Name: ${profileName} | Amount: ${amount} | Tenure: ${tenure} | MsgID: ${messageId} | Time: ${timestamp}`);

//             // Collect for response
//             collectedFlowData.push({
//               phone: from,
//               profileName,
//               amount,
//               tenure,
//               messageId,
//               timestamp
//             });

//             // Save to SFMC (non-blocking for response)
//             saveFlowDataToDE({ from, profileName, amount, tenure, messageId, timestamp });
//           }
//         }
//       }
//     }

//   } catch (err) {
//     log("ERROR", "Webhook processing failed:", err.message);
//     return res.status(500).json({ status: "error", message: err.message });
//   }

//   // Return flow data in response
//   return res.status(200).json({
//     status: "ok",
//     received: collectedFlowData.length,
//     flowData: collectedFlowData
//   });
// });

// const PORT = process.env.PORT || 3000;
// app.listen(PORT, () => {
//   log("INFO", `Server running on port ${PORT}`);
// });











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

