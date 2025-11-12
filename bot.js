// bot.js – Advanced Refer & Earn Telegram Bot (patched + secure version)
// by Vishal + GPT-5 (2025)
//
// Dependencies: npm i telegraf mongodb dotenv uuid winston
require("dotenv").config();
const { Telegraf, Markup } = require("telegraf");
const { MongoClient, ObjectId } = require("mongodb");
const { v4: uuidv4 } = require("uuid");
const winston = require("winston");

// ─────────────────────────────────────────────
// CONFIGURATION
// ─────────────────────────────────────────────
const BOT_TOKEN = process.env.BOT_TOKEN;
const MONGO_URI = process.env.MONGO_URI;
const ADMIN_ID_STRINGS = (process.env.ADMIN_IDS || "")
  .split(",")
  .map((s) => s.trim())
  .filter(Boolean);
const ADMIN_IDS = ADMIN_ID_STRINGS.reduce((arr, entry) => {
  const idNum = Number(entry);
  if (Number.isFinite(idNum)) arr.push(idNum);
  else console.warn(`⚠️ Ignoring invalid ADMIN_IDS entry: ${entry}`);
  return arr;
}, []);
const DB_NAME = process.env.DB_NAME || "tg_refbot_v2";
const CONFIRM_DELAY_HOURS = Number(process.env.CONFIRM_DELAY_HOURS || 48);
const MIN_WITHDRAWAL = Number(process.env.MIN_WITHDRAWAL || 50);
const REFERRAL_REWARD = Number(process.env.REFERRAL_REWARD || 0.5);
const REF_LIMIT_PER_HOUR = Number(process.env.REF_LIMIT_PER_HOUR || 20);
const NODE_ENV = process.env.NODE_ENV || "production";

if (!BOT_TOKEN || !MONGO_URI) {
  console.error("❌ Missing BOT_TOKEN or MONGO_URI in .env");
  process.exit(1);
}
if (ADMIN_IDS.length === 0)
  console.warn("⚠️ No valid ADMIN_IDS parsed — admin commands disabled");

// ─────────────────────────────────────────────
// LOGGER
// ─────────────────────────────────────────────
const logger = winston.createLogger({
  level: NODE_ENV === "production" ? "info" : "debug",
  format: winston.format.combine(
    winston.format.timestamp(),
    winston.format.printf(
      ({ timestamp, level, message }) => `${timestamp} ${level}: ${message}`
    )
  ),
  transports: [new winston.transports.Console()],
});

logger.info(`Admins configured: ${ADMIN_IDS.length ? ADMIN_IDS.join(", ") : "none"}`);

const UPI_REGEX = /^[\w.\-]{2,}@[a-zA-Z]{2,}$/;

// ─────────────────────────────────────────────
// INIT BOT + DB
// ─────────────────────────────────────────────
const bot = new Telegraf(BOT_TOKEN);
let db,
  usersCol,
  pendingCol,
  withdrawalsCol,
  refsCol,
  tasksCol,
  supportCol,
  requiredChannelsCol;

async function connectDB() {
  const client = new MongoClient(MONGO_URI); // modern driver
  await client.connect();
  db = client.db(DB_NAME);
  usersCol = db.collection("users");
  pendingCol = db.collection("pending");
  withdrawalsCol = db.collection("withdrawals");
  refsCol = db.collection("refs");
  tasksCol = db.collection("tasks");
  supportCol = db.collection("support");
  requiredChannelsCol = db.collection("requiredChannels");

  await usersCol.createIndex({ telegramId: 1 }, { unique: true });
  await usersCol.createIndex({ referralCode: 1 }, { unique: true });
  await pendingCol.createIndex({ createdAt: 1 });
  await withdrawalsCol.createIndex({ status: 1 });
  await requiredChannelsCol.createIndex({ chatId: 1 }, { unique: true });
  logger.info("✅ MongoDB connected (modern driver)");
}

// ─────────────────────────────────────────────
// HELPERS
// ─────────────────────────────────────────────
function makeReferralCode() {
  return "ref_" + uuidv4().split("-")[0];
}
function isAdmin(id) {
  return ADMIN_IDS.includes(Number(id));
}
async function ensureUserProfile(from) {
  if (!from) return null;
  const tgId = from.id;
  let u = await usersCol.findOne({ telegramId: tgId });
  if (!u) {
    const code = makeReferralCode();
    u = {
      telegramId: tgId,
      username: from.username || null,
      first_name: from.first_name || null,
      referralCode: code,
      balance: 0,
      confirmedReferrals: 0,
      createdAt: new Date(),
      badges: [],
      primaryUPI: null,
      needsUpiSetup: true,
    };
    await usersCol.insertOne(u);
    logger.info(`New user created: ${tgId}`);
  } else {
    const updates = {};
    if (from.username && from.username !== u.username)
      updates.username = from.username;
    if (from.first_name && from.first_name !== u.first_name)
      updates.first_name = from.first_name;
    if (typeof u.primaryUPI === "undefined") updates.primaryUPI = null;
    if (typeof u.needsUpiSetup === "undefined")
      updates.needsUpiSetup = !u.primaryUPI;
    if (Object.keys(updates).length) {
      await usersCol.updateOne({ telegramId: tgId }, { $set: updates });
      Object.assign(u, updates);
      logger.debug(`Profile auto-updated for ${tgId}`);
    }
  }
  return u;
}
async function getBotUsername() {
  if (bot.options.username) return bot.options.username;
  const me = await bot.telegram.getMe();
  bot.options.username = me.username;
  return me.username;
}

const ALLOWED_MEMBER_STATUSES = new Set([
  "creator",
  "administrator",
  "member",
]);

function buildChannelLink(channel) {
  if (channel.link) return channel.link;
  if (typeof channel.chatId === "string" && channel.chatId.startsWith("@")) {
    return `https://t.me/${channel.chatId.slice(1)}`;
  }
  return null;
}

function describeChannel(channel) {
  return channel.title || channel.chatId;
}
function channelTypePrefix(channel) {
  const type = channel?.chatType || "";
  if (type.includes("group")) return "Group";
  if (type === "channel") return "Channel";
  return "Channel";
}

function buildChannelSequenceLabel(channel, index) {
  return `${channelTypePrefix(channel)} ${index + 1}`;
}

function buildJoinButtonLabel(channel, index) {
  const prefix = channelTypePrefix(channel);
  return `Join ${prefix} ${index + 1}`;
}

function formatChannelSummary(channels) {
  if (!channels.length) return "No required channels configured yet.";
  return channels
    .map((channel, idx) => {
      const label = buildChannelSequenceLabel(channel, idx);
      return `${label} — ${describeChannel(channel)} (ID: ${channel.chatId})`;
    })
    .join("\n");
}

function formatChannelDetails(channel) {
  const parts = [
    `📣 ${describeChannel(channel)}`,
    `ID: ${channel.chatId}`,
  ];
  if (channel.chatType) parts.push(`Type: ${channel.chatType}`);
  if (channel.link) parts.push(`Link: ${channel.link}`);
  parts.push("\nUse the buttons below to manage this channel.");
  return parts.join("\n");
}

function buildAdminChannelsKeyboard(channels) {
  const rows = [];
  channels.forEach((channel, idx) => {
    if (!channel?._id) return;
    rows.push([
      Markup.button.callback(
        buildChannelSequenceLabel(channel, idx),
        `ADMIN_CHANNEL_DETAIL:${channel._id}`
      ),
    ]);
  });
  rows.push([Markup.button.callback("🔄 Refresh", "ADMIN_CHANNELS_REFRESH")]);
  return rows;
}

async function showAdminChannels(ctx, options = {}) {
  const channels = await fetchRequiredChannels();
  const summary = formatChannelSummary(channels);
  const text =
    "📣 Channel guard\n\n" +
    summary +
    "\n\nUse /admin_addchannel <chatId> [link] to add and /admin_removechannel <chatId> to delete.";
  const extra = {
    disable_web_page_preview: true,
    ...Markup.inlineKeyboard(buildAdminChannelsKeyboard(channels)),
  };

  if (ctx.updateType === "callback_query" && !options.forceReply) {
    try {
      await ctx.editMessageText(text, extra);
      return;
    } catch (err) {
      logger.debug(`editMessageText failed, falling back to reply: ${err.message}`);
    }
  }
  await ctx.reply(text, extra);
}

async function fetchRequiredChannels() {
  if (!requiredChannelsCol) return [];
  return requiredChannelsCol.find().sort({ createdAt: 1 }).toArray();
}

async function collectMissingChannels(ctx, userId) {
  const channels = await fetchRequiredChannels();
  if (!channels.length) return { channels, missing: [] };
  const missing = [];
  for (const [index, channel] of channels.entries()) {
    try {
      const chatIdentifier =
        typeof channel.chatId === "string" && /^-?\d+$/.test(channel.chatId)
          ? Number(channel.chatId)
          : channel.chatId;
      const member = await ctx.telegram.getChatMember(
        chatIdentifier,
        userId
      );
      const status = member?.status;
      if (!ALLOWED_MEMBER_STATUSES.has(status)) {
        if (status === "restricted" && member?.is_member) continue;
        missing.push({ channel, index });
      }
    } catch (err) {
      logger.warn(
        `Failed to verify membership for ${userId} in ${channel.chatId}: ${err.message}`
      );
      missing.push({ channel, index });
    }
  }
  return { channels, missing };
}

async function enforceChannelRequirements(ctx, options = {}) {
  const { silent = false } = options;
  if (!ctx.from || isAdmin(ctx.from.id)) return true;
  if (ctx.callbackQuery?.data === "CHECK_REQUIREMENTS") return true;
  const { channels, missing } = await collectMissingChannels(ctx, ctx.from.id);
  if (!missing.length) return true;
  if (silent) return false;

  try {
    if (ctx.callbackQuery) {
      await ctx.answerCbQuery("Join required channels to continue.", {
        show_alert: true,
      });
    }
  } catch (err) {
    logger.debug(`answerCbQuery failed: ${err.message}`);
  }

  if (ctx.chat) {
    let text = "🚪 Access locked\nJoin the required channels/groups first:\n\n";
    const rows = [];
    const withoutLinks = [];
    
    for (const { channel, index } of missing) {
      const link = buildChannelLink(channel);
      const buttonLabel = buildJoinButtonLabel(channel, index);
      
      if (link) {
        rows.push([Markup.button.url(buttonLabel, link)]);
      } else {
        withoutLinks.push(`${buttonLabel}: ${describeChannel(channel)}`);
      }
    }
    
    if (withoutLinks.length) {
      text += withoutLinks.join("\n") + "\n(Contact admin for invite links)\n\n";
    }
    
    rows.push([Markup.button.callback("✅ I've joined all", "CHECK_REQUIREMENTS")]);
    await ctx.reply(text.trim(), {
      ...Markup.inlineKeyboard(rows),
      disable_web_page_preview: true,
    });
  }

  return false;
}

function buildHelpText(isAdminUser) {
  let text =
    "🤖 *Refer & Earn Bot Help*\n\n" +
    "• /start – receive your personal referral link and quick actions.\n" +
    `• *How referrals work*: share your link; when a friend joins and stays ${CONFIRM_DELAY_HOURS}h, you earn ₹${REFERRAL_REWARD}. Self-referrals or duplicates are rejected.\n` +
    "• Track progress: /balance shows earnings, /profile lists stats, /leaderboard shows the top promoters.\n" +
  "• /setupi – register or update your payout UPI so withdrawals are faster.\n" +
  "• /status – check the last few withdrawal requests and their status.\n" +
    `• Payments: once you reach ₹${MIN_WITHDRAWAL}, run /withdraw. Enter a valid UPI ID; the amount is locked until an admin pays. You can type ‘cancel’ to abort before approval.\n` +
    "• Bonus tasks (if any) appear under the Tasks button for extra rewards.\n";
  if (isAdminUser) {
    text +=
      "\n\n👮 *Admin tools*:\n" +
      "• /admin – verify admin status, view quick links.\n" +
      "• /adminpanel – open inline admin panel (withdrawals, confirm referrals).\n" +
      "• /admin_withdrawals – list pending payouts.\n" +
  "• /admin_channels – list required join targets.\n" +
  "• /admin_addchannel <chatId> [link] – require joining a channel before usage.\n" +
  "• /admin_removechannel <chatId> – drop a channel from the requirement list.\n" +
      "• /admin_confirm [force] – run referral confirmation now (add 'force' to ignore delay).\n" +
      "• /admin_credit <telegramId> <amount> [note] – manually credit a user.\n" +
      "• /pay <withdrawalId> – mark a withdrawal as paid and notify the user.\n" +
      "• /cancelwithdraw <withdrawalId> – cancel and refund a withdrawal.\n" +
      "• Inline buttons also provide quick access to these actions.";
  }
  return text;
}

function logCommand(ctx, commandName) {
  const userId = ctx.from?.id;
  const username = ctx.from?.username ? `@${ctx.from.username}` : ctx.from?.first_name || "unknown";
  logger.info(`${commandName} triggered by ${username} (${userId})`);
}

async function listPendingWithdrawalsReply(ctx) {
  const pending = await withdrawalsCol.find({ status: "pending" }).toArray();
  if (pending.length === 0) {
    logger.info("No pending withdrawals to display");
    await ctx.reply("No pending withdrawals.");
    return;
  }
  logger.info(`Listing ${pending.length} pending withdrawals`);
  let msg = "Pending withdrawals:\n\n";
  for (const p of pending) {
    msg += `ID:${p._id}\nUser:${p.userId}\nAmt:₹${p.amount}\nUPI:${p.upi}\n\n`;
  }
  await ctx.reply(msg);
}

async function showAdminPanel(ctx) {
  logger.info(`Rendering admin panel for ${ctx.from.id}`);
  await ctx.reply(
    "🛠 Admin panel actions:\nUse the buttons below or commands like /pay and /cancelwithdraw for specific withdrawals. 'Confirm referrals now' ignores the waiting period.",
    Markup.inlineKeyboard([
      [Markup.button.callback("📋 Pending withdrawals", "ADMIN_WITHDRAWALS")],
      [Markup.button.callback("� Channel guard", "ADMIN_CHANNELS")],
      [Markup.button.callback("�📖 Admin help", "ADMIN_HELP")],
      [Markup.button.callback("✅ Confirm referrals now", "ADMIN_CONFIRM")],
    ])
  );
}

async function buildReferralLink(user) {
  const username = await getBotUsername();
  return `https://t.me/${username}?start=${user.referralCode}`;
}

function buildMainMenu(user, link, isAdminUser) {
  const rows = [
    [Markup.button.url("🔗 Share Referral", link)],
    [
      Markup.button.callback("💰 Balance", "BALANCE"),
      Markup.button.callback("🧾 Profile", "PROFILE"),
    ],
    [
      Markup.button.callback("🏆 Leaderboard", "LEADERBOARD"),
      Markup.button.callback("🎯 Tasks", "TASKS"),
    ],
    [
      Markup.button.callback("💸 Withdraw", "WITHDRAW"),
      Markup.button.callback("🏦 Set UPI", "SETUP_UPI"),
    ],
    [
      Markup.button.callback("📊 Status", "STATUS"),
      Markup.button.callback("🆘 Support", "SUPPORT"),
    ],
    [Markup.button.callback("❓ Help", "HELP")],
  ];
  if (isAdminUser) {
    rows.push([Markup.button.callback("🛠 Admin panel", "ADMIN_PANEL")]);
  }
  return Markup.inlineKeyboard(rows);
}

async function sendMainMenu(ctx, user) {
  const link = await buildReferralLink(user);
  const isAdminUser = isAdmin(ctx.from.id);
  let greeting = `Hi ${ctx.from.first_name || ""} 👋\n\nYour referral link:\n${link}\nEarn ₹${REFERRAL_REWARD} per confirmed referral.`;
  if (!user.primaryUPI) {
    greeting += "\n\n📥 Tip: set your default UPI with the '🏦 Set UPI' button for faster payouts.";
  }
  if (isAdminUser) {
    greeting +=
      "\n\n(Admin) Shortcuts available via the 🛠 Admin panel button.";
  }
  await ctx.reply(greeting, buildMainMenu(user, link, isAdminUser));
}

async function replyBalance(ctx) {
  const u = await ensureUserProfile(ctx.from);
  await ctx.reply(
    `💰 Balance: ₹${(u.balance || 0).toFixed(2)}\nReferrals: ${
      u.confirmedReferrals || 0
    }`
  );
}

async function replyProfile(ctx) {
  const u = await ensureUserProfile(ctx.from);
  const link = await buildReferralLink(u);
  await ctx.reply(
    `🧾 Profile summary:\nName: ${u.first_name || "—"}\nUsername: ${
      u.username ? "@" + u.username : "—"
    }\nReferral link: ${link}\nBalance: ₹${(u.balance || 0).toFixed(2)}\nConfirmed referrals: ${
      u.confirmedReferrals || 0
    }\nSaved UPI: ${u.primaryUPI || "—"}\nBadges: ${
      u.badges?.length ? u.badges.join(", ") : "—"
    }`
  );
}

async function replyLeaderboard(ctx) {
  const top = await usersCol
    .find()
    .sort({ confirmedReferrals: -1, balance: -1 })
    .limit(10)
    .toArray();
  if (!top.length) {
    await ctx.reply("No referrers yet.");
    return;
  }
  let text = "🏆 Leaderboard:\n";
  top.forEach((entry, idx) => {
    const label = entry.username
      ? "@" + entry.username
      : entry.first_name || entry.telegramId;
    text += `${idx + 1}. ${label} — ${
      entry.confirmedReferrals || 0
    } referrals — ₹${(entry.balance || 0).toFixed(2)}\n`;
  });
  await ctx.reply(text);
}

async function startWithdrawFlow(ctx, user) {
  if (user.balanceLocked && user.balanceLocked > 0) {
    await ctx.reply("⚠️ You already have a pending withdrawal.");
    return;
  }
  if ((user.balance || 0) < MIN_WITHDRAWAL) {
    await ctx.reply(
      `Minimum ₹${MIN_WITHDRAWAL}. Current ₹${(user.balance || 0).toFixed(2)}`
    );
    return;
  }
  const spendable = user.balance;
  logger.info(
    `Locking ₹${spendable} for withdrawal request by ${user.telegramId}`
  );
  const setFields = {
    awaitingWithdrawUPI: true,
    balanceLocked: spendable,
    lastWithdrawAt: new Date(),
    awaitingWithdrawUPIConfirm: false,
    draftWithdrawUPI: null,
  };
  let instructions =
    "Reply with your UPI ID (e.g. name@bank). Type 'cancel' to abort.";
  if (user.primaryUPI) {
    setFields.draftWithdrawUPI = user.primaryUPI;
    setFields.awaitingWithdrawUPIConfirm = true;
    instructions = `Saved UPI: ${user.primaryUPI}. Reply 'confirm' to use it, send a different UPI to change, or type 'cancel' to abort.`;
  }
  await usersCol.updateOne(
    { telegramId: user.telegramId },
    {
      $set: setFields,
      $inc: { balance: -spendable },
    }
  );
  await ctx.reply(instructions);
}

async function startUpiSetup(ctx, user) {
  await usersCol.updateOne(
    { telegramId: user.telegramId },
    { $set: { awaitingUpiSetup: true, draftUpiSetup: null } }
  );
  const note = user.primaryUPI
    ? `Your current saved UPI is ${user.primaryUPI}.`
    : "No UPI saved yet.";
  await ctx.reply(
    `${note} Send a new UPI now. Reply 'confirm' to save it, or 'cancel' to exit.`,
    { disable_web_page_preview: true }
  );
}

async function replyStatus(ctx) {
  const u = await ensureUserProfile(ctx.from);
  const pending = await withdrawalsCol
    .find({ userId: u.telegramId })
    .sort({ requestedAt: -1 })
    .limit(5)
    .toArray();
  if (!pending.length) {
    await ctx.reply("No withdrawal history yet.");
    return;
  }
  let text = "📊 Recent withdrawals:\n";
  pending.forEach((w) => {
    const status = w.status.toUpperCase();
    const when = new Date(w.requestedAt).toLocaleString();
    text += `• ₹${w.amount.toFixed(2)} — ${status} — ${
      w.upi || "—"
    } — ${when}\n`;
  });
  await ctx.reply(text.trim());
}

// ─────────────────────────────────────────────
// START COMMAND
// ─────────────────────────────────────────────
bot.use(async (ctx, next) => {
  try {
    const allowed = await enforceChannelRequirements(ctx);
    if (!allowed) return;
  } catch (err) {
    logger.error(`Requirement guard failed: ${err.message}`);
    // fall through to avoid locking users out if check fails unexpectedly
  }
  return next();
});

bot.start(async (ctx) => {
  try {
    logCommand(ctx, "/start");
    const raw = ctx.message.text.trim();
    const parts = raw.split(" ");
    const payload = parts[1] || "";
    const user = await ensureUserProfile(ctx.from);

    // Handle referral link
    if (payload.startsWith("ref_")) {
      const referrer = await usersCol.findOne({ referralCode: payload });
      if (referrer && referrer.telegramId !== user.telegramId) {
        const exists =
          (await pendingCol.findOne({
            referredId: user.telegramId,
          })) ||
          (await refsCol.findOne({ referredId: user.telegramId }));
        if (!exists) {
          logger.info(`Referral detected: referrer ${referrer.telegramId} -> referred ${user.telegramId}`);
          await pendingCol.insertOne({
            referredId: user.telegramId,
            referrerId: referrer.telegramId,
            referralCode: payload,
            createdAt: new Date(),
            status: "pending",
          });
          try {
            await ctx.telegram.sendMessage(
              referrer.telegramId,
              `🎉 You referred ${
                ctx.from.username ? "@" + ctx.from.username : ctx.from.first_name
              }! Confirmation in ${CONFIRM_DELAY_HOURS}h.`
            );
          } catch (notifyErr) {
            logger.debug(
              `Could not notify referrer ${referrer.telegramId}: ${notifyErr.message}`
            );
          }
        }
      }
    }

    await sendMainMenu(ctx, user);
    logger.debug(`/start completed for ${ctx.from.id}`);
  } catch (e) {
    logger.error("/start: " + e.message);
  }
});

// ─────────────────────────────────────────────
// HELP + BALANCE
// ─────────────────────────────────────────────
bot.command("help", async (ctx) => {
  logCommand(ctx, "/help");
  await ctx.reply(buildHelpText(isAdmin(ctx.from.id)));
});
bot.command("balance", async (ctx) => {
  try {
    logCommand(ctx, "/balance");
    await replyBalance(ctx);
  } catch (e) {
    ctx.reply("Error loading balance.");
    logger.error("/balance: " + e.message);
  }
});

bot.command("profile", async (ctx) => {
  try {
    logCommand(ctx, "/profile");
    await replyProfile(ctx);
  } catch (e) {
    ctx.reply("Error loading profile.");
    logger.error("/profile: " + e.message);
  }
});

bot.command("leaderboard", async (ctx) => {
  try {
    logCommand(ctx, "/leaderboard");
    await replyLeaderboard(ctx);
  } catch (e) {
    ctx.reply("Error loading leaderboard.");
    logger.error("/leaderboard: " + e.message);
  }
});

bot.command("setupi", async (ctx) => {
  try {
    logCommand(ctx, "/setupi");
    const u = await ensureUserProfile(ctx.from);
    await startUpiSetup(ctx, u);
  } catch (e) {
    ctx.reply("Could not start UPI setup.");
    logger.error("/setupi: " + e.message);
  }
});

bot.command("status", async (ctx) => {
  try {
    logCommand(ctx, "/status");
    await replyStatus(ctx);
  } catch (e) {
    ctx.reply("Could not load status.");
    logger.error("/status: " + e.message);
  }
});

// ─────────────────────────────────────────────
// WITHDRAWAL SYSTEM (hardened)
// ─────────────────────────────────────────────
bot.command("withdraw", async (ctx) => {
  try {
    logCommand(ctx, "/withdraw");
    const u = await ensureUserProfile(ctx.from);
    await startWithdrawFlow(ctx, u);
  } catch (e) {
    logger.error("/withdraw: " + e.message);
    ctx.reply("Error starting withdrawal.");
  }
});

bot.on("text", async (ctx, next) => {
  try {
    const text = ctx.message.text.trim();
    let u = await usersCol.findOne({ telegramId: ctx.from.id });
    if (!u) {
      logger.debug(`text handler creating profile for ${ctx.from.id}`);
      u = await ensureUserProfile(ctx.from);
    }

    const lower = text.toLowerCase();

    // handle global UPI setup
    if (u.awaitingUpiSetup) {
      if (lower === "cancel") {
        await usersCol.updateOne(
          { telegramId: u.telegramId },
          { $unset: { awaitingUpiSetup: "", draftUpiSetup: "" } }
        );
        await ctx.reply("UPI setup cancelled.");
        return;
      }
      if (lower === "confirm") {
        if (!u.draftUpiSetup) {
          await ctx.reply("Send a UPI first, then reply 'confirm'.");
          return;
        }
        await usersCol.updateOne(
          { telegramId: u.telegramId },
          {
            $set: {
              primaryUPI: u.draftUpiSetup,
              needsUpiSetup: false,
            },
            $unset: { awaitingUpiSetup: "", draftUpiSetup: "" },
          }
        );
        await ctx.reply(
          `✅ Saved UPI ${u.draftUpiSetup}. You can update it anytime with /setupi.`
        );
        return;
      }
      if (UPI_REGEX.test(text)) {
        await usersCol.updateOne(
          { telegramId: u.telegramId },
          { $set: { draftUpiSetup: text } }
        );
        await ctx.reply(
          `UPI captured: ${text}. Reply 'confirm' to save, send another to change, or 'cancel' to exit.`
        );
        return;
      }
      await ctx.reply(
        "Please send a valid UPI (e.g. name@bank), 'confirm' to save, or 'cancel' to exit."
      );
      return;
    }

    // handle withdrawal UPI
    if (u.awaitingWithdrawUPI) {
      if (lower === "cancel") {
        await usersCol.updateOne(
          { telegramId: u.telegramId },
          {
            $inc: { balance: u.balanceLocked || 0 },
            $unset: {
              awaitingWithdrawUPI: "",
              awaitingWithdrawUPIConfirm: "",
              draftWithdrawUPI: "",
              balanceLocked: "",
            },
          }
        );
        await ctx.reply("❌ Withdrawal cancelled. Balance restored.");
        return;
      }
      if (lower === "confirm") {
        if (!u.awaitingWithdrawUPIConfirm || !u.draftWithdrawUPI) {
          await ctx.reply(
            "Send your UPI first (or type a new one) before confirming."
          );
          return;
        }
        const amount = u.balanceLocked || 0;
        const upi = u.draftWithdrawUPI;
        await withdrawalsCol.insertOne({
          userId: u.telegramId,
          upi,
          amount,
          requestedAt: new Date(),
          status: "pending",
        });
        await usersCol.updateOne(
          { telegramId: u.telegramId },
          {
            $unset: {
              awaitingWithdrawUPI: "",
              awaitingWithdrawUPIConfirm: "",
              draftWithdrawUPI: "",
            },
            $set: {
              primaryUPI: upi,
              needsUpiSetup: false,
            },
          }
        );
        await ctx.reply(
          `✅ Withdrawal ₹${amount.toFixed(
            2
          )} to ${upi}. Admin will process soon.`
        );
        for (const a of ADMIN_IDS)
          try {
            await ctx.telegram.sendMessage(
              a,
              `💸 Withdrawal:\nUser:${u.telegramId}\nAmt:₹${amount}\nUPI:${upi}`
            );
          } catch {}
        logger.info(`Withdrawal request stored for ${u.telegramId}`);
        return;
      }
      if (UPI_REGEX.test(text)) {
        await usersCol.updateOne(
          { telegramId: u.telegramId },
          {
            $set: {
              draftWithdrawUPI: text,
              awaitingWithdrawUPIConfirm: true,
            },
          }
        );
        await ctx.reply(
          `UPI noted: ${text}. Reply 'confirm' to submit, send another to change, or 'cancel' to abort.`
        );
        return;
      }
      await ctx.reply(
        "Please send a valid UPI (e.g. name@bank), 'confirm' to submit, or 'cancel' to abort."
      );
      return;
    }

    if (u.awaitingSupportMessage) {
      logger.info(`Support message received from ${u.telegramId}`);
      await usersCol.updateOne(
        { telegramId: u.telegramId },
        { $unset: { awaitingSupportMessage: "" } }
      );
      for (const adminId of ADMIN_IDS) {
        try {
          await ctx.telegram.sendMessage(
            adminId,
            `🆘 Support request from ${u.telegramId} (${ctx.from.username ? "@" + ctx.from.username : ctx.from.first_name || ""}):\n${text}`
          );
        } catch (err) {
          logger.debug(`Failed to forward support message to ${adminId}: ${err.message}`);
        }
      }
      await ctx.reply("Thanks! An admin will reach out soon.");
      return;
    }
  } catch (e) {
    logger.error("text handler: " + e.message);
  }
  return next();
});

// ─────────────────────────────────────────────
// INLINE CALLBACKS
// ─────────────────────────────────────────────
bot.action("BALANCE", async (ctx) => {
  logger.info(`BALANCE button tapped by ${ctx.from.id}`);
  await ctx.answerCbQuery();
  await replyBalance(ctx);
});

bot.action("PROFILE", async (ctx) => {
  logger.info(`PROFILE button tapped by ${ctx.from.id}`);
  await ctx.answerCbQuery();
  await replyProfile(ctx);
});

bot.action("LEADERBOARD", async (ctx) => {
  logger.info(`LEADERBOARD button tapped by ${ctx.from.id}`);
  await ctx.answerCbQuery();
  await replyLeaderboard(ctx);
});
bot.action("HELP", async (ctx) => {
  logger.info(`HELP button tapped by ${ctx.from.id}`);
  await ctx.answerCbQuery();
  await ctx.reply(buildHelpText(isAdmin(ctx.from.id)));
});
bot.action("SUPPORT", async (ctx) => {
  logger.info(`SUPPORT button tapped by ${ctx.from.id}`);
  await usersCol.updateOne(
    { telegramId: ctx.from.id },
    { $set: { awaitingSupportMessage: true } }
  );
  await ctx.answerCbQuery();
  await ctx.reply("Type your support message; admins will be notified.");
});

bot.action("CHECK_REQUIREMENTS", async (ctx) => {
  logger.info(`CHECK_REQUIREMENTS tapped by ${ctx.from.id}`);
  await ctx.answerCbQuery();
  const { missing } = await collectMissingChannels(ctx, ctx.from.id);
  if (missing.length) {
    await ctx.reply("Still missing required channels. Please join all listed channels and try again.");
    await enforceChannelRequirements(ctx);
    return;
  }
  const user = await ensureUserProfile(ctx.from);
  await ctx.reply("✅ All requirements satisfied. Welcome!");
  await sendMainMenu(ctx, user);
});

bot.action("TASKS", async (ctx) => {
  logger.info(`TASKS button tapped by ${ctx.from.id}`);
  await ctx.answerCbQuery();
  const available = await tasksCol
    .find({ active: true })
    .sort({ priority: -1, createdAt: -1 })
    .toArray();
  if (!available.length) {
    await ctx.reply("No bonus tasks available right now. Check back soon!");
    return;
  }
  let msg = "🎯 Active tasks:\n";
  available.forEach((task, idx) => {
    msg += `${idx + 1}. ${task.title} — reward ₹${task.reward}\n${
      task.description || ""
    }\n\n`;
  });
  await ctx.reply(msg.trim());
});

bot.action("WITHDRAW", async (ctx) => {
  logger.info(`WITHDRAW button tapped by ${ctx.from.id}`);
  await ctx.answerCbQuery();
  const u = await ensureUserProfile(ctx.from);
  await startWithdrawFlow(ctx, u);
});

bot.action("SETUP_UPI", async (ctx) => {
  logger.info(`SETUP_UPI button tapped by ${ctx.from.id}`);
  await ctx.answerCbQuery();
  const u = await ensureUserProfile(ctx.from);
  await startUpiSetup(ctx, u);
});

bot.action("STATUS", async (ctx) => {
  logger.info(`STATUS button tapped by ${ctx.from.id}`);
  await ctx.answerCbQuery();
  await replyStatus(ctx);
});

// ─────────────────────────────────────────────
// ADMIN COMMANDS
// ─────────────────────────────────────────────
bot.command("admin", async (ctx) => {
  logCommand(ctx, "/admin");
  const id = ctx.from.id;
  const is = isAdmin(id);
  await ctx.reply(
    `👮 Your ID: ${id}\nAdmins: ${ADMIN_IDS.join(
      ", "
    )}\nStatus: ${is ? "✅ You are admin" : "❌ Not admin"}`
  );
  if (is) await ctx.reply(buildHelpText(true));
  if (is) await showAdminPanel(ctx);
});

bot.command("admin_withdrawals", async (ctx) => {
  logCommand(ctx, "/admin_withdrawals");
  if (!isAdmin(ctx.from.id)) return ctx.reply("Unauthorized");
  await listPendingWithdrawalsReply(ctx);
});

bot.command("adminpanel", async (ctx) => {
  logCommand(ctx, "/adminpanel");
  if (!isAdmin(ctx.from.id)) return ctx.reply("Unauthorized");
  await showAdminPanel(ctx);
});

bot.command("admin_channels", async (ctx) => {
  logCommand(ctx, "/admin_channels");
  if (!isAdmin(ctx.from.id)) return ctx.reply("Unauthorized");
  await showAdminChannels(ctx, { forceReply: true });
});

bot.command("admin_addchannel", async (ctx) => {
  logCommand(ctx, "/admin_addchannel");
  if (!isAdmin(ctx.from.id)) return ctx.reply("Unauthorized");
  const parts = ctx.message.text.split(" ").filter(Boolean);
  if (parts.length < 2)
    return ctx.reply(
      "Usage: /admin_addchannel <chatIdOrUsername> [join_link]"
    );

  const chatIdInput = parts[1];
  const storedChatId = String(chatIdInput);
  const joinLink = parts[2] || null;
  const lookupIdentifier = /^-?\d+$/.test(chatIdInput)
    ? Number(chatIdInput)
    : chatIdInput;
  let chatInfo;
  try {
    chatInfo = await ctx.telegram.getChat(lookupIdentifier);
  } catch (err) {
    logger.warn(
      `Could not fetch chat info for ${chatIdInput}: ${err.message}`
    );
  }

  const doc = {
    chatId: storedChatId,
    title:
      chatInfo?.title || chatInfo?.username || chatInfo?.first_name || storedChatId,
    link: joinLink || chatInfo?.invite_link || null,
    chatType: chatInfo?.type || null,
    updatedAt: new Date(),
    addedBy: ctx.from.id,
  };

  await requiredChannelsCol.updateOne(
    { chatId: storedChatId },
    { $set: doc, $setOnInsert: { createdAt: new Date() } },
    { upsert: true }
  );

  await ctx.reply(
    `Saved channel requirement for ${doc.title}.` +
      (doc.link ? " Invite link stored." : " No invite link supplied.")
  );
});

bot.command("admin_removechannel", async (ctx) => {
  logCommand(ctx, "/admin_removechannel");
  if (!isAdmin(ctx.from.id)) return ctx.reply("Unauthorized");
  const parts = ctx.message.text.split(" ").filter(Boolean);
  if (parts.length < 2)
    return ctx.reply("Usage: /admin_removechannel <chatIdOrUsername>");
  const chatIdInput = parts[1];
  const storedChatId = String(chatIdInput);
  const result = await requiredChannelsCol.deleteOne({ chatId: storedChatId });
  if (!result.deletedCount) return ctx.reply("No matching channel requirement found.");
  await ctx.reply(`Removed channel requirement for ${storedChatId}.`);
});

bot.action("ADMIN_PANEL", async (ctx) => {
  logger.info(`ADMIN_PANEL button tapped by ${ctx.from.id}`);
  await ctx.answerCbQuery();
  if (!isAdmin(ctx.from.id)) return ctx.reply("Unauthorized");
  await showAdminPanel(ctx);
});

bot.action("ADMIN_WITHDRAWALS", async (ctx) => {
  logger.info(`ADMIN_WITHDRAWALS button tapped by ${ctx.from.id}`);
  await ctx.answerCbQuery();
  if (!isAdmin(ctx.from.id)) return ctx.reply("Unauthorized");
  await listPendingWithdrawalsReply(ctx);
});

bot.action("ADMIN_CHANNELS", async (ctx) => {
  logger.info(`ADMIN_CHANNELS button tapped by ${ctx.from.id}`);
  await ctx.answerCbQuery();
  if (!isAdmin(ctx.from.id)) return ctx.reply("Unauthorized");
  await showAdminChannels(ctx);
});

bot.action("ADMIN_CHANNELS_REFRESH", async (ctx) => {
  logger.info(`ADMIN_CHANNELS_REFRESH tapped by ${ctx.from.id}`);
  await ctx.answerCbQuery();
  if (!isAdmin(ctx.from.id)) return ctx.reply("Unauthorized");
  await showAdminChannels(ctx);
});

bot.action(/^ADMIN_CHANNEL_DETAIL:(.+)$/i, async (ctx) => {
  logger.info(`ADMIN_CHANNEL_DETAIL tapped by ${ctx.from.id}`);
  await ctx.answerCbQuery();
  if (!isAdmin(ctx.from.id)) return ctx.reply("Unauthorized");
  const id = ctx.match[1];
  let objectId;
  try {
    objectId = new ObjectId(id);
  } catch (err) {
    await ctx.reply("Invalid channel reference.");
    return;
  }
  const channel = await requiredChannelsCol.findOne({ _id: objectId });
  if (!channel) {
    await ctx.reply("Channel entry not found (maybe removed already).");
    return;
  }
  const rows = [
    [
      Markup.button.callback(
        "🗑 Remove",
        `ADMIN_CHANNEL_REMOVE:${channel._id}`
      ),
    ],
    [Markup.button.callback("⬅ Back", "ADMIN_CHANNELS_REFRESH")],
  ];
  const extra = {
    disable_web_page_preview: true,
    ...Markup.inlineKeyboard(rows),
  };
  try {
    await ctx.editMessageText(formatChannelDetails(channel), extra);
  } catch (err) {
    logger.debug(`editMessageText (detail) failed, replying: ${err.message}`);
    await ctx.reply(formatChannelDetails(channel), extra);
  }
});

bot.action(/^ADMIN_CHANNEL_REMOVE:(.+)$/i, async (ctx) => {
  logger.info(`ADMIN_CHANNEL_REMOVE tapped by ${ctx.from.id}`);
  if (!isAdmin(ctx.from.id)) {
    await ctx.answerCbQuery("Unauthorized", { show_alert: true });
    return;
  }
  const id = ctx.match[1];
  let objectId;
  try {
    objectId = new ObjectId(id);
  } catch (err) {
    await ctx.answerCbQuery("Invalid channel reference.", {
      show_alert: true,
    });
    return;
  }
  const result = await requiredChannelsCol.deleteOne({ _id: objectId });
  if (!result.deletedCount) {
    await ctx.answerCbQuery("Channel not found or already removed.", {
      show_alert: true,
    });
    await showAdminChannels(ctx);
    return;
  }
  await ctx.answerCbQuery("Channel requirement removed.");
  await showAdminChannels(ctx);
});

bot.action("ADMIN_HELP", async (ctx) => {
  logger.info(`ADMIN_HELP button tapped by ${ctx.from.id}`);
  await ctx.answerCbQuery();
  if (!isAdmin(ctx.from.id)) return ctx.reply("Unauthorized");
  await ctx.reply(buildHelpText(true));
});

bot.action("ADMIN_CONFIRM", async (ctx) => {
  logger.info(`ADMIN_CONFIRM button tapped by ${ctx.from.id}`);
  await ctx.answerCbQuery();
  if (!isAdmin(ctx.from.id)) return ctx.reply("Unauthorized");
  const result = await confirmPendingReferrals({ force: true });
  const confirmed = result?.confirmed?.length || 0;
  const invalidated = result?.invalidated?.length || 0;
  const summary = confirmed
    ? `Confirmed ${confirmed} referrals${
        invalidated ? `, invalidated ${invalidated}` : ""
      }.`
    : "No referrals ready for confirmation.";
  await ctx.reply(`Referral audit complete. ${summary}`);
});

bot.command("pay", async (ctx) => {
  logCommand(ctx, "/pay");
  if (!isAdmin(ctx.from.id)) return ctx.reply("Unauthorized");
  const parts = ctx.message.text.split(" ");
  if (parts.length < 2) return ctx.reply("Usage: /pay <withdrawalId>");
  const id = parts[1];
  try {
    const w = await withdrawalsCol.findOne({
      _id: new ObjectId(id),
      status: "pending",
    });
    if (!w) return ctx.reply("Not found.");
    await withdrawalsCol.updateOne(
      { _id: w._id },
      { $set: { status: "paid", paidAt: new Date(), paidBy: ctx.from.id } }
    );
    await usersCol.updateOne(
      { telegramId: w.userId },
      { $unset: { balanceLocked: "" } }
    );
    await ctx.reply("✅ Marked paid.");
    await bot.telegram.sendMessage(
      w.userId,
      `✅ Withdrawal ₹${w.amount} has been paid.`
    );
  } catch (e) {
    ctx.reply("Error: " + e.message);
  }
});

bot.command("cancelwithdraw", async (ctx) => {
  logCommand(ctx, "/cancelwithdraw");
  if (!isAdmin(ctx.from.id)) return ctx.reply("Unauthorized");
  const parts = ctx.message.text.split(" ");
  if (parts.length < 2) return ctx.reply("Usage: /cancelwithdraw <id>");
  const id = parts[1];
  const w = await withdrawalsCol.findOne({
    _id: new ObjectId(id),
    status: "pending",
  });
  if (!w) return ctx.reply("Not found.");
  await withdrawalsCol.updateOne(
    { _id: w._id },
    { $set: { status: "cancelled", cancelledAt: new Date() } }
  );
  await usersCol.updateOne(
    { telegramId: w.userId },
    {
      $inc: { balance: w.amount },
      $unset: { balanceLocked: "" },
    }
  );
  ctx.reply("❌ Cancelled and refunded.");
  bot.telegram.sendMessage(
    w.userId,
    "❌ Your withdrawal was cancelled and refunded."
  );
});

bot.command("admin_confirm", async (ctx) => {
  logCommand(ctx, "/admin_confirm");
  if (!isAdmin(ctx.from.id)) return ctx.reply("Unauthorized");
  const force = ctx.message.text.toLowerCase().includes("force");
  const result = await confirmPendingReferrals({ force });
  const confirmedCount = result?.confirmed?.length || 0;
  const invalidCount = result?.invalidated?.length || 0;
  await ctx.reply(
    `Manual confirmation complete (${force ? "forced" : "standard"}). Confirmed: ${confirmedCount}, invalidated: ${invalidCount}.`
  );
});

bot.command("admin_credit", async (ctx) => {
  logCommand(ctx, "/admin_credit");
  if (!isAdmin(ctx.from.id)) return ctx.reply("Unauthorized");
  const parts = ctx.message.text.split(" ").filter(Boolean);
  if (parts.length < 3)
    return ctx.reply("Usage: /admin_credit <telegramId> <amount> [note]");

  const targetId = Number(parts[1]);
  const amount = Number(parts[2]);
  const note = parts.slice(3).join(" ") || "manual adjustment";

  if (!Number.isFinite(targetId))
    return ctx.reply("telegramId must be a number.");
  if (!Number.isFinite(amount))
    return ctx.reply("amount must be a number.");
  if (amount === 0) return ctx.reply("Amount cannot be zero.");

  const user = await usersCol.findOne({ telegramId: targetId });
  if (!user) return ctx.reply("User not found.");

  await usersCol.updateOne(
    { telegramId: targetId },
    { $inc: { balance: amount } }
  );

  logger.info(
    `Admin ${ctx.from.id} credited ₹${amount} to ${targetId} (note: ${note})`
  );

  try {
    await bot.telegram.sendMessage(
      targetId,
      `💸 Admin credited ₹${amount.toFixed(
        2
      )} to your account. Note: ${note}`
    );
  } catch (notifyErr) {
    logger.debug(
      `Could not notify user ${targetId} about credit: ${notifyErr.message}`
    );
  }

  await ctx.reply(
    `Credited ₹${amount.toFixed(2)} to ${
      user.username ? "@" + user.username : targetId
    }. Note stored: ${note}`
  );
});

// ─────────────────────────────────────────────
// REFERRAL CONFIRMATION JOB
// ─────────────────────────────────────────────
async function confirmPendingReferrals(options = {}) {
  const { force = false } = options;
  const summary = { confirmed: [], invalidated: [] };
  const cutoff = new Date(Date.now() - CONFIRM_DELAY_HOURS * 3600 * 1000);
  const query = force
    ? { status: "pending" }
    : { status: "pending", createdAt: { $lte: cutoff } };
  const pendings = await pendingCol.find(query).toArray();
  if (pendings.length) {
    logger.info(
      `Confirm job picked ${pendings.length} pending referrals (${force ? "force" : "cutoff " + cutoff.toISOString()})`
    );
  }
  for (const p of pendings) {
    try {
      if (p.referredId === p.referrerId) {
        await pendingCol.updateOne(
          { _id: p._id },
          { $set: { status: "invalid", reason: "self_referral" } }
        );
        logger.warn(`Referral invalid (self): ${p._id}`);
        summary.invalidated.push(p._id);
        continue;
      }
      const referred = await usersCol.findOne({ telegramId: p.referredId });
      if (!referred) {
        await pendingCol.updateOne(
          { _id: p._id },
          { $set: { status: "invalid", reason: "no_user_record" } }
        );
        logger.warn(`Referral invalid (missing referred user): ${p._id}`);
        summary.invalidated.push(p._id);
        continue;
      }
      const updateResult = await usersCol.findOneAndUpdate(
        { telegramId: p.referrerId },
        { $inc: { balance: REFERRAL_REWARD, confirmedReferrals: 1 } },
        { returnDocument: "after" }
      );
      if (!updateResult.value) {
        logger.warn(`Referral invalid (missing referrer record): ${p._id}`);
        await pendingCol.updateOne(
          { _id: p._id },
          { $set: { status: "invalid", reason: "no_referrer_record" } }
        );
        summary.invalidated.push(p._id);
        continue;
      }
      await pendingCol.updateOne(
        { _id: p._id },
        { $set: { status: "confirmed", confirmedAt: new Date() } }
      );
      await refsCol.insertOne({
        referrerId: p.referrerId,
        referredId: p.referredId,
        referralCode: p.referralCode,
        confirmedAt: new Date(),
      });
      logger.info(`Referral confirmed: referrer ${p.referrerId} credited ₹${REFERRAL_REWARD}`);
      const newBalance = updateResult.value?.balance ?? "unknown";
      try {
        await bot.telegram.sendMessage(
          p.referrerId,
          `✅ Referral confirmed! Earned ₹${REFERRAL_REWARD}. Balance: ₹${newBalance}`
        );
      } catch (notifyErr) {
        logger.debug(
          `Could not send referral confirmation to ${p.referrerId}: ${notifyErr.message}`
        );
      }
      summary.confirmed.push(p._id);
    } catch (e) {
      logger.error("confirm job err: " + e.message);
    }
  }
  return summary;
}
setInterval(() => {
  confirmPendingReferrals().catch((err) =>
    logger.error(`confirm job interval error: ${err.message}`)
  );
}, 1000 * 60 * 5);

// ─────────────────────────────────────────────
// START BOT
// ─────────────────────────────────────────────
(async () => {
  try {
    await connectDB();
    const bootSummary = await confirmPendingReferrals();
    if (bootSummary.confirmed.length || bootSummary.invalidated.length) {
      logger.info(
        `Startup referral audit – confirmed ${bootSummary.confirmed.length}, invalidated ${bootSummary.invalidated.length}`
      );
    }
    await bot.launch();
    logger.info("🚀 Bot launched (polling) — secure version");
    process.once("SIGINT", () => bot.stop("SIGINT"));
    process.once("SIGTERM", () => bot.stop("SIGTERM"));
  } catch (e) {
    logger.error("Startup failed: " + e.message);
    process.exit(1);
  }
})();
