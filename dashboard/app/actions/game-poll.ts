"use server";

import { headers } from "next/headers";
import { revalidatePath } from "next/cache";

import { getActiveSession } from "@/auth";
import { env } from "@/lib/env";
import { recordAudit } from "@/lib/db/audit";
import { checkRateLimit, clientIp } from "@/lib/db/rate-limit";
import { addGame, updateGame, deleteGame, getDb, GamePollUnavailable } from "@/lib/game-poll/store";

/**
 * Game Poll mutations.
 *
 * Like every other action file here, each of these re-authenticates, pins the
 * guild, and rate-limits before touching anything, then records what changed —
 * a server action is a public endpoint, so none of that can be assumed from the
 * fact that our own form made the call. The callers wrap these in try/catch and
 * toast on failure, so throwing on a rejected request is the right contract.
 */

async function guard(guildId: string) {
  const session = await getActiveSession();
  if (!session) throw new Error("unauthorized");

  // The session proves admin in *our* guild; a request naming another is a bug
  // or a probe.
  if (guildId !== env.VIBEY_GUILD_ID) throw new Error("unknown server");

  const limit = checkRateLimit("mutation", session.user.id);
  if (!limit.allowed) throw new Error("rate limited");

  const headerList = await headers();
  return {
    session,
    ip: clientIp(headerList),
    userAgent: headerList.get("user-agent") ?? "",
  };
}

export async function saveGame(
  data: { id?: number; name: string; banner_url: string | null },
  guildId: string,
) {
  const { session, ip, userAgent } = await guard(guildId);
  if (!data.name) throw new Error("Name is required");

  if (data.id) {
    updateGame(data.id, data.name, data.banner_url);
  } else {
    addGame(data.name, data.banner_url);
  }

  recordAudit({
    actorId: session.user.id,
    actorName: session.user.name ?? session.user.id,
    guildId,
    action: data.id ? "game-poll.game.update" : "game-poll.game.add",
    target: data.id ? String(data.id) : data.name,
    changes: { name: { from: null, to: data.name }, banner_url: { from: null, to: data.banner_url } },
    ip,
    userAgent,
  });

  revalidatePath(`/dashboard/${guildId}/modules/game-poll/games`);
}

export async function removeGame(id: number, guildId: string) {
  const { session, ip, userAgent } = await guard(guildId);
  deleteGame(id);

  recordAudit({
    actorId: session.user.id,
    actorName: session.user.name ?? session.user.id,
    guildId,
    action: "game-poll.game.remove",
    target: String(id),
    ip,
    userAgent,
  });

  revalidatePath(`/dashboard/${guildId}/modules/game-poll/games`);
}

export async function setDraftPoll(
  gameIds: number[],
  endTime: Date | null,
  gameNightTime: Date | null,
  guildId: string,
  postNow: boolean = false,
) {
  const { session, ip, userAgent } = await guard(guildId);

  const idsStr = gameIds.join(",");
  const timeStr = endTime ? (endTime.getTime() / 1000).toString() : "";
  const gnTimeStr = gameNightTime ? (gameNightTime.getTime() / 1000).toString() : "";

  try {
    const db = getDb();
    db.transaction(() => {
      db.prepare("INSERT OR REPLACE INTO settings (key, value) VALUES ('draft_game_ids', ?)").run(idsStr);
      db.prepare("INSERT OR REPLACE INTO settings (key, value) VALUES ('draft_end_time', ?)").run(timeStr);
      db.prepare("INSERT OR REPLACE INTO settings (key, value) VALUES ('draft_game_night_time', ?)").run(gnTimeStr);
      if (postNow) {
        db.prepare("INSERT OR REPLACE INTO settings (key, value) VALUES ('dashboard_trigger_post', '1')").run();
      }
    })();
  } catch (error) {
    if (error instanceof GamePollUnavailable) {
      throw new Error("Vibey isn't running, so the poll can't be saved right now.");
    }
    throw error;
  }

  recordAudit({
    actorId: session.user.id,
    actorName: session.user.name ?? session.user.id,
    guildId,
    action: postNow ? "game-poll.poll.post" : "game-poll.poll.draft",
    target: idsStr,
    changes: {
      games: { from: null, to: idsStr },
      endTime: { from: null, to: timeStr },
      gameNightTime: { from: null, to: gnTimeStr },
    },
    ip,
    userAgent,
  });

  revalidatePath(`/dashboard/${guildId}/modules/game-poll`);
}
