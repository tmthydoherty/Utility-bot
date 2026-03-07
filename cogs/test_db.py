import sqlite3
conn=sqlite3.connect('/home/tmthy/Vibey/vc_data.db')
cursor = conn.execute("SELECT vc_id, owner_id, message_id, knock_mgmt_msg_id, thread_id, ghost, unlocked, bans, mute_knock_pings, guild_id, is_basic, last_seen_occupied, created_at FROM active_vcs")
rows = cursor.fetchall()
for r in rows:
    print(r)
