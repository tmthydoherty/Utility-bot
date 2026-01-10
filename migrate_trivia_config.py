#!/usr/bin/env python3
"""
Trivia Config Migration Script
Converts old guild-specific format to new global format
"""

import json
import os
import copy
from datetime import datetime

# Path to your config file - adjust if needed
CONFIG_FILE = os.path.join(os.path.dirname(__file__), 'trivia_config.json')
BACKUP_FILE = CONFIG_FILE.replace('.json', f'_backup_{datetime.now().strftime("%Y%m%d_%H%M%S")}.json')

def load_old_config():
    """Load the existing config file"""
    if not os.path.exists(CONFIG_FILE):
        print(f"❌ Config file not found: {CONFIG_FILE}")
        return None
    
    try:
        with open(CONFIG_FILE, 'r', encoding='utf-8') as f:
            return json.load(f)
    except Exception as e:
        print(f"❌ Error loading config: {e}")
        return None

def migrate_config(old_config):
    """Migrate from old guild-specific format to new global format"""
    
    # Check if already in new format
    if "global_data" in old_config and "guild_settings" in old_config:
        print("✅ Config is already in the new format!")
        return old_config
    
    print("🔄 Starting migration from old format to new format...")
    
    # Initialize new structure
    new_config = {
        "global_data": {
            "scores": {},
            "user_stats": {},
            "question_cache": [],
            "daily_question_data": None,
            "daily_don_question_data": None,
            "daily_interactions": [],
            "daily_don_interactions": [],
            "daily_don_answer_times": [],
            "daily_answer_times": [],
            "yesterdays_recap_data": None,
            "blocked_users": [],
            "cheater_test_users": {}
        },
        "guild_settings": {}
    }
    
    # Track what we're merging
    guilds_processed = 0
    users_merged = set()
    
    # Process each guild from old format
    for guild_id, guild_data in old_config.items():
        if not isinstance(guild_data, dict):
            continue
            
        guilds_processed += 1
        print(f"\n📂 Processing guild: {guild_id}")
        
        # --- Merge SCORES (monthly) ---
        if "scores" in guild_data and isinstance(guild_data["scores"], dict):
            for user_id, score_data in guild_data["scores"].items():
                # If user already exists in global scores, keep the higher score
                if user_id in new_config["global_data"]["scores"]:
                    existing_score = new_config["global_data"]["scores"][user_id].get("score", 0)
                    new_score = score_data.get("score", 0) if isinstance(score_data, dict) else score_data
                    
                    if new_score > existing_score:
                        print(f"  📊 User {user_id}: Updating score from {existing_score} to {new_score}")
                        new_config["global_data"]["scores"][user_id] = score_data if isinstance(score_data, dict) else {"score": score_data}
                    else:
                        print(f"  📊 User {user_id}: Keeping existing score {existing_score} (new: {new_score})")
                else:
                    new_config["global_data"]["scores"][user_id] = score_data if isinstance(score_data, dict) else {"score": score_data}
                    users_merged.add(user_id)
        
        # --- Merge USER_STATS (all-time) ---
        if "user_stats" in guild_data and isinstance(guild_data["user_stats"], dict):
            for user_id, stats_data in guild_data["user_stats"].items():
                if user_id in new_config["global_data"]["user_stats"]:
                    # Merge stats - sum up correct/incorrect, keep highest streaks, etc.
                    existing = new_config["global_data"]["user_stats"][user_id]
                    
                    existing["correct"] = existing.get("correct", 0) + stats_data.get("correct", 0)
                    existing["incorrect"] = existing.get("incorrect", 0) + stats_data.get("incorrect", 0)
                    existing["longest_streak"] = max(existing.get("longest_streak", 0), stats_data.get("longest_streak", 0))
                    existing["current_streak"] = max(existing.get("current_streak", 0), stats_data.get("current_streak", 0))
                    existing["all_time_score"] = existing.get("all_time_score", 0) + stats_data.get("all_time_score", 0)
                    existing["don_declined"] = existing.get("don_declined", 0) + stats_data.get("don_declined", 0)
                    existing["don_accepted"] = existing.get("don_accepted", 0) + stats_data.get("don_accepted", 0)
                    existing["don_successes"] = existing.get("don_successes", 0) + stats_data.get("don_successes", 0)
                    
                    # Merge categories
                    for cat, cat_data in stats_data.get("categories", {}).items():
                        if cat not in existing.setdefault("categories", {}):
                            existing["categories"][cat] = cat_data
                        else:
                            existing["categories"][cat]["correct"] = existing["categories"][cat].get("correct", 0) + cat_data.get("correct", 0)
                            existing["categories"][cat]["incorrect"] = existing["categories"][cat].get("incorrect", 0) + cat_data.get("incorrect", 0)
                    
                    print(f"  📈 User {user_id}: Merged stats (all-time: {existing['all_time_score']})")
                else:
                    new_config["global_data"]["user_stats"][user_id] = copy.deepcopy(stats_data)
                    users_merged.add(user_id)
        
        # --- Copy GUILD-SPECIFIC SETTINGS to new section ---
        guild_settings = {
            "channel_id": guild_data.get("channel_id"),
            "enabled": guild_data.get("enabled", False),
            "admin_role_id": guild_data.get("admin_role_id"),
            "last_winner_announcement": guild_data.get("last_winner_announcement"),
            "last_posted_date": guild_data.get("last_posted_date"),
            "gateway_message_id": guild_data.get("gateway_message_id"),
            "last_post_hour": guild_data.get("last_post_hour", -1),
            "yesterdays_recap_data": guild_data.get("yesterdays_recap_data"),
            "winner_role_id": guild_data.get("winner_role_id"),
            "last_month_winner_id": guild_data.get("last_month_winner_id"),
            "anti_cheat_results_channel_id": guild_data.get("anti_cheat_results_channel_id")
        }
        
        new_config["guild_settings"][guild_id] = guild_settings
        print(f"  ⚙️  Guild settings migrated (enabled: {guild_settings['enabled']})")
        
        # --- Copy GLOBAL data from first guild only (question cache, blocked users, etc.) ---
        if guilds_processed == 1:  # Only take from first guild
            if "question_cache" in guild_data:
                new_config["global_data"]["question_cache"] = copy.deepcopy(guild_data["question_cache"])
                print(f"  🗂️  Copied question cache ({len(guild_data['question_cache'])} questions)")
            
            if "daily_question_data" in guild_data:
                new_config["global_data"]["daily_question_data"] = copy.deepcopy(guild_data["daily_question_data"])
            
            if "daily_don_question_data" in guild_data:
                new_config["global_data"]["daily_don_question_data"] = copy.deepcopy(guild_data["daily_don_question_data"])
            
            if "blocked_users" in guild_data:
                new_config["global_data"]["blocked_users"] = copy.deepcopy(guild_data["blocked_users"])
                print(f"  🚫 Copied blocked users ({len(guild_data['blocked_users'])} users)")
            
            if "cheater_test_users" in guild_data:
                new_config["global_data"]["cheater_test_users"] = copy.deepcopy(guild_data["cheater_test_users"])
    
    print(f"\n✅ Migration complete!")
    print(f"   - Processed {guilds_processed} guilds")
    print(f"   - Merged data for {len(users_merged)} users")
    print(f"   - Monthly scores: {len(new_config['global_data']['scores'])} users")
    print(f"   - All-time stats: {len(new_config['global_data']['user_stats'])} users")
    
    return new_config

def main():
    print("=" * 60)
    print("🔧 Trivia Config Migration Tool")
    print("=" * 60)
    
    # Load old config
    print(f"\n📖 Loading config from: {CONFIG_FILE}")
    old_config = load_old_config()
    if not old_config:
        return
    
    print(f"✅ Loaded {len(old_config)} top-level keys")
    
    # Create backup
    print(f"\n💾 Creating backup: {BACKUP_FILE}")
    try:
        with open(BACKUP_FILE, 'w', encoding='utf-8') as f:
            json.dump(old_config, f, indent=4)
        print("✅ Backup created successfully")
    except Exception as e:
        print(f"❌ Failed to create backup: {e}")
        return
    
    # Migrate
    new_config = migrate_config(old_config)
    if not new_config:
        return
    
    # Save new config
    print(f"\n💾 Saving migrated config to: {CONFIG_FILE}")
    try:
        with open(CONFIG_FILE, 'w', encoding='utf-8') as f:
            json.dump(new_config, f, indent=4)
        print("✅ Migration saved successfully!")
    except Exception as e:
        print(f"❌ Failed to save migrated config: {e}")
        print(f"💡 Your original config is safe in: {BACKUP_FILE}")
        return
    
    print("\n" + "=" * 60)
    print("🎉 Migration Complete!")
    print("=" * 60)
    print(f"✅ Original config backed up to: {BACKUP_FILE}")
    print(f"✅ New config saved to: {CONFIG_FILE}")
    print("\n⚠️  IMPORTANT: Test your bot thoroughly!")
    print("   If there are issues, restore from backup:")
    print(f"   cp {BACKUP_FILE} {CONFIG_FILE}")
    print("=" * 60)

if __name__ == "__main__":
    main()
