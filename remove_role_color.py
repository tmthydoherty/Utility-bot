import asyncio
import os
import discord

async def main():
    token = os.environ.get("DISCORD_TOKEN")
    if not token:
        from dotenv import load_dotenv
        load_dotenv("/home/tmthy/Vibey/.env")
        token = os.environ.get("DISCORD_TOKEN")

    client = discord.Client(intents=discord.Intents.default())
    
    @client.event
    async def on_ready():
        print(f'Logged in as {client.user}')
        role_id = 1455556375827451966
        found = False
        for guild in client.guilds:
            role = guild.get_role(role_id)
            if role:
                found = True
                print(f"Found role {role.name} in guild {guild.name}")
                try:
                    await role.edit(color=discord.Color.default(), reason="User requested via CLI")
                    print("Color removed.")
                except Exception as e:
                    print(f"Failed to edit role: {e}")
        
        if not found:
            print("Role not found in any guilds.")
        
        await client.close()

    await client.start(token)

asyncio.run(main())
