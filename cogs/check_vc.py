import asyncio
import discord

async def main():
    client = discord.Client(intents=discord.Intents.all())
    
    @client.event
    async def on_ready():
        print(f'Logged in as {client.user}')
        vc_id = 1477873112736469145
        channel = client.get_channel(vc_id)
        if not channel:
            try:
                channel = await client.fetch_channel(vc_id)
            except discord.NotFound:
                print("Channel not found")
            except Exception as e:
                print(f"Error fetching: {e}")
        
        if channel:
            print(f"Channel {channel.name} exists! Members: {len(channel.members)}")
        
        await client.close()
        
    # Read token from env or a file if needed, maybe the bot runs under systemd?
    # I don't have the token. Let's just ask systemctl status
if __name__ == "__main__":
    pass
