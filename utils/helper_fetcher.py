import aiohttp
import re
import os
from bs4 import BeautifulSoup
from markdownify import markdownify as md

def filter_bug_fixes(content: str) -> str:
    """Finds 'Bug Fixes' headers and truncates the text."""
    # Look for headers like **Bug Fixes**, **Known Issues**, # Bug Fixes, etc.
    pattern = r"(?i)(\*\*Bug\s*Fixes\*\*|\*\*Known\s*Issues\*\*|#+\s*Bug\s*Fixes|#+\s*Known\s*Issues)"
    match = re.search(pattern, content)
    
    if match:
        content = content[:match.start()].strip()
        content += "\n\n*...and various bug fixes. Click the link above to read the full list.*"
    
    if len(content) > 3900:
        content = content[:3900] + "...\n\n*[Patch notes truncated for length]*"
        
    return content

def format_steam_content(raw_content: str) -> str:
    """Formats steam HTML/BBCode content using markdownify."""
    # Convert common steam BBCode into HTML equivalents before markdownifying
    text = re.sub(r'\[b\](.*?)\[/b\]', r'<b>\1</b>', raw_content, flags=re.IGNORECASE)
    text = re.sub(r'\[i\](.*?)\[/i\]', r'<i>\1</i>', text, flags=re.IGNORECASE)
    text = re.sub(r'\[u\](.*?)\[/u\]', r'<u>\1</u>', text, flags=re.IGNORECASE)
    text = re.sub(r'\[s\](.*?)\[/s\]', r'<s>\1</s>', text, flags=re.IGNORECASE)
    text = re.sub(r'\[h([1-6])\](.*?)\[/h\1\]', r'<h\1>\2</h\1>', text, flags=re.IGNORECASE)
    text = re.sub(r'\[url=(.*?)\](.*?)\[/url\]', r'<a href="\1">\2</a>', text, flags=re.IGNORECASE)
    text = re.sub(r'\[list\]', r'<ul>', text, flags=re.IGNORECASE)
    text = re.sub(r'\[/list\]', r'</ul>', text, flags=re.IGNORECASE)
    text = re.sub(r'\[\*\](.*?)(?=\[\*\]|</ul>|$)', r'<li>\1</li>', text, flags=re.IGNORECASE)
    text = re.sub(r'\[p.*?\](.*?)\[/p\]', r'<p>\1</p>', text, flags=re.IGNORECASE)
    
    # Strip remaining generic tags that aren't mapped
    text = re.sub(r'\[/?.*?\]', '', text)
    
    # Convert to markdown
    markdown_text = md(text, heading_style="ATX", strip=['img', 'script', 'style'])
    
    # Clean up excess newlines
    markdown_text = re.sub(r'\n{3,}', '\n\n', markdown_text)
    
    return markdown_text.strip()

async def scrape_valorant_article(url: str) -> str:
    try:
        async with aiohttp.ClientSession() as session:
            async with session.get(url) as response:
                if response.status != 200:
                    return "Click the link to read the full details."
                html = await response.text()
                soup = BeautifulSoup(html, "html.parser")
                
                content_div = soup.find(lambda tag: tag.name == "div" and tag.has_attr("class") and any("global-styles" in c for c in tag.get("class")))
                if not content_div:
                    return "Click the link to read the full details."
                
                markdown_text = md(str(content_div), heading_style="ATX", strip=['a', 'img', 'script', 'style'])
                markdown_text = re.sub(r'\n{3,}', '\n\n', markdown_text)
                
                return markdown_text.strip()
    except Exception as e:
        return "Click the link to read the full details."

async def fetch_valorant_patch() -> dict | None:
    """Fetches the latest Valorant patch notes using the HenrikDev community API."""
    url = "https://api.henrikdev.xyz/valorant/v1/website/en-us"
    api_key = os.getenv("HENRIK_API_KEY")
    headers = {"Authorization": api_key} if api_key else {}
    
    async with aiohttp.ClientSession() as session:
        async with session.get(url, headers=headers) as response:
            if response.status == 200:
                data = await response.json()
                patch_notes = [item for item in data['data'] if item['category'] == 'patch_notes']
                if patch_notes:
                    latest = patch_notes[0]
                    content = await scrape_valorant_article(latest['url'])
                    clean_content = filter_bug_fixes(content)
                    
                    return {
                        "id": latest['url'],
                        "title": latest['title'],
                        "url": latest['url'],
                        "image": latest['banner_url'],
                        "content": clean_content 
                    }
    return None

async def fetch_overwatch_patch() -> dict | None:
    """Fetches the latest Overwatch 2 patch notes by scraping the official site."""
    url = "https://overwatch.blizzard.com/en-us/news/patch-notes/"
    headers = {"User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36"}
    
    try:
        async with aiohttp.ClientSession() as session:
            async with session.get(url, headers=headers) as response:
                if response.status == 200:
                    html = await response.text()
                    soup = BeautifulSoup(html, "html.parser")
                    latest_patch = soup.find('div', class_='PatchNotes-patch')
                    if latest_patch:
                        title_tag = latest_patch.find('h3', class_='PatchNotes-patchTitle')
                        title = title_tag.text.strip() if title_tag else "Overwatch 2 Updates"
                        
                        # Generate a unique ID using the title or section id
                        patch_id = latest_patch.get('id', title)
                        
                        markdown_text = md(str(latest_patch), heading_style="ATX", strip=['a', 'img', 'script', 'style'])
                        markdown_text = re.sub(r'\n{3,}', '\n\n', markdown_text).strip()
                        
                        clean_content = filter_bug_fixes(markdown_text)
                        
                        return {
                            "id": patch_id,
                            "title": title,
                            "url": url,
                            "image": None,
                            "content": clean_content
                        }
    except Exception as e:
        print(f"Error fetching Overwatch patch: {e}")
    return None

async def fetch_steam_patch(app_id: str) -> dict | None:
    """Fetches the latest Steam patch notes for a given App ID (e.g., Marvel Rivals)."""
    api_key = os.getenv("STEAM_API_KEY")
    url = f"https://api.steampowered.com/ISteamNews/GetNewsForApp/v2/?appid={app_id}&count=5"
    
    async with aiohttp.ClientSession() as session:
        async with session.get(url) as response:
            if response.status == 200:
                data = await response.json()
                news_items = data.get('appnews', {}).get('newsitems', [])
                
                for item in news_items:
                    if "patch" in item['title'].lower() or "update" in item['title'].lower() or "addendum" in item['title'].lower():
                        raw_content = item['contents']
                        
                        clean_content = format_steam_content(raw_content)
                        clean_content = filter_bug_fixes(clean_content)
                        
                        return {
                            "id": item['gid'],
                            "title": item['title'],
                            "url": item['url'],
                            "image": None,
                            "content": clean_content
                        }
    return None
