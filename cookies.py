#!/usr/bin/env python3
"""
🍪 COOKIE PROFILE GENERATOR - CAMOUFOX EDITION
=====================================
- Uses Camoufox (stealth Firefox) or CloakBrowser fallback
- Generates 10,000+ high-quality cookie profiles
- Headless mode for speed
- Maximum CPM sites for best cookies
"""

import os
import json
import time
import random
import threading
from pathlib import Path
from datetime import datetime
from fake_useragent import UserAgent

# Try Camoufox first, then CloakBrowser
CAMOUFOX = False
CLOAKBROWSER = False

try:
    from camoufox.addons import DefaultAddons, cf_options
    from camoufox.sync_api import sync_playwright as camoufox_pw
    CAMOUFOX = True
    print("✅ Camoufox loaded")
except ImportError:
    try:
        from cloakbrowser.browser import launch_context as cb_launch
        CLOAKBROWSER = True
        print("✅ CloakBrowser loaded")
    except ImportError:
        print("❌ No browser! Install: pip install camoufox cloakbrowser")
        exit(1)

# Config
PROFILES_DIR = Path("profiles")
PROFILES_DIR.mkdir(exist_ok=True)
DEFAULT_PROXIES = Path.home() / "Desktop" / "proxies.txt"

# Highest CPM sites
HIGH_CPM_SITES = [
    # Finance - $15-50 CPM
    "https://www.chase.com", "https://www.wellsfargo.com", "https://www.bankofamerica.com",
    "https://www.capitalone.com", "https://www.fidelity.com", "https://www.nerdwallet.com",
    # Insurance - $10-30 CPM  
    "https://www.geico.com", "https://www.progressive.com", "https://www.statefarm.com",
    # Crypto - $20-50 CPM
    "https://www.coinbase.com", "https://www.kraken.com", "https://www.coinmarketcap.com",
    # VPNs - $15-40 CPM
    "https://www.nordvpn.com", "https://www.expressvpn.com", "https://www.surfshark.com",
    # Tech/Software - $8-20 CPM
    "https://www.github.com", "https://www.stackoverflow.com", "https://www.digitalocean.com",
    # Shopping - $5-15 CPM
    "https://www.amazon.com", "https://www.ebay.com", "https://www.walmart.com",
    # Social/Traffic - $3-10 CPM
    "https://www.facebook.com", "https://www.reddit.com", "https://www.youtube.com",
    # Search - $5-10 CPM
    "https://www.google.com", "https://www.bing.com", "https://www.yahoo.com",
    # High Traffic
    "https://www.wikipedia.org", "https://www.cnn.com", "https://www.forbes.com",
    # Auto
    "https://www.cars.com", "https://www.kbb.com", "https://www.edmunds.com",
    # Real Estate
    "https://www.zillow.com", "https://www.realtor.com", "https://www.trulia.com",
]

def get_random_ua():
    return random.choice([
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 Chrome/124.0.0.0 Safari/537.36",
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 Chrome/123.0.0.0 Safari/537.36",
        "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 Chrome/124.0.0.0 Safari/537.36",
    ])

def get_viewport():
    return random.choice([
        {"width": 1366, "height": 768},
        {"width": 1440, "height": 900},
        {"width": 1920, "height": 1080},
    ])

def parse_proxy(s):
    import re
    s = s.strip()
    if s.startswith(("http://", "https://")): return s
    m = re.match(r'^([^:]+):(\d+):([^:]+):(.+)$', s)
    if m: return f"http://{m.group(3)}:{m.group(4)}@{m.group(1)}:{m.group(2)}"
    m = re.match(r'^([^:]+):(\d+)$', s)
    if m: return f"http://{m.group(1)}:{m.group(2)}"
    return None

def load_proxies(path):
    if not os.path.exists(path): return []
    proxies = []
    with open(path) as f:
        for line in f:
            line = line.strip()
            if line and not line.startswith('#'):
                p = parse_proxy(line)
                if p: proxies.append(p)
    print(f"✅ {len(proxies)} proxies")
    return proxies

def human_scroll(page):
    for _ in range(random.randint(3, 6)):
        page.evaluate(f"window.scrollBy({{top: {random.randint(200,800)}, behavior: 'smooth'}})")
        time.sleep(random.uniform(0.5, 1.5))

def generate_profile(idx, proxy, headless=True):
    path = PROFILES_DIR / f"profile_{idx}.json"
    meta = PROFILES_DIR / f"profile_{idx}_meta.json"
    
    if path.exists():
        return "skip"
    
    print(f"🍪 [{idx}] Generating...")
    
    browser = ctx = pw = None
    try:
        if CAMOUFOX:
            ff = cf_options(
                headless=headless, os="windows", humanize=True,
                geoip=False, proxy=proxy,
                block_webrtc=True,
                exclude_addons=[DefaultAddons.UBO],
            )
            pw = camoufox_pw().start()
            browser = pw.firefox.launch(**ff)
            ctx = browser.new_context(
                user_agent=get_random_ua(), viewport=get_viewport(),
                timezone_id=random.choice(["America/New_York", "America/Chicago"]),
                locale="en-US", color_scheme=random.choice(["dark", "light"]),
            )
        else:
            ctx = cb_launch(
                headless=headless, humanize=True, human_preset="careful",
                geoip=False, proxy=proxy, viewport=get_viewport(),
                color_scheme=random.choice(["dark", "light"]),
            )
        
        page = ctx.new_page()
        sites = random.sample(HIGH_CPM_SITES, 8)
        
        for site in sites:
            try:
                page.goto(site, wait_until="domcontentloaded", timeout=30000)
                time.sleep(random.uniform(2, 4))
                human_scroll(page)
                time.sleep(random.uniform(4, 10))
            except: continue
        
        cookies = ctx.cookies()
        with open(path, 'w') as f:
            json.dump({"cookies": cookies}, f)
        
        with open(meta, 'w') as f:
            json.dump({
                "proxy": proxy, "cookies": len(cookies), "sites": len(sites),
                "created": datetime.now().isoformat()
            }, f)
        
        print(f"✅ [{idx}] {len(cookies)} cookies")
        
        try: ctx.close()
        except: pass
        try: browser.close()
        except: pass
        try: pw.stop()
        except: pass
        return "success"
    except Exception as e:
        print(f"❌ [{idx}] {str(e)[:50]}")
        return "failed"

def main():
    print("="*50)
    print("🍪 COOKIE GENERATOR - CAMOUFOX")
    print("="*50)
    
    proxies = load_proxies(input("Proxies [default]: ").strip() or DEFAULT_PROXIES)
    if not proxies: return
    
    try: num = int(input("Profiles (10000): ") or 10000)
    except: num = 10000
    
    threads = int(input("Threads (5): ") or 5)
    headless = input("Headless (Y/n): ").strip().lower() != 'n'
    
    print(f"\n🚀 Generating {num} profiles...")
    
    for i in range(0, num, threads):
        t = []
        for j in range(min(threads, num - i)):
            idx = i + j + 1
            p = proxies[(idx-1) % len(proxies)]
            th = threading.Thread(target=generate_profile, args=(idx, p, headless))
            t.append(th)
            th.start()
            time.sleep(1)
        for th in t: th.join()
        print(f"Progress: {i+threads}/{num}")
        time.sleep(2)
    
    print("\n🎉 DONE!")

if __name__ == "__main__": main()
