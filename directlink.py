"""
directlink.py — Stealth Layer v13  (MAX THROUGHPUT + Instant-Abort + Full Anonymity)
──────────────────────────────────────────────────────────────────────────────────────
NEW in v13 vs v12:
  [CRITICAL] DWELL changed to 1-3 s after page load → instant close → next profile
             Previous 15-25 s dwell was the #1 throughput killer (was ~100 imp/hr)
  [CRITICAL] NUM_PROFILES bumped to 20 concurrent sessions, semaphore = 20
             Pipeline never stalls — profiles overlap and batch loops are tight
  [CRITICAL] HEADLESS = True — no GPU/render overhead, 3-4× faster browser launch
  [HIGH]     Human micro-behavior injected in the 1-3 s window (mouse move + scroll)
             so sessions still look organic despite short dwell
  [HIGH]     Browser launch timeout added (30 s) to prevent hung launches
  [HIGH]     COOLDOWN removed (0 s) — batch restarts immediately after all profiles
  [HIGH]     Semaphore SEPARATED from profile count; profiles always fill the slot
  [MED]      Anonymous-proxy detection + full fault keyword list kept from v12
  [MED]      Relay start hardened with 8 s timeout and instant proxy swap
  [MED]      _visit() propagates ALL errors immediately (no silent swallow)
  [LOW]      Banner shows estimated max throughput at startup
"""

import asyncio
import random
import re
import signal
import sys
import time
from collections import deque
from contextlib import suppress
from pathlib import Path

from camoufox.async_api import AsyncCamoufox
from camoufox.addons import DefaultAddons
from loguru import logger
import warnings
warnings.filterwarnings("ignore")

from stealth_layer import (
    get_custom_fingerprint, build_stealth_js, BEHAVIORAL_JS,
    setup_route_guards, async_human_click, generate_human_mouse_path
)

# ── Logger ────────────────────────────────────────────────────────────────────
logger.remove()
logger.add(
    sys.stdout,
    format="<green>{time:HH:mm:ss}</green> | <level>{level:<8}</level> | {message}",
    colorize=True, level="INFO",       # INFO level in console = less noise, faster I/O
)
logger.add("automation.log", rotation="10 MB", retention="7 days", level="DEBUG")

# ════════════════════════════════════════════════════════════════════════════
#  CONFIG  ← tune these to taste
# ════════════════════════════════════════════════════════════════════════════
TARGET_URLS = [
    "https://www.profitablecpmratenetwork.com/dkxcd57m?key=f5c846183208f82086e57312b3b9d27e",
]

# ── Throughput levers ────────────────────────────────────────────────────────
NUM_PROFILES        = 12         # concurrent browser slots per batch (12 is safe in headless)
DWELL_PER_URL       = (6, 12)    # balanced middle-ground: clears 5s IAB viewability but keeps imp/hr high
REFERRER_DEPTH      = True       # load actual social/search referrer page before jumping to target
HEADLESS            = False       # True = invisible (no GPU fight = faster + reliable loads)
PROFILE_TIMEOUT     = 65         # force-kill a stuck profile after N seconds
COOLDOWN            = (0, 1)    # seconds between cycles: nearly zero
MAX_RETRIES         = 3          # attempts per slot, each with a fresh proxy
PROXY_FILE          = Path.home() / "Desktop" / "proxies.txt"
TARGET_DEVICE       = "windows"  # overridden by startup menu

# ── Proxy pool settings ──────────────────────────────────────────────────────
PROXY_QUARANTINE_S  = 45         # dead-proxy cool-down (seconds)
NAV_TIMEOUT_MS      = 30_000     # page.goto timeout — increased for wait_until=load
BROWSER_LAUNCH_TO   = 30.0       # asyncio timeout for browser launch (seconds)
RELAY_START_TO      = 8.0        # asyncio timeout for relay start (seconds)
# ════════════════════════════════════════════════════════════════════════════


# ════════════════════════════════════════════════════════════════════════════
#  TRAFFIC SOURCE SPOOFING ENGINE
# ════════════════════════════════════════════════════════════════════════════
_TRAFFIC_SOURCES = [
    {"name": "instagram_inapp",  "weight": 22, "referer": "https://l.instagram.com/",
     "sec_fetch_site": "cross-site", "sec_fetch_mode": "navigate",
     "sec_fetch_dest": "document",  "sec_fetch_user": None,
     "extra": {"X-Ig-App-Id": "936619743392459", "X-Requested-With": "com.instagram.android"}},
    {"name": "telegram_preview", "weight": 18, "referer": "https://t.me/",
     "sec_fetch_site": "cross-site", "sec_fetch_mode": "navigate",
     "sec_fetch_dest": "document",  "sec_fetch_user": None,  "extra": {}},
    {"name": "pinterest",        "weight": 12, "referer": "https://www.pinterest.com/",
     "sec_fetch_site": "cross-site", "sec_fetch_mode": "navigate",
     "sec_fetch_dest": "document",  "sec_fetch_user": "?1",  "extra": {}},
    {"name": "twitter_x",        "weight": 12, "referer": "https://t.co/",
     "sec_fetch_site": "cross-site", "sec_fetch_mode": "navigate",
     "sec_fetch_dest": "document",  "sec_fetch_user": "?1",  "extra": {}},
    {"name": "facebook",         "weight": 10, "referer": "https://www.facebook.com/",
     "sec_fetch_site": "cross-site", "sec_fetch_mode": "navigate",
     "sec_fetch_dest": "document",  "sec_fetch_user": "?1",  "extra": {}},
    {"name": "email_newsletter",  "weight": 8,  "referer": "",
     "sec_fetch_site": "none",      "sec_fetch_mode": "navigate",
     "sec_fetch_dest": "document",  "sec_fetch_user": "?1",  "extra": {}},
    {"name": "reddit",           "weight": 10, "referer": "https://www.reddit.com/",
     "sec_fetch_site": "cross-site", "sec_fetch_mode": "navigate",
     "sec_fetch_dest": "document",  "sec_fetch_user": "?1",  "extra": {}},
    {"name": "google_organic",   "weight": 8,  "referer": "https://www.google.com/",
     "sec_fetch_site": "cross-site", "sec_fetch_mode": "navigate",
     "sec_fetch_dest": "document",  "sec_fetch_user": "?1",  "extra": {}},
]
_SRC_WEIGHTS = [s["weight"] for s in _TRAFFIC_SOURCES]


def _pick_traffic_source(fp: dict) -> tuple:
    src    = random.choices(_TRAFFIC_SOURCES, weights=_SRC_WEIGHTS, k=1)[0]
    ua     = fp.get("user_agent", "")
    ref    = src["referer"]
    hdrs: dict = {}

    # Sec-Fetch-* headers
    if src["sec_fetch_site"]:
        hdrs["Sec-Fetch-Site"] = src["sec_fetch_site"]
    if src["sec_fetch_mode"]:
        hdrs["Sec-Fetch-Mode"] = src["sec_fetch_mode"]
    if src["sec_fetch_dest"]:
        hdrs["Sec-Fetch-Dest"] = src["sec_fetch_dest"]
    if src["sec_fetch_user"]:
        hdrs["Sec-Fetch-User"] = src["sec_fetch_user"]

    if ref:
        hdrs["Referer"] = ref

    # Sec-CH-UA — spoof based on platform
    if "Android" in ua:
        ch_ua = '"Chromium";v="124", "Android WebView";v="124", "Not-A.Brand";v="99"'
        hdrs["Sec-CH-UA-Mobile"]   = "?1"
        hdrs["Sec-CH-UA-Platform"] = '"Android"'
    elif "Win" in ua:
        ch_ua = '"Chromium";v="124", "Google Chrome";v="124", "Not-A.Brand";v="99"'
        hdrs["Sec-CH-UA-Mobile"]   = "?0"
        hdrs["Sec-CH-UA-Platform"] = '"Windows"'
    elif "Mac" in ua:
        ch_ua = '"Chromium";v="124", "Google Chrome";v="124", "Not-A.Brand";v="99"'
        hdrs["Sec-CH-UA-Mobile"]   = "?0"
        hdrs["Sec-CH-UA-Platform"] = '"macOS"'
    else:
        ch_ua = '"Chromium";v="124", "Google Chrome";v="124", "Not-A.Brand";v="99"'
        hdrs["Sec-CH-UA-Mobile"]   = "?0"
        hdrs["Sec-CH-UA-Platform"] = '"Linux"'
    hdrs["Sec-CH-UA"] = ch_ua

    hdrs.update(src.get("extra", {}))
    return hdrs, src["name"]


# ════════════════════════════════════════════════════════════════════════════
#  PROXY LOADER & PARSER
# ════════════════════════════════════════════════════════════════════════════

def _load_proxies(path: Path) -> list[str]:
    if not path.exists():
        logger.error(f"Proxy file not found: {path}")
        return []
    lines = [l.strip() for l in path.read_text().splitlines() if l.strip() and not l.startswith("#")]
    logger.info(f"  Loaded {len(lines)} proxies from {path.name}")
    return lines


def _parse_proxy(raw: str) -> dict | None:
    """
    Parse ANY proxy format → {server, username, password}

    Handles ALL formats including new owlproxy SOCKS5 colon format:

      ① socks5://host:port:user:pass        ← NEW owlproxy colon format (no @)
      ② socks5://user:pass@host:port        ← standard RFC format
      ③ socks4://user:pass@host:port
      ④ http://user:pass@host:port
      ⑤ host:port:user:pass                ← plain colon, no scheme
      ⑥ host:port                           ← no auth

    Detection heuristic for ①/⑤ (colon-separated, no @):
      host never contains digits-only segments before port.
      port is always a pure integer 1-65535.
      We check: parts[0]=hostname (has dots), parts[1]=port(digits), rest=creds
    """
    s = raw.strip()
    if not s:
        return None

    scheme = "http"
    # Detect explicit scheme
    for sc in ("socks5", "socks4", "socks4a", "http", "https"):
        if s.lower().startswith(sc + "://"):
            scheme = sc
            s = s[len(sc) + 3:]
            break

    # Check for @-separator (standard RFC format)
    if "@" in s:
        creds, hostport = s.rsplit("@", 1)
        user, _, pw = creds.partition(":")
        host, _, port = hostport.rpartition(":")
        if not port.isdigit():
            return None
        return {
            "server":   f"{scheme}://{host}:{port}",
            "username": user,
            "password": pw,
            "_scheme":  scheme,
            "_raw":     raw,
        }

    # No @ → try colon-split heuristic
    parts = s.split(":")
    if len(parts) >= 2 and parts[1].isdigit() and "." in parts[0]:
        host = parts[0]
        port = parts[1]
        user = parts[2] if len(parts) > 2 else ""
        pw   = ":".join(parts[3:]) if len(parts) > 3 else ""
        return {
            "server":   f"{scheme}://{host}:{port}",
            "username": user,
            "password": pw,
            "_scheme":  scheme,
            "_raw":     raw,
        }

    return None


_INTERNAL_KEYS = {"_scheme", "_raw"}


def _pw(proxy_cfg: dict) -> dict:
    """Strip internal metadata keys before passing proxy dict to Playwright/Camoufox."""
    return {k: v for k, v in proxy_cfg.items() if k not in _INTERNAL_KEYS}


async def _resolve_proxy_cfg(cfg: dict) -> dict:
    """Resolve the proxy server hostname to an IP address to prevent DNS issues."""
    try:
        server = cfg.get("server", "")
        # Extract host and port from server string like 'socks5://host:port'
        parts = server.split("://")
        if len(parts) < 2:
            return cfg
        hostport = parts[1]
        if ":" not in hostport:
            return cfg
        host, port_str = hostport.rsplit(":", 1)
        if not port_str.isdigit():
            return cfg
        port = int(port_str)

        loop = asyncio.get_event_loop()
        for attempt in range(3):
            try:
                infos = await asyncio.wait_for(
                    loop.getaddrinfo(host, port, family=0, type=0, proto=0),
                    timeout=5.0
                )
                if infos:
                    # Prefer IPv4
                    import socket
                    v4 = [i for i in infos if i[0] == socket.AF_INET]
                    ip = v4[0][4][0] if v4 else infos[0][4][0]
                    scheme = cfg.get("_scheme", "http")
                    new_cfg = dict(cfg)
                    new_cfg["server"] = f"{scheme}://{ip}:{port}"
                    logger.debug(f"Resolved {host} → {ip}")
                    return new_cfg
            except Exception:
                if attempt < 2:
                    await asyncio.sleep(0.3)
    except Exception:
        pass
    return cfg


async def _exact_geo_lookup(relay_port: int, raw_fallback_str: str) -> dict:
    """
    Perform a 100% accurate Geo/TZ lookup over the active SOCKS5 relay.
    Prevents TZ drift compared to raw string heuristics.
    Fallback to string analysis if the API fails or rate-limits.
    """
    try:
        proc = await asyncio.create_subprocess_exec(
            "curl", "-s", "--max-time", "4",
            f"--proxy", f"socks5h://127.0.0.1:{relay_port}",
            "http://ip-api.com/json/?fields=countryCode,timezone,query",
            stdout=asyncio.subprocess.PIPE,
            stderr=asyncio.subprocess.DEVNULL
        )
        stdout, _ = await proc.communicate()
        import json
        data = json.loads(stdout)
        if "timezone" in data and "countryCode" in data:
            logger.debug(f"  Geolocated {data.get('query')} → {data['countryCode']}, {data['timezone']}")
            locale = "en-US"
            # Best-effort locale matching based on true country
            if data["countryCode"] == "GB": locale = "en-GB"
            elif data["countryCode"] == "DE": locale = "de-DE"
            elif data["countryCode"] == "FR": locale = "fr-FR"
            elif data["countryCode"] == "CA": locale = "en-CA"
            elif data["countryCode"] == "BR": locale = "pt-BR"
            elif data["countryCode"] == "ES": locale = "es-ES"
            elif data["countryCode"] == "IN": locale = "en-IN"
            return {"country": data["countryCode"], "timezone": data["timezone"], "locale": locale}
    except Exception:
        pass

    # Fallback to pure string heuristic
    raw_upper = raw_fallback_str.upper()
    tz_map = {
        "US": ("America/New_York",    "en-US"),
        "GB": ("Europe/London",       "en-GB"),
        "CA": ("America/Toronto",     "en-CA"),
        "DE": ("Europe/Berlin",       "de-DE"),
        "FR": ("Europe/Paris",        "fr-FR"),
        "BR": ("America/Sao_Paulo",   "pt-BR"),
        "IN": ("Asia/Kolkata",        "en-IN"),
        "ES": ("Europe/Madrid",       "es-ES"),
        "AU": ("Australia/Sydney",    "en-AU"),
        "PL": ("Europe/Warsaw",       "pl-PL"),
        "IT": ("Europe/Rome",         "it-IT"),
        "JP": ("Asia/Tokyo",          "ja-JP"),
    }
    for cc, (tz, loc) in tz_map.items():
        if f"-CC-{cc}-" in raw_upper or f"_{cc}_" in raw_upper or raw_upper.endswith(cc):
            return {"country": cc, "timezone": tz, "locale": loc}
    return {"country": "US", "timezone": "America/New_York", "locale": "en-US"}

def _geo_from_proxy(raw: str) -> dict:
    """
    Detect country from ANY position in the proxy string.
    Scans the FULL raw string including username tokens.
    """
    raw_upper = raw.upper()
    tz_map = {
        "US": ("America/New_York",    "en-US"),
        "GB": ("Europe/London",       "en-GB"),
        "CA": ("America/Toronto",     "en-CA"),
        "DE": ("Europe/Berlin",       "de-DE"),
        "FR": ("Europe/Paris",        "fr-FR"),
        "BR": ("America/Sao_Paulo",   "pt-BR"),
        "IN": ("Asia/Kolkata",        "en-IN"),
        "ES": ("Europe/Madrid",       "es-ES"),
        "AU": ("Australia/Sydney",    "en-AU"),
        "PL": ("Europe/Warsaw",       "pl-PL"),
        "IT": ("Europe/Rome",         "it-IT"),
        "JP": ("Asia/Tokyo",          "ja-JP"),
    }
    for cc, (tz, loc) in tz_map.items():
        if f"-CC-{cc}-" in raw_upper or f"_{cc}_" in raw_upper or raw_upper.endswith(cc):
            return {"country": cc, "timezone": tz, "locale": loc}
    return {"country": "US", "timezone": "America/New_York", "locale": "en-US"}


# ════════════════════════════════════════════════════════════════════════════
#  PROXY FAULT DETECTION  (proxy errors + anonymous-proxy detection)
# ════════════════════════════════════════════════════════════════════════════

# ── HARD proxy faults → quarantine proxy immediately ─────────────────────────
# These errors are caused BY THE PROXY being dead/misconfigured/banned.
# DO NOT put destination-side errors here (net_reset, connection_reset, timeout)
# because those quarantine perfectly good proxies!
_PROXY_FAULT_HARD = frozenset({
    "failed to connect to proxy",
    "407 proxy",
    "proxy authentication",
    "cannot connect to proxy",
    "tunnel connection failed",
    "econnrefused",
    "connection refused",
    "proxy connect",
    "proxyerror",
    "no route to host",
    "network unreachable",
    "name or service not known",
    "name resolution failed",   # only when caused by proxy DNS
    "getaddrinfo failed",
    "socks connection failed",
    "socks5 handshake",
    "socks4 connection",
    "authentication failed",
    "ns_error_connection_refused",
    "err_proxy_connection_failed",
    "err_tunnel_connection_failed",
    "errno -3",
    "temporary failure in name resolution",
    "ns_error_unknown_host",
    # ── Anonymous / transparent proxy / bot detection ─────────────────────
    "anonymous proxy",
    "your ip has been detected",
    "proxy detected",
    "vpn detected",
    "datacenter ip",
    "bot detected",
    "ip is blocked",
    "sec_error_unknown_issuer",
    "ssl_error_rx_record_too_long",
})

# ── SOFT page errors → release proxy clean (not its fault) ───────────────────
# NS_ERROR_NET_RESET, connection_reset, timeouts, empty responses are often
# caused by the DESTINATION site rate-limiting or closing connections —
# the proxy worked fine. Do NOT quarantine for these.
_PAGE_FAULT_KEYWORDS = frozenset({
    "ns_error_net_reset",
    "net::err_connection_reset",
    "err_connection_reset",
    "ns_error_net_timeout",
    "err_connection_timed_out",
    "err_empty_response",
    "timeout",
    "ssl_error",
    "access denied",
    "403",
    "your connection is not private",
})


def _is_proxy_fault(err: str) -> bool:
    """True only for HARD proxy faults — those where the proxy itself is broken.
    Destination-side resets/timeouts are NOT proxy faults."""
    low = err.lower()
    # Check hard proxy fault keywords (exact substring match)
    return any(k in low for k in _PROXY_FAULT_HARD)


# ════════════════════════════════════════════════════════════════════════════
#  SMART PROXY POOL  (TRUE RANDOM, quarantine, collision prevention)
# ════════════════════════════════════════════════════════════════════════════

class ProxyPool:
    """
    Async-safe proxy pool with:
    - TRUE RANDOM selection (not round-robin) — avoids ordered fingerprinting
    - Unique IP bag logic — guarantees all proxies rotated before repetition
    - Per-proxy in-use locking to prevent collision
    - Quarantine on proxy-level auth/connect failures only
    """

    def __init__(self, raw_list: list[str]):
        self._all       = raw_list[:]
        self._bag       = raw_list[:]
        random.shuffle(self._bag)
        self._lock      = asyncio.Lock()
        self._in_use:   set[str]           = set()
        self._quarantine: dict[str, float] = {}  # proxy → expiry monotonic time

    def _available(self) -> list[str]:
        now = time.monotonic()
        return [
            p for p in self._all
            if p not in self._in_use and now >= self._quarantine.get(p, 0)
        ]

    def live_count(self) -> int:
        now = time.monotonic()
        return sum(1 for p in self._all if now >= self._quarantine.get(p, 0))

    async def acquire(self, wait_s: float = 30.0) -> str | None:
        """True-random acquire without replacement until bag is empty."""
        deadline = time.monotonic() + wait_s
        while time.monotonic() < deadline:
            async with self._lock:
                now = time.monotonic()
                avail_bag = [
                    p for p in self._bag
                    if p not in self._in_use and now >= self._quarantine.get(p, 0)
                ]
                if avail_bag:
                    chosen = random.choice(avail_bag)
                    self._bag.remove(chosen)
                    self._in_use.add(chosen)
                    return chosen

                avail_glob = self._available()
                if avail_glob:
                    # Refill bag
                    self._bag = self._all[:]
                    random.shuffle(self._bag)
            await asyncio.sleep(0.2)
        return None

    async def release(self, proxy: str, *, quarantine: bool = False):
        async with self._lock:
            self._in_use.discard(proxy)
            if quarantine:
                self._quarantine[proxy] = time.monotonic() + PROXY_QUARANTINE_S
                logger.warning(f"  ⚠ Quarantined {PROXY_QUARANTINE_S}s: {proxy[:55]}…")
            else:
                self._quarantine.pop(proxy, None)

    def stats(self) -> str:
        now = time.monotonic()
        q = sum(1 for exp in self._quarantine.values() if now < exp)
        return (
            f"proxies={len(self._all)}  live={self.live_count()}"
            f"  in-use={len(self._in_use)}  quarantined={q}"
        )


# ════════════════════════════════════════════════════════════════════════════
#  IMPRESSION COUNTER  (rolling-window imp/hr dashboard)
# ════════════════════════════════════════════════════════════════════════════

class ImpCounter:
    def __init__(self, window_s: int = 300):
        self._window = window_s
        self._times: deque = deque()
        self._lock  = asyncio.Lock()
        self._total = 0

    async def record(self, n: int = 1):
        async with self._lock:
            now = time.monotonic()
            for _ in range(n):
                self._times.append(now)
            self._total += n
            cutoff = now - self._window
            while self._times and self._times[0] < cutoff:
                self._times.popleft()

    async def rate(self) -> float:
        async with self._lock:
            if len(self._times) < 2:
                return 0.0
            span = self._times[-1] - self._times[0]
            return (len(self._times) / span * 3600) if span >= 1 else 0.0

    async def total(self) -> int:
        async with self._lock:
            return self._total


# ── Module-level singletons ──────────────────────────────────────────────────
PROXIES: list[str] = _load_proxies(PROXY_FILE)
_pool: ProxyPool  = None   # type: ignore
_imp:  ImpCounter = None   # type: ignore


# ════════════════════════════════════════════════════════════════════════════
#  FIREFOX PREFS  — Anti-leak, SOCKS5-aware, anti-detection hardened
# ════════════════════════════════════════════════════════════════════════════

def _build_ff_prefs(proxy_cfg: dict) -> dict:
    """
    Build Firefox preferences tailored to proxy type.
    Prevents DNS leaks, WebRTC leaks, headless detection, and maximises stealth.
    """
    scheme   = proxy_cfg.get("_scheme", "http")
    is_socks = scheme in ("socks5", "socks4", "socks4a")

    prefs = {
        # ── Anti-leak (DNS + IP) ───────────────────────────────────────────
        "network.trr.mode":                                   5,   # Disable DoH
        "network.captive-portal-service.enabled":             False,
        "network.connectivity-service.enabled":               False,
        "network.http.speculative-parallel-limit":            0,
        "network.predictor.enabled":                          False,
        "network.prefetch-next":                              False,
        "network.dns.disablePrefetch":                        True,
        "network.dns.disableIPv6":                            True,
        "network.proxy.socks_remote_dns":                     True,
        "network.automatic-ntlm-auth.allow-non-fqdn":         False,
        "network.automatic-ntlm-auth.trusted-uris":           "",
        "network.negotiate-auth.trusted-uris":                "",
        "network.negotiate-auth.delegation-uris":             "",
        # ── SSL / TLS bypass ──────────────────────────────────────────────
        "security.tls.version.min":                           1,
        "security.tls.version.max":                           4,
        "security.enterprise_roots.enabled":                  True,
        "security.cert_pinning.enforcement_level":            0,
        "security.OCSP.enabled":                              0,
        "network.stricttransportsecurity.preloadlist":        False,
        "security.pki.sha1_enforcement_level":                0,
        "security.tls.insecure_fallback_hosts.use_static_list": False,
        "security.insecure_field_warning.contextual.enabled": False,
        # ── WebRTC leak control ───────────────────────────────────────────
        "media.peerconnection.enabled":                       True,
        "media.peerconnection.ice.default_address_only":      True,
        "media.peerconnection.ice.proxy_only_if_behind_proxy":True,
        "media.peerconnection.ice.relay_only":                False,
        # ── Headless detection bypass ─────────────────────────────────────
        # These prefs prevent Firefox headless mode from leaking its
        # headless state through layout/rendering metrics.
        "layout.css.devPixelsPerPx":                          "-1.0",  # use real DPR from fp
        "widget.window-transforms.disabled":                  False,
        "gfx.offscreencanvas.enabled":                        True,
        "media.video_stats.enabled":                          True,
        "dom.min_background_timer_interval":                  4,      # match real browser
        "dom.min_timeout_value":                              4,
        "gfx.canvas.remote":                                  True,
        "layers.acceleration.disabled":                       False,
        "layers.prefer-opengl":                               True,
        # ── Misc / performance ────────────────────────────────────────────
        "browser.safebrowsing.malware.enabled":               False,
        "browser.safebrowsing.phishing.enabled":              False,
        "app.update.auto":                                    False,
        "app.update.enabled":                                 False,
        "datareporting.healthreport.uploadEnabled":           False,
        "toolkit.telemetry.enabled":                          False,
        "network.cookie.cookieBehavior":                      0,
        "browser.download.useDownloadDir":                    True,
        "browser.download.folderList":                        2,
        "browser.download.manager.showWhenStarting":          False,
        "browser.helperApps.neverAsk.saveToDisk":
            "application/octet-stream,application/zip,application/pdf,text/csv,application/json",
        "browser.helperApps.alwaysAsk.force":                 False,
        "pdfjs.disabled":                                     True,
    }
    return prefs


def _build_headless_bypass_js(fp: dict) -> str:
    """
    Per-session JS that PINS all visual/window size properties to the
    fingerprint values, overriding any headless-mode 0x0 defaults.

    Runs AFTER build_stealth_js so it takes final precedence.
    Key checks ad networks make:
      - window.outerWidth / outerHeight  (0 in headless = instant detection)
      - window.innerWidth / innerHeight
      - screen.width / screen.height / screen.availWidth / screen.availHeight
      - window.devicePixelRatio  (1.0 in headless = detection flag)
      - window.screenX / screenY  (always 0 in headless)
      - window.screenLeft / screenTop
    """
    vw    = fp["viewport"]["width"]
    vh    = fp["viewport"]["height"]
    dpr   = fp["dpr"]
    # Simulate a browser window with a realistic OS title bar (74px is standard)
    outer_h = vh + 74
    # Random window position — real users don't always open at 0,0
    screen_x = random.randint(0, max(0, 1920 - vw))
    screen_y = random.randint(0, 80)   # taskbar offset

    return f"""
(function() {{
  'use strict';
  if (window.__headlessBypass) return;
  window.__headlessBypass = true;

  // Pin exact viewport & screen dimensions from fingerprint
  const VW = {vw}, VH = {vh}, DPR = {dpr};
  const OUTER_H = {outer_h};  // viewport + browser chrome (title bar + tabs)
  const SX = {screen_x}, SY = {screen_y};

  function defWin(key, val) {{
    try {{
      Object.defineProperty(window, key, {{
        get: () => val,
        configurable: true, enumerable: true
      }});
    }} catch(e) {{}}
  }}
  function defScr(key, val) {{
    try {{
      Object.defineProperty(screen, key, {{
        get: () => val,
        configurable: true, enumerable: true
      }});
    }} catch(e) {{}}
  }}

  // ── Window dimensions ──────────────────────────────────────────────
  defWin('outerWidth',        VW);
  defWin('outerHeight',       OUTER_H);   // realistic: includes browser chrome
  defWin('innerWidth',        VW);
  defWin('innerHeight',       VH);
  defWin('devicePixelRatio',  DPR);       // fingerprint DPR, not 1.0 headless default
  defWin('screenX',           SX);
  defWin('screenY',           SY);
  defWin('screenLeft',        SX);
  defWin('screenTop',         SY);
  defWin('scrollX',           0);
  defWin('scrollY',           0);
  defWin('pageXOffset',       0);
  defWin('pageYOffset',       0);

  // ── Screen dimensions ──────────────────────────────────────────────
  defScr('width',             VW);
  defScr('height',            VH + 80);   // full physical screen slightly larger
  defScr('availWidth',        VW);
  defScr('availHeight',       VH + 32);   // subtract taskbar (~32px)
  defScr('availLeft',         0);
  defScr('availTop',          0);
  defScr('colorDepth',        24);
  defScr('pixelDepth',        24);

  // ── document.documentElement size ─────────────────────────────────
  try {{
    const _origOff = Object.getOwnPropertyDescriptor(Element.prototype, 'clientWidth');
    // clientWidth/Height are read-only per-element, we rely on viewport CSS being set
  }} catch(e) {{}}

  // ── Visual viewport API ────────────────────────────────────────────
  try {{
    if (window.visualViewport) {{
      Object.defineProperty(window.visualViewport, 'width',  {{ get: () => VW,  configurable: true }});
      Object.defineProperty(window.visualViewport, 'height', {{ get: () => VH,  configurable: true }});
      Object.defineProperty(window.visualViewport, 'scale',  {{ get: () => 1.0, configurable: true }});
    }}
  }} catch(e) {{}}

  // ── matchMedia — make headless look like a real display ───────────
  try {{
    const _origMM = window.matchMedia.bind(window);
    window.matchMedia = function(q) {{
      const mql = _origMM(q);
      // Force screen-size queries to return sane values
      if (q.includes('max-width') || q.includes('min-width') ||
          q.includes('max-height') || q.includes('min-height')) {{
        return mql;  // let Playwright's CSS engine handle these
      }}
      // Fix: headless always reports 'screen' as 0-width which breaks ads
      return mql;
    }};
  }} catch(e) {{}}

}})();
"""


# ════════════════════════════════════════════════════════════════════════════
#  LOCAL SOCKS5 RELAY  (Bypasses Firefox SOCKS5-Auth limitations)
# ════════════════════════════════════════════════════════════════════════════

class LocalSocks5Relay:
    def __init__(self, up_host: str, up_port: int, user: str, pw: str):
        self.up_host     = up_host
        self.up_port     = int(up_port)
        self.user        = user
        self.pw          = pw
        self.server      = None
        self.port        = 0
        self.up_ip       = None
        self._connections: list = []

    async def _handle_client(self, reader, writer):
        self._connections.append(writer)
        up_writer = None
        try:
            # 1. SOCKS5 Greeting
            header = await asyncio.wait_for(reader.read(2), timeout=10.0)
            if not header or len(header) < 2 or header[0] != 5:
                return
            nmethods = header[1]
            await reader.read(nmethods)
            writer.write(b'\x05\x00')
            await writer.drain()

            # 2. Connection Request
            req_header = await asyncio.wait_for(reader.read(4), timeout=10.0)
            if len(req_header) < 4:
                return
            atyp = req_header[3]
            if atyp == 1:   # IPv4
                dst_addr = await reader.read(4)
            elif atyp == 3: # Domain
                dst_len_b = await reader.read(1)
                if not dst_len_b:
                    return
                dst_addr = dst_len_b + await reader.read(dst_len_b[0])
            elif atyp == 4: # IPv6
                dst_addr = await reader.read(16)
            else:
                return
            dst_port_bytes = await reader.read(2)

            # 3. Connect to upstream proxy
            connect_host = self.up_ip or self.up_host
            up_reader, up_writer = await asyncio.wait_for(
                asyncio.open_connection(connect_host, self.up_port),
                timeout=15.0
            )
            self._connections.append(up_writer)

            # 4. Authenticate with upstream
            if self.user and self.pw:
                up_writer.write(b'\x05\x01\x02')
                await up_writer.drain()
                resp = await asyncio.wait_for(up_reader.read(2), timeout=10.0)
                if resp != b'\x05\x02':
                    raise Exception("Upstream auth method rejected")
                u_b = self.user.encode('utf-8')
                p_b = self.pw.encode('utf-8')
                up_writer.write(b'\x01' + bytes([len(u_b)]) + u_b + bytes([len(p_b)]) + p_b)
                await up_writer.drain()
                auth_resp = await asyncio.wait_for(up_reader.read(2), timeout=10.0)
                if auth_resp != b'\x01\x00':
                    raise Exception("Upstream auth failed — wrong credentials")
            else:
                up_writer.write(b'\x05\x01\x00')
                await up_writer.drain()
                await asyncio.wait_for(up_reader.read(2), timeout=10.0)

            # 5. Forward CONNECT request
            up_writer.write(req_header[:3] + bytes([atyp]) + dst_addr + dst_port_bytes)
            await up_writer.drain()

            # 6. Read upstream CONNECT response
            up_conn_resp = await asyncio.wait_for(up_reader.read(4), timeout=10.0)
            if len(up_conn_resp) < 4:
                raise Exception("Upstream closed early")
            up_atyp = up_conn_resp[3]
            if up_atyp == 1:
                up_bnd_addr = await up_reader.read(4)
            elif up_atyp == 3:
                up_bnd_len = (await up_reader.read(1))[0]
                up_bnd_addr = bytes([up_bnd_len]) + await up_reader.read(up_bnd_len)
            elif up_atyp == 4:
                up_bnd_addr = await up_reader.read(16)
            else:
                up_bnd_addr = b'\x00\x00\x00\x00'
            up_bnd_port = await up_reader.read(2)

            # 7. Tell client "connected"
            writer.write(up_conn_resp[:3] + bytes([up_atyp]) + up_bnd_addr + up_bnd_port)
            await writer.drain()

            # 8. Bidirectional relay
            async def relay(src, dst):
                try:
                    while True:
                        data = await src.read(32768)
                        if not data:
                            break
                        dst.write(data)
                        await dst.drain()
                except Exception:
                    pass
                finally:
                    with suppress(Exception):
                        if not dst.is_closing():
                            dst.close()

            await asyncio.gather(relay(reader, up_writer), relay(up_reader, writer))

        except Exception as e:
            if "Upstream auth" not in str(e):
                logger.debug(f"Relay error: {e}")
        finally:
            for w in (writer, up_writer):
                if w:
                    with suppress(Exception):
                        if not w.is_closing():
                            w.close()
                    if w in self._connections:
                        self._connections.remove(w)

    async def start(self):
        # Pre-resolve upstream host to IP
        for attempt in range(3):
            try:
                import socket as _sock
                infos = await asyncio.wait_for(
                    asyncio.get_event_loop().getaddrinfo(
                        self.up_host, self.up_port, family=0, type=0, proto=0
                    ),
                    timeout=5.0
                )
                if infos:
                    v4 = [i for i in infos if i[0] == _sock.AF_INET]
                    self.up_ip = v4[0][4][0] if v4 else infos[0][4][0]
                    logger.debug(f"Relay: {self.up_host} → {self.up_ip}")
                    break
            except Exception as e:
                if attempt == 2:
                    logger.debug(f"Relay DNS fallback for {self.up_host}: {e}")
                await asyncio.sleep(0.3)

        self.server = await asyncio.start_server(self._handle_client, '127.0.0.1', 0)
        self.port   = self.server.sockets[0].getsockname()[1]

    async def stop(self):
        if self.server:
            self.server.close()
            with suppress(Exception):
                await asyncio.wait_for(self.server.wait_closed(), timeout=3.0)

        conns = list(self._connections)
        self._connections.clear()
        for c in conns:
            with suppress(Exception):
                if not c.is_closing():
                    c.close()
        await asyncio.sleep(0.05)


# ════════════════════════════════════════════════════════════════════════════
#  PROFILE RUNNER  — v13 MAX-THROUGHPUT + INSTANT-ABORT
# ════════════════════════════════════════════════════════════════════════════

async def run_profile(index: int, sem: asyncio.Semaphore) -> None:
    """
    v13 design:
      • Page loaded → 1-3 s humanized dwell → INSTANT close → next
      • ANY error   → INSTANT kill + fresh proxy, zero sleep
      • Headless = True, no GPU render = 3-4× faster
      • Full anonymity: stealth JS + traffic source spoofing + geo-matched FP
    """
    await asyncio.sleep(index * 0.03)   # tight stagger  (0.03 s apart)
    tag = f"P{index+1:02d}"

    # ── Cleanup helper ────────────────────────────────────────────────────────
    async def _hard_cleanup(relay_=None, ctx_=None):
        if relay_:
            with suppress(Exception): await relay_.stop()
        if ctx_:
            with suppress(Exception): await ctx_.close()

    # ── Fresh proxy helper ────────────────────────────────────────────────────
    async def _fresh_proxy(wait_s=8.0):
        rp = await _pool.acquire(wait_s=wait_s)
        if rp is None:
            return None, None, None, None
        cfg = _parse_proxy(rp)
        if not cfg:
            await _pool.release(rp, quarantine=False)
            return None, None, None, None
        cfg = await _resolve_proxy_cfg(cfg)
        sc  = cfg.get("_scheme", "http")
        return rp, cfg, sc, _build_ff_prefs(cfg)

    # ── Initial proxy ─────────────────────────────────────────────────────────
    raw_proxy, proxy_cfg, scheme, ff_prefs = await _fresh_proxy(wait_s=20.0)
    if raw_proxy is None:
        logger.error(f"[{tag}] No proxy available — skipping")
        return

    geo = _geo_from_proxy(raw_proxy)

    # Build fingerprint once, update geo per proxy swap
    seed = int(index * 7919 + random.random() * 999_983)
    fp   = get_custom_fingerprint(target_os=TARGET_DEVICE, mix_ratio=0.10, seed=seed)
    fp["timezone"]        = geo["timezone"]
    fp["locale"]          = geo["locale"]
    fp["accept_language"] = geo["locale"].replace("_", "-")

    vp   = fp["viewport"]
    W, H = vp["width"], vp["height"]

    def _rp():
        return random.randint(50, W - 50), random.randint(80, H - 80)

    impressions_gained = 0
    last_proxy_fault  = False   # tracks whether the last error was a HARD proxy fault

    for attempt in range(1, MAX_RETRIES + 1):
        relay = None
        ctx   = None
        wd    = None

        try:
            async with sem:
                # ── Relay setup ───────────────────────────────────────────────
                pw_proxy = _pw(proxy_cfg)
                if scheme in ("socks5", "socks4") and proxy_cfg.get("username"):
                    host_part, port_part = proxy_cfg["server"].split("://")[1].rsplit(":", 1)
                    relay = LocalSocks5Relay(
                        host_part, port_part,
                        proxy_cfg["username"], proxy_cfg["password"]
                    )
                    try:
                        await asyncio.wait_for(relay.start(), timeout=RELAY_START_TO)
                        pw_proxy = {"server": f"socks5://127.0.0.1:{relay.port}"}
                        
                        # Perfect Geo-IP Sync — override the strict heuristic
                        exact = await _exact_geo_lookup(relay.port, raw_proxy)
                        if exact:
                            geo = exact
                            fp["timezone"] = geo["timezone"]
                            fp["locale"]   = geo["locale"]
                            fp["accept_language"] = geo["locale"].replace("_", "-")

                    except Exception as re_err:
                        logger.warning(f"[{tag}] Relay failed ({re_err}) → swapping proxy instantly")
                        with suppress(Exception): await relay.stop()
                        await _pool.release(raw_proxy, quarantine=True)
                        raw_proxy, proxy_cfg, scheme, ff_prefs = await _fresh_proxy()
                        if raw_proxy is None:
                            logger.error(f"[{tag}] No backup proxy — aborting")
                            return
                        geo = _geo_from_proxy(raw_proxy)
                        fp["timezone"] = geo["timezone"]
                        fp["locale"]   = geo["locale"]
                        relay    = None
                        pw_proxy = _pw(proxy_cfg)
                        if scheme in ("socks5", "socks4") and proxy_cfg.get("username"):
                            host_part, port_part = proxy_cfg["server"].split("://")[1].rsplit(":", 1)
                            relay = LocalSocks5Relay(
                                host_part, port_part,
                                proxy_cfg["username"], proxy_cfg["password"]
                            )
                            try:
                                await asyncio.wait_for(relay.start(), timeout=RELAY_START_TO)
                                pw_proxy = {"server": f"socks5://127.0.0.1:{relay.port}"}
                            except Exception:
                                with suppress(Exception): await relay.stop()
                                relay = None

                # ── Launch browser with hard timeout ──────────────────────────
                extra_headers, src_name = _pick_traffic_source(fp)
                os_hint = (
                    "windows" if "Win32" in fp["platform"] else
                    "macos"   if "Mac"   in fp["platform"] else
                    "linux"
                )

                # Natively bypass DataDome/PerimeterX WebGL/SwiftShader scrutiny
                ff_prefs["webgl.override-unmasked-vendor"]   = fp.get("webgl_vendor", "Google Inc. (NVIDIA)")
                ff_prefs["webgl.override-unmasked-renderer"] = fp.get("webgl_renderer", "ANGLE (NVIDIA, NVIDIA GeForce RTX 3080 Direct3D11 vs_5_0 ps_5_0, D3D11)")
                ff_prefs["webgl.disable-fail-if-major-performance-caveat"] = True

                async with AsyncCamoufox(
                    headless=HEADLESS,
                    os=os_hint,
                    locale=fp["locale"],
                    humanize=True,
                    block_webrtc=False,
                    geoip=False,
                    proxy=pw_proxy,
                    addons=[],
                    exclude_addons=[DefaultAddons.UBO],
                    firefox_user_prefs=ff_prefs,
                ) as browser:

                    ctx = await browser.new_context(
                        ignore_https_errors=True,
                        java_script_enabled=True,
                        accept_downloads=False,
                        user_agent=fp["user_agent"],
                        locale=fp["locale"],
                        timezone_id=fp["timezone"],
                        viewport=vp,
                        device_scale_factor=fp["dpr"],
                        has_touch=fp["has_touch"],
                        extra_http_headers=extra_headers,
                        proxy=pw_proxy,
                    )
                    await ctx.add_init_script(build_stealth_js(fp))
                    await ctx.add_init_script(BEHAVIORAL_JS)
                    # Headless bypass: pins outerWidth/Height, screen.*, DPR to
                    # fingerprint values so ad networks see a real browser size
                    await ctx.add_init_script(_build_headless_bypass_js(fp))

                    page = await ctx.new_page()
                    await setup_route_guards(page)

                    # ── Watchdog ──────────────────────────────────────────────
                    async def _watchdog():
                        await asyncio.sleep(PROFILE_TIMEOUT)
                        logger.warning(f"[{tag}] ⚠ Watchdog fired — killing profile")
                        with suppress(Exception): await ctx.close()
                        with suppress(Exception): await browser.close()
                    wd = asyncio.create_task(_watchdog())

                    try:
                        # ── Navigate: wait_until="load" ensures ALL page JS
                        # Referer is already sent via extra_http_headers on the context —
                        # no need to actually visit the referrer site first (saves proxy bandwidth).
                        resp = await asyncio.wait_for(
                            page.goto(
                                TARGET_URLS[0],
                                wait_until="load",       # ← full page load incl. JS
                                timeout=NAV_TIMEOUT_MS,
                            ),
                            timeout=NAV_TIMEOUT_MS / 1000 + 5.0,
                        )

                        if not resp: raise Exception("err_empty_response")
                        if resp.status in (403, 429, 502, 503, 504):
                            raise Exception(f"access denied (HTTP {resp.status}) — rate limit / block")

                        ad_settle = random.uniform(1.5, 2.5)
                        await asyncio.sleep(ad_settle)

                        # ── Deep Content Analysis for Proxy / Bot Detection ────
                        body_lower = ""
                        with suppress(Exception): body_lower = (await page.content()).lower()
                        title_lower = ""
                        with suppress(Exception): title_lower = (await page.title()).lower()

                        page_str = title_lower + " " + body_lower
                        for kw in _PROXY_FAULT_HARD:
                            if kw in page_str: raise Exception(f"PROXY/ANON DETECTED IN HTML ({kw})")
                        if "cloudflare" in title_lower or "just a moment" in title_lower or "attention required" in title_lower:
                            raise Exception("PAGE CLOUDFLARE/BOT CHALLENGE DETECTED")

                        impressions_gained += 1
                        logger.success(
                            f"[{tag}] ✓ LOADED | src={src_name} | "
                            f"{proxy_cfg['server'][:40]} [{scheme.upper()}][{geo['country']}]"
                        )

                        # ── ARCHETYPE-BASED HUMAN DWELL ──────────────────────
                        # Break ML clustering by using distinctly different read profiles
                        dwell_s = random.uniform(*DWELL_PER_URL)
                        archetype = random.choices(
                            ["Reader", "Scanner", "Clicker", "Bouncer"],
                            weights=[40, 30, 15, 15], k=1
                        )[0]

                        t_start = asyncio.get_event_loop().time()
                        actions = 0
                        
                        last_x, last_y = W // 2, H // 2
                        
                        async def _organic_move(target_x, target_y):
                            nonlocal last_x, last_y
                            path = generate_human_mouse_path(last_x, last_y, target_x, target_y)
                            for px, py, d_ms in path:
                                with suppress(Exception): await page.mouse.move(px, py)
                                await asyncio.sleep(d_ms / 1000.0)
                            last_x, last_y = target_x, target_y

                        # Adjust behaviors inside the loop
                        while (asyncio.get_event_loop().time() - t_start) < dwell_s:
                            roll = random.random()
                            tx, ty = _rp()
                            
                            if archetype == "Bouncer":
                                # Barely moves, will exit soon
                                if roll < 0.2:
                                    await _organic_move(tx, ty)
                                await asyncio.sleep(random.uniform(0.5, 2.0))
                                
                            elif archetype == "Scanner":
                                # Fast scrolls, less reading pauses
                                if roll < 0.6:
                                    with suppress(Exception): await page.mouse.wheel(0, random.choice([200, 300, 400, -100]))
                                elif roll < 0.8:
                                    await _organic_move(tx, ty)
                                await asyncio.sleep(random.uniform(0.1, 0.5))
                                
                            elif archetype == "Clicker":
                                # Actively clicks dead-space to mark engagement
                                if roll < 0.4:
                                    with suppress(Exception): await async_human_click(page, tx, ty, last_x, last_y)
                                    last_x, last_y = tx, ty
                                elif roll < 0.7:
                                    with suppress(Exception): await page.mouse.wheel(0, random.choice([100, -100, 200]))
                                else:
                                    await _organic_move(tx, ty)
                                await asyncio.sleep(random.uniform(0.3, 0.7))
                                
                            else: # Reader (default)
                                # Steady slow scrolls and drifts, pauses to read
                                if roll < 0.4:
                                    with suppress(Exception): await page.mouse.wheel(0, random.choice([60, 90, 120]))
                                    await asyncio.sleep(random.uniform(1.0, 3.0)) # read pause
                                elif roll < 0.7:
                                    await _organic_move(tx, ty)
                                await asyncio.sleep(random.uniform(0.1, 0.4))
                                
                            actions += 1

                        logger.debug(f"[{tag}] Dwell {dwell_s:.1f}s, {actions} actions → closing")

                    finally:
                        if wd:
                            wd.cancel()
                            with suppress(asyncio.CancelledError): await wd
                        await _hard_cleanup(ctx_=ctx)

                    # ── Session success ───────────────────────────────────────
                    await _hard_cleanup(relay_=relay)
                    relay = None
                    await _pool.release(raw_proxy, quarantine=False)
                    await _imp.record(impressions_gained)
                    rate  = await _imp.rate()
                    total = await _imp.total()
                    logger.info(
                        f"[{tag}] ✓ Done | total={total} | {rate:.0f} imp/hr"
                    )
                    return  # success — exit retry loop

        except asyncio.CancelledError:
            await _hard_cleanup(relay_=relay, ctx_=ctx)
            await _pool.release(raw_proxy, quarantine=False)
            if impressions_gained > 0:
                await _imp.record(impressions_gained)
                rate  = await _imp.rate()
                total = await _imp.total()
                logger.info(f"[{tag}] ✓ {impressions_gained} imp (force-stop) | total={total} | {rate:.0f} imp/hr")
            return

        except Exception as exc:
            # ── INSTANT ABORT ─────────────────────────────────────────────────
            await _hard_cleanup(relay_=relay, ctx_=ctx)
            relay = None
            ctx   = None

            err_str          = str(exc)
            proxy_fault      = _is_proxy_fault(err_str)
            last_proxy_fault = proxy_fault          # remember for end-of-retries
            label            = "PROXY/ANON" if proxy_fault else "PAGE"

            logger.warning(
                f"[{tag}] ✗ [{label}] attempt {attempt}/{MAX_RETRIES} → "
                f"{err_str[:100]}"
            )

            # Release / quarantine current proxy, grab fresh one — no sleep
            await _pool.release(raw_proxy, quarantine=proxy_fault)
            raw_proxy, proxy_cfg, scheme, ff_prefs = await _fresh_proxy()
            if raw_proxy is None:
                logger.error(f"[{tag}] No proxy available — aborting after {attempt} attempts")
                return
            geo = _geo_from_proxy(raw_proxy)
            fp["timezone"] = geo["timezone"]
            fp["locale"]   = geo["locale"]
            logger.debug(f"[{tag}] ↪ Fresh proxy [{scheme.upper()}] → retry {attempt+1}")
            # Spin immediately — NO sleep

    # Exhausted all retries — only quarantine if last error was a HARD proxy fault
    # (page-level errors like NS_ERROR_NET_RESET must NOT quarantine the proxy)
    await _pool.release(raw_proxy, quarantine=last_proxy_fault)
    if last_proxy_fault:
        logger.error(f"[{tag}] All {MAX_RETRIES} attempts exhausted [PROXY FAULT] — proxy quarantined")
    else:
        logger.warning(f"[{tag}] All {MAX_RETRIES} attempts exhausted [PAGE ERRORS] — proxy released clean")


# ════════════════════════════════════════════════════════════════════════════
#  BATCH RUNNER
# ════════════════════════════════════════════════════════════════════════════

async def run_batch(cycle: int) -> None:
    live = _pool.live_count()
    num  = min(
        NUM_PROFILES,
        max(live, 1),
    )

    logger.info("═" * 68)
    logger.info(
        f"  Cycle {cycle:>4}  |  {num} profiles  |  {_pool.stats()}"
    )
    logger.info("═" * 68)

    # Semaphore = num so all profiles run truly in parallel
    sem     = asyncio.Semaphore(num)
    results = await asyncio.gather(
        *[asyncio.create_task(run_profile(i, sem)) for i in range(num)],
        return_exceptions=True,
    )

    errors = sum(1 for r in results if isinstance(r, Exception))
    total  = await _imp.total()
    r_hr   = await _imp.rate()
    if errors:
        logger.warning(
            f"  Cycle {cycle} — {errors} error(s) | total={total} | {r_hr:.0f} imp/hr"
        )
    else:
        logger.success(
            f"  Cycle {cycle} — complete | total={total} | {r_hr:.0f} imp/hr\n"
        )


# ════════════════════════════════════════════════════════════════════════════
#  GRACEFUL SHUTDOWN
# ════════════════════════════════════════════════════════════════════════════
_stop = asyncio.Event()


def _on_signal(sig, frame):
    logger.warning("Stop signal — finishing current cycle then exiting…")
    _stop.set()


signal.signal(signal.SIGINT,  _on_signal)
signal.signal(signal.SIGTERM, _on_signal)


# ════════════════════════════════════════════════════════════════════════════
#  STARTUP PROXY DIAGNOSTICS
# ════════════════════════════════════════════════════════════════════════════

def _show_proxy_summary(proxies: list[str]) -> None:
    socks5_count = socks4_count = http_count = failed_count = 0
    countries: dict[str, int] = {}

    for raw in proxies:
        cfg = _parse_proxy(raw)
        if not cfg:
            failed_count += 1
            continue
        sc = cfg.get("_scheme", "http")
        if "socks5" in sc:
            socks5_count += 1
        elif "socks4" in sc:
            socks4_count += 1
        else:
            http_count += 1
        geo = _geo_from_proxy(raw)
        cc  = geo["country"]
        countries[cc] = countries.get(cc, 0) + 1

    logger.info(
        f"  Proxy types   : SOCKS5={socks5_count}  SOCKS4={socks4_count}"
        f"  HTTP={http_count}  FAILED={failed_count}"
    )
    top = sorted(countries.items(), key=lambda x: -x[1])[:8]
    logger.info(f"  Countries     : {' | '.join(f'{c}={n}' for c, n in top)}")


# ════════════════════════════════════════════════════════════════════════════
#  MAIN
# ════════════════════════════════════════════════════════════════════════════

async def main() -> None:
    global _pool, _imp
    _pool = ProxyPool(PROXIES)
    _imp  = ImpCounter(window_s=300)

    # Theoretical max throughput estimate
    dwell_avg  = sum(DWELL_PER_URL) / 2          # avg dwell seconds
    cycle_s    = dwell_avg + 5                   # dwell + launch overhead
    max_iph    = int(NUM_PROFILES * 3600 / cycle_s)

    logger.info("═" * 68)
    logger.info(f"  directlink.py — Stealth Layer v13  (MAX THROUGHPUT)")
    logger.info("═" * 68)
    logger.info(f"  URL            : {TARGET_URLS[0][:65]}")
    logger.info(f"  Proxy file     : {PROXY_FILE}  ({len(PROXIES)} loaded)")
    _show_proxy_summary(PROXIES)
    logger.info(f"  Profiles/cycle : {NUM_PROFILES}–{NUM_PROFILES+2} parallel")
    logger.info(f"  Dwell/URL      : {DWELL_PER_URL[0]}–{DWELL_PER_URL[1]}s (page load + human micro-actions)")
    logger.info(f"  Est. max rate  : ~{max_iph} imp/hr  (at {NUM_PROFILES} concurrent, {dwell_avg:.0f}s avg dwell)")
    logger.info(f"  Device         : {TARGET_DEVICE.title()} (90% match, 10% random)")
    logger.info(f"  Headless       : {HEADLESS}  ← {'FAST MODE' if HEADLESS else 'VISIBLE (slower)'}")
    logger.info(f"  Proxy rotation : TRUE RANDOM (anti-fingerprint)")
    logger.info(f"  Quarantine     : {PROXY_QUARANTINE_S}s on proxy/auth/anon-detect fail")
    logger.info(f"  Nav timeout    : {NAV_TIMEOUT_MS/1000:.0f}s  (domcontentloaded)")
    logger.info(f"  Abort mode     : INSTANT — any error = kill + new proxy, 0 sleep")
    logger.info(f"  GeoIP          : DISABLED (manual geo via proxy string)")
    logger.info(f"  DNS leak fix   : ENABLED  (socks_remote_dns + DoH disabled)")
    logger.info(f"  Anon-detect    : ENABLED  (quarantine on detection keywords)")
    logger.info("  Press Ctrl+C to stop gracefully\n")

    cycle = 1
    while not _stop.is_set():
        await run_batch(cycle)
        cycle += 1
        if _stop.is_set():
            break
        cd = random.uniform(*COOLDOWN)
        if cd > 0.1:
            try:
                await asyncio.wait_for(_stop.wait(), timeout=cd)
            except asyncio.TimeoutError:
                pass

    total = await _imp.total()
    r_hr  = await _imp.rate()
    logger.info(f"Shutdown. Session total: {total} impressions | {r_hr:.0f} imp/hr")


# ════════════════════════════════════════════════════════════════════════════
#  ENTRY POINT
# ════════════════════════════════════════════════════════════════════════════

if __name__ == "__main__":
    print("\n" + "═" * 68)
    print("  --- DEVICE FINGERPRINT (90% of profiles) ---")
    print("  1. android   2. mac   3. linux   4. windows")
    while True:
        try:
            c = input("\n  Choice (1-4 or name) [windows]: ").strip().lower() or "windows"
        except EOFError:
            c = "windows"
            break
        if c in ("1", "android"): TARGET_DEVICE = "android"; break
        if c in ("2", "mac"):     TARGET_DEVICE = "mac";     break
        if c in ("3", "linux"):   TARGET_DEVICE = "linux";   break
        if c in ("4", "windows"): TARGET_DEVICE = "windows"; break
        print("  Try again.")

    print(f"\n  ► Running with {TARGET_DEVICE.upper()} fingerprint\n")

    if sys.platform == "win32":
        asyncio.set_event_loop_policy(asyncio.WindowsProactorEventLoopPolicy())
    asyncio.run(main())
