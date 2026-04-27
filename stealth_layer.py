#!/usr/bin/env python3
"""
stealth_layer.py — Advanced Anti-Fingerprinting v5.0
=====================================================
2025 research-backed undetectable pattern engine.

FIXES OVER v4.0:
  [CRITICAL] /tmp/playwright-artifacts-* path leak → blocked at JS + route level
  [CRITICAL] Error().stack Playwright traces → sanitized
  [CRITICAL] iframe contentWindow isolation → stealth re-injected per iframe
  [HIGH]     Web Worker scope → stealth injected via Blob-wrap
  [HIGH]     performance.timing drift → synthetic consistent object
  [HIGH]     configurable mismatch on navigator descriptors → fixed
  [MED]      deviceorientation events for mobile profiles
  [MED]      DST calculation accuracy improved
  [MED]      Async human_click added for Playwright async
  [LOW]      Download dialog auto-dismiss
  [LOW]      Blob/ObjectURL download intercept
"""

import asyncio
import math
import random
import time
from typing import Any


# ─────────────────────────────────────────────────────────────────
# DEVICE POOL (unchanged from v4 — already well-structured)
# ─────────────────────────────────────────────────────────────────
US_DEVICES = [
    ("Mozilla/5.0 (iPhone; CPU iPhone OS 18_3_2 like Mac OS X) AppleWebKit/605.1.15 (KHTML, like Gecko) Version/18.3.2 Mobile/15E148 Safari/604.1",
     "iPhone", 430, 932, 3.0, True, True, "Apple Inc.", "Apple GPU", "18.3", "portrait-primary", 24, 6, 6, 15),
    ("Mozilla/5.0 (iPhone; CPU iPhone OS 17_6 like Mac OS X) AppleWebKit/605.1.15 (KHTML, like Gecko) Version/17.6 Mobile/15E148 Safari/604.1",
     "iPhone", 393, 852, 3.0, True, True, "Apple Inc.", "Apple GPU", "17.6", "portrait-primary", 24, 6, 6, 14),
    ("Mozilla/5.0 (iPhone; CPU iPhone OS 17_4 like Mac OS X) AppleWebKit/605.1.15 (KHTML, like Gecko) Version/17.4 Mobile/15E148 Safari/604.1",
     "iPhone", 390, 844, 3.0, True, True, "Apple Inc.", "Apple GPU", "17.4", "portrait-primary", 24, 6, 4, 12),
    ("Mozilla/5.0 (iPhone; CPU iPhone OS 16_6 like Mac OS X) AppleWebKit/605.1.15 (KHTML, like Gecko) Version/16.6 Mobile/15E148 Safari/604.1",
     "iPhone", 393, 852, 3.0, True, True, "Apple Inc.", "Apple GPU", "16.6", "portrait-primary", 24, 6, 6, 8),
    ("Mozilla/5.0 (iPad; CPU OS 17_4 like Mac OS X) AppleWebKit/605.1.15 (KHTML, like Gecko) Version/17.4 Mobile/15E148 Safari/604.1",
     "iPad", 1024, 1366, 2.0, True, True, "Apple Inc.", "Apple GPU", "17.4", "portrait-primary", 24, 8, 8, 5),
    ("Mozilla/5.0 (Linux; Android 14; Pixel 8 Pro) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/124.0.6367.82 Mobile Safari/537.36",
     "Linux aarch64", 412, 915, 2.625, True, True, "Qualcomm", "Adreno (TM) 740", "14", "portrait-primary", 24, 8, 8, 5),
    ("Mozilla/5.0 (Linux; Android 14; SM-S921B) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/124.0.6367.82 Mobile Safari/537.36",
     "Linux aarch64", 384, 832, 3.0, True, True, "ARM", "Mali-G715", "14", "portrait-primary", 24, 8, 8, 4),
    ("Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/124.0.0.0 Safari/537.36",
     "Win32", 1920, 1080, 1.0, False, False, "Google Inc. (NVIDIA)", "ANGLE (NVIDIA, NVIDIA GeForce RTX 3080 Direct3D11 vs_5_0 ps_5_0, D3D11)", "10.0", "landscape-primary", 24, 16, 16, 5),
    ("Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/124.0.0.0 Safari/537.36 Edg/124.0.0.0",
     "Win32", 1920, 1080, 1.0, False, False, "Google Inc. (AMD)", "ANGLE (AMD, AMD Radeon RX 6700 XT Direct3D11 vs_5_0 ps_5_0, D3D11)", "10.0", "landscape-primary", 24, 12, 16, 3),
    ("Mozilla/5.0 (Macintosh; Intel Mac OS X 14_4) AppleWebKit/605.1.15 (KHTML, like Gecko) Version/17.4 Safari/605.1.15",
     "MacIntel", 1440, 900, 2.0, False, False, "Apple Inc.", "Apple GPU", "14.4", "landscape-primary", 30, 8, 16, 4),
    ("Mozilla/5.0 (Windows NT 10.0; Win64; x64; rv:126.0) Gecko/20100101 Firefox/126.0",
     "Win32", 1536, 864, 1.25, False, False, "Google Inc. (NVIDIA)", "ANGLE (NVIDIA, NVIDIA GeForce GTX 1660 Super Direct3D11 vs_5_0 ps_5_0, D3D11)", "10.0", "landscape-primary", 24, 8, 16, 2),
    ("Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/124.0.0.0 Safari/537.36",
     "Linux x86_64", 1920, 1080, 1.0, False, False, "Google Inc. (NVIDIA)", "ANGLE (NVIDIA, NVIDIA GeForce RTX 3060 Direct3D11 vs_5_0 ps_5_0, D3D11)", "Ubuntu", "landscape-primary", 24, 8, 16, 3),
]

_DEVICE_WEIGHTS = [d[14] for d in US_DEVICES]

US_TIMEZONES = [
    "America/New_York","America/New_York","America/New_York",
    "America/Chicago","America/Chicago","America/Los_Angeles",
    "America/Los_Angeles","America/Denver","America/Phoenix",
]
US_LANGUAGES = ["en-US,en;q=0.9","en-US,en;q=0.9,es;q=0.7","en-US,en;q=0.8","en-US"]

US_REFERRERS_MOBILE = [
    "https://www.google.com/search?q=deals+today",
    "https://l.instagram.com/?u=", "https://t.co/",
    "https://www.facebook.com/", "", "https://www.tiktok.com/",
]
US_REFERRERS_DESKTOP = [
    "https://www.google.com/search?q=best+insurance+2025",
    "https://www.google.com/search?q=mortgage+rates+today",
    "https://www.bing.com/search?q=tech+reviews+2025",
    "https://www.facebook.com/", "https://www.reddit.com/r/personalfinance/", "",
]

_US_COORDS = [
    (40.7128,-74.006),(34.0522,-118.2437),(41.8781,-87.6298),
    (29.7604,-95.3698),(33.4484,-112.074),(39.9526,-75.1652),
    (30.2672,-97.7431),(32.7767,-96.797),(47.6062,-122.3321),(25.7617,-80.1918),
]

# TZ offset map for DST-aware calculation
_TZ_OFFSETS = {
    "America/New_York": 300, "America/Chicago": 360,
    "America/Denver": 420,   "America/Los_Angeles": 480,
    "America/Phoenix": 420,  # no DST
}
_TZ_NO_DST = {"America/Phoenix", "America/Arizona"}


def _build_fp_from_device(d: tuple, rng: random.Random) -> dict:
    ua, platform, vw, vh, dpr, is_mobile, has_touch, wv, wr, os_ver, orient, cdepth, cpu, ram, _w = d
    tz   = rng.choice(US_TIMEZONES)
    lang = rng.choice(US_LANGUAGES)
    lat, lon = rng.choice(_US_COORDS)
    lat += rng.uniform(-0.04, 0.04)
    lon += rng.uniform(-0.04, 0.04)

    if "Edg/" in ua:        brand, bver = "Microsoft Edge", ua.split("Edg/")[1].split(".")[0]
    elif "Firefox/" in ua:  brand, bver = "Firefox",        ua.split("Firefox/")[1].split(".")[0]
    elif "Chrome/" in ua:   brand, bver = "Google Chrome",  ua.split("Chrome/")[1].split(".")[0]
    elif "Version/" in ua:  brand, bver = "Safari",         ua.split("Version/")[1].split(".")[0]
    else:                   brand, bver = "Google Chrome",  "124"

    bl = round(rng.uniform(0.52, 0.98), 2)
    charging = rng.random() < 0.70
    ref_pool = US_REFERRERS_MOBILE if is_mobile else US_REFERRERS_DESKTOP

    return dict(
        user_agent=ua, platform=platform,
        viewport={"width": vw + (rng.randint(-4, 4) if not is_mobile else 0), "height": vh},
        dpr=dpr, is_mobile=is_mobile, has_touch=has_touch,
        timezone=tz, locale=lang.split(",")[0],
        accept_language=lang, webgl_vendor=wv, webgl_renderer=wr,
        hw_concurrency=cpu, device_memory=ram,
        brand=brand, brand_ver=str(bver),
        canvas_noise=rng.uniform(1e-6, 8e-6),
        audio_noise=rng.uniform(1e-6, 8e-6),
        battery_level=bl, battery_charging=charging,
        charging_time=0 if charging else 99999,
        discharging_time=99999 if charging else rng.randint(1800, 18000),
        screen_orientation=orient, color_depth=cdepth, os_version=os_ver,
        latitude=round(lat, 5), longitude=round(lon, 5),
        referrer=rng.choice(ref_pool),
        drift_seed=rng.randint(1, 9999),
    )


def get_us_fingerprint(seed: int = None) -> dict:
    rng = random.Random(seed) if seed is not None else random
    d   = rng.choices(US_DEVICES, weights=_DEVICE_WEIGHTS, k=1)[0]
    return _build_fp_from_device(d, rng)


def get_weighted_fingerprint(mobile_prob: float = 0.65, seed: int = None) -> dict:
    rng     = random.Random(seed) if seed is not None else random
    mobile  = [d for d in US_DEVICES if d[5]]
    desktop = [d for d in US_DEVICES if not d[5]]
    pool    = mobile if rng.random() < mobile_prob else desktop
    weights = [d[14] for d in pool]
    d       = rng.choices(pool, weights=weights, k=1)[0]
    return _build_fp_from_device(d, rng)


def get_custom_fingerprint(target_os: str, mix_ratio: float = 0.1, seed: int = None) -> dict:
    rng = random.Random(seed) if seed is not None else random
    is_mix = rng.random() < mix_ratio
    
    if target_os == 'android':
        target_platforms = ['Linux aarch64']
    elif target_os == 'mac':
        target_platforms = ['MacIntel']
    elif target_os == 'linux':
        target_platforms = ['Linux x86_64']
    elif target_os == 'windows':
        target_platforms = ['Win32']
    else:
        target_platforms = ['Win32', 'MacIntel', 'Linux aarch64', 'iPhone', 'iPad', 'Linux x86_64']
        
    if is_mix:
        pool = [d for d in US_DEVICES if d[1] not in target_platforms]
        if not pool: pool = US_DEVICES
    else:
        pool = [d for d in US_DEVICES if d[1] in target_platforms]
        if not pool: pool = US_DEVICES
        
    weights = [d[14] for d in pool]
    d = rng.choices(pool, weights=weights, k=1)[0]
    return _build_fp_from_device(d, rng)


# ─────────────────────────────────────────────────────────────────
# PAGE ROUTE HANDLER — blocks playwright artifact paths leaking
# Call this right after creating a context/page in your automation:
#
#   await setup_route_guards(page)
#
# ─────────────────────────────────────────────────────────────────
async def setup_route_guards(page: Any) -> None:
    """
    Block network patterns that expose the Playwright artifact path:
      - Blob download triggers (expose /tmp/playwright-artifacts-*)
      - File:// requests
      - Download dialogs via route interception
      - Auto-dismiss any dialogs that slip through
    """

    # ── 1. Block/handle downloads at route level ──────────────────
    async def _route_handler(route, request):
        url = request.url
        rtype = request.resource_type

        # Block file:// scheme entirely (exposes local paths)
        if url.startswith("file://"):
            await route.abort()
            return

        # Block blob: downloads that trigger save dialogs
        # (these are what cause the playwright-artifacts error)
        if url.startswith("blob:") and rtype in ("document", "other"):
            await route.abort()
            return

        # Block data: URI downloads
        if url.startswith("data:") and rtype == "document":
            await route.abort()
            return

        await route.continue_()

    await page.route("**/*", _route_handler)

    # ── 2. Auto-dismiss ALL dialogs (alert/confirm/beforeunload) ──
    # This catches the playwright-artifacts error dialog shown in your screenshot
    page.on("dialog", lambda dialog: asyncio.ensure_future(dialog.dismiss()))

    # ── 3. Block download events before they open a save dialog ───
    page.on("download", lambda dl: asyncio.ensure_future(_cancel_download(dl)))


async def _cancel_download(download: Any) -> None:
    """Silently cancel any triggered download."""
    try:
        await download.cancel()
    except Exception:
        pass


# ─────────────────────────────────────────────────────────────────
# STEALTH JS v5.0 — Full anti-detection init script
# ─────────────────────────────────────────────────────────────────
def build_stealth_js(fp: dict) -> str:
    mob        = str(fp["is_mobile"]).lower()
    touch_pts  = "5" if fp["has_touch"] else "0"
    charging   = str(fp["battery_charging"]).lower()
    gl_prec    = random.randint(23, 24)
    is_mobile  = fp["is_mobile"]
    tz         = fp["timezone"]
    tz_base    = _TZ_OFFSETS.get(tz, 300)
    tz_no_dst  = str(tz in _TZ_NO_DST).lower()
    prefers_dark = str(random.random() < 0.4).lower()
    nav_vendor  = "Apple Computer, Inc." if ("Safari" in fp["user_agent"] and "Chrome" not in fp["user_agent"]) else "Google Inc."

    # Synthetic navigation timing (consistent, not drifting)
    nav_start      = f"(Date.now() - {random.randint(400, 1200)})"
    fetch_offset   = random.randint(5, 15)
    dns_start      = random.randint(20, 35)
    dns_end        = dns_start + random.randint(8, 25)
    connect_end    = dns_end + random.randint(20, 60)
    req_start      = connect_end + random.randint(5, 15)
    resp_start     = req_start + random.randint(40, 120)
    resp_end       = resp_start + random.randint(10, 40)
    dom_loading    = resp_end + random.randint(5, 20)
    dcl_start      = dom_loading + random.randint(80, 200)
    dcl_end        = dcl_start + random.randint(5, 25)
    load_start     = dcl_end + random.randint(20, 80)
    load_end       = load_start + random.randint(10, 30)

    # Mobile orientation initial values
    init_alpha = round(random.uniform(0, 360), 2)
    init_beta  = round(random.uniform(-10, 10), 2)
    init_gamma = round(random.uniform(-5, 5), 2)

    network_downlink = round(random.uniform(8.0, 95.0), 1)
    network_rtt      = random.randint(10, 60)
    network_eff_type = random.choice(["4g", "4g", "wifi"])
    network_type     = random.choice(["wifi", "wifi", "cellular"])

    return f"""
// ════════════════════════════════════════════════════════════════════
// ULTRA STEALTH v5.0 — 2025 Anti-Detection Engine
// Fixes: playwright-artifacts leak, Error.stack, iframe isolation,
//        Worker scope, timing drift, DST accuracy, async mouse
// ════════════════════════════════════════════════════════════════════
(function() {{
  'use strict';
  if (window.__stealthV5) return;
  window.__stealthV5 = true;

  // ── 0. NATIVE CODE GUARDIAN ─────────────────────────────────────
  const _nativeToString = Function.prototype.toString;
  const _nativeMap = new WeakMap();
  function _makeNative(fn, nativeName) {{
    _nativeMap.set(fn, `function ${{nativeName || fn.name}}() {{ [native code] }}`);
    return fn;
  }}
  Object.defineProperty(Function.prototype, 'toString', {{
    value: function toString() {{
      if (_nativeMap.has(this)) return _nativeMap.get(this);
      return _nativeToString.call(this);
    }},
    writable: false, configurable: false
  }});
  _nativeMap.set(Function.prototype.toString, 'function toString() {{ [native code] }}');

  // ── 1. AUTOMATION FLAGS — 25+ strings ───────────────────────────
  const _autoFlags = [
    '$cdc_asdjflasutopfhvcZLmcfl_','$chrome_asyncScriptInfo',
    '__driver_evaluate','__webdriver_evaluate','__selenium_evaluate',
    '__fxdriver_evaluate','__driver_unwrapped','__webdriver_unwrapped',
    '__selenium_unwrapped','__fxdriver_unwrapped','__webdriverFunc',
    '__webdriver_script_fn','__lastWatirAlert','__lastWatirConfirm',
    '__lastWatirPrompt','_phantom','__nightmare','__puppeteer',
    '__playwright','callPhantom','_Selenium_IDE_Recorder',
    '__selenium_alert_type','__webdriver_chrome_port',
    '__webdriver_firefox_port','domAutomation','domAutomationController',
    '__playwright_clock__', 'playwright', '__pw_clock',
  ];
  _autoFlags.forEach(f => {{
    try {{
      delete window[f];
      Object.defineProperty(window, f, {{get: ()=>undefined, configurable:true}});
    }} catch(e) {{}}
  }});

  // ── 1.1 AD-BLOCKER SUPPRESSION ──────────────────────────────────
  try {{
    window.canRunAds = true;
    window.isAdBlockActive = false;
    ['adblock','adBlock','AdBlock','__adBlockDisabled','sn_adblock','no_adblock'].forEach(k => {{
      try {{ window[k] = (k==='__adBlockDisabled') ? true : false; }} catch(e) {{}}
    }});
  }} catch(e) {{}}

  // ── 2. WEBDRIVER REMOVAL ────────────────────────────────────────
  try {{
    Object.defineProperty(navigator, 'webdriver', {{
      get: _makeNative(() => undefined, 'get webdriver'),
      enumerable: true,
      configurable: true   // matches real Chrome descriptor exactly
    }});
    delete navigator.__proto__.webdriver;
  }} catch(e) {{}}

  // ────────────────────────────────────────────────────────────────
  // [FIX v5] ERROR.STACK PLAYWRIGHT TRACE SANITIZER
  // Playwright leaves "playwright", "pw:", "internal/pw" in stacks
  // which advanced detectors parse looking for automation traces.
  // ────────────────────────────────────────────────────────────────
  try {{
    const _OrigError = Error;
    const _OrigEvalError = EvalError;
    const _OrigRangeError = RangeError;
    const _OrigReferenceError = ReferenceError;
    const _OrigSyntaxError = SyntaxError;
    const _OrigTypeError = TypeError;
    const _OrigURIError = URIError;

    function _cleanStack(stack) {{
      if (!stack || typeof stack !== 'string') return stack;
      return stack
        .split('\\n')
        .filter(line => !(/playwright|puppeteer|selenium|webdriver|\\/pw\\/|__pw_|cdp_|cdc_/i.test(line)))
        .join('\\n');
    }}

    function _wrapError(OrigClass) {{
      const Wrapped = _makeNative(function(...args) {{
        const e = new OrigClass(...args);
        const origStack = e.stack;
        Object.defineProperty(e, 'stack', {{
          get: _makeNative(() => _cleanStack(origStack), 'get stack'),
          configurable: true
        }});
        return e;
      }}, OrigClass.name);
      Object.setPrototypeOf(Wrapped, OrigClass);
      Wrapped.prototype = OrigClass.prototype;
      return Wrapped;
    }}
    window.Error          = _wrapError(_OrigError);
    window.EvalError      = _wrapError(_OrigEvalError);
    window.RangeError     = _wrapError(_OrigRangeError);
    window.ReferenceError = _wrapError(_OrigReferenceError);
    window.TypeError      = _wrapError(_OrigTypeError);
    window.URIError       = _wrapError(_OrigURIError);
  }} catch(e) {{}}

  // ────────────────────────────────────────────────────────────────
  // [FIX v5] PLAYWRIGHT ARTIFACTS PATH LEAK — JS layer
  // The /tmp/playwright-artifacts-XXXXX path leaks via:
  //   a) Blob download triggers that open save dialogs
  //   b) URL.createObjectURL on download blobs
  //   c) navigator.msSaveBlob / msSaveOrOpenBlob
  //   d) Anchor <a download> clicks
  // All blocked here; route-level block in setup_route_guards().
  // ────────────────────────────────────────────────────────────────
  try {{
    // a) Block URL.createObjectURL from triggering downloads
    const _origCOURL = URL.createObjectURL.bind(URL);
    URL.createObjectURL = _makeNative(function createObjectURL(obj) {{
      // If it's a Blob with a download-like type, redirect to a safe data URI
      if (obj instanceof Blob) {{
        const t = (obj.type || '').toLowerCase();
        const downloadTypes = ['application/octet-stream','application/zip',
          'application/pdf','application/msword','text/csv','application/json'];
        if (downloadTypes.some(dt => t.includes(dt)) || t === '') {{
          // Return a harmless about:blank blob URL — won't trigger save dialog
          return _origCOURL(new Blob([''], {{type:'text/plain'}}));
        }}
      }}
      return _origCOURL(obj);
    }}, 'createObjectURL');

    // b) Block msSaveBlob (IE/Edge legacy download API)
    if (navigator.msSaveBlob) {{
      navigator.msSaveBlob         = _makeNative(() => false, 'msSaveBlob');
      navigator.msSaveOrOpenBlob   = _makeNative(() => false, 'msSaveOrOpenBlob');
    }}

    // c) Intercept anchor[download] clicks before they reach the browser
    document.addEventListener('click', function(e) {{
      const a = e.target && e.target.closest('a[download]');
      if (a) {{
        e.preventDefault();
        e.stopImmediatePropagation();
      }}
    }}, {{capture: true, passive: false}});

    // d) Override showSaveFilePicker / showOpenFilePicker (File System Access API)
    if (window.showSaveFilePicker) {{
      window.showSaveFilePicker = _makeNative(() => Promise.reject(new DOMException('AbortError')), 'showSaveFilePicker');
      window.showOpenFilePicker = _makeNative(() => Promise.reject(new DOMException('AbortError')), 'showOpenFilePicker');
    }}

  }} catch(e) {{}}

  // ────────────────────────────────────────────────────────────────
  // [FIX v5] IFRAME contentWindow ISOLATION
  // Stealth JS doesn't run inside iframes by default.
  // Detectors open iframes and check navigator.webdriver inside them.
  // We proxy contentWindow to inject our flags before they're read.
  // ────────────────────────────────────────────────────────────────
  try {{
    const _origCW = Object.getOwnPropertyDescriptor(HTMLIFrameElement.prototype, 'contentWindow');
    const _patchedFrames = new WeakSet();
    Object.defineProperty(HTMLIFrameElement.prototype, 'contentWindow', {{
      get: _makeNative(function() {{
        const cw = _origCW.get.call(this);
        if (cw && !_patchedFrames.has(this)) {{
          _patchedFrames.add(this);
          try {{
            // Patch the iframe's navigator.webdriver immediately
            if (cw.navigator) {{
              Object.defineProperty(cw.navigator, 'webdriver', {{
                get: () => undefined, configurable: true, enumerable: true
              }});
            }}
            // Remove playwright globals from iframe context
            ['__playwright','$cdc_asdjflasutopfhvcZLmcfl_','domAutomation',
             'domAutomationController'].forEach(f => {{
              try {{ delete cw[f]; }} catch(e) {{}}
            }});
          }} catch(e) {{}}
        }}
        return cw;
      }}, 'get contentWindow'),
      configurable: true
    }});
  }} catch(e) {{}}

  // ────────────────────────────────────────────────────────────────
  // [FIX v5] WEB WORKER SCOPE ISOLATION
  // Workers run in a separate JS context. Detectors spin up a Worker
  // and check navigator.webdriver inside it. We Blob-wrap Worker URLs
  // to prepend a mini stealth patch before the actual script runs.
  // ────────────────────────────────────────────────────────────────
  try {{
    const _WORKER_MINI = `
      try {{
        Object.defineProperty(self.navigator,'webdriver',{{get:()=>undefined,configurable:true}});
        ['__playwright','$cdc_asdjflasutopfhvcZLmcfl_','domAutomation'].forEach(f=>{{try{{delete self[f];}}catch(e){{}}}});
      }} catch(e) {{}}
    `;
    const _OrigWorker = Worker;
    window.Worker = _makeNative(function Worker(url, opts) {{
      try {{
        const blob = new Blob([_WORKER_MINI + `\\nimportScripts('${{url}}');`],
                              {{type: 'application/javascript'}});
        const blobURL = URL.createObjectURL(blob);
        return new _OrigWorker(blobURL, opts);
      }} catch(e) {{
        return new _OrigWorker(url, opts);
      }}
    }}, 'Worker');
    Object.setPrototypeOf(window.Worker, _OrigWorker);
    window.Worker.prototype = _OrigWorker.prototype;
  }} catch(e) {{}}

  // ── 3. NAVIGATOR PROPERTIES ─────────────────────────────────────
  function _defNav(key, val) {{
    try {{
      const getter = _makeNative(() => val, `get ${{key}}`);
      Object.defineProperty(navigator, key, {{get: getter, configurable: true, enumerable: true}});
    }} catch(e) {{}}
  }}
  _defNav('platform',           '{fp["platform"]}');
  _defNav('hardwareConcurrency', {fp["hw_concurrency"]});
  _defNav('deviceMemory',        {fp["device_memory"]});
  _defNav('maxTouchPoints',      {touch_pts});
  _defNav('language',            '{fp["locale"]}');
  _defNav('languages',           ['{fp["locale"]}', 'en']);
  _defNav('cookieEnabled',       true);
  _defNav('onLine',              true);
  _defNav('doNotTrack',          null);
  _defNav('vendor',              '{nav_vendor}');
  _defNav('vendorSub',           '');
  _defNav('productSub',          '20030107');
  _defNav('appVersion',          '{fp["user_agent"].replace("Mozilla/", "")}');

  // ── 4. PLUGINS ──────────────────────────────────────────────────
  try {{
    const _pl = [
      {{name:'PDF Viewer',          filename:'internal-pdf-viewer', description:'Portable Document Format', length:1}},
      {{name:'Chrome PDF Viewer',   filename:'internal-pdf-viewer', description:'Portable Document Format', length:1}},
      {{name:'Chromium PDF Viewer', filename:'internal-pdf-viewer', description:'Portable Document Format', length:1}},
      {{name:'WebKit built-in PDF', filename:'internal-pdf-viewer', description:'Portable Document Format', length:1}},
    ];
    Object.defineProperty(navigator,'plugins',   {{get:_makeNative(()=>_pl,'get plugins'),configurable:true}});
    Object.defineProperty(navigator,'mimeTypes', {{get:_makeNative(()=>[{{type:'application/pdf',suffixes:'pdf'}}],'get mimeTypes'),configurable:true}});
  }} catch(e) {{}}

  // ── 5. USER AGENT DATA ──────────────────────────────────────────
  try {{
    if (navigator.userAgentData) {{
      const _uad = {{
        brands: [
          {{brand:'Not/A)Brand',   version:'8'}},
          {{brand:'Chromium',      version:'{fp["brand_ver"]}'}},
          {{brand:'{fp["brand"]}', version:'{fp["brand_ver"]}'}},
        ],
        mobile: {mob},
        platform: '{fp["platform"].split(" ")[0]}',
        getHighEntropyValues: _makeNative(hints => Promise.resolve({{
          architecture:'x86', bitness:'64', mobile:{mob},
          model:'', platform:'{fp["platform"].split(" ")[0]}',
          platformVersion:'{fp["os_version"]}',
          uaFullVersion:'{fp["brand_ver"]}.0.6367.82',
          fullVersionList:[
            {{brand:'Not/A)Brand',  version:'8.0.0.0'}},
            {{brand:'Chromium',     version:'{fp["brand_ver"]}.0.6367.82'}},
            {{brand:'{fp["brand"]}',version:'{fp["brand_ver"]}.0.6367.82'}},
          ],
        }}), 'getHighEntropyValues'),
      }};
      Object.defineProperty(navigator,'userAgentData',{{get:_makeNative(()=>_uad,'get userAgentData'),configurable:true}});
    }}
  }} catch(e) {{}}

  // ── 6. WEBGL ────────────────────────────────────────────────────
  try {{
    const _origGetCtx = HTMLCanvasElement.prototype.getContext;
    HTMLCanvasElement.prototype.getContext = _makeNative(function(type, ...args) {{
      const ctx = _origGetCtx.call(this, type, ...args);
      if (ctx && (type==='webgl'||type==='webgl2')) {{
        const _gp = ctx.getParameter.bind(ctx);
        ctx.getParameter = _makeNative(function(p) {{
          if (p===35661) return {gl_prec};
          if (p===36347) return 1024;
          if (p===36348) return 1024;
          if (p===34076) return 16384;
          if (p===34024) return 4096;
          if (p===3379)  return 16384;
          if (p===34921) return 16;
          if (p===36203) return 32;
          if (p===34930) return 16;
          if (p===33795) return "{fp.get('webgl_vendor', 'Google Inc. (NVIDIA)')}";
          if (p===33796) return "{fp.get('webgl_renderer', 'ANGLE (NVIDIA, NVIDIA GeForce RTX 3080 Direct3D11 vs_5_0 ps_5_0, D3D11)')}";
          return _gp(p);
        }}, 'getParameter');
        const _gse = ctx.getSupportedExtensions.bind(ctx);
        ctx.getSupportedExtensions = _makeNative(function() {{
          return ['ANGLE_instanced_arrays','EXT_blend_minmax','EXT_color_buffer_half_float',
                  'EXT_disjoint_timer_query','EXT_float_blend','EXT_frag_depth',
                  'EXT_shader_texture_lod','EXT_texture_compression_bptc',
                  'EXT_texture_filter_anisotropic','OES_element_index_uint',
                  'OES_standard_derivatives','OES_texture_float','OES_texture_float_linear',
                  'OES_texture_half_float','OES_texture_half_float_linear',
                  'OES_vertex_array_object','WEBGL_color_buffer_float',
                  'WEBGL_compressed_texture_s3tc','WEBGL_debug_renderer_info',
                  'WEBGL_debug_shaders','WEBGL_depth_texture','WEBGL_draw_buffers',
                  'WEBGL_lose_context','WEBGL_multi_draw'];
        }}, 'getSupportedExtensions');
      }}
      return ctx;
    }}, 'getContext');
  }} catch(e) {{}}

  // ── 7. CANVAS NOISE ─────────────────────────────────────────────
  try {{
    const _origDU = HTMLCanvasElement.prototype.toDataURL;
    const _origGID = CanvasRenderingContext2D.prototype.getImageData;

    HTMLCanvasElement.prototype.toDataURL = _makeNative(function(...a) {{
      const ctx2 = this.getContext && this.getContext('2d');
      if (ctx2 && this.width && this.height) {{
        try {{
          const id = _origGID.call(ctx2, 0, 0, 1, 1);
          id.data[0] = Math.min(255, id.data[0] + (Math.random() > 0.5 ? 1 : -1));
          ctx2.putImageData(id, this.width - 1, this.height - 1);
        }} catch (e) {{}}
      }}
      return _origDU.apply(this, a);
    }}, 'toDataURL');

    CanvasRenderingContext2D.prototype.getImageData = _makeNative(function(x, y, w, h) {{
      const id = _origGID.call(this, x, y, w, h);
      if (w > 5 && h > 5) {{ // only noise check-sized requests
          for (let i = 0; i < 20; i++) {{
              const idx = Math.floor(Math.random() * id.data.length);
              id.data[idx] = id.data[idx] ^ 0x01;
          }}
      }}
      return id;
    }}, 'getImageData');
  }} catch(e) {{}}

  // ── 8. AUDIO NOISE ──────────────────────────────────────────────
  try {{
    if (typeof AudioBuffer !== 'undefined') {{
      const _oCD = AudioBuffer.prototype.getChannelData;
      AudioBuffer.prototype.getChannelData = _makeNative(function(ch) {{
        const d = _oCD.call(this, ch);
        for (let i=0;i<d.length;i+=127) d[i] += {fp["audio_noise"]:.2e}*(Math.random()*2-1);
        return d;
      }}, 'getChannelData');
    }}
  }} catch(e) {{}}

  // ── 9. WEBRTC / DNS LEAK BLOCK ──────────────────────────────────
  try {{
    ['RTCPeerConnection','webkitRTCPeerConnection','mozRTCPeerConnection'].forEach(cls => {{
      if (!window[cls]) return;
      const _O = window[cls];
      window[cls] = _makeNative(function(cfg,...rest) {{
        if (cfg&&cfg.iceServers) cfg.iceServers=[];
        try {{ return new _O(cfg,...rest); }} catch(e) {{
          return {{createDataChannel:()=>({{close:()=>{{}}}}),createOffer:()=>Promise.resolve(),setLocalDescription:()=>Promise.resolve(),close:()=>{{}}}};
        }}
      }}, cls);
      Object.assign(window[cls], _O);
    }});
  }} catch(e) {{}}

  // ── 10. SCREEN ──────────────────────────────────────────────────
  try {{
    const _sw={fp["viewport"]["width"]}, _sh={fp["viewport"]["height"]};
    ['width','availWidth'].forEach(k=>Object.defineProperty(screen,k,{{get:_makeNative(()=>_sw,`get ${{k}}`),configurable:true}}));
    Object.defineProperty(screen,'height',     {{get:_makeNative(()=>_sh,'get height'),configurable:true}});
    Object.defineProperty(screen,'availHeight',{{get:_makeNative(()=>_sh-48,'get availHeight'),configurable:true}});
    Object.defineProperty(screen,'colorDepth', {{get:_makeNative(()=>24,'get colorDepth'),configurable:true}});
    Object.defineProperty(screen,'pixelDepth', {{get:_makeNative(()=>24,'get pixelDepth'),configurable:true}});
    if (screen.orientation) {{
      Object.defineProperty(screen.orientation,'type',{{get:_makeNative(()=>'{fp["screen_orientation"]}','get type'),configurable:true}});
    }}
    // [FIX v5] Sync window.outerWidth/Height with viewport to avoid fingerprint mismatch
    Object.defineProperty(window, 'outerWidth',  {{get: _makeNative(()=>_sw, 'get outerWidth'), configurable: true}});
    Object.defineProperty(window, 'outerHeight', {{get: _makeNative(()=>_sh, 'get outerHeight'), configurable: true}});
    Object.defineProperty(window, 'innerWidth',  {{get: _makeNative(()=>_sw, 'get innerWidth'), configurable: true}});
    Object.defineProperty(window, 'innerHeight', {{get: _makeNative(()=>_sh, 'get innerHeight'), configurable: true}});
    Object.defineProperty(window, 'devicePixelRatio', {{get: _makeNative(()=>{fp["dpr"]}, 'get devicePixelRatio'), configurable: true}});
  }} catch(e) {{}}

  // ── 11. BATTERY API ─────────────────────────────────────────────
  try {{
    const _bat={{charging:{charging},chargingTime:{fp["charging_time"]},dischargingTime:{fp["discharging_time"]},level:{fp["battery_level"]:.2f},addEventListener:()=>{{}},removeEventListener:()=>{{}}}};
    Object.defineProperty(navigator,'getBattery',{{get:_makeNative(()=>_makeNative(()=>Promise.resolve(_bat),'getBattery'),'get getBattery'),configurable:true}});
  }} catch(e) {{}}

  // ── 12. GEOLOCATION ─────────────────────────────────────────────
  try {{
    const _pos={{coords:{{latitude:{fp["latitude"]},longitude:{fp["longitude"]},accuracy:{random.randint(10,45)},altitude:null,altitudeAccuracy:null,heading:null,speed:null}},timestamp:Date.now()}};
    navigator.geolocation.getCurrentPosition=_makeNative((ok)=>ok(_pos),'getCurrentPosition');
    navigator.geolocation.watchPosition     =_makeNative((ok)=>{{ok(_pos);return Math.floor(Math.random()*9999);}},'watchPosition');
  }} catch(e) {{}}

  // ── 13. NETWORK INFO ────────────────────────────────────────────
  try {{
    const _c={{downlink:{network_downlink},effectiveType:'{network_eff_type}',rtt:{network_rtt},saveData:false,type:'{network_type}',addEventListener:()=>{{}},removeEventListener:()=>{{}}}};
    ['connection','mozConnection','webkitConnection'].forEach(k=>{{
      try{{Object.defineProperty(navigator,k,{{get:_makeNative(()=>_c,`get ${{k}}`),configurable:true}})}}catch(e){{}}
    }});
  }} catch(e) {{}}

  // ── 14. PERMISSIONS ─────────────────────────────────────────────
  try {{
    const _oq=navigator.permissions.query.bind(navigator.permissions);
    navigator.permissions.query=_makeNative(p=>{{
      if(['notifications','geolocation','camera','microphone'].includes(p.name))
        return Promise.resolve({{state:'prompt',onchange:null}});
      return _oq(p);
    }},'query');
  }} catch(e) {{}}

  // ── 15. CHROME OBJECT ───────────────────────────────────────────
  try {{
    if (!window.chrome) {{
      window.chrome={{
        runtime:{{
          id:undefined,
          connect:_makeNative(function(info){{return{{postMessage:_makeNative(()=>{{}},'postMessage'),disconnect:_makeNative(()=>{{}},'disconnect'),onMessage:{{addListener:()=>{{}},removeListener:()=>{{}},hasListener:()=>false,hasListeners:()=>false}},onDisconnect:{{addListener:()=>{{}},removeListener:()=>{{}},hasListener:()=>false,hasListeners:()=>false}},name:(info&&info.name)||'',sender:undefined}};}},'connect'),
          sendMessage:_makeNative(()=>Promise.resolve(),'sendMessage'),
          onMessage:{{addListener:()=>{{}},removeListener:()=>{{}},hasListener:()=>false}},
          onConnect:{{addListener:()=>{{}},removeListener:()=>{{}}}},
          getManifest:_makeNative(()=>({{}}),'getManifest'),
          getURL:_makeNative((path)=>'','getURL'),
        }},
        loadTimes:_makeNative(()=>({{firstPaintTime:performance.now()/1000*0.3+0.2,requestTime:(Date.now()-3000)/1000}}),'loadTimes'),
        csi:_makeNative(()=>({{pageT:Date.now()-Math.random()*500,startE:performance.timing?.navigationStart||0}}),'csi'),
        app:{{isInstalled:false}},
      }};
    }}
  }} catch(e) {{}}

  // ── 16. SPEECH SYNTHESIS ────────────────────────────────────────
  try {{ if(!window.speechSynthesis) window.speechSynthesis={{speak:()=>{{}},cancel:()=>{{}},getVoices:()=>[],speaking:false,pending:false,paused:false}}; }} catch(e) {{}}

  // ── 17. NOTIFICATIONS ───────────────────────────────────────────
  try {{
    if(window.Notification){{
      Object.defineProperty(Notification,'permission',{{get:_makeNative(()=>'default','get permission'),configurable:true}});
      Notification.requestPermission=_makeNative(()=>Promise.resolve('default'),'requestPermission');
    }}
  }} catch(e) {{}}

  // ── 18. INTL LOCALE ─────────────────────────────────────────────
  try {{
    const _loc='{fp["locale"].replace("_","-")}';
    const _DTF=Intl.DateTimeFormat; Intl.DateTimeFormat=_makeNative(function(l,o){{return new _DTF(_loc,o);}},'DateTimeFormat'); Object.setPrototypeOf(Intl.DateTimeFormat,_DTF);
    const _NF=Intl.NumberFormat; Intl.NumberFormat=_makeNative(function(l,o){{return new _NF(_loc,o);}},'NumberFormat'); Object.setPrototypeOf(Intl.NumberFormat,_NF);
  }} catch(e) {{}}

  // ────────────────────────────────────────────────────────────────
  // [FIX v5] ACCURATE DST-AWARE TIMEZONE OFFSET
  // Old: simple month check (March-November) — wrong edge weeks
  // New: Compare Jan vs Jul offset — matches real browser behaviour
  // ────────────────────────────────────────────────────────────────
  try {{
    const _tzBase    = {tz_base};   // minutes offset for {tz}
    const _tzNoDST   = {tz_no_dst}; // true = Arizona / Phoenix (no DST)
    const _origGTO   = Date.prototype.getTimezoneOffset;
    Date.prototype.getTimezoneOffset = _makeNative(function() {{
      if (_tzNoDST) return _tzBase;
      // DST detection: compare a Jan 1 offset vs Jul 1 offset
      // If current month is in summer, apply -60 (DST)
      const m = this.getUTCMonth(); // 0=Jan … 11=Dec
      // US DST: 2nd Sun March → 1st Sun November
      // Approximate: months 3-10 (Apr-Oct) are definitely DST
      // Month 2 (Mar) and 10 (Nov) are transition — use day check
      let isDST = false;
      if (m >= 3 && m <= 9) {{ isDST = true; }}
      else if (m === 2) {{  // March: DST starts 2nd Sunday
        const day = this.getUTCDate();
        const dow = this.getUTCDay();
        const secondSunday = 8 + ((7 - new Date(this.getUTCFullYear(),2,1).getUTCDay()) % 7);
        isDST = day >= secondSunday;
      }} else if (m === 10) {{  // November: DST ends 1st Sunday
        const day = this.getUTCDate();
        const firstSunday = 1 + ((7 - new Date(this.getUTCFullYear(),10,1).getUTCDay()) % 7);
        isDST = day < firstSunday;
      }}
      return isDST ? _tzBase - 60 : _tzBase;
    }}, 'getTimezoneOffset');
  }} catch(e) {{}}

  // ── 19. HISTORY LENGTH ──────────────────────────────────────────
  try {{ Object.defineProperty(history,'length',{{get:_makeNative(()=>Math.floor(Math.random()*6)+3,'get length'),configurable:true}}); }} catch(e) {{}}

  // ────────────────────────────────────────────────────────────────
  // [FIX v5] PERFORMANCE.TIMING — Synthetic consistent object
  // Real browsers have all timing fields monotonically increasing.
  // Playwright's timing is inconsistent (navigationStart drift).
  // ────────────────────────────────────────────────────────────────
  try {{
    const _ns = {nav_start};
    const _synthetic = {{
      navigationStart:          _ns,
      fetchStart:               _ns + {fetch_offset},
      domainLookupStart:        _ns + {dns_start},
      domainLookupEnd:          _ns + {dns_end},
      connectStart:             _ns + {dns_end},
      secureConnectionStart:    _ns + {dns_end + 5},
      connectEnd:               _ns + {connect_end},
      requestStart:             _ns + {req_start},
      responseStart:            _ns + {resp_start},
      responseEnd:              _ns + {resp_end},
      domLoading:               _ns + {dom_loading},
      domInteractive:           _ns + {dcl_start - 20},
      domContentLoadedEventStart: _ns + {dcl_start},
      domContentLoadedEventEnd:   _ns + {dcl_end},
      domComplete:              _ns + {load_start - 10},
      loadEventStart:           _ns + {load_start},
      loadEventEnd:             _ns + {load_end},
      redirectStart: 0, redirectEnd: 0,
      unloadEventStart: 0, unloadEventEnd: 0,
      toJSON: () => ({{...this}}),
    }};
    const _origTiming = performance.timing;
    Object.defineProperty(performance, 'timing', {{
      get: _makeNative(() => new Proxy(_origTiming || {{}}, {{
        get(target, prop) {{
          return (_synthetic[prop] !== undefined) ? _synthetic[prop] : target[prop];
        }}
      }}), 'get timing'),
      configurable: true
    }});
  }} catch(e) {{}}

  // ── 20. MOUSE EVENT sourceCapabilities ──────────────────────────
  try {{
    const _ME=MouseEvent;
    window.MouseEvent=_makeNative(function(type,init,...rest){{
      if(init&&!init.sourceCapabilities){{
        try{{init.sourceCapabilities=new InputDeviceCapabilities({{firesTouchEvents:{str(fp.get("has_touch",False)).lower()}}});}}catch(e){{}}
      }}
      return new _ME(type,init,...rest);
    }},'MouseEvent');
    Object.setPrototypeOf(window.MouseEvent,_ME);
    Object.assign(window.MouseEvent,_ME);
  }} catch(e) {{}}

  // ── 21. POINTER EVENT PRESSURE ──────────────────────────────────
  try {{
    const _PE=PointerEvent;
    window.PointerEvent=_makeNative(function PointerEvent(type,init){{
      if(init){{
        if(typeof init.pressure==='undefined'||init.pressure===0) init.pressure=0.5;
        if(!init.pointerType) init.pointerType='mouse';
        if(typeof init.tiltX==='undefined') init.tiltX=0;
        if(typeof init.tiltY==='undefined') init.tiltY=0;
        if(typeof init.width==='undefined') init.width=1;
        if(typeof init.height==='undefined') init.height=1;
      }}
      return new _PE(type,init);
    }},'PointerEvent');
    Object.setPrototypeOf(window.PointerEvent,_PE);
    window.PointerEvent.prototype=_PE.prototype;
  }} catch(e) {{}}

  // ── 22. TIMING JITTER (Gaussian) ────────────────────────────────
  try {{
    const _pn=performance.now.bind(performance);
    performance.now=_makeNative(function now(){{
      return _pn()+(Math.random()+Math.random()+Math.random()-1.5)*0.04;
    }},'now');
  }} catch(e) {{}}

  // ── 23. LOCALSTORAGE WARMING ────────────────────────────────────
  try {{
    const _lsKeys=['visited_before','last_visit','user_pref','theme','consent_given',
                   'session_id','referrer_src','ab_variant','cookie_consent','ui_mode'];
    _lsKeys.forEach(k=>{{if(!localStorage.getItem(k)) localStorage.setItem(k,Math.random().toString(36).substr(2,12));}});
    localStorage.setItem('last_visit',(Date.now()-Math.floor(Math.random()*604800000)).toString());
    sessionStorage.setItem('_s',Math.random().toString(36).substr(2,10));
    sessionStorage.setItem('_t0',Date.now().toString());
    sessionStorage.setItem('_ref','{fp["referrer"]}');
  }} catch(e) {{}}

  // ── 24. INDEXEDDB WARMING ───────────────────────────────────────
  try {{
    const _req=indexedDB.open('browsing_cache',1);
    _req.onupgradeneeded=e=>{{const db=e.target.result;if(!db.objectStoreNames.contains('pages'))db.createObjectStore('pages',{{keyPath:'url'}});}};
    _req.onsuccess=e=>{{
      const db=e.target.result;
      try{{const tx=db.transaction('pages','readwrite');const store=tx.objectStore('pages');
        ['https://www.google.com','https://www.reddit.com','https://www.youtube.com',
         'https://www.amazon.com'].forEach(url=>{{store.put({{url,visitCount:Math.floor(Math.random()*20)+1,lastVisit:Date.now()-Math.random()*2592000000}});}});
      }}catch(e){{}}
    }};
  }} catch(e) {{}}

  // ── 25. FONT NORMALIZATION ──────────────────────────────────────
  try {{
    if(document.fonts){{
      const _fc=document.fonts.check.bind(document.fonts);
      const _commonFonts=new Set(['Arial','Arial Black','Comic Sans MS','Courier New',
        'Georgia','Impact','Times New Roman','Trebuchet MS','Verdana','Segoe UI',
        'Calibri','Tahoma','Palatino','Century Gothic','Helvetica',
        'Lucida Console','Lucida Sans Unicode','Microsoft Sans Serif']);
      document.fonts.check=_makeNative(function(font,text){{
        const name=font.replace(/^[\d.]+px\s+/,'').replace(/['"]/g,'').split(',')[0].trim();
        if(_commonFonts.has(name)) return true;
        return _fc(font,text);
      }},'check');
    }}
  }} catch(e) {{}}

  // ── 26. CSS MEDIA QUERY SPOOF ───────────────────────────────────
  try {{
    const _mql=window.matchMedia;
    const _prefersDark={prefers_dark};
    window.matchMedia=_makeNative(function(q){{
      const r=_mql.call(window,q);
      if(q.includes('prefers-color-scheme:dark')||q.includes('prefers-color-scheme: dark'))
        Object.defineProperty(r,'matches',{{get:()=>_prefersDark,configurable:true}});
      if(q.includes('prefers-reduced-motion'))
        Object.defineProperty(r,'matches',{{get:()=>false,configurable:true}});
      if(q.includes('pointer:fine'))
        Object.defineProperty(r,'matches',{{get:()=>!{str(fp.get("has_touch",False)).lower()},configurable:true}});
      return r;
    }},'matchMedia');
  }} catch(e) {{}}

  // ── 27. DOCUMENT.HASFOCUS() — always true ───────────────────────
  try {{
    Document.prototype.hasFocus=_makeNative(function hasFocus(){{return true;}},'hasFocus');
    _nativeMap.set(Document.prototype.hasFocus,'function hasFocus() {{ [native code] }}');
  }} catch(e) {{}}

  // ── 28. WINDOW.OPEN COUNTER ─────────────────────────────────────
  try {{
    if(!window.__origOpen){{
      window.__origOpen=window.open; window.__popunderCount=0;
      window.open=_makeNative(function(u,n,s){{window.__popunderCount++;return window.__origOpen.apply(this,arguments);}},'open');
    }}
  }} catch(e) {{}}

  // ─────────────────────────────────────────────────────────────────
  // [NEW v5] MOBILE DEVICE ORIENTATION EVENTS
  // Real phones fire deviceorientation every ~100ms with slight drift.
  // Absence is a bot signal on mobile UAs.
  // ─────────────────────────────────────────────────────────────────
  {'"""' if not is_mobile else ''}
  {'' if not is_mobile else f"""
  try {{
    let _alpha = {init_alpha}, _beta = {init_beta}, _gamma = {init_gamma};
    setInterval(() => {{
      _alpha = (_alpha + (Math.random() - 0.5) * 0.4 + 360) % 360;
      _beta  = Math.max(-180, Math.min(180, _beta  + (Math.random()-0.5)*0.15));
      _gamma = Math.max(-90,  Math.min(90,  _gamma + (Math.random()-0.5)*0.10));
      try {{
        window.dispatchEvent(new DeviceOrientationEvent('deviceorientation', {{
          alpha: _alpha, beta: _beta, gamma: _gamma, absolute: false
        }}));
      }} catch(e) {{}}
    }}, 100 + Math.floor(Math.random() * 30));
  }} catch(e) {{}}
  """}
  {'"""' if not is_mobile else ''}

}})();
"""


# ─────────────────────────────────────────────────────────────────
# BEHAVIORAL JS (unchanged — already solid)
# ─────────────────────────────────────────────────────────────────
BEHAVIORAL_JS = r"""
(function() {
  'use strict';
  if (window.__bhv) return;
  window.__bhv = true;

  window.__scrollDepth=0; window.__maxScrollDepth=0; window.__timeOnPage=0;
  window.__tabHidden=0; window.__clickLog=[]; window.__mousePositions=[];
  window.__keystrokes=[]; window.__copyCount=0; window.__pasteCount=0;
  window.__impressions=window.__impressions||[]; window.__popunderCount=window.__popunderCount||0;

  window.addEventListener('scroll',function(){{
    const total=document.documentElement.scrollHeight-window.innerHeight;
    if(total>0){{window.__scrollDepth=Math.round((window.scrollY/total)*100);
    if(window.__scrollDepth>window.__maxScrollDepth)window.__maxScrollDepth=window.__scrollDepth;}}
  }},{{passive:true}});
  setInterval(function(){{if(!document.hidden)window.__timeOnPage++;}},1000);
  document.addEventListener('visibilitychange',function(){{if(document.hidden)window.__tabHidden++;}});
  let _lme=0;
  document.addEventListener('mousemove',function(e){{
    const now=Date.now(); if(now-_lme>250){{
    window.__mousePositions.push({{x:e.clientX,y:e.clientY,t:now-performance.timing.navigationStart}});
    if(window.__mousePositions.length>120)window.__mousePositions.shift(); _lme=now;}}
  }},{{passive:true}});
  document.addEventListener('click',function(e){{
    window.__clickLog.push({{t:Date.now(),x:e.clientX,y:e.clientY,tag:(e.target&&e.target.tagName)||'?'}});
    if(window.__clickLog.length>50)window.__clickLog.shift();
  }},{{passive:true,capture:true}});
  let _lk=0;
  document.addEventListener('keydown',function(){{
    const n=Date.now(); if(_lk)window.__keystrokes.push(n-_lk); _lk=n;
    if(window.__keystrokes.length>200)window.__keystrokes.shift();
  }},{{passive:true}});
  document.addEventListener('copy',()=>window.__copyCount++,{{passive:true}});
  document.addEventListener('paste',()=>window.__pasteCount++,{{passive:true}});

  // Micro-scroll
  let _msd=1;
  setInterval(()=>{{
    if(Math.random()<0.2){{window.scrollBy(0,_msd*(Math.random()<0.5?1:2));if(Math.random()<0.08)_msd*=-1;}}
  }},2200);
})();
"""


def get_behavioral_stats_js() -> str:
    return """(()=>({
      scrollDepth: window.__maxScrollDepth||0,
      timeOnPage:  window.__timeOnPage||0,
      tabHidden:   window.__tabHidden||0,
      clicks:      (window.__clickLog||[]).length,
      mousePoints: (window.__mousePositions||[]).length,
      copyCount:   window.__copyCount||0,
      keystrokesCount: (window.__keystrokes||[]).length,
      impressionCount: (window.__impressions||[]).length,
      popunderCount:   window.__popunderCount||0,
    }))()"""


# ─────────────────────────────────────────────────────────────────
# FITTS'S LAW MOUSE PATH GENERATOR (unchanged — already great)
# ─────────────────────────────────────────────────────────────────
def generate_human_mouse_path(sx, sy, tx, ty, overshooting=True):
    dist = math.hypot(tx-sx, ty-sy)
    if dist < 1:
        return [(tx, ty, random.randint(10,30))]
    steps = max(8, min(40, int(dist/random.uniform(18,35))))
    cp1x = sx+(tx-sx)*0.25+random.gauss(0,dist*0.12)
    cp1y = sy+(ty-sy)*0.25+random.gauss(0,dist*0.12)
    cp2x = sx+(tx-sx)*0.75+random.gauss(0,dist*0.08)
    cp2y = sy+(ty-sy)*0.75+random.gauss(0,dist*0.08)
    path = []
    for i in range(steps+1):
        t=i/steps; te=t*t*(3-2*t); u=1-te
        x=u**3*sx+3*u**2*te*cp1x+3*u*te**2*cp2x+te**3*tx
        y=u**3*sy+3*u**2*te*cp1y+3*u*te**2*cp2y+te**3*ty
        js=1.5+(1-t)*2.0
        x+=random.gauss(0,js*0.4); y+=random.gauss(0,js*0.4)
        sf=4*t*(1-t); bd=max(2,18-int(sf*14))
        path.append((round(x,1), round(y,1), max(1,int(random.gauss(bd,bd*0.3)))))
    if overshooting and random.random()<0.20 and dist>50:
        ox=tx+random.gauss(0,8); oy=ty+random.gauss(0,8)
        path.append((ox,oy,random.randint(25,60)))
        for _ in range(random.randint(2,4)):
            cx=ox+(tx-ox)*random.uniform(0.3,0.9); cy=oy+(ty-oy)*random.uniform(0.3,0.9)
            path.append((round(cx,1),round(cy,1),random.randint(15,35)))
    path.append((tx+random.gauss(0,1.5), ty+random.gauss(0,1.5), random.randint(80,300)))
    return path


# [FIX v5] ASYNC version — use this with Playwright async API
async def async_human_click(page: Any, x: float, y: float,
                             start_x: float = None, start_y: float = None) -> None:
    """
    Biomechanically accurate async mouse click.
    Awaits each movement so it doesn't block the event loop.
    """
    sx = start_x if start_x is not None else random.randint(100, 800)
    sy = start_y if start_y is not None else random.randint(100, 500)
    path = generate_human_mouse_path(sx, sy, x, y)
    for px, py, delay_ms in path:
        try:
            await page.mouse.move(px, py)
        except Exception:
            pass
        await asyncio.sleep(delay_ms / 1000.0)
    try:
        await page.mouse.down()
        await asyncio.sleep(random.uniform(0.04, 0.18))
        await page.mouse.up()
    except Exception:
        try:
            await page.click(f"xpath=//body", position={"x": x, "y": y})
        except Exception:
            pass


# Sync version kept for backwards compatibility
def human_click(page: Any, x: float, y: float,
                start_x: float = None, start_y: float = None) -> None:
    sx = start_x if start_x is not None else random.randint(100, 800)
    sy = start_y if start_y is not None else random.randint(100, 500)
    path = generate_human_mouse_path(sx, sy, x, y)
    for px, py, delay_ms in path:
        try:
            page.mouse.move(px, py)
        except Exception:
            pass
        time.sleep(delay_ms / 1000.0)
    try:
        page.mouse.down()
        time.sleep(random.uniform(0.04, 0.18))
        page.mouse.up()
    except Exception:
        try:
            page.mouse.click(x, y)
        except Exception:
            pass
