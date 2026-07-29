"""Report which curl_cffi impersonation profiles ESPN's WAF accepts from here.

ESPN TLS-fingerprints clients and serves flagged ones a challenge with no
`window['__espnfitt__']` payload, which surfaces as empty DataFrames. The
profile that works is environment-dependent — a fingerprint accepted from a
residential IP can be challenged from a datacenter one — so when the HTML
transport starts coming back empty, run this from the affected machine before
suspecting the parser:

    python tests/waf_probe.py

Prints one row per profile: HTTP status, response size, and whether the JSON
payload the scraper needs is present. Only the API transport is unaffected;
it is probed too, as a control.
"""
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "src"))

from curl_cffi import requests as r  # noqa: E402

from cbbpy.utils import cbbpy_utils as cu  # noqa: E402

PROFILES = [
    "safari", "safari15_5", "safari17_0", "safari18_0",
    "chrome", "chrome110", "chrome124", "chrome131",
    "edge99", "edge101", "firefox133",
]

GAME_ID = "401581583"
HTML_URL = cu.MENS_GAME_URL.format(GAME_ID)
API_URL = f"https://site.api.espn.com/apis/site/v2/sports/basketball/mens-college-basketball/summary?event={GAME_ID}"


def probe(url, profile, needle):
    try:
        resp = r.get(url, headers={"Referer": cu.REFERERS[0]}, impersonate=profile, timeout=30)
    except Exception as ex:  # an unsupported profile name raises here
        return f"ERROR {type(ex).__name__}: {str(ex)[:60]}"
    ok = "PAYLOAD" if needle.encode() in resp.content else "CHALLENGED"
    return f"HTTP {resp.status_code}  {len(resp.content):>8,}b  {ok}"


print(f"curl_cffi profiles vs {HTML_URL}\n")
for profile in PROFILES:
    print(f"  {profile:<12} {probe(HTML_URL, profile, cu.WINDOW_STRING)}")

print(f"\ncontrol — API transport ({API_URL.split('?')[0]}):")
print(f"  {cu.IMPERSONATE:<12} {probe(API_URL, cu.IMPERSONATE, 'boxscore')}")
