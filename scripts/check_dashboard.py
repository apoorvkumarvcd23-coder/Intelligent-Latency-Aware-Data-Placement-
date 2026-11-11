"""Browser verification and optional public-synthetic screenshot log transport."""
import argparse
import base64
from pathlib import Path
import textwrap

from playwright.sync_api import sync_playwright


parser = argparse.ArgumentParser()
parser.add_argument('--empty',action='store_true')
parser.add_argument('--log-image',action='store_true')
args = parser.parse_args()
with sync_playwright() as api:
    browser = api.chromium.launch()
    try:
        page = browser.new_page(viewport={'width':1440,'height':1100})
        page.goto('http://127.0.0.1:8508')
        if args.empty:
            page.get_by_text('Waiting for the placement engine',exact=False).wait_for(timeout=30000)
        else:
            page.get_by_text('seeded-event-time-replay',exact=False).wait_for(timeout=30000)
            page.get_by_text('invalid events: 3',exact=False).wait_for()
            page.get_by_text('duplicates skipped: 1',exact=False).wait_for()
            page.get_by_text('SIMULATED cloud (configured)',exact=False).wait_for()
            page.get_by_role('heading',name='Edge cache vs ground-truth hot set').wait_for()
            page.locator('[data-testid="stVegaLiteChart"] canvas').last.wait_for(timeout=30000)
            assert not page.locator('[data-testid="stException"]').count()
            path = Path('artifacts/placement-dashboard.png')
            path.parent.mkdir(parents=True,exist_ok=True)
            page.screenshot(path=str(path),full_page=True)
            if args.log_image:
                print('CORE06_SCREENSHOT_BEGIN')
                print('\n'.join(textwrap.wrap(base64.b64encode(path.read_bytes()).decode(),4096)))
                print('CORE06_SCREENSHOT_END')
        print('Dashboard browser verification passed')
    finally:
        browser.close()
