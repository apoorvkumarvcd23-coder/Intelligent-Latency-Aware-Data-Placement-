"""Browser verification and optional public-synthetic screenshot log transport."""
import argparse
import base64
from pathlib import Path
import textwrap

from playwright.sync_api import sync_playwright


parser = argparse.ArgumentParser()
parser.add_argument('--empty',action='store_true')
parser.add_argument('--log-image',action='store_true')
parser.add_argument('--backend',choices=['simulated','hdfs','degraded'],default='simulated')
parser.add_argument('--clock',default='seeded-event-time-replay')
parser.add_argument('--invalid',type=int,default=3)
parser.add_argument('--duplicates',type=int,default=1)
parser.add_argument('--output',type=Path,default=Path('artifacts/placement-dashboard.png'))
args = parser.parse_args()
with sync_playwright() as api:
    browser = api.chromium.launch()
    try:
        page = browser.new_page(viewport={'width':1440,'height':1100})
        page.goto('http://127.0.0.1:8508')
        if args.empty:
            page.get_by_text('Waiting for the placement engine',exact=False).wait_for(timeout=30000)
        else:
            page.get_by_text(args.clock,exact=False).wait_for(timeout=30000)
            page.get_by_text('invalid events: '+str(args.invalid),exact=False).wait_for()
            page.get_by_text('duplicates skipped: '+str(args.duplicates),exact=False).wait_for()
            badge={'simulated':'SIMULATED cloud (configured)','hdfs':'REAL HDFS','degraded':'DEGRADED verified mirror'}[args.backend]
            page.get_by_text(badge,exact=False).wait_for()
            if args.backend == 'degraded':
                page.get_by_test_id('stAlert').filter(has_text='DEGRADED verified mirror').wait_for()
            page.get_by_role('heading',name='Edge cache vs ground-truth hot set').wait_for()
            page.locator('[data-testid="stVegaLiteChart"] canvas').last.wait_for(timeout=30000)
            assert not page.locator('[data-testid="stException"]').count()
            path = args.output
            path.parent.mkdir(parents=True,exist_ok=True)
            page.screenshot(path=str(path),full_page=True)
            if args.log_image:
                print('CORE06_SCREENSHOT_BEGIN')
                print('\n'.join(textwrap.wrap(base64.b64encode(path.read_bytes()).decode(),4096)))
                print('CORE06_SCREENSHOT_END')
        print('Dashboard browser verification passed')
    finally:
        browser.close()
