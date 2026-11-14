"""Control only a named disposable Compose HDFS lab and record real transitions."""
import json
import os
from pathlib import Path
import re
import subprocess
import sys
import time
from uuid import uuid4

ROOT=Path(__file__).resolve().parents[1]


def main():
    project=os.environ.get('COMPOSE_PROJECT_NAME','')
    if not re.fullmatch(r'core09-[a-z0-9][a-z0-9-]{0,48}',project):
        raise RuntimeError('Set COMPOSE_PROJECT_NAME=core09-<unique-demo-name>; never use a shared project')
    run=uuid4().hex
    environment={**os.environ,'HDFS_DRILL_RUN':run}
    compose=['docker','compose','-f','compose.hdfs-demo.yml']
    def command(args,timeout=180,capture=False):
        return subprocess.run(args,cwd=ROOT,env=environment,check=True,timeout=timeout,
                              capture_output=capture,text=True)
    def dc(*args,**kwargs):
        return command(compose+list(args),**kwargs)
    control=ROOT/'artifacts/hdfs-control'/run
    # Host owns the handshake directory so it can signal a root container on Linux.
    control.mkdir(parents=True,exist_ok=False)
    def wait(name):
        deadline=time.monotonic()+300
        while not (control/(name+'.json')).is_file():
            state=dc('ps','--all','--format','json','hdfs-drill',capture=True).stdout
            # Compose outputs one object per line (or an array with older versions).
            rows=json.loads(state) if state.lstrip().startswith('[') else [json.loads(s) for s in state.splitlines() if s.strip()]
            if rows and rows[0].get('State') in {'exited','dead'}:
                dc('logs','--no-color','hdfs-drill')
                raise RuntimeError('Actual HDFS drill exited before '+name)
            if time.monotonic()>deadline:
                dc('logs','--no-color','hdfs-drill')
                raise RuntimeError('Timed out waiting for '+name)
            time.sleep(0.5)
        return json.loads((control/(name+'.json')).read_text())
    def screenshot(backend,name):
        command([sys.executable,'scripts/check_dashboard.py','--backend',backend,
                 '--clock','hdfs-outage-recovery-drill','--invalid','0','--duplicates','0',
                 '--output','artifacts/'+name])
    dc('up','-d','--no-deps','hdfs-drill')
    try:
        wait('healthy')
        screenshot('hdfs','hdfs-healthy.png')
        dc('stop','-t','10','namenode','datanode')
        running=dc('ps','--status','running','--services',capture=True).stdout.splitlines()
        assert 'namenode' not in running and 'datanode' not in running
        (control/'outage-now').write_text('Both actual HDFS service containers are stopped.\n')
        wait('outage')
        screenshot('degraded','hdfs-outage.png')
        dc('up','-d','--wait','--wait-timeout','180','namenode','datanode',timeout=240)
        dc('exec','-T','namenode','hdfs','dfsadmin','-safemode','wait',timeout=120)
        version=dc('exec','-T','namenode','hdfs','version',capture=True).stdout.splitlines()[0]
        (control/'recovery-now').write_text('Both HDFS services healthy and safe mode exited.\n')
        report=wait('recovered')
        screenshot('hdfs','hdfs-recovered.png')
        dc('wait','hdfs-drill',timeout=120)
        state=dc('ps','--all','--format','json','hdfs-drill',capture=True).stdout
        rows=json.loads(state) if state.lstrip().startswith('[') else [json.loads(s) for s in state.splitlines() if s.strip()]
        assert rows and rows[0]['ExitCode']==0
        report.update(hadoop_version=version,host_observed_both_hdfs_containers_stopped=True,
                      host_observed_service_health_and_safe_mode_exit=True,
                      screenshot_files=['hdfs-healthy.png','hdfs-outage.png','hdfs-recovered.png'])
        (ROOT/'artifacts/hdfs-drill.json').write_text(json.dumps(report,indent=2)+'\n')
        print('Actual HDFS strict/outage/verified-mirror/recovery and browser checks passed')
    finally:
        dc('logs','--no-color','hdfs-drill')
        dc('stop','-t','10','hdfs-drill')


if __name__=='__main__':
    main()
