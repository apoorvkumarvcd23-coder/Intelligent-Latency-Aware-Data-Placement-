"""Read-only configuration preflight; requires PyYAML, no running services."""
import argparse
import json

from common.contracts import ContractError, load_config


def main(argv=None):
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--config', help='YAML file; otherwise use PLACEMENT_CONFIG or the default')
    args = parser.parse_args(argv)
    try:
        cfg = load_config(args.config)
    except ContractError as exc:
        parser.error(str(exc))
    print(json.dumps({
        'status': 'valid',
        'backend_policy': cfg['cloud']['backend'],
        'predictor': cfg['placement']['predictor'],
        'datasets': cfg['datasets']['count'],
        'synthetic_payload_bytes': cfg['datasets']['count'] * cfg['datasets']['size_kb'] * 1024,
        'edge_capacity': cfg['edge']['capacity'],
        'edge_payload_bytes': cfg['edge']['capacity'] * cfg['datasets']['size_kb'] * 1024,
        'connectivity': 'not checked',
    }, sort_keys=True))
    return 0


if __name__ == '__main__':
    raise SystemExit(main())
