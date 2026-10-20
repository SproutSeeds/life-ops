import sys

from life_ops.cmail_cloud import legacy_main


if __name__ == "__main__":
    result = legacy_main(sys.argv[1:])
    if result is None:
        from life_ops.cli import main
        main()
    else:
        raise SystemExit(result)

