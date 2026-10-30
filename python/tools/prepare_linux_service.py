"""Write a reviewable user-service file; no systemd activation or credential discovery."""

import argparse
from pathlib import Path

from ahc.cli.linux_service import render_user_service


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    for name in ("interpreter", "config", "agent", "workspace", "credential-file"):
        parser.add_argument("--" + name, required=True)
    parser.add_argument("--output", type=Path, required=True)
    args = parser.parse_args()
    content = render_user_service(interpreter=args.interpreter, config=args.config,
        agent=args.agent, workspace=args.workspace, credential_file=args.credential_file)
    with args.output.open("x", encoding="utf-8", newline="\n") as output:
        output.write(content)
    print("Prepared user service; not enabled or started.")


if __name__ == "__main__":
    main()
