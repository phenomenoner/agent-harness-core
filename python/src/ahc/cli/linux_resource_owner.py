"""ExecStartPre entry point for explicitly rendered resource-partition services."""

import os


def main():
    from ahc.infrastructure.process.linux_native import _service_directory
    from ahc.infrastructure.process.linux_service_resources import enable_service_controllers

    descriptor = _service_directory(setup=True)
    try:
        enable_service_controllers(descriptor)
    finally:
        os.close(descriptor)


if __name__ == "__main__":
    main()
