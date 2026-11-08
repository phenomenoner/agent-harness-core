# Python/Linux Documentation

The active implementation is the pre-release package in [python/](../../python/). It requires Linux and Python 3.14 or later; WSL is a supported deployment shape. Rust crates and Windows-specific root references remain historical material.

| Guide | Scope |
| --- | --- |
| [Product Guide](PRODUCT-GUIDE.md) | User workflows and completion boundaries. |
| [Technical Overview](TECHNICAL-OVERVIEW.md) | Components, state and ownership. |
| [Configuration](LINUX-NATIVE-CONFIGURATION.md) | Application and foreground configuration. |
| [Linux Service](LINUX-SERVICE.md) | Service preparation and explicit operation. |
| [Execution Access](EXECUTION-ACCESS.md) | Interpreter, subprocess and host-path access. |
| [Channel Notifications](CHANNEL-NOTIFICATIONS.md) | Interactive progress, scheduled output and durable silence. |
| [Task Lifecycles](TASK-LIFECYCLES.md) | Tasks, workspace recovery, schedules and delivery. |
| [Backend Upgrades](BACKEND-UPGRADES.md) | Source pin, qualification and guarded selection boundaries. |
| [Source Verification](SOURCE-VERIFICATION.md) | Dated checks and unresolved gaps. |

Use the [root README](../../README.md) for quick start and positioning. A source check, package, installed instance and live channel result establish different claims. Historical test counts and release tags do not establish current Python acceptance.
