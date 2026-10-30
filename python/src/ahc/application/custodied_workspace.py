"""Runtime workspace factory with durable restart custody and a lifetime lease."""

from ahc.domain.sessions import ContractError


class CustodiedPort:
    def __init__(self, port, lease):
        self.port = port
        self.lease = lease

    @property
    def identity(self):
        return self.port.identity

    def execute(self, expected, code):
        return self.port.execute(expected, code)

    def inspect(self, expected, names):
        return self.port.inspect(expected, names)

    def checkpoint(self, expected, names):
        return self.port.checkpoint(expected, names)

    def restore(self, expected, checkpoint):
        raise ContractError("runtime restore requires close and a newly admitted generation")

    def close(self):
        self.port.close()
        # Failed cleanup retains ownership until process exit or a successful close.
        self.lease.close()


def open_custodied_workspace(
    store, expected, workspace_id, runner, *, checkpoint_digest=None, timeout=5
):
    from ahc.adapters.docker_workspace import StatefulDockerWorkspace
    from ahc.infrastructure.workspace_custody import WorkspaceCustody

    from .runtime_workspace import RuntimeWorkspace

    custody = WorkspaceCustody(store)
    lease = custody.acquire(expected.session, workspace_id)
    try:
        custody.recover(lease, runner)

        def factory(identity, checkpoint):
            port = StatefulDockerWorkspace(
                runner,
                identity,
                timeout=timeout,
                checkpoint=checkpoint,
                on_created=lambda created_identity, owned: custody.record_created(
                    lease, created_identity, owned
                ),
            )
            return CustodiedPort(port, lease)

        return RuntimeWorkspace.open(store, expected, workspace_id, factory, checkpoint_digest)
    except BaseException:
        lease.close()
        raise
