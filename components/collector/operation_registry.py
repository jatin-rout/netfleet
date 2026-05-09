from shared.config.settings import OPERATIONS


class OperationRegistry:
    """
    Resolves operation → command list for a given vendor and protocol.

    For SSH/Telnet operations: returns a list of {"cmd": str, "timeout": int} dicts.
    For SNMP operations:       returns a list of {"oid": str, "label": str} dicts.
    For SET operations:        returns the full step config dict (backup/apply/rollback).
    """

    _ops: dict = OPERATIONS.get("operations", {})

    @classmethod
    def get_type(cls, operation: str) -> str:
        """GET or SET."""
        spec = cls._ops.get(operation)
        if not spec:
            raise ValueError(f"Unknown operation: {operation}")
        return spec["type"]

    @classmethod
    def get_commands(cls, operation: str, vendor: str) -> list[dict]:
        """Return CLI commands for SSH/Telnet GET operations."""
        spec = cls._ops.get(operation)
        if not spec:
            raise ValueError(f"Unknown operation: {operation}")
        cmds = spec.get("commands", {}).get(vendor)
        if not cmds:
            raise ValueError(
                f"No CLI commands defined for operation={operation} vendor={vendor}"
            )
        return cmds

    @classmethod
    def get_snmp_oids(cls, operation: str, vendor: str) -> list[dict]:
        """Return SNMP OID list for SNMP GET operations."""
        spec = cls._ops.get(operation)
        if not spec:
            raise ValueError(f"Unknown operation: {operation}")
        oids = spec.get("snmp_oids", {}).get(vendor)
        if not oids:
            raise ValueError(
                f"No SNMP OIDs defined for operation={operation} vendor={vendor}"
            )
        return oids

    @classmethod
    def get_set_steps(cls, operation: str, vendor: str) -> dict:
        """
        Return the full SET step configuration:
            {
              "backup":   {"commands": [{"cmd": ..., "timeout": ...}]},
              "apply":    {"mode": ..., "verify_command": ..., "commit_command": ...},
              "rollback": [{"cmd": ...}, ...]
            }
        Normalised from raw YAML into a vendor-specific view.
        """
        spec = cls._ops.get(operation)
        if not spec:
            raise ValueError(f"Unknown operation: {operation}")
        if spec["type"] != "SET":
            raise ValueError(f"Operation {operation} is not a SET operation")

        steps = spec.get("steps", {})
        backup_cmds = steps.get("backup", {}).get("commands", {}).get(vendor)
        if not backup_cmds:
            raise ValueError(
                f"No backup commands for operation={operation} vendor={vendor}"
            )

        apply_spec = steps.get("apply", {})
        verify_cmd = apply_spec.get("verify_command", {}).get(vendor)
        commit_cmd = apply_spec.get("commit_command", {}).get(vendor)

        rollback_cmds = steps.get("rollback", {}).get(vendor)
        if not rollback_cmds:
            raise ValueError(
                f"No rollback commands for operation={operation} vendor={vendor}"
            )

        return {
            "backup": backup_cmds,
            "apply": {
                "mode": apply_spec.get("mode", "config"),
                "verify_command": verify_cmd,
                "commit_command": commit_cmd,
            },
            "rollback": rollback_cmds,
        }

    @classmethod
    def has_operation(cls, operation: str) -> bool:
        return operation in cls._ops
