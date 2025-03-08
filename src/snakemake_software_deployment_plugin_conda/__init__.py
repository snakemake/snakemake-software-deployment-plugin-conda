import json
import os
import shutil
import sys
from dataclasses import dataclass, field
from pathlib import Path
from typing import Dict, Iterable, List, Optional

import yaml

from snakemake_interface_common.exceptions import WorkflowError
from snakemake_interface_software_deployment_plugins.settings import (
    SoftwareDeploymentSettingsBase,
)
from snakemake_interface_software_deployment_plugins import (
    EnvBase,
    DeployableEnvBase,
    EnvSpecBase,
    SoftwareReport,
    EnvSpecSourceFile,
)

from rattler.shell import Shell, activate, ActivationVariables
from rattler.match_spec import MatchSpec
from rattler import solve, install, VirtualPackage
from rattler.platform import Platform
from rattler.repo_data import RepoDataRecord


# Optional:
# Define settings for your storage plugin (e.g. host url, credentials).
# They will occur in the Snakemake CLI as --sdm-<plugin-name>-<param-name>
# Make sure that all defined fields are 'Optional' and specify a default value
# of None or anything else that makes sense in your case.
# Note that we allow storage plugin settings to be tagged by the user. That means,
# that each of them can be specified multiple times (an implicit nargs=+), and
# the user can add a tag in front of each value (e.g. tagname1:value1 tagname2:value2).
# This way, a storage plugin can be used multiple times within a workflow with different
# settings.
@dataclass
class SoftwareDeploymentSettings(SoftwareDeploymentSettingsBase):
    # TODO think about necessary settings here. All the current --conda settings
    # in the main snakemake source can be generalized to all software deployment
    # plugins instead.
    cache_dir: Optional[Path] = field(
        default=os.environ.get("RATTLER_CACHE_DIR"),
        metadata={
            "help": "Rattler cache dir to use (default: $RATTLER_CACHE_DIR).",
            "env_var": False,
            "required": False,
        },
    )


@dataclass
class EnvSpec(EnvSpecBase):
    envfile: Optional[EnvSpecSourceFile] = None
    directory: Optional[Path] = None
    name: Optional[str] = None
    pinfile: Optional[EnvSpecSourceFile] = None

    def __post_init__(self):
        if sum(x is not None for x in (self.envfile, self.name, self.directory)) != 1:
            raise WorkflowError(
                "Exactly one of envfile, name, or directory must be set."
            )

        if self.envfile is not None:
            self.pinfile = EnvSpecSourceFile(
                Path(self.envfile.path_or_uri).with_suffix(
                    f".{Platform.current()}.pin.txt"
                )
            )

    @classmethod
    def identity_attributes(cls) -> Iterable[str]:
        # attributes that uniquely identify the environment spec
        yield "envfile"
        yield "name"
        yield "directory"

    @classmethod
    def source_path_attributes(cls) -> Iterable[str]:
        # attributes that represent paths
        yield "envfile"
        yield "pinfile"


class Env(EnvBase, DeployableEnvBase):
    # For compatibility with future changes, you should not overwrite the __init__
    # method. Instead, use __post_init__ to set additional attributes and initialize
    # futher stuff.

    def __post_init__(self):
        self._envfile_content = None
        if self.shell_executable == "bash":
            self.rattler_shell = Shell.bash
        elif self.shell_executable == "zsh":
            self.rattler_shell = Shell.zsh
        elif self.shell_executable == "xonsh":
            self.rattler_shell = Shell.xonsh
        elif self.shell_executable == "fish":
            self.rattler_shell = Shell.fish
        else:
            raise WorkflowError(
                f"Unsupported shell executable: {self.shell_executable}"
            )

    @EnvBase.once
    def conda_env_directories(self) -> List[Path]:
        # TODO implement this for micromamba, conda, mamba and any future conda client
        ...

    def env_prefix(self) -> Path:
        if self.spec.envfile is not None:
            return self.deployment_path
        elif self.spec.directory is not None:
            return self.spec.directory
        else:
            # TODO convert name into path of the deployed environment
            # Use something like $CONDA_ENVS_PATH / self.spec.name here
            # Question is how t
            for env_dir in self.conda_env_directories():
                if (env_dir / self.spec.name).is_dir():
                    return env_dir / self.spec.name
            raise WorkflowError(f"Could not find environment {self.spec.name}")

    @property
    def envfile_content(self) -> Dict[str, list]:
        if self._envfile_content is None:
            try:
                with open(self.spec.envfile.cached, "r") as f:
                    self._envfile_content = yaml.load(f, Loader=yaml.SafeLoader)
            except Exception as e:
                raise WorkflowError(
                    f"Could not read envfile {self.spec.envfile.path_or_uri}",
                    e,
                )
        return self._envfile_content

    def decorate_shellcmd(self, cmd: str) -> str:
        # Decorate given shell command such that it runs within the environment.

        act_obj = activate(
            prefix=self.deployment_path,
            activation_variables=ActivationVariables(None, sys.path),
            shell=self.rattler_shell,
        )
        return f"""
        {act_obj.script}
        {cmd}
        """

    def record_hash(self, hash_object) -> None:
        if self.spec.envfile is not None:
            hash_object.update(
                json.dumps(self.envfile_content, sort_keys=True).encode()
            )
        elif self.spec.directory is not None:
            hash_object.update(self.spec.directory.encode())
        else:
            hash_object.update(self.spec.name.encode())

    def report_software(self) -> Iterable[SoftwareReport]:
        # Report the software contained in the environment. This should be a list of
        # snakemake_interface_software_deployment_plugins.SoftwareReport data class.
        # Use SoftwareReport.is_secondary = True if the software is just some
        # less important technical dependency. This allows Snakemake's report to
        # hide those for clarity. In case of containers, it is also valid to
        # return the container URI as a "software".
        # Return an empty list if no software can be reported.
        if self.spec.envfile is not None:

            def entry_to_report(entry):
                entry = MatchSpec(entry)
                return SoftwareReport(
                    name=entry.name,
                    version=entry.version,
                )

            with open(self.spec.envfile.cached, "r") as f:
                env_spec = yaml.load(f, Loader=yaml.SafeLoader)
                return list(map(entry_to_report, env_spec["dependencies"]))
        else:
            # TODD dynamically obtain software list from the deployed environment
            return ()

    async def _package_records(self) -> List[RepoDataRecord]:
        if self.spec.pinfile.cached.exists():
            with open(self.spec.pinfile.cached, "r") as f:
                header = True
                records = []
                for record in f:
                    if header:
                        if record.strip() == "@EXPLICIT":
                            header = False
                    else:
                        records.append(RepoDataRecord(url=record.strip()))
        else:
            return list(
                await solve(
                    channels=self.envfile_content["channels"],
                    # The specs to solve for
                    specs=self.envfile_content["dependencies"],
                    # Virtual packages define the specifications of the environment
                    virtual_packages=VirtualPackage.detect(),
                )
            )

    async def deploy(self) -> None:
        # Remove method if not deployable!
        # Deploy the environment to self.deployment_path, using self.spec
        # (the EnvSpec object).

        # When issuing shell commands, the environment should use
        # self.run_cmd(cmd: str) -> subprocess.CompletedProcess in order to ensure that
        # it runs within eventual parent environments (e.g. a container or an env
        # module).
        assert self.spec.envfile is not None

        await install(
            records=await self._package_records(),
            target_prefix=self.deployment_path,
            cache_dir=self.settings.cache_dir,
        )

    def is_deployment_path_portable(self) -> bool:
        # Deployment isn't portable because RPATHs are hardcoded as absolute paths by
        # rattler.
        return False

    def remove(self) -> None:
        # Remove method if not deployable!
        # Remove the deployed environment from self.deployment_path and perform
        # any additional cleanup.
        assert self.spec.envfile is not None
        shutil.rmtree(self.deployment_path)
