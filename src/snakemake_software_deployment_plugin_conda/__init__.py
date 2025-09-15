from itertools import chain
import json
import os
import shutil
import sys
from dataclasses import dataclass
from pathlib import Path
from typing import Dict, Iterable, List, Optional
import subprocess as sp

import httpx
import yaml
import aiofiles

from snakemake_interface_common.exceptions import WorkflowError
from snakemake_interface_software_deployment_plugins.settings import (
    CommonSettings,
)
from snakemake_interface_software_deployment_plugins import (
    EnvBase,
    DeployableEnvBase,
    CacheableEnvBase,
    PinnableEnvBase,
    EnvSpecBase,
    SoftwareReport,
    EnvSpecSourceFile,
)

from rattler.shell import Shell, activate, ActivationVariables
from rattler.match_spec import MatchSpec
from rattler import solve, install, VirtualPackage
from rattler.platform import Platform
from rattler.repo_data import RepoDataRecord, Gateway
from snakemake_software_deployment_plugin_conda.pinfiles import (
    get_match_specs_from_conda_pinfile,
)


PINFILE_SUFFIX = f".{Platform.current()}.pin.txt"


common_settings = CommonSettings(
    provides="conda",
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
                Path(self.envfile.path_or_uri).with_suffix(PINFILE_SUFFIX)
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

    def __str__(self) -> str:
        if self.envfile is not None:
            return str(self.envfile.path_or_uri)
        elif self.directory is not None:
            return str(self.directory)
        else:
            assert self.name is not None
            return self.name


class Env(PinnableEnvBase, CacheableEnvBase, DeployableEnvBase, EnvBase):
    shell_executable: str
    spec: EnvSpec  # type: ignore

    # For compatibility with future changes, you should not overwrite the __init__
    # method. Instead, use __post_init__ to set additional attributes and initialize
    # futher stuff.

    def __post_init__(self):
        self.gateway = Gateway()
        self._package_records_cache: Optional[List[RepoDataRecord]] = None
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

    def is_cacheable(self) -> bool:
        return self.spec.envfile is not None

    def is_pinnable(self) -> bool:
        return self.spec.envfile is not None

    def is_deployable(self) -> bool:
        return self.spec.envfile is not None

    @EnvBase.once
    def conda_env_directories(self) -> Iterable[Path]:
        errors = {}
        success = False
        for client in ("micromamba", "conda", "mamba"):
            try:
                output = self.run_cmd(f"{client} info --json", check=True)
                info = json.loads(output.stdout)
                env_dirs = info.get("envs directories", info.get("envs_dirs", []))
                if not isinstance(env_dirs, list):
                    errors[client] = (
                        f"Expected environment dirs to be a list, got {type(env_dirs)}."
                    )
                    continue
                success = True
                yield from (Path(d) for d in env_dirs)
            except sp.CalledProcessError as e:
                errors[client] = f"Failed to run {client}: {e}"
                continue
        if errors and not success:
            raise WorkflowError(
                "Could not determine conda environment directories. Tried the following clients:\n"
                + "\n".join(f"{client}: {error}" for client, error in errors.items())
            )

    def env_prefix(self) -> Path:
        if self.spec.envfile is not None:
            return self.deployment_path
        elif self.spec.directory is not None:
            return self.spec.directory
        else:
            assert self.spec.name is not None
            candidates = {
                env_dir / self.spec.name
                for env_dir in self.conda_env_directories()
                if (env_dir / self.spec.name).is_dir()
            }
            if len(candidates) == 1:
                return next(iter(candidates))
            elif len(candidates) > 1:
                raise WorkflowError(
                    f"Multiple environments found with name {self.spec.name}: "
                    f"{', '.join(map(str, candidates))}"
                )
            else:
                raise WorkflowError(f"Could not find environment {self.spec.name}")

    @property
    def envfile_content(self) -> Dict[str, list]:
        if self._envfile_content is None:
            assert self.spec.envfile is not None
            assert self.spec.envfile.cached is not None
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
            prefix=self.env_prefix(),
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
            hash_object.update(str(self.spec.directory).encode())
        else:
            assert self.spec.name is not None
            hash_object.update(self.spec.name.encode())

    def report_software(self) -> Iterable[SoftwareReport]:
        # Report the software contained in the environment. This should be a list of
        # snakemake_interface_software_deployment_plugins.SoftwareReport data class.
        # Use SoftwareReport.is_secondary = True if the software is just some
        # less important technical dependency. This allows Snakemake's report to
        # hide those for clarity. In case of containers, it is also valid to
        # return the container URI as a "software".
        # Return an empty list if no software can be reported.
        assert isinstance(self.spec, EnvSpec)
        if self.spec.envfile is not None:

            def entry_to_report(entry):
                entry = MatchSpec(entry)
                assert entry.name is not None
                return SoftwareReport(
                    name=entry.name.normalized,
                    version=entry.version,
                )

            return list(map(entry_to_report, chain(self.conda_specs, self.pypi_specs)))
        else:
            # TODD dynamically obtain software list from the deployed environment
            return ()

    @property
    def conda_specs(self) -> List[str]:
        return [
            spec
            for spec in self.envfile_content["dependencies"]
            if not isinstance(spec, dict)
        ]

    @property
    def pypi_specs(self) -> List[str]:
        for spec in self.envfile_content["dependencies"]:
            if isinstance(spec, dict):
                pypi_specs = spec["pip"]
                if not isinstance(pypi_specs, list):
                    raise WorkflowError("pypi/pip dependencies must be a list")
                return spec["pip"]
        return []

    async def _package_records(self) -> List[RepoDataRecord]:
        if self._package_records_cache is None:
            assert isinstance(self.spec, EnvSpec)
            assert self.spec.pinfile is not None
            assert self.spec.pinfile.cached is not None
            assert self.spec.envfile is not None

            pinfile = (
                self.spec.pinfile.cached
                if self.spec.pinfile.cached.exists()
                else self.pinfile
            )

            if pinfile.exists():
                records = list(
                    chain.from_iterable(
                        await self.gateway.query(
                            channels=self.envfile_content["channels"],
                            platforms=[Platform.current()],
                            specs=list(get_match_specs_from_conda_pinfile(pinfile)),
                            recursive=False,
                        )
                    )
                )
                self._package_records_cache = records
            else:
                self._package_records_cache = list(
                    await solve(
                        channels=self.envfile_content["channels"],
                        # The specs to solve for
                        specs=self.conda_specs,
                        # Virtual packages define the specifications of the environment
                        virtual_packages=VirtualPackage.detect(),
                    )
                )
        return self._package_records_cache

    @classmethod
    def pinfile_extension(cls) -> str:
        return PINFILE_SUFFIX

    async def pin(self) -> None:
        records = await self._package_records()
        async with aiofiles.open(self.pinfile, "w") as f:
            await f.write("@EXPLICIT\n")
            for record in records:
                await f.write(f"{record.url}\n")

    async def get_cache_assets(self) -> Iterable[str]:
        return (
            record_to_asset_name(record) for record in await self._package_records()
        )

    async def cache_assets(self) -> None:
        for record in await self._package_records():
            pkg_name = record_to_asset_name(record)
            async with httpx.AsyncClient() as http_client:
                response = await http_client.get(record.url)
                response.raise_for_status()
                # The naming scheme used here follows the same pattern as rsync.
                # This way, we benefit from rsync specific optimizations in network
                # filtesystems like GlusterFS (see
                # https://developers.redhat.com/blog/2018/08/14/improving-rsync-performance-with-glusterfs)
                tmp_cache_path = self.cache_path / f".{pkg_name}.part"
                cache_path = self.cache_path / pkg_name
                if not cache_path.exists():
                    async with aiofiles.open(tmp_cache_path, "wb") as f:
                        async for chunk in response.aiter_bytes(chunk_size=1024):
                            await f.write(chunk)
                    os.replace(tmp_cache_path, cache_path)

    async def deploy(self) -> None:
        # Remove method if not deployable!
        # Deploy the environment to self.deployment_path, using self.spec
        # (the EnvSpec object).

        # When issuing shell commands, the environment should use
        # self.run_cmd(cmd: str) -> subprocess.CompletedProcess in order to ensure that
        # it runs within eventual parent environments (e.g. a container or an env
        # module).
        assert self.spec.envfile is not None

        records = await self._package_records()

        await install(
            records=records,
            target_prefix=self.deployment_path,
            cache_dir=self.cache_path,
        )

        pypi_specs = [spec.replace(" ", "") for spec in self.pypi_specs]
        if pypi_specs:

            def raise_python_error(errmsg: str):
                raise WorkflowError(
                    f"No working python found in the given environment {self.spec}. "
                    "Unable to install additional pypi packages. Please add python as "
                    f"a conda package to the environment. {errmsg}"
                )

            python_path = self.deployment_path / "bin" / "python"
            if not python_path.exists():
                raise_python_error(
                    f"No python found under {self.deployment_path}. If your environment contains pypi packages, please add python to the non-pypi packages list."
                )

            try:
                sp.run(
                    [
                        "uv",
                        "pip",
                        "install",
                        "--prefix",
                        str(self.deployment_path),
                        "--python",
                        python_path,
                        *pypi_specs,
                    ],
                    check=True,
                    stdout=sp.PIPE,
                    stderr=sp.PIPE,
                )
            except sp.CalledProcessError as e:
                raise WorkflowError(f"Failed to install pypi packages: {e.stderr}", e)

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


def record_to_asset_name(record: RepoDataRecord) -> str:
    return record.url.split("/")[-1]
