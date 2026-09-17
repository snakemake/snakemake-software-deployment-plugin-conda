from collections.abc import Set
import os
import shutil
from pathlib import Path
from typing import Optional, Type
import subprocess as sp

import pytest
from snakemake_interface_software_deployment_plugins.tests import (
    TestSoftwareDeploymentBase,
    ShellExecutable,
)
from snakemake_interface_software_deployment_plugins import (
    EnvSpecBase,
    EnvBase,
    EnvSpecSourceFile,
)
from snakemake_interface_software_deployment_plugins.settings import (
    SoftwareDeploymentSettingsBase,
)
from snakemake_software_deployment_plugin_conda import (
    Env,
    EnvSpec,
)
from snakemake_software_deployment_plugin_container import Env as ContainerEnv
from snakemake_software_deployment_plugin_container import EnvSpec as ContainerEnvSpec
from snakemake_software_deployment_plugin_container import Settings as ContainerSettings
from snakemake_software_deployment_plugin_container import Runtime


# There can be multiple subclasses of SoftwareDeploymentProviderBase here.
# This way, you can implement multiple test scenarios.
# For each subclass, the test suite tests the environment activation and execution
# within, and, if applicable, environment deployment and archiving.
class Test(TestSoftwareDeploymentBase):
    __test__ = True  # activate automatic testing

    def get_contained_executable(self) -> str:
        # just provide something that is available inside of the container
        return "stress-ng"

    def get_env_spec(self) -> EnvSpecBase:
        return EnvSpec(
            envfile=EnvSpecSourceFile(Path(__file__).parent / "test_env.yaml")
        )

    def get_env_cls(self) -> Type[EnvBase]:
        # Return the environment class that should be tested.
        return Env

    def get_settings_cls(self) -> Optional[Type[SoftwareDeploymentSettingsBase]]:
        # Return the settings class that should be used for this plugin.
        return None

    def get_settings(
        self,
    ) -> Optional[SoftwareDeploymentSettingsBase]:
        # If your plugin has settings, return a valid settings object here.
        # Otherwise, return None.
        return None

    def get_test_cmd(self) -> str:
        # Return a test command that should be executed within the environment
        # with exit code 0 (i.e. without error).
        return "stress-ng --cpu 1 --timeout 1s"


class TestPostDeploy(Test):
    __test__ = True

    def get_env_spec(self) -> EnvSpecBase:
        return EnvSpec(
            envfile=EnvSpecSourceFile(
                Path(__file__).parent / "test_env_post_deploy.yaml"
            )
        )

    def get_test_cmd(self) -> str:
        # Return a test command that should be executed within the environment
        # with exit code 0 (i.e. without error).
        cmd = super().get_test_cmd()
        return f"{cmd} && test -e post_deploy_success.txt && rm post_deploy_success.txt"


class TestPinned(Test):
    __test__ = True

    def get_env_spec(self) -> EnvSpecBase:
        return EnvSpec(
            envfile=EnvSpecSourceFile(Path(__file__).parent / "test_env_pinned.yaml")
        )


class TestPypi(Test):
    __test__ = True

    def get_env_spec(self) -> EnvSpecBase:
        return EnvSpec(
            envfile=EnvSpecSourceFile(Path(__file__).parent / "test_env_pypi.yaml")
        )

    def get_test_cmd(self) -> str:
        # Return a test command that should be executed within the environment
        # with exit code 0 (i.e. without error).
        return "which python; python -c 'import humanfriendly'"


class TestWithinContainer(Test):
    __test__ = True
    # Do not use login shell here, we don't need an external conda but rather the udocker installed by pixi.
    shell_executable = ShellExecutable("bash", args=[], command_arg="-c")

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        sp.run(["pixi", "run", "check-build"], check=True, capture_output=True)
        self.dist_dir = (Path(__file__).parent.parent / "dist").absolute()
        os.environ["PIP_FIND_LINKS"] = self.dist_dir.as_posix()

    def get_within_cls(self) -> Optional[Type[EnvBase]]:
        return ContainerEnv

    def get_within_spec(self) -> Optional[EnvSpecBase]:
        return ContainerEnvSpec("condaforge/miniforge3:26.1.0-0")

    def get_within_settings(self) -> Optional[SoftwareDeploymentSettingsBase]:
        dist_dir = self.dist_dir.as_posix()
        return ContainerSettings(mountpoints=[f"{dist_dir}:{dist_dir}"])

    def get_envvars(self) -> Set[str]:
        return {"PIP_FIND_LINKS"}


@pytest.mark.skipif(
    shutil.which("apptainer") is None, reason="apptainer is not available"
)
class TestWithinContainerApptainer(TestWithinContainer):
    """Reproduces the apptainer squashfuse FUSE hang.

    In contrast to the default udocker runtime, apptainer mounts SIF images (squashfs) via FUSE.
    Importing rattler (dependency of snakemake_software_deployment_plugin_conda)
      inside such a container (as done by `_run_method`, e.g. from `_platforms`)
      intermittently deadlocks or panics when reading files from the image,
    seems like there is a race in `squashfuse_ll`.

    related to https://github.com/snakemake/snakemake/pull/3339#issuecomment-5216168878
      and the test `test_singularity_conda`
    """

    __test__ = True

    def _get_env(self, tmp_path):
        # Reuse the base construction, then attach spec.within exactly like
        # snakemake.deployment.SoftwareDeploymentManager.get_env does.
        env = super()._get_env(tmp_path)
        within_spec = self.get_within_spec()
        assert within_spec is not None
        env.spec.within = within_spec
        return env

    def get_within_spec(self):
        return ContainerEnvSpec("condaforge/miniforge3:26.3.2-3")

    def get_within_settings(self):
        dist_dir = self.dist_dir.as_posix()
        return ContainerSettings(
            runtime=Runtime.APPTAINER, mountpoints=[f"{dist_dir}:{dist_dir}"]
        )


class TestPypiWithinContainer(TestPypi):
    __test__ = True
    # Do not use login shell here, we don't need an external conda but rather the udocker installed by pixi.
    shell_executable = ShellExecutable("bash", args=[], command_arg="-c")

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        sp.run(["pixi", "run", "check-build"], check=True, capture_output=True)
        self.dist_dir = (Path(__file__).parent.parent / "dist").absolute()
        os.environ["PIP_FIND_LINKS"] = self.dist_dir.as_posix()

    def get_within_cls(self) -> Optional[Type[EnvBase]]:
        return ContainerEnv

    def get_within_spec(self) -> Optional[EnvSpecBase]:
        return ContainerEnvSpec("condaforge/miniforge3:26.1.0-0")

    def get_within_settings(self) -> Optional[SoftwareDeploymentSettingsBase]:
        dist_dir = self.dist_dir.as_posix()
        return ContainerSettings(mountpoints=[f"{dist_dir}:{dist_dir}"])

    def get_envvars(self) -> Set[str]:
        return {"PIP_FIND_LINKS"}


class TestNamed(Test):
    __test__ = os.environ.get("TEST_NAMED_ENV") == "1"

    def get_env_spec(self) -> EnvSpecBase:
        return EnvSpec(name="test-env")

    def get_test_cmd(self) -> str:
        # Return a test command that should be executed within the environment
        # with exit code 0 (i.e. without error).
        return "stress-ng --cpu 1 --timeout 1s"


class TestDirectory(Test):
    __test__ = os.environ.get("TEST_DIRECTORY_ENV") == "1"

    def get_env_spec(self) -> EnvSpecBase:
        return EnvSpec(directory=Path(os.environ["TEST_ENV_DIR"]))

    def get_test_cmd(self) -> str:
        # Return a test command that should be executed within the environment
        # with exit code 0 (i.e. without error).
        return "stress-ng --cpu 1 --timeout 1s"
