import os
from pathlib import Path
from typing import Optional, Type
from snakemake_interface_software_deployment_plugins.tests import (
    TestSoftwareDeploymentBase,
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


# There can be multiple subclasses of SoftwareDeploymentProviderBase here.
# This way, you can implement multiple test scenarios.
# For each subclass, the test suite tests the environment activation and execution
# within, and, if applicable, environment deployment and archiving.
class Test(TestSoftwareDeploymentBase):
    __test__ = True  # activate automatic testing

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

    def get_within_cls(self) -> Optional[Type[EnvBase]]:
        return ContainerEnv

    def get_within_spec(self) -> Optional[EnvSpecBase]:
        return ContainerEnvSpec("condaforge/miniforge3:26.1.0-0")

    def get_within_settings(self) -> Optional[SoftwareDeploymentSettingsBase]:
        return ContainerSettings(
            mountpoints=["/tmp:/tmp"],
        )


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
