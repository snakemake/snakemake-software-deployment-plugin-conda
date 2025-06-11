import asyncio
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
from snakemake_interface_software_deployment_plugins.platforms import (
    System, Arch, Bits
)
from snakemake_interface_software_deployment_plugins.settings import (
    SoftwareDeploymentSettingsBase,
)
from snakemake_software_deployment_plugin_conda import (
    Env,
    EnvSpec,
    SoftwareDeploymentSettings,
)


# There can be multiple subclasses of SoftwareDeploymentProviderBase here.
# This way, you can implement multiple test scenarios.
# For each subclass, the test suite tests the environment activation and execution
# within, and, if applicable, environment deployment and archiving.
class TestSoftwareDeployment(TestSoftwareDeploymentBase):
    __test__ = True  # activate automatic testing
    # optional, default is "bash" change if your test suite requires a different
    # shell or you want to have multiple instance of this class testing various shells
    shell_executable = "bash"

    def get_env_spec(self) -> EnvSpecBase:
        return EnvSpec(
            envfile=EnvSpecSourceFile(Path(__file__).parent / "test_env.yaml")
        )

    def get_env_cls(self) -> Type[EnvBase]:
        # Return the environment class that should be tested.
        return Env

    def get_software_deployment_provider_settings(
        self,
    ) -> Optional[SoftwareDeploymentSettingsBase]:
        # If your plugin has settings, return a valid settings object here.
        # Otherwise, return None.
        return SoftwareDeploymentSettings(cache_dir=None)

    def get_test_cmd(self) -> str:
        # Return a test command that should be executed within the environment
        # with exit code 0 (i.e. without error).
        return "stress-ng --cpu 1 --timeout 1s"


# TODO requires https://github.com/conda/rattler/pull/1206 to be released
class TestSoftwareDeploymentPinned(TestSoftwareDeployment):
    def get_env_spec(self) -> EnvSpecBase:
        return EnvSpec(
            envfile=EnvSpecSourceFile(Path(__file__).parent / "test_env_pinned.yaml")
        )


class TestSoftwareDeploymentPypi(TestSoftwareDeployment):
    def get_env_spec(self) -> EnvSpecBase:
        return EnvSpec(
            envfile=EnvSpecSourceFile(Path(__file__).parent / "test_env_pypi.yaml")
        )

    def get_test_cmd(self) -> str:
        # Return a test command that should be executed within the environment
        # with exit code 0 (i.e. without error).
        return "which python; python -c 'import humanfriendly'"