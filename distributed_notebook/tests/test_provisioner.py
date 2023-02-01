"""Test Provisioner"""

import asyncio
import json
import os
import signal
import sys
from subprocess import PIPE
from typing import List, Optional

import pytest
from entrypoints import EntryPoint
from entrypoints import NoSuchEntryPoint
from jupyter_core import paths

from jupyter_client.connect import KernelConnectionInfo
from jupyter_client.kernelspec import KernelSpecManager
from jupyter_client.kernelspec import NoSuchKernel
from jupyter_client.launcher import launch_kernel
from jupyter_client.manager import AsyncKernelManager
from jupyter_client.provisioning import KernelProvisionerFactory

from ..provisioner import GatewayProvisioner

pjoin = os.path.join


def build_kernelspec(name: str, provisioner: Optional[str] = None) -> None:
    spec = {
        'argv': [
            sys.executable,
            '-m',
            'distributed_notebook.tests.signalkernel',
            '-f',
            '{connection_file}',
        ],
        'display_name': f"Signal Test Kernel w {provisioner}",
        'env': {'TEST_VARS': '${TEST_VARS}:test_var_2'},
        "metadata": {}
    }

    if provisioner:
        kernel_provisioner = {'kernel_provisioner': {'provisioner_name': provisioner}}
        spec['metadata'].update(kernel_provisioner)
        if provisioner == 'gateway-provisioner':
            spec['metadata']['kernel_provisioner']['config'] = {
                'gateway': "127.0.0.1:8080",
            }

    kernel_dir = pjoin(paths.jupyter_data_dir(), 'kernels', name)
    os.makedirs(kernel_dir, exist_ok=True)
    with open(pjoin(kernel_dir, 'kernel.json'), 'w') as f:
        f.write(json.dumps(spec))

def gateway_provisioner():
    build_kernelspec('gateway_provisioner', 'gateway-provisioner')


@pytest.fixture
def all_provisioners():
    build_kernelspec('missing_provisioner', 'missing-provisioner')
    gateway_provisioner()


@pytest.fixture(
    params=[
        'missing_provisioner',
        'gateway_provisioner',
    ]
)
def akm(request, all_provisioners):
    return AsyncKernelManager(kernel_name=request.param)


initial_provisioner_map = {
    'gateway-provisioner': ('distributed_notebook.provisioner', 'GatewayProvisioner'),
}


def mock_get_all_provisioners() -> List[EntryPoint]:
    result = []
    for name, epstr in initial_provisioner_map.items():
        result.append(EntryPoint(name, epstr[0], epstr[1]))
    return result


def mock_get_provisioner(factory, name) -> EntryPoint:
    if name in initial_provisioner_map:
        return EntryPoint(name, initial_provisioner_map[name][0], initial_provisioner_map[name][1])

    raise NoSuchEntryPoint(KernelProvisionerFactory.GROUP_NAME, name)


@pytest.fixture
def kpf(monkeypatch):
    """Setup the Kernel Provisioner Factory, mocking the entrypoint fetch calls."""
    monkeypatch.setattr(
        KernelProvisionerFactory, '_get_all_provisioners', mock_get_all_provisioners
    )
    monkeypatch.setattr(KernelProvisionerFactory, '_get_provisioner', mock_get_provisioner)
    factory = KernelProvisionerFactory.instance()
    return factory


class TestDiscovery:
    def test_find_all_specs(self, kpf, all_provisioners):
        ksm = KernelSpecManager()
        kernels = ksm.get_all_specs()

        # Ensure specs for initial provisioners exist,
        # and missing_provisioner don't
        assert 'gateway_provisioner' in kernels
        assert 'missing_provisioner' not in kernels

class TestRuntime:
    async def akm_test(self, kernel_mgr):
        """Starts a kernel, validates the associated provisioner's config, shuts down kernel"""

        assert kernel_mgr.provisioner is None
        if kernel_mgr.kernel_name == 'missing_provisioner':
            with pytest.raises(NoSuchKernel):
                await kernel_mgr.start_kernel()
        else:
            await kernel_mgr.start_kernel()

            TestRuntime.validate_provisioner(kernel_mgr)

            await kernel_mgr.shutdown_kernel()
            assert kernel_mgr.provisioner.has_process is False

    @pytest.mark.asyncio
    async def test_gateway(self, kpf):
        gateway_provisioner()  # Introduce provisioner after initialization of KPF
        akm = AsyncKernelManager(kernel_name='gateway_provisioner')
        await self.akm_test(akm)

    @pytest.mark.asyncio
    async def test_gateway_lifecycle(self, kpf):
        gateway_provisioner()
        async_km = AsyncKernelManager(kernel_name='gateway_provisioner')

        await async_km.start_kernel(stdout=PIPE, stderr=PIPE)
        is_alive = await async_km.is_alive()
        assert is_alive
        await async_km.restart_kernel(now=True)
        is_alive = await async_km.is_alive()
        assert is_alive
        await async_km.interrupt_kernel()
        assert isinstance(async_km, AsyncKernelManager)
        await async_km.shutdown_kernel(now=True)
        is_alive = await async_km.is_alive()
        assert is_alive is False
        assert async_km.context.closed

    @staticmethod
    def validate_provisioner(akm: AsyncKernelManager):
        # Ensure the provisioner is managing a process at this point
        assert akm.provisioner is not None and akm.provisioner.has_process

        if akm.kernel_name == 'gateway_provisioner':
            # Validate provisioner class
            assert isinstance(akm.provisioner, GatewayProvisioner)

            # Assert configs are loaded
            assert akm.provisioner.gateway != None
