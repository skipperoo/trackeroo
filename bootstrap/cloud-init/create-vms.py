import os
import re
import yaml
import subprocess
import time
from typing import List, Literal, Optional
from pydantic import BaseModel

BASE_IMAGE = "debian13"
CLOUD_INIT_PATH = "cloud-init"
BASE_IMAGE_PATH = f"{CLOUD_INIT_PATH}/debian-13-cloud-init.qcow2"
DRY_RUN = os.environ.get("DRY_RUN", "false").lower() == "true"
STORAGE_POOL = "/home/gigio/Data/fast_storage/VMs/disks/k3s_cluster"

class NetworkSpec(BaseModel):
    network: Optional[str] = None
    net_type: Optional[str] = None
    source: Optional[str] = None
    source_mode: Optional[str] = None
    model: Optional[str] = None
    ip: Optional[str] = None
    mac: str

class VMSpec(BaseModel):
    machine_type: Literal["server", "node"]
    name: str
    ram: int
    cpu: int
    os_storage: int
    longhorn_storage: int
    networks: List[NetworkSpec]



def load_specs(file_path) -> list[VMSpec]:
    with open(file_path, 'r') as file:
        return [VMSpec(**spec) for spec in yaml.safe_load(file)]

def create_command(spec: VMSpec) -> str:
    cmd = ["sudo", "virt-install"]
    cmd.append(f"--name {spec.name}")
    cmd.append(f"--memory {spec.ram}")
    cmd.append(f"--vcpus {spec.cpu}")
    cmd.append(f"--disk path={STORAGE_POOL}/{spec.name}.qcow2,format=qcow2")
    if spec.longhorn_storage:
        cmd.append(f"--disk path={STORAGE_POOL}/{spec.name}-longhorn.qcow2,format=qcow2")
    cmd.append(f"--os-variant {BASE_IMAGE}")
    for network in spec.networks:
        if network.network:
            cmd.append(f"--network network={network.network},mac={network.mac}")
        else:
            cmd.append(f"--network type={network.net_type},source={network.source},source_mode={network.source_mode},model={network.model},mac={network.mac}")
    cmd.append(f"--graphics spice --noautoconsole")
    user_data_path = f"{CLOUD_INIT_PATH}/{'servers' if spec.machine_type == 'server' else 'nodes'}/user-data.yaml"
    meta_data_path = f"{CLOUD_INIT_PATH}/{spec.name}-meta-data.yaml"
    network_config_path = f"{CLOUD_INIT_PATH}/network-config.yaml"
    cmd.append(f"--cloud-init user-data={user_data_path},meta-data={meta_data_path},network-config={network_config_path}")
    cmd.append(f"--import")
    return " ".join(cmd)

def validate_spec(spec: VMSpec) -> bool:
    if os.path.exists(f"{spec.name}.qcow2") or os.path.exists(f"{spec.name}-longhorn.qcow2"):
        print(f"VM already exists: {spec.name}")
        return False
    return True

def prepare_env(spec: VMSpec) -> bool:
    """
    Prepare the environment for the VM:
        - Create cloud-init metadata file
        - Create the os storage file
        - Optionally create the longhorn storage file
    """

    if not validate_spec(spec):
        print(f"VM {spec.name} already exists, skipping...")
        return False

    with open(f"{CLOUD_INIT_PATH}/{spec.name}-meta-data.yaml", 'w') as file:
        file.write(f"instance-id: {spec.name}-01\n")
        file.write(f"local-hostname: {spec.name}\n")

    if DRY_RUN:
        print(f"DRY RUN: qemu-img create -f qcow2 -F qcow2 -b {BASE_IMAGE_PATH} {STORAGE_POOL}/{spec.name}.qcow2 {spec.os_storage}G")
        print(f"DRY RUN: qemu-img create -f qcow2 {STORAGE_POOL}/{spec.name}-longhorn.qcow2 {spec.longhorn_storage}")
    else:
        os.system(f"qemu-img create -f qcow2 -F qcow2 -b {BASE_IMAGE_PATH} {STORAGE_POOL}/{spec.name}.qcow2 {spec.os_storage}G")
        if spec.longhorn_storage:
            os.system(f"qemu-img create -f qcow2 {STORAGE_POOL}/{spec.name}-longhorn.qcow2 {spec.longhorn_storage}G")
    return True


def get_dhcp_reservation_cmd(vm_name: str, network_name: str, mac_address: str, ip_address: str) -> str:
    """
    Generate a command to add a DHCP reservation for a VM in a libvirt network.
    """
    cmd = ["sudo", "virsh"]
    cmd.append("net-update")
    cmd.append(network_name)
    cmd.append("add")
    cmd.append("ip-dhcp-host")
    cmd.append(f"\"<host mac='{mac_address}' name='{vm_name}' ip='{ip_address}'/>\"")
    cmd.append("--live")
    cmd.append("--config")
    return " ".join(cmd)

def reserve_mac_address(spec: VMSpec):
    """
    Generate a MAC address for a VM in a libvirt network.
    """
    for network in spec.networks:
        if network.network and network.ip:
            if DRY_RUN:
                print(f"DRY RUN: {get_dhcp_reservation_cmd(spec.name, network.network, network.mac, network.ip)}")
                continue

            os.system(get_dhcp_reservation_cmd(spec.name, network.network, network.mac, network.ip))

def deploy_vm(spec: VMSpec):
    """
    Deploy virtual machines based on the provided specifications.
    """
    deploy_command = create_command(spec)
    if DRY_RUN:
        print(f"DRY RUN: {deploy_command}")
    else:
        os.system(deploy_command)
        time.sleep(2)
        os.system(f"sudo virsh autostart {spec.name}")

def main():
    specs = load_specs(f"{CLOUD_INIT_PATH}/specs.yaml")

    for spec in specs:
        if not prepare_env(spec):
            continue
        deploy_vm(spec)
        reserve_mac_address(spec)


if __name__ == "__main__":
    main()
