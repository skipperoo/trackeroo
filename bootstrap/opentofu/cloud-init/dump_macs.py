#!/usr/bin/env python3

import os
import sys
import yaml
import hashlib

def generate_mac_from_name(name):
    """Generate MAC address suffix from VM name using hash"""
    hash_obj = hashlib.sha256(name.encode())
    hex_str = hash_obj.hexdigest()[:6]
    # Convert to 3 bytes and return as integers for easier increment
    byte1 = int(hex_str[0:2], 16)
    byte2 = int(hex_str[2:4], 16)
    byte3 = int(hex_str[4:6], 16)
    return [byte1, byte2, byte3]

def increment_mac_suffix(mac_suffix):
    """Increment MAC address suffix by 1"""
    # Convert back to a 24-bit number, increment, then convert back
    value = (mac_suffix[0] << 16) + (mac_suffix[1] << 8) + mac_suffix[2]
    value = (value + 1) & 0xFFFFFF  # Keep within 24-bit range

    return [
        (value >> 16) & 0xFF,
        (value >> 8) & 0xFF,
        value & 0xFF
    ]

def mac_suffix_to_string(mac_suffix):
    """Convert MAC suffix list to string format"""
    return f"52:54:00:{mac_suffix[0]:02x}:{mac_suffix[1]:02x}:{mac_suffix[2]:02x}"

def process_vm_configs(vm_configs):
    """Process VM configurations and add MAC addresses"""
    for vm in vm_configs:
        name = vm['name']
        networks = vm.get('networks', [])

        if not networks:
            continue

        # Generate base MAC from name for first network
        mac_suffix = generate_mac_from_name(name)

        for i, network in enumerate(networks):
            if i == 0:
                # First network gets the hash-based MAC
                network['mac'] = mac_suffix_to_string(mac_suffix)
            else:
                # Subsequent networks get incremented MACs
                mac_suffix = increment_mac_suffix(mac_suffix)
                network['mac'] = mac_suffix_to_string(mac_suffix)

    return vm_configs


if __name__ == "__main__":
    if len(sys.argv) != 2:
        print("Usage: python create_macs.py <input_file>")
        sys.exit(1)
    if not os.path.isfile(sys.argv[1]):
        print(f"Error: {sys.argv[1]} is not a valid file")
        sys.exit(1)
    with open(sys.argv[1], 'r') as file:
        vm_configs = yaml.safe_load(file)
    updated_configs = process_vm_configs(vm_configs)

    # Print the updated configurations in YAML format
    print("# Updated VM configurations with MAC addresses")
    with open(sys.argv[1], 'w') as file:
        yaml.dump(updated_configs, file, default_flow_style=False, sort_keys=False)

    # Also show a summary of generated MACs
    print("\n# Summary of generated MAC addresses:")
    for vm in updated_configs:
        print(f"# {vm['name']}:")
        for i, network in enumerate(vm['networks']):
            network_info = network.get('network', network.get('source', 'unknown'))
            print(f"#   Network {i+1} ({network_info}): {network['mac']}")
        print("#")
