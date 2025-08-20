#!/bin/bash

VMS=(
    k3s-server-cronus
	k3s-server-hyperion
	k3s-server-oceanus
	k3s-node-hermes
	k3s-node-achilles
	k3s-node-odysseus
)

for vm in "${VMS[@]}"; do
    echo "Shutting down $vm..."
    sudo virsh shutdown "$vm"

    # Wait for VM to actually shut down
    while sudo virsh list --all | grep -q "$vm.*running"; do
        echo "Waiting for $vm to shut down..."
        sleep 2
    done

    echo "$vm has shut down, removing..."
    sudo virsh undefine "$vm" --remove-all-storage
done
