#!/bin/bash
RUN_TAG=""
if [[ -n "$1" ]]; then
    RUN_TAG="-t $1"
fi
ansible-playbook -i inventory/hosts.yml playbooks/site.yml $RUN_TAG
