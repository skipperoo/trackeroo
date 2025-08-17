#!/bin/bash

ansible-playbook -i inventory/hosts.yml playbooks/site.yml -u k3s-admin
