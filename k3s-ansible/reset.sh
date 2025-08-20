#!/bin/bash

ansible-playbook -i inventory/hosts.yml playbooks/reset.yml
