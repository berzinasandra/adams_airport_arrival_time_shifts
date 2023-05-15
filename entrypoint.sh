#! /usr/bin/env bash


ansible-vault decrypt --output variables.yml encrypted_variables.yml --vault-password-file vault_password.txt
# source variables.yml
airflow variables import variables.yml

ansible-vault decrypt --output connections.yml encrypted_connections.yml --vault-password-file vault_password.txt
airflow connections import connections.yml

exec /entrypoint "${@}"