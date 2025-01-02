# Deploy `distributed-notebook` Docker Stack

The `setup/ansible/roles/deploy-distr-notebook-docker-stack/files/dashboard_backend/workload_templates` contains 
several provided/example workload template files. You may add other files to this directory, and they will be copied to
the other nodes in the Docker Swarm cluster. 

The Ansible playbook will look for a 
`setup/ansible/roles/deploy-distr-notebook-docker-stack/templates/dashboard/dashboard_workload_templates.yaml.j2` file.
If found, then that file will be used to generate the `workload_templates.yaml` file for the backend. You must explicitly
create this file. It will not be added to the GitHub repo due to the `.gitignore` in the containing directory. If you
do not supply that file, then the `default_dashboard_workload_templates.yaml.j2` file (found in the same directory) will
be used instead.

Note: any files added to that directory will not be committed to the GitHub repo due to the `.gitignore` file in that directory.

