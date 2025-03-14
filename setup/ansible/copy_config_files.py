import paramiko
from scp import SCPClient

# Define the list of remote hosts
HOSTS = [
    {"hostname": "3.84.114.222", "username": "ubuntu"},
    {"hostname": "18.207.116.199", "username": "ubuntu"},
    {"hostname": "54.165.167.142", "username": "ubuntu"},
    {"hostname": "54.224.23.53", "username": "ubuntu"},
    {"hostname": "34.200.241.239", "username": "ubuntu"},
    {"hostname": "44.193.83.152", "username": "ubuntu"},
    {"hostname": "3.216.79.216", "username": "ubuntu"},
    {"hostname": "3.80.206.183", "username": "ubuntu"},
    {"hostname": "3.82.191.2", "username": "ubuntu"},
    {"hostname": "18.212.56.160", "username": "ubuntu"},
    {"hostname": "52.55.19.66", "username": "ubuntu"},
    {"hostname": "34.201.66.14", "username": "ubuntu"},
    {"hostname": "35.171.25.216", "username": "ubuntu"},
    {"hostname": "54.147.77.33", "username": "ubuntu"},
    {"hostname": "3.86.253.37", "username": "ubuntu"},
    {"hostname": "54.152.95.89", "username": "ubuntu"},
    {"hostname": "3.84.219.62", "username": "ubuntu"},
]

# Define the local and remote paths
LOCAL_CONFIG_FILE = '/home/scusemua/go/pkg/distributed-notebook/setup/ansible/roles/deploy-distr-notebook-docker-stack/files/gateway/gateway.yml'
REMOTE_CONFIG_FILE = '/home/ubuntu/go/pkg/distributed-notebook/setup/ansible/roles/deploy-distr-notebook-docker-stack/files/gateway/gateway2.yml'

def create_ssh_client(hostname, username):
    client = paramiko.SSHClient()
    client.load_system_host_keys()
    client.set_missing_host_key_policy(paramiko.AutoAddPolicy())
    client.connect(hostname, username=username)
    return client

def copy_file_to_host(hostname, username, local_file, remote_file):
    try:
        ssh = create_ssh_client(hostname, username)
        scp = SCPClient(ssh.get_transport())
        scp.put(local_file, remote_file)
        print(f"File copied to {username}@{hostname}:{remote_file}")
        scp.close()
        ssh.close()
    except Exception as e:
        print(f"Failed to copy file to {username}@{hostname}: {e}")

def main():
    # Prompt for SSH password if not using SSH keys
    for host in HOSTS:
        hostname = host['hostname']
        username = host['username']
        copy_file_to_host(hostname, username, LOCAL_CONFIG_FILE, REMOTE_CONFIG_FILE)

if __name__ == "__main__":
    main()