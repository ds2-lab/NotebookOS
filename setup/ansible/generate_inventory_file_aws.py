import boto3

# This assumes you've named the "main" VM "Jupyter NaaS Leader"
# and each of the followers is named "Jupyter NaaS Follower" (or at least prefixed with that string).

def get_jupyter_naas_public_ips(name_filter_value:str = ""):
    # Initialize a session using Boto3
    ec2 = boto3.client('ec2', region_name = 'us-east-1')

    # Describe EC2 instances
    response = ec2.describe_instances(
        Filters=[
            {'Name': 'tag:Name', 'Values': [name_filter_value]},  # Prefix-based search
            {'Name': 'instance-state-name', 'Values': ['running']}
        ]
    )

    public_ips = []

    # Parse the response for public IPv4 addresses
    for reservation in response['Reservations']:
        for instance in reservation['Instances']:
            if instance.get('State', {}).get('Name') == 'running':
                public_ip_address = instance.get('PublicIpAddress')
                if public_ip_address:
                    public_ips.append(public_ip_address)

    return public_ips

def get_jupyter_naas_private_ips(name_filter_value:str = ""):
    # Initialize a session using Boto3
    ec2 = boto3.client('ec2', region_name = 'us-east-1')

    # Describe EC2 instances
    response = ec2.describe_instances(
        Filters=[
            {'Name': 'tag:Name', 'Values': [name_filter_value]},  # Prefix-based search
            {'Name': 'instance-state-name', 'Values': ['running']}
        ]
    )

    private_ips = []

    # Parse the response for public IPv4 addresses
    for reservation in response['Reservations']:
        for instance in reservation['Instances']:
            if instance.get('State', {}).get('Name') == 'running':
                private_ip_address = instance.get('PrivateIpAddress')
                if private_ip_address:
                    private_ips.append(private_ip_address)

    return private_ips

follower_private_ips = get_jupyter_naas_private_ips(name_filter_value = 'Jupyter NaaS Follower*')
dn_private_ips = get_jupyter_naas_private_ips(name_filter_value = 'Jupyter NaaS Follower*(DN*')
leader_private_ip = get_jupyter_naas_private_ips(name_filter_value = 'Jupyter NaaS Leader')[0]

follower_public_ips = get_jupyter_naas_public_ips(name_filter_value = 'Jupyter NaaS Follower*')
dn_public_ips = get_jupyter_naas_public_ips(name_filter_value = 'Jupyter NaaS Follower*(DN*')
leader_public_ip = get_jupyter_naas_public_ips(name_filter_value = 'Jupyter NaaS Leader')[0]

print("[vms]")
print(f"vm0 ansible_host={leader_public_ip} private_ip={leader_private_ip} ansible_user=ubuntu")

for i, ips in enumerate(zip(follower_public_ips, follower_private_ips)):
    public_ip, private_ip = ips
    print(f"vm{i+1} ansible_host={public_ip} private_ip={private_ip} ansible_user=ubuntu")

print("\n[namenode]")
print(f"namenode1 ansible_host={leader_public_ip} private_ip={leader_private_ip} ansible_user=ubuntu")

print("\n[datanode]")
for i, ips in enumerate(zip(dn_public_ips, dn_private_ips)):
    public_ip, private_ip = ips
    print(f"datanode{i+1} ansible_host={public_ip} private_ip={private_ip} ansible_user=ubuntu")

print("\n[zookeepers]")
print(f"zookeeper1 ansible_host={leader_public_ip} private_ip={leader_private_ip} ansible_user=ubuntu")
for i, ips in enumerate(zip(dn_public_ips, dn_private_ips)):
    public_ip, private_ip = ips
    print(f"zookeeper{i+2} ansible_host={public_ip} private_ip={private_ip} ansible_user=ubuntu")

print("\n[hadoop_hdfs:children]")
print("namenode")
print("datanode")
print("zookeepers")

print("\n[docker_swarm_manager]")
print(f"swarm-leader ansible_host={leader_public_ip} private_ip={leader_private_ip} ansible_user=ubuntu swarm_node_name=swarm-node1")

print("\n[docker_swarm_worker]")
for i, ips in enumerate(zip(follower_public_ips, follower_private_ips)):
    public_ip, private_ip = ips
    print(f"swarm-worker{i+1} ansible_host={public_ip} private_ip={private_ip} ansible_user=ubuntu swarm_node_name=swarm-node{i+2}")

print("\n[docker_swarm:children]")
print("docker_swarm_manager")
print("docker_swarm_worker")