import boto3

# This assumes you've named the "main" VM "Jupyter NaaS Leader"
# and each of the followers is named "Jupyter NaaS Follower" (or at least prefixed with that string).

def get_jupyter_naas_public_ips(name_filter_value:str = ""):
    # Initialize a session using Boto3
    ec2 = boto3.client('ec2')

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
                public_ip = instance.get('PublicIpAddress')
                if public_ip:
                    public_ips.append(public_ip)

    return public_ips

follower_ips = get_jupyter_naas_public_ips(name_filter_value = 'Jupyter NaaS Follower*')
dn_ips = get_jupyter_naas_public_ips(name_filter_value = 'Jupyter NaaS Follower*(DN*')
leader_ip = get_jupyter_naas_public_ips(name_filter_value = 'Jupyter NaaS Leader')[0]

print("[vms]")
print(f"vm0 ansible_host={leader_ip} ansible_user=ubuntu")

for i, follower_ip in enumerate(follower_ips):
    print(f"vm{i+1} ansible_host={follower_ip} ansible_user=ubuntu")

print("\n[namenode]")
print(f"namenode1 ansible_host={leader_ip} ansible_user=ubuntu")

print("\n[datanode]")
for i, dn_ip in enumerate(dn_ips):
    print(f"datanode{i+1} ansible_host={dn_ip} ansible_user=ubuntu")

print("\n[zookeepers]")
print(f"zookeeper1 ansible_host={leader_ip} ansible_user=ubuntu")
for i, dn_ip in enumerate(dn_ips):
    print(f"zookeeper{i+2} ansible_host={dn_ip} ansible_user=ubuntu")

print("\n[hadoop_hdfs:children]")
print("namenode")
print("datanode")
print("zookeepers")

print("\n[docker_swarm_manager]")
print(f"swarm-leader ansible_host={leader_ip} ansible_user=ubuntu swarm_node_name=swarm-node1")
for i, dn_ip in enumerate(dn_ips):
    print(f"swarm-manager{i+1} ansible_host={dn_ip} ansible_user=ubuntu swarm_node_name=swarm-node{i+2}")

print("\n[docker_swarm_worker]")
swarm_worker_ips = [ip for ip in follower_ips if ip not in dn_ips]
for i, swarm_worker_ip in enumerate(swarm_worker_ips):
    print(f"swarm-worker{i+1} ansible_host={swarm_worker_ip} ansible_user=ubuntu swarm_node_name=swarm-node{i+len(dn_ips)+2}")

print("\n[docker_swarm:children]")
print("docker_swarm_manager")
print("docker_swarm_worker")