import boto3

# This assumes you've named the "main" VM "Jupyter NaaS Leader"
# and each of the followers is named "Jupyter Jupyer Follower " (or at least prefixed with that string).

def get_jupyter_naas_ips(name_filter_value:str = "", tag_filters: list[dict[str, str|list[str]]]|dict[str, str|list[str]] = [], public: bool = True):
    # Initialize a session using Boto3
    ec2 = boto3.client('ec2', region_name = 'us-east-1', )

    filters=[
        {'Name': 'instance-state-name', 'Values': ['running']}
    ]

    if name_filter_value is not None and name_filter_value != "":
        filters.append({'Name': 'tag:Name', 'Values': [name_filter_value]}) # Prefix-based

    if isinstance(tag_filters, dict):
        tag_filters = [tag_filters]

    for tag_filter in tag_filters:
        for k, v in tag_filter.items():
            if isinstance(v, list):
                filters.append({'Name': k, 'Values': v})
            else:
                filters.append({'Name': k, 'Values': [v]})

    # Describe EC2 instances
    response = ec2.describe_instances(Filters=filters)

    key: str = "PublicIpAddress"
    if not public:
        key = "PrivateIpAddress"

    ips = []

    # Parse the response for public IPv4 addresses
    for reservation in response['Reservations']:
        for instance in reservation['Instances']:
            if instance.get('State', {}).get('Name') == 'running':
                # print(instance)
                ip_address = instance.get(key)
                if ip_address:
                    ips.append(ip_address)

    return ips

follower_private_ips = get_jupyter_naas_ips(public=False, tag_filters = [{"tag:swarm": "follower"}])
dn_private_ips = get_jupyter_naas_ips(public=False, tag_filters = [{"tag:swarm": "follower"}])
leader_private_ip = get_jupyter_naas_ips(public=False, name_filter_value = 'Jupyter NaaS Leader')[0]

follower_public_ips = get_jupyter_naas_ips(public=True, tag_filters = [{"tag:swarm": "follower"}])
dn_public_ips = get_jupyter_naas_ips(public=True, tag_filters = [{"tag:swarm": "follower"}])
leader_public_ip = get_jupyter_naas_ips(public=True, name_filter_value = 'Jupyter NaaS Leader')[0]

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

print("\n\n\n\n\n\n\n\n\n\n\n\nFollower IPs:")
for ip in follower_public_ips:
    print('{"hostname": "%s", "username": "ubuntu"}, ' % ip)