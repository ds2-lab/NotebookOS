import argparse 

default_output_directory:str = "./kind-config.yaml"
default_number_of_nodes:int = 4 
default_memory_reserved_per_node:str = "5Gi"
default_api_version = "kind.x-k8s.io/v1alpha4"
default_cluster_name = "distributed-notebook"

units = {"Mi": 10**6, "Gi": 10**9}

def parse_size(size):
    number, unit = [string.strip() for string in size.split()]
    return int(float(number)*units[unit])

def parse_arguments() -> argparse.Namespace:
    parser: argparse.ArgumentParser = argparse.ArgumentParser()
    
    parser.add_argument("-i", "--interactive", action = 'store_true', help = "If passed, then use the interactive script for specifying configuration values, rather than commandline arguments. If this is passed, then any command-line arguments will simply be used as the default values for the interactive script.")
    
    parser.add_argument("-c", "--cluster-name", dest = "cluster_name", type = str, default = "distributed-notebook", help = "The name of your Kind Kubernetes cluster. Default: \"%s\"" % default_cluster_name)
    parser.add_argument("-v", "--api-version", type = str, default = "kind.x-k8s.io/v1alpha4", help = "The (Kind) API version of your Kind Kubernetes cluster. Default: \"%s\"" % default_api_version)
    parser.add_argument("-n", "--num-nodes", type = int, dest = "num_nodes", default = default_number_of_nodes, help = "Number of Kind Kubernetes nodes to create within the cluster. This cannot be changed once the cluster is created. (You would have to delete and recreate the cluster in order to change the number of nodes.) Default: %d" % default_number_of_nodes)
    parser.add_argument("-m", "--memory", type = str, dest = "memory", default = default_memory_reserved_per_node, help = "The amount of memory (RAM) to reserve for each individual Kubernetes node within the cluster. Units can be either 'Gi' or 'Mi'. Default: %s" % default_memory_reserved_per_node)
    parser.add_argument("-o", "--output-directory", dest = "output_directory", type = str, default = default_output_directory, help = "The output file path for the generated .yaml configuration file. Default: \"%s\"" % default_output_directory)
        
    return parser.parse_args() 

def interactive(default_num_nodes: int = default_number_of_nodes, default_memory: str = default_memory_reserved_per_node, default_output_dir:str = default_output_directory, default_api_version:str = default_api_version, default_cluster_name:str = default_cluster_name) -> None:
    """
    Interactive script.
    """
    while True:
        num_nodes_str:str = input("\nNumber of nodes? (Enter nothing to default to %d)\n>" % default_num_nodes)
        
        if num_nodes_str == "":
            print("Using default number of nodes: %d" % default_num_nodes)
            num_nodes = default_num_nodes
            break 
        
        try:
            num_nodes = int(num_nodes_str)
            
            if num_nodes <= 0:
                raise ValueError("Cannot be negative or zero.")
            
            break 
        except Exception as ex:
            print("[ERROR] Please enter a valid, positive integer. You entered: \"%s\"" % num_nodes_str)
            continue
    
    while True:
        memory_str:str = input("\nAmount of memory (RAM) to reserve for each node? (Enter nothing to default to %s)\n>" % default_memory_reserved_per_node)
        
        if memory_str == "":
            print("Using default memory reservation: \"%s\"" % default_memory)
            memory_str = default_memory
            break 
        
        try:
            parse_size(memory_str) # Just try to parse it to ensure it is valid. We don't need the parsed value.
            
            break 
        except Exception as ex:
            print("[ERROR] Invalid size specified: \"%s\". Please specify something like one of these examples: \"2.5 Gi\", \"5 Gi\", \"500 Mi\"" % memory_str)
            continue
    
    while True:
        cluster_name: str = input("\nCuster name? (Enter nothing to default to \"%s\")\n>" % default_cluster_name)
        
        if cluster_name == "":
            print("Using default cluster name: \"%s\"" % default_cluster_name)
            cluster_name = default_cluster_name
        
        break 
    
    while True:
        api_version:str = input("\nAPI version? (Enter nothing to default to \"%s\")\n>" % default_api_version)
        
        if api_version == "":
            print("Using default API version: \"%s\"" % api_version)
            api_version = default_api_version
            
        break
    
    while True:
        output_directory:str = input("\nOutput directory? (Enter nothing to default to \"%s\")\n>" % default_output_directory)
        
        if output_directory == "":
            print("Using default output directory: \"%s\"" % default_output_dir)
            output_directory = default_output_dir
        
        if not output_directory.endswith(".yaml"):
            print("[ERROR] The configuration file must end with the \".yaml\" extension.")
            continue 
        
        break 
    
    generate_config(num_nodes = num_nodes, memory = memory_str, output_dir = output_directory, cluster_name = cluster_name, api_version = api_version)

def generate_config(num_nodes: int = default_number_of_nodes, memory: str = default_memory_reserved_per_node, output_dir:str = default_output_directory, api_version:str = default_api_version, cluster_name:str = default_cluster_name) -> None:
    """
    Generate the configuration file given the necessary configuration parameters.
    """
    print("Creating configuration file for cluster \"%s\" with %d node(s) and %s memory reserved per node. API version: \"%s\". Output file path: \"%s\"" % (cluster_name, num_nodes, memory, output_dir, api_version))
    with open(output_dir, "w") as config_file:
        config_file.write(
"""
kind: Cluster
apiVersion: %s
name: %s
nodes:
- role: control-plane""" % (api_version, cluster_name))
        
        for _ in range(0, num_nodes):
            config_file.write(
"""
- role: worker
  kubeadmConfigPatches:
  - |
    kind: JoinConfiguration
    nodeRegistration:
      kubeletExtraArgs:
        system-reserved: memory=%s""" % memory)

        config_file.write(
"""
featureGates:
  StatefulSetStartOrdinal: true
  DynamicResourceAllocation: true
  InPlacePodVerticalScaling: true 
""")

    print("Finished writing configuration to file \"%s\"" % output_dir)

if __name__ == "__main__":
    print("Welcome to the Kind configuration file generator.")
    
    args = parse_arguments() 
    
    if args.interactive:
        interactive(default_num_nodes = args.num_nodes, default_memory = args.memory, default_output_dir = args.output_directory, default_api_version = args.api_version, default_cluster_name = args.cluster_name) 
    else:
        generate_config(num_nodes = args.num_nodes, memory = args.memory, output_dir = args.output_directory, api_version = args.api_version, cluster_name = args.cluster_name)
    
    print("Exiting.")