import argparse

import yaml
from ansible_runner import run

parser = argparse.ArgumentParser()

parser.add_argument("-p", "--playbook", type=str, default="ansible_setup.yml", help="The name of the playbook to run.")
parser.add_argument("-i", "--inventory-file", type=str, default="./inventory_file.ini",
                    help="Path to the Ansible inventory file.")
parser.add_argument("-v", "--vars-file", type=str, default="./ansible_vars.yml",
                    help="Path to the Ansible 'vars' file.")
parser.add_argument("-l", "--list-playbooks", action="store_true", help="List all possible playbooks.")

args = parser.parse_args()


class bcolors:
    HEADER = '\033[95m'
    OKBLUE = '\033[94m'
    OKCYAN = '\033[96m'
    OKGREEN = '\033[92m'
    YELLOW = '\033[33m'
    WARNING = '\033[93m'
    FAIL = '\033[91m'
    ENDC = '\033[0m'
    BOLD = '\033[1m'
    UNDERLINE = '\033[4m'


# AllPlaybooks is a dictionary that maps "colloquial" names to the file.
AllPlaybooks: dict[str, str] = {
    "BuildSMR": "ansible_build_smr.yml",
    "CloneRepo": "ansible_clone_repo.yml",
    "InstallDocker": "ansible_install_docker.yml",
    "InstallGit": "ansible_install_git.yml",
    "InstallGolang": "ansible_install_golang.yml",
    "InstallHadoopHdfs": "ansible_install_hadoop_hdfs.yml",
    "InstallHelm": "ansible_install_helm.yml",
    "InstallKubectl": "ansible_install_kubectl.yml",
    "InstallProtoc": "ansible_install_protoc.yml",
    "InstallPython": "ansible_install_python.yml",
    "InstallPythonModule": "ansible_install_python_module.yml",
    "AnsibleSetup": "ansible_setup.yml",
    "build_smr": "ansible_build_smr.yml",
    "clone_repo": "ansible_clone_repo.yml",
    "install_docker": "ansible_install_docker.yml",
    "install_git": "ansible_install_git.yml",
    "install_golang": "ansible_install_golang.yml",
    "install_hadoop_hdfs": "ansible_install_hadoop_hdfs.yml",
    "install_helm": "ansible_install_helm.yml",
    "install_kubectl": "ansible_install_kubectl.yml",
    "install_protoc": "ansible_install_protoc.yml",
    "install_python": "ansible_install_python.yml",
    "install_python_module": "ansible_install_python_module.yml",
}

PlaybookFiles: set[str] = set(AllPlaybooks.values())


def get_playbook_listing(colloquialName: str, playbookFile: str) -> str:
    """
    Return the formatted listing to be output when listing playbooks for the given entry.
    """
    return (f"{bcolors.OKBLUE}{colloquialName}{bcolors.YELLOW}{bcolors.ENDC}\n\t"
            f"{bcolors.HEADER}Playbook File{bcolors.ENDC}: {bcolors.YELLOW}\"{bcolors.OKCYAN}{playbookFile}{bcolors.YELLOW}\"{bcolors.ENDC}")

playbook: str = args.playbook
inventory_file: str = args.inventory_file
vars_file: str = args.vars_file

NewEntries: dict[str, str] = {}
for key, value in AllPlaybooks.items():
    # Add lowercase versions of the colloquial names as keys.
    NewEntries[key.lower()] = value

    # Add versions of the keys without "install" -- both in lowercase and non-lowercase.
    key_without_prefix: str = key.removeprefix("Install")

    NewEntries[key_without_prefix] = value
    NewEntries[key_without_prefix.lower()] = value

    # Map the filenames to themselves.
    NewEntries[value] = value

AllPlaybooks.update(NewEntries)

if args.list_playbooks:
    print(bcolors.HEADER + bcolors.BOLD + "Available Playbooks:" + bcolors.ENDC)

    DefaultPlaybooks: dict[str, str] = {
        "BuildSMR": "ansible_build_smr.yml",
        "CloneRepo": "ansible_clone_repo.yml",
        "InstallDocker": "ansible_install_docker.yml",
        "InstallGit": "ansible_install_git.yml",
        "InstallGolang": "ansible_install_golang.yml",
        "InstallHadoopHdfs": "ansible_install_hadoop_hdfs.yml",
        "InstallHelm": "ansible_install_helm.yml",
        "InstallKubectl": "ansible_install_kubectl.yml",
        "InstallProtoc": "ansible_install_protoc.yml",
        "InstallPython": "ansible_install_python.yml",
        "InstallPythonModule": "ansible_install_python_module.yml",
    }

    counter: int = 1
    for colloquial_name, playbook_file in DefaultPlaybooks.items():
        print(f"{counter}: {get_playbook_listing(colloquial_name, playbook_file)}")
        counter += 1

    print(f"\n{bcolors.HEADER}{bcolors.BOLD}Set of all valid Playbooks {bcolors.ENDC}{bcolors.HEADER}(including aliases): {bcolors.ENDC}")
    print(", ".join(sorted(list(AllPlaybooks.keys()), key = lambda x: x.lower())))
    exit(0)

if playbook in AllPlaybooks or playbook.lower() in AllPlaybooks:
    playbook_file: str = AllPlaybooks.get(playbook, AllPlaybooks[playbook.lower()])
else:
    raise ValueError(f"Unknown or unsupported playbook specified: \"{playbook}\"")

print(f"{bcolors.HEADER}Selected playbook: {bcolors.OKBLUE}\"{playbook}\"{bcolors.ENDC} → {bcolors.YELLOW}{playbook_file}{bcolors.ENDC}")
print(f"{bcolors.HEADER}Inventory file path: {bcolors.OKCYAN}\"{inventory_file}\"{bcolors.ENDC}")
print(f"{bcolors.HEADER}Ansible variables file path: {bcolors.OKGREEN}\"{vars_file}\"{bcolors.ENDC}")

with open(vars_file, "r") as f:
    extra_vars = yaml.load(f, Loader=yaml.FullLoader)

with open(inventory_file, "r") as f:
    inventory_file: list[str] = f.readlines()
    inventory_file_as_string: str = ""
    for line in inventory_file:
        inventory_file_as_string += line

run(playbook=playbook_file, extravars=extra_vars, private_data_dir="./", inventory=inventory_file_as_string)
