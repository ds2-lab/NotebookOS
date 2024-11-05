import random
import argparse
from typing import TextIO

parser = argparse.ArgumentParser()
parser.add_argument("-n", "--num-operations", type = int, default = 8)
parser.add_argument("-o", "--output-file-path", type = str, default = "test.go")
args = parser.parse_args()

class Kernel(object):
    def __init__(self, kernel_id: int, cpu: float, mem: float, gpu: float, vram: float):
        self.kernel_id = kernel_id
        self.cpu = cpu
        self.mem = mem
        self.gpu = gpu
        self.vram = vram

    def __repr__(self):
        return f"Kernel {self.kernel_id}"

    def __str__(self):
        return f"Kernel {self.kernel_id}"

class Resources:
    def __init__(self):
        self.CPU: float  = 8000
        self.MEM: float  = 64000
        self.GPU: float  = 8
        self.VRAM: float = 32

    def __str__(self):
        return f"CPU:{self.CPU}, MEM:{self.MEM}, GPU:{self.GPU}, VRAM:{self.VRAM}"

    @property
    def committed_cpu(self)->float:
        return 8000-self.CPU

    @property
    def committed_mem(self)->float:
        return 64000-self.MEM

    @property
    def committed_gpu(self)->float:
        return 8-self.GPU

    @property
    def committed_vram(self)->float:
        return 32-self.VRAM

    def copy(self):
        r: Resources = Resources()
        r.CPU = self.CPU
        r.MEM = self.MEM
        r.GPU = self.GPU
        r.VRAM = self.VRAM

        return r

class Outcome:
    def __init__(self, success: bool, reasons: list[str] | None = None):
        self.success = success
        self.reasons = reasons

OP_COMMIT:str = "COMMIT"
OP_DEALLOC:str = "DEALLOC"

targets: list[Kernel] = []
operations: list[str] = []
outcomes: list[Outcome] = []
resources_list: list[Resources] = []

kernel1: Kernel  = Kernel(1, 1000,  2000, 2,  4)
kernel2: Kernel  = Kernel(2, 2000,  4000, 6, 12)
kernel3: Kernel  = Kernel(3, 5000, 12000, 2,  3)
kernel4: Kernel  = Kernel(4, 500,   1000, 1,  1)

resources: Resources = Resources()
kernels = [kernel1, kernel2, kernel3, kernel4]
dealloc = [kernel1, kernel2, kernel3, kernel4]
alloc = []

NUM_OPERATIONS = args.num_operations

def try_allocate(kernel: Kernel, resources: Resources)->bool:
    global alloc
    global dealloc
    global targets
    global operations
    global outcomes

    targets.append(kernel)
    operations.append(OP_COMMIT)

    enough_cpu: bool  = resources.CPU > kernel.cpu
    enough_mem: bool  = resources.MEM > kernel.mem
    enough_gpu: bool  = resources.GPU > kernel.gpu
    enough_vram: bool = resources.VRAM > kernel.vram
    if enough_cpu and enough_mem and enough_gpu and enough_vram:
        alloc.append(kernel)
        dealloc.remove(kernel)

        resources.CPU  -= kernel.cpu
        resources.MEM  -= kernel.mem
        resources.GPU  -= kernel.gpu
        resources.VRAM -= kernel.vram
        outcomes.append(Outcome(True, None))

        print(f"commit->kernel {kernel.kernel_id}: success")
        return True

    print(f"commit->kernel {kernel.kernel_id}: failure")

    reasons: list[str] = []
    if not enough_cpu:
        reasons.append("scheduling.CPU")
    if not enough_mem:
        reasons.append("scheduling.Memory")
    if not enough_gpu:
        reasons.append("scheduling.GPU")
    if not enough_vram:
        reasons.append("scheduling.VRAM")

    outcomes.append(Outcome(False, reasons))
    return False

def deallocate(kernel: Kernel, resources: Resources):
    targets.append(kernel)
    operations.append(OP_DEALLOC)
    outcomes.append(Outcome(True, None))
    alloc.remove(kernel)
    dealloc.append(kernel)
    print(f"dealloc->kernel {kernel.kernel_id}: success")

    resources.CPU  += kernel.cpu
    resources.MEM  += kernel.mem
    resources.GPU  += kernel.gpu
    resources.VRAM += kernel.vram

for _ in range(0, NUM_OPERATIONS):
    if len(alloc) == 0:
        target = random.choice(dealloc)
        try_allocate(target, resources)
    elif len(dealloc) == 0:
        target = random.choice(alloc)
        deallocate(target, resources)
    else:
        r = random.randint(0, 1)
        if r == 0:
            target = random.choice(dealloc)
            try_allocate(target, resources)
        elif r == 1:
            target = random.choice(alloc)
            deallocate(target, resources)
        else:
            print("No")
            exit(1)
    print(f"Scheduled: {alloc}")
    print(f"Deallocated: {dealloc}")
    print(f"Resources: {resources}\n")
    resources_list.append(resources.copy())
    if len(alloc) > 4:
        print(f"No, alloc {alloc}")
        exit(1)
    elif len(dealloc) > 4:
        print(f"No, dealloc {dealloc}")
        exit(1)

def generate_commit(targetCommitKernel: Kernel, currentOutcome: Outcome, res: Resources, num_commit: int, num_pending: int, out: TextIO):
    target_id: int = targetCommitKernel.kernel_id
    print(f'\n\terr = resourceManager.CommitResources(1, "Kernel{target_id}", kernel{target_id}Spec, false)')
    out.write(f'\n\terr = resourceManager.CommitResources(1, "Kernel{target_id}", kernel{target_id}Spec, false)\n')
    if currentOutcome.success:
        print("\tExpect(err).To(BeNil())")
        out.write("\tExpect(err).To(BeNil())\n")
    else:
        print("\tExpect(err).ToNot(BeNil())")
        print("\tok = errors.As(err, &insufficientResourcesError)")
        print("\tExpect(ok).To(BeTrue())")
        print("\tExpect(insufficientResourcesError).ToNot(BeNil())")
        print(f"\tExpect(len(insufficientResourcesError.OffendingResourceKinds)).To(Equal({len(currentOutcome.reasons)}))")

        out.write("\tExpect(err).ToNot(BeNil())\n")
        out.write("\tok = errors.As(err, &insufficientResourcesError)\n")
        out.write("\tExpect(ok).To(BeTrue())\n")
        out.write("\tExpect(insufficientResourcesError).ToNot(BeNil())\n")
        out.write(f"\tExpect(len(insufficientResourcesError.OffendingResourceKinds)).To(Equal({len(currentOutcome.reasons)}))\n")

        for reason in currentOutcome.reasons:
            print(f"\tExpect(containsOffendingResourceKind(insufficientResourcesError.OffendingResourceKinds, {reason})).To(BeTrue())")
            out.write(f"\tExpect(containsOffendingResourceKind(insufficientResourcesError.OffendingResourceKinds, {reason})).To(BeTrue())\n")

        print(f"\tExpect(kernel{target_id}Spec.Equals(insufficientResourcesError.RequestedResources)).To(BeTrue())")
        print("\tExpect(resourceManager.IdleResources().Equals(insufficientResourcesError.AvailableResources)).To(BeTrue())")

        out.write(f"\tExpect(kernel{target_id}Spec.Equals(insufficientResourcesError.RequestedResources)).To(BeTrue())\n")
        out.write("\tExpect(resourceManager.IdleResources().Equals(insufficientResourcesError.AvailableResources)).To(BeTrue())\n")

    print('\tGinkgoWriter.Printf("resourceManager.CommittedResources(): %s\\n", resourceManager.CommittedResources().String())')
    print(f"\tExpect(resourceManager.NumAllocations()).To(Equal(4))")
    print(f"\tExpect(resourceManager.NumCommittedAllocations()).To(Equal({num_commit}))")
    print(f"\tExpect(resourceManager.NumPendingAllocations()).To(Equal({num_pending}))")
    print(f"\tExpect(resourceManager.IdleResources().Equals(types.NewDecimalSpec({res.CPU},{res.MEM},{res.GPU},{res.VRAM}))).To(BeTrue())")
    print(f"\tExpect(resourceManager.CommittedResources().Equals(types.NewDecimalSpec({res.committed_cpu},{res.committed_mem},{res.committed_gpu},{res.committed_vram}))).To(BeTrue())")

    out.write('\tGinkgoWriter.Printf("resourceManager.CommittedResources(): %s\\n", resourceManager.CommittedResources().String())\n')
    out.write(f"\tExpect(resourceManager.NumAllocations()).To(Equal(4))\n")
    out.write(f"\tExpect(resourceManager.NumCommittedAllocations()).To(Equal({num_commit}))\n")
    out.write(f"\tExpect(resourceManager.NumPendingAllocations()).To(Equal({num_pending}))\n")
    out.write(f"\tExpect(resourceManager.IdleResources().Equals(types.NewDecimalSpec({res.CPU},{res.MEM},{res.GPU},{res.VRAM}))).To(BeTrue())\n")
    out.write(f"\tExpect(resourceManager.CommittedResources().Equals(types.NewDecimalSpec({res.committed_cpu},{res.committed_mem},{res.committed_gpu},{res.committed_vram}))).To(BeTrue())\n")


def generate_dealloc(targetDeallocKernel: Kernel, currentResources: Resources, num_commit: int, num_pending: int, out: TextIO):
    target_id: int = targetDeallocKernel.kernel_id
    print(f'\n\terr = resourceManager.ReleaseCommittedResources(1, "Kernel{target_id}")')
    print("\tExpect(err).To(BeNil())")

    out.write(f'\n\terr = resourceManager.ReleaseCommittedResources(1, "Kernel{target_id}")\n')
    out.write("\tExpect(err).To(BeNil())\n")

    print(f"\tExpect(resourceManager.IdleResources().Equals(types.NewDecimalSpec({currentResources.CPU},{currentResources.MEM},{currentResources.GPU},{currentResources.VRAM}))).To(BeTrue())")
    print(f"\tExpect(resourceManager.CommittedResources().Equals(types.NewDecimalSpec({currentResources.committed_cpu},{currentResources.committed_mem},{currentResources.committed_gpu},{currentResources.committed_vram}))).To(BeTrue())")
    print(f"\tExpect(resourceManager.NumAllocations()).To(Equal(4))")
    print(f"\tExpect(resourceManager.NumCommittedAllocations()).To(Equal({num_commit}))")
    print(f"\tExpect(resourceManager.NumPendingAllocations()).To(Equal({num_pending}))")

    out.write(f"\tExpect(resourceManager.IdleResources().Equals(types.NewDecimalSpec({currentResources.CPU},{currentResources.MEM},{currentResources.GPU},{currentResources.VRAM}))).To(BeTrue())\n")
    out.write(f"\tExpect(resourceManager.CommittedResources().Equals(types.NewDecimalSpec({currentResources.committed_cpu},{currentResources.committed_mem},{currentResources.committed_gpu},{currentResources.committed_vram}))).To(BeTrue())\n")
    out.write(f"\tExpect(resourceManager.NumAllocations()).To(Equal(4))\n")
    out.write(f"\tExpect(resourceManager.NumCommittedAllocations()).To(Equal({num_commit}))\n")
    out.write(f"\tExpect(resourceManager.NumPendingAllocations()).To(Equal({num_pending}))\n")

if __name__ == "__main__":
    print("\n\n\n\n\n\n\n")

    with open(args.output_file_path, "w") as output_file:
        print("\tvar err error")
        print("\tvar ok bool")
        print("\tvar insufficientResourcesError *scheduling.InsufficientResourcesError")

        output_file.write("\tvar err error\n")
        output_file.write("\tvar ok bool\n")
        output_file.write("\tvar insufficientResourcesError *scheduling.InsufficientResourcesError\n")

        num_pendings = 4
        num_commits = 0

        for i in range(0, NUM_OPERATIONS):
            targetKernel = targets[i]
            operation = operations[i]
            outcome = outcomes[i]
            currentRes = resources_list[i]

            if operation == OP_COMMIT:
                if outcome.success:
                    num_commits += 1
                    num_pendings -= 1
                generate_commit(targetKernel, outcome, currentRes, num_commits, num_pendings, output_file)
            else:
                num_commits -= 1
                num_pendings += 1
                generate_dealloc(targetKernel, currentRes, num_commits, num_pendings, output_file)

            if num_pendings < 0:
                print(f"\n\nERROR: num_pendings={num_pendings}")
                exit(1)

            if num_commits < 0:
                print(f"\n\nERROR: num_commits={num_pendings}")
                exit(1)