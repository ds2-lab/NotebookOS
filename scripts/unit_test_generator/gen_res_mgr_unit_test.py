import argparse
import random
from math import floor
from typing import TextIO

from scipy.stats import truncnorm


def get_truncated_normal(mean=0.0, sd=1.0, low=0.0, upp=10.0):
    return truncnorm((low - mean) / sd, (upp - mean) / sd, loc=mean, scale=sd)


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
    def __init__(self, cpu=8000, mem=64000, gpu=8, vram=32):
        self.CPU: float = cpu
        self.MEM: float = mem
        self.GPU: float = gpu
        self.VRAM: float = vram

        self.initial_cpu: float = cpu
        self.initial_mem: float = mem
        self.initial_gpu: float = gpu
        self.initial_vram: float = vram

    def __repr__(self):
        return self.__str__()

    def __str__(self):
        return f"CPU:{self.committed_cpu}/{self.CPU}, MEM:{self.committed_mem}/{self.MEM}, GPU:{self.committed_gpu}/{self.GPU}, VRAM:{self.committed_vram}/{self.VRAM}"

    @property
    def committed_cpu(self) -> float:
        return self.initial_cpu - self.CPU

    @property
    def committed_mem(self) -> float:
        return self.initial_mem - self.MEM

    @property
    def committed_gpu(self) -> float:
        return self.initial_gpu - self.GPU

    @property
    def committed_vram(self) -> float:
        return self.initial_vram - self.VRAM

    def validate(self)->bool:
        return self.CPU >= 0 and self.MEM >= 0 and self.GPU >= 0 and self.VRAM >= 0 and self.committed_cpu >= 0 and self.committed_mem >= 0 and self.committed_gpu >= 0 and self.committed_vram >= 0

    def copy(self):
        r: Resources = Resources()
        r.CPU = self.CPU
        r.MEM = self.MEM
        r.GPU = self.GPU
        r.VRAM = self.VRAM
        r.initial_cpu = self.initial_cpu
        r.initial_mem = self.initial_mem
        r.initial_gpu = self.initial_gpu
        r.initial_vram = self.initial_vram

        return r


class Outcome:
    def __init__(self, success: bool, reasons: list[str] | None = None):
        self.success = success
        self.reasons = reasons


OP_COMMIT: str = "COMMIT"
OP_DEALLOC: str = "DEALLOC"


# kernel1: Kernel  = Kernel(1, 1000,  2000, 2,  4)
# kernel2: Kernel  = Kernel(2, 2000,  4000, 6, 12)
# kernel3: Kernel  = Kernel(3, 5000, 12000, 2,  3)
# kernel4: Kernel  = Kernel(4, 500,   1000, 1,  1)
# kernels = [kernel1, kernel2, kernel3, kernel4]
# dealloc = [kernel1, kernel2, kernel3, kernel4]

def generate_kernels(num_kernels: int = 4) -> tuple[list[Kernel], list[Kernel]]:
    millicpus_rv = get_truncated_normal(mean=2000, sd=800, low=1.0e-3, upp=8000)
    mem_mb_rv = get_truncated_normal(mean=12000, sd=6400, low=1.0e-3, upp=64000)
    num_gpus_rv = get_truncated_normal(mean=4, sd=1, low=1, upp=8)
    vram_rv = get_truncated_normal(mean=2, sd=1, low=0, upp=4)

    cpu_vals = millicpus_rv.rvs(num_kernels)
    mem_vals = mem_mb_rv.rvs(num_kernels)
    num_gpus_vals = num_gpus_rv.rvs(num_kernels)
    vram_vals = vram_rv.rvs(num_kernels)

    k: list[Kernel] = []
    d: list[Kernel] = []
    for i in range(0, num_kernels):
        kernel = Kernel(i + 1, floor(cpu_vals[i]), floor(mem_vals[i]), floor(num_gpus_vals[i]), floor(vram_vals[i]))
        k.append(kernel)
        d.append(kernel)

    return k, d


def try_allocate(
        kernel: Kernel,
        currResources: Resources,
        alloc: list[Kernel],
        dealloc: list[Kernel],
        targets: list[Kernel],
        operations: list[str],
        outcomes: list[Outcome]) -> bool:
    targets.append(kernel)
    operations.append(OP_COMMIT)

    enough_cpu: bool = currResources.CPU >= kernel.cpu
    enough_mem: bool = currResources.MEM >= kernel.mem
    enough_gpu: bool = currResources.GPU >= kernel.gpu
    enough_vram: bool = currResources.VRAM >= kernel.vram
    if enough_cpu and enough_mem and enough_gpu and enough_vram:
        alloc.append(kernel)
        dealloc.remove(kernel)

        currResources.CPU -= kernel.cpu
        currResources.MEM -= kernel.mem
        currResources.GPU -= kernel.gpu
        currResources.VRAM -= kernel.vram
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


def deallocate(
        kernel: Kernel,
        currResources: Resources,
        alloc: list[Kernel],
        dealloc: list[Kernel],
        targets: list[Kernel],
        operations: list[str],
        outcomes: list[Outcome],
):
    targets.append(kernel)
    operations.append(OP_DEALLOC)
    outcomes.append(Outcome(True, None))
    alloc.remove(kernel)
    dealloc.append(kernel)
    print(f"dealloc->kernel {kernel.kernel_id}: success")

    currResources.CPU += kernel.cpu
    currResources.MEM += kernel.mem
    currResources.GPU += kernel.gpu
    currResources.VRAM += kernel.vram


def generate_operations(
        resources: Resources,
        resources_list: list[Resources],
        alloc: list[Kernel],
        dealloc: list[Kernel],
        targets: list[Kernel],
        operations: list[str],
        outcomes: list[Outcome],
        k: int = 4,
        n: int = 8
):
    for i in range(0, n):
        print(f"\nOperation #{i}")
        if len(alloc) == 0:
            target = random.choice(dealloc)
            try_allocate(target, resources, alloc, dealloc, targets, operations, outcomes)
        elif len(dealloc) == 0:
            target = random.choice(alloc)
            deallocate(target, resources, alloc, dealloc, targets, operations, outcomes)
        else:
            r = random.randint(0, 1)
            if r == 0:
                target = random.choice(dealloc)
                try_allocate(target, resources, alloc, dealloc, targets, operations, outcomes)
            elif r == 1:
                target = random.choice(alloc)
                deallocate(target, resources, alloc, dealloc, targets, operations, outcomes)
            else:
                print("No")
                exit(1)
        print(f"Scheduled: {alloc}")
        print(f"Deallocated: {dealloc}")
        print(f"Resources: {resources}\n")
        resources_list.append(resources.copy())
        if len(alloc) > k:
            print(f"Alloc already has {len(alloc)} kernels: {alloc}")
            exit(1)
        if len(dealloc) > k:
            print(f"Deallocated already has {len(dealloc)} kernels: {dealloc}")
            exit(1)
        if not resources.validate():
            print(f"\n\nERROR: Invalid resources during operation generation: {resources}")
            exit(1)


def generate_commit(targetCommitKernel: Kernel, currentOutcome: Outcome, currentResources: Resources, num_commit: int,
                    num_pending: int, out: TextIO):
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
        print(
            f"\tExpect(len(insufficientResourcesError.OffendingResourceKinds)).To(Equal({len(currentOutcome.reasons)}))")

        out.write("\tExpect(err).ToNot(BeNil())\n")
        out.write("\tok = errors.As(err, &insufficientResourcesError)\n")
        out.write("\tExpect(ok).To(BeTrue())\n")
        out.write("\tExpect(insufficientResourcesError).ToNot(BeNil())\n")
        out.write(
            f"\tExpect(len(insufficientResourcesError.OffendingResourceKinds)).To(Equal({len(currentOutcome.reasons)}))\n")

        for reason in currentOutcome.reasons:
            print(
                f"\tExpect(containsOffendingResourceKind(insufficientResourcesError.OffendingResourceKinds, {reason})).To(BeTrue())")
            out.write(
                f"\tExpect(containsOffendingResourceKind(insufficientResourcesError.OffendingResourceKinds, {reason})).To(BeTrue())\n")

        print(f"\tExpect(kernel{target_id}Spec.Equals(insufficientResourcesError.RequestedResources)).To(BeTrue())")
        print(
            "\tExpect(resourceManager.IdleResources().Equals(insufficientResourcesError.AvailableResources)).To(BeTrue())")

        out.write(
            f"\tExpect(kernel{target_id}Spec.Equals(insufficientResourcesError.RequestedResources)).To(BeTrue())\n")
        out.write(
            "\tExpect(resourceManager.IdleResources().Equals(insufficientResourcesError.AvailableResources)).To(BeTrue())\n")

    print(
        '\tGinkgoWriter.Printf("resourceManager.CommittedResources(): %s\\n", resourceManager.CommittedResources().String())')
    print(f"\tExpect(resourceManager.NumAllocations()).To(Equal(4))")
    print(f"\tExpect(resourceManager.NumCommittedAllocations()).To(Equal({num_commit}))")
    print(f"\tExpect(resourceManager.NumPendingAllocations()).To(Equal({num_pending}))")
    print(
        f"\tExpect(resourceManager.IdleResources().Equals(types.NewDecimalSpec({currentResources.CPU},{currentResources.MEM},{currentResources.GPU},{currentResources.VRAM}))).To(BeTrue())")
    print(
        f"\tExpect(resourceManager.CommittedResources().Equals(types.NewDecimalSpec({currentResources.committed_cpu},{currentResources.committed_mem},{currentResources.committed_gpu},{currentResources.committed_vram}))).To(BeTrue())")

    out.write(
        '\tGinkgoWriter.Printf("resourceManager.CommittedResources(): %s\\n", resourceManager.CommittedResources().String())\n')
    out.write(f"\tExpect(resourceManager.NumAllocations()).To(Equal(4))\n")
    out.write(f"\tExpect(resourceManager.NumCommittedAllocations()).To(Equal({num_commit}))\n")
    out.write(f"\tExpect(resourceManager.NumPendingAllocations()).To(Equal({num_pending}))\n")
    out.write(
        f"\tExpect(resourceManager.IdleResources().Equals(types.NewDecimalSpec({currentResources.CPU},{currentResources.MEM},{currentResources.GPU},{currentResources.VRAM}))).To(BeTrue())\n")
    out.write(
        f"\tExpect(resourceManager.CommittedResources().Equals(types.NewDecimalSpec({currentResources.committed_cpu},{currentResources.committed_mem},{currentResources.committed_gpu},{currentResources.committed_vram}))).To(BeTrue())\n")


def generate_dealloc(targetDeallocKernel: Kernel, currentResources: Resources, num_commit: int, num_pending: int,
                     out: TextIO):
    target_id: int = targetDeallocKernel.kernel_id
    print(f'\n\terr = resourceManager.ReleaseCommittedResources(1, "Kernel{target_id}")')
    print("\tExpect(err).To(BeNil())")

    out.write(f'\n\terr = resourceManager.ReleaseCommittedResources(1, "Kernel{target_id}")\n')
    out.write("\tExpect(err).To(BeNil())\n")

    print(
        f"\tExpect(resourceManager.IdleResources().Equals(types.NewDecimalSpec({currentResources.CPU},{currentResources.MEM},{currentResources.GPU},{currentResources.VRAM}))).To(BeTrue())")
    print(
        f"\tExpect(resourceManager.CommittedResources().Equals(types.NewDecimalSpec({currentResources.committed_cpu},{currentResources.committed_mem},{currentResources.committed_gpu},{currentResources.committed_vram}))).To(BeTrue())")
    print(f"\tExpect(resourceManager.NumAllocations()).To(Equal(4))")
    print(f"\tExpect(resourceManager.NumCommittedAllocations()).To(Equal({num_commit}))")
    print(f"\tExpect(resourceManager.NumPendingAllocations()).To(Equal({num_pending}))")

    out.write(
        f"\tExpect(resourceManager.IdleResources().Equals(types.NewDecimalSpec({currentResources.CPU},{currentResources.MEM},{currentResources.GPU},{currentResources.VRAM}))).To(BeTrue())\n")
    out.write(
        f"\tExpect(resourceManager.CommittedResources().Equals(types.NewDecimalSpec({currentResources.committed_cpu},{currentResources.committed_mem},{currentResources.committed_gpu},{currentResources.committed_vram}))).To(BeTrue())\n")
    out.write(f"\tExpect(resourceManager.NumAllocations()).To(Equal(4))\n")
    out.write(f"\tExpect(resourceManager.NumCommittedAllocations()).To(Equal({num_commit}))\n")
    out.write(f"\tExpect(resourceManager.NumPendingAllocations()).To(Equal({num_pending}))\n")


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("-n", "--num-operations", type=int, default=8)
    parser.add_argument("-o", "--output-file-path", type=str, default="test.go")
    parser.add_argument("-k", "--num-kernels", type=int, default=4)
    parser.add_argument("--allocatable-cpus", type=int, default=128000, help="In millicpus")
    parser.add_argument("--allocatable-memory", type=float, default=512000, help="In MB")
    parser.add_argument("--allocatable-gpus", type=int, default=64)
    parser.add_argument("--allocatable-vram", type=int, default=512, help="In GB")
    args = parser.parse_args()

    print(f"Num operations: {args.num_operations}")
    print(f"Num kernels: {args.num_kernels}")
    print(f"Allocatable CPUs: {args.allocatable_cpus} millicpus")
    print(f"Allocatable Memory: {args.allocatable_memory} megabytes")
    print(f"Allocatable GPUs: {args.allocatable_gpus} gpus")
    print(f"Allocatable VRAM: {args.allocatable_vram} gigabytes")

    input("Ok?")

    targets: list[Kernel] = []
    operations: list[str] = []
    outcomes: list[Outcome] = []
    resources_list: list[Resources] = []

    kernels, dealloc = generate_kernels(num_kernels=args.num_kernels)

    print(dealloc)

    resources: Resources = Resources(cpu = args.allocatable_cpus, mem = args.allocatable_memory, gpu = args.allocatable_gpus, vram = args.allocatable_vram)
    alloc = []

    NUM_OPERATIONS = args.num_operations

    generate_operations(resources, resources_list, alloc, dealloc, targets, operations, outcomes, n=args.num_operations,
                        k=args.num_kernels)

    with open(args.output_file_path, "w") as output_file:
        print("\tvar err error")
        print("\tvar ok bool")
        print("\tvar insufficientResourcesError *scheduling.InsufficientResourcesError")

        output_file.write("\tvar err error\n")
        output_file.write("\tvar ok bool\n")
        output_file.write("\tvar insufficientResourcesError *scheduling.InsufficientResourcesError\n")

        num_pendings = args.num_kernels
        num_commits = 0

        for i in range(0, NUM_OPERATIONS):
            targetKernel = targets[i]
            operation = operations[i]
            outcome = outcomes[i]
            currentRes = resources_list[i]

            if not currentRes.validate():
                print(f"\n\nERROR: Invalid resources while outputting Golang code: {currentRes}")
                exit(1)

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

if __name__ == "__main__":
    main()
