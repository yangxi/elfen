#include <linux/module.h>
#include <linux/miscdevice.h>
#include <linux/fs.h>
#include <linux/cpu.h>
#include <linux/moduleparam.h>
#include <linux/kernel.h>
#include <linux/gfp.h>
#include <linux/io.h>
#include <linux/mm.h>
#include <linux/uaccess.h>
#include <linux/sched.h>
#include <linux/kallsyms.h>
#include <linux/kprobes.h>
#include <linux/dcache.h>
#include <linux/ctype.h>
#include <linux/syscore_ops.h>
#include <trace/events/sched.h>
#include <asm/msr.h>
#include <asm/processor.h>
#include <asm/mwait.h>

#define NR_SHIM_CPU (100)

DECLARE_PER_CPU(unsigned long, shim_curr_task);
DECLARE_PER_CPU(int, shim_curr_syscall);

static unsigned long task_signal[NR_SHIM_CPU];
module_param_array(task_signal, ulong, NULL, 0644);
MODULE_PARM_DESC(task_signal, "Per-CPU signal shows the running task.");

static unsigned long syscall_signal[NR_SHIM_CPU];
module_param_array(syscall_signal, ulong, NULL, 0644);
MODULE_PARM_DESC(syscall_signal, "Per-CPUsignal shows the current systemcall.");

static int ksignals_init(void)
{
  int cpu;
  volatile int * shim_syscall_signal = NULL;
  volatile unsigned long * shim_task_signal = NULL;

  pr_debug("syscall %p, task %p\n", &shim_curr_syscall, &shim_curr_task);
  for_each_possible_cpu (cpu) {
    if (cpu >= NR_SHIM_CPU)
      break;

    shim_syscall_signal = per_cpu_ptr(&shim_curr_syscall, cpu);
    shim_task_signal = per_cpu_ptr(&shim_curr_task, cpu);

    task_signal[cpu] = (unsigned long)__pa(shim_task_signal);
    syscall_signal[cpu] = (unsigned long)__pa(shim_syscall_signal);


    pr_debug("CPU %d SHIM_SYSCALL, va %p, pa %lx\n", cpu, shim_syscall_signal, __pa(shim_syscall_signal));
    pr_debug("CPU %d SHIM_TASK, va %p, pa %lx\n", cpu, shim_task_signal, __pa(shim_task_signal));
  }
  return 0;
}

static void ksignals_exit(void)
{
}

module_init(ksignals_init);
module_exit(ksignals_exit);
MODULE_LICENSE("Dual BSD/GPL");
MODULE_AUTHOR("Xi Yang");
