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

DECLARE_PER_CPU(unsigned long, shim_sleep_flag);

static long c1latency_ioctl(struct file *file, unsigned int cmd,
			    unsigned long arg)
{

  unsigned long *ptr = per_cpu_ptr(&shim_sleep_flag, smp_processor_id());
  __monitor((void *)ptr, 0, 0);
  __sti_mwait(0,0);
  return 0;
}

static const struct file_operations c1latency_fops = {
	.owner = THIS_MODULE,
	.unlocked_ioctl = c1latency_ioctl,
	.compat_ioctl = c1latency_ioctl,
	.llseek = noop_llseek,
};

static struct miscdevice c1latency_miscdev = {
  MISC_DYNAMIC_MINOR,
  "nanonap",
  &c1latency_fops
};


static int c1latency_init(void)
{
  int err;
  int cpu;
  unsigned long *ptr;
  printk(KERN_INFO "Init the miscdev nanonap\n");
  err = misc_register(&c1latency_miscdev);
  if (err < 0) {
    pr_err("Cannot register c1latency device\n");
    return err;
  }
  for_each_possible_cpu (cpu) {
    ptr = per_cpu_ptr(&shim_sleep_flag, cpu);
    printk(KERN_INFO "CPU%d should waits on virt:%p,  phy:%lx\n", cpu, ptr, __pa(ptr));
  }
  return 0;
}




static void c1latency_exit(void)
{
  misc_deregister(&c1latency_miscdev);
}

module_init(c1latency_init);
module_exit(c1latency_exit);
MODULE_LICENSE("Dual BSD/GPL");
MODULE_AUTHOR("Xi Yang");
