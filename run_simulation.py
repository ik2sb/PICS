#!/usr/bin/python

import sys
import time
import os
import threading
import Queue
import logging
import datetime
import sim_lib_common as clib
import sim_global_variables as simgval
import sim_object as simobj
import sim_clock as sim_clock
import sim_event_handler as sim_event
import sim_entity_workgen as workload_gen
import sim_entity_cloudbroker as broker
import sim_entity_iaas as iaas

def set_debug_log (path):
    logger = logging.getLogger('debug_log')
    log_file = path + '/[0]-debug.log'
    hdlr = logging.FileHandler(log_file)
    formatter = logging.Formatter('%(message)s')
    hdlr.setFormatter(formatter)
    logger.addHandler(hdlr)
    logger.setLevel(logging.INFO)
    return logger

class thread_sim_clock (threading.Thread):
    def __init__(self, threadID, conf_obj, qIn, qEvtHandler):
        threading.Thread.__init__(self)
        self.threadID = threadID
        self.conf_obj = conf_obj
        self.q_in = qIn
        self.q_evt_handler = qEvtHandler

    def run(self):
        sim_clock.th_sim_clock(self.threadID, self.conf_obj, self.q_in, self.q_evt_handler)

class thread_sim_event (threading.Thread):
    def __init__(self, threadID, config_obj, qIn, qClock, qWorkGen, qBroker, qIaaS):
        threading.Thread.__init__(self)
        self.threadID = threadID
        self.conf_obj = config_obj
        self.q_in = qIn
        self.q_clock = qClock
        self.q_work_gen = qWorkGen
        self.q_broker = qBroker
        self.q_IaaS = qIaaS

    def run(self):
        sim_event.th_sim_event(self.threadID, self.conf_obj, self.q_in, self.q_clock, self.q_work_gen, self.q_broker, self.q_IaaS)

class thread_sim_entity_workload_generator (threading.Thread):
    def __init__(self, threadID, config_obj, qIn, qOut):
        threading.Thread.__init__(self)
        self.threadID = threadID
        self.conf_obj = config_obj
        self.q_in = qIn
        self.q_out = qOut # out is always event handler

    def run(self):
        workload_gen.th_sim_entity_workgen(self.threadID, self.conf_obj, self.q_in, self.q_out)

class thread_sim_entity_broker (threading.Thread):
    def __init__(self, threadID, config_obj, qIn, qOut):
        threading.Thread.__init__(self)
        self.threadID = threadID
        self.conf_obj = config_obj
        self.q_in = qIn
        self.q_out = qOut # out is always event handler

    def run(self):
        broker.th_sim_entity_cloud_broker(self.threadID, self.conf_obj, self.q_in, self.q_out)

class thread_sim_entity_IaaS (threading.Thread):
    def __init__(self, threadID, config_obj, qIn, qOut):
        threading.Thread.__init__(self)
        self.threadID = threadID
        self.config_obj = config_obj
        self.q_in = qIn
        self.q_out = qOut # out is always event handler

    def run(self):
        iaas.th_sim_entity_iaas_cloud (self.threadID, self.config_obj, self.q_in, self.q_out)

def load_sim_config (sim_conf_obj):

    perf_data_trans_cloud   = 0
    perf_data_trans_in      = 0
    perf_data_trans_out     = 0

    cost_data_trans_cloud   = 0
    cost_data_trans_in      = 0
    cost_data_trans_out     = 0

    conf_txt_vm_sd_policy_name = None
    with open('./config/config.txt','r') as f:
        for l in f.readlines():
            if l.startswith('#'):
                continue

            # VM SD Policy
            if "VM_SCALE_DOWN_POLICY_NAME" in l:
                conf_txt_vm_sd_policy_name = l.split('=')[1].strip()
                simobj.set_Config_vm_scale_down_policy (sim_conf_obj, conf_txt_vm_sd_policy_name)
            if "VM_SCALE_DOWN_POLICY_UNIT" in l:
                simobj.set_Config_vm_scale_down_policy_unit (sim_conf_obj, l.split('=')[1].strip())
            if "VM_SCALE_DOWN_POLICY_RECENT_SAMPLE_CNT" in l:
                simobj.set_Config_vm_scale_down_policy_recent_sample_cnt (sim_conf_obj, l.split('=')[1].strip())
            if "VM_SCALE_DOWN_POLICY_PARAM1" in l:
                simobj.set_Config_vm_scale_down_policy_param1 (sim_conf_obj, l.split('=')[1].strip())
            if "VM_SCALE_DOWN_POLICY_PARAM2" in l:
                simobj.set_Config_vm_scale_down_policy_param2 (sim_conf_obj, l.split('=')[1].strip())
            if "VM_SCALE_DOWN_POLICY_PARAM3" in l:
                simobj.set_Config_vm_scale_down_policy_param3 (sim_conf_obj, l.split('=')[1].strip())
            if "VM_SCALE_DOWN_POLICY_MIN_WAIT_TIME" in l:
                simobj.set_Config_vm_scale_down_policy_min_wait_time (sim_conf_obj, l.split('=')[1].strip())
            if "VM_SCALE_DOWN_POLICY_MAX_WAIT_TIME" in l:
                simobj.set_Config_vm_scale_down_policy_max_wait_time (sim_conf_obj, l.split('=')[1].strip())

            if "WORK_LOAD_FILE" in l:
                simobj.set_Config_workload_file (sim_conf_obj, l.split('=')[1].strip())
            #if "WORK_LOAD_TYPE" in l:
            #    simobj.set_Config_workload_type (sim_conf_obj, l.split('=')[1].strip())
            if "JOB_ASSIGNMENT_POLICY" in l:
                simobj.set_Config_job_assign_policy (sim_conf_obj, l.split('=')[1].strip())
            if "MAX_NUM_OF_CONCURRENT_VMS" in l:
                simobj.set_Config_job_assign_max_running_VMs (sim_conf_obj, l.split('=')[1].strip())

            # vertical scaling
            if "ENABLE_VERTICAL_SCALING=YES" in l:       
                simobj.set_Config_enable_vertical_scaling (sim_conf_obj, l.split('=')[1].strip())
            if "VERTICAL_SCALING_OPERATION" in l:
                simobj.set_Config_vscale_operation (sim_conf_obj, l.split('=')[1].strip())

            # startup lag
            if "MIN_STARTUP_LAG" in l:
                simobj.set_Config_min_startup_lag (sim_conf_obj, int(l.split('=')[1].strip()))
            if "MAX_STARTUP_LAG" in l:
                simobj.set_Config_max_startup_lag (sim_conf_obj, int(l.split('=')[1].strip()))

            # multi vms
            if "VM_CONFIG_FILE" in l:
                simobj.set_Config_vm_config_file (sim_conf_obj, l.split('=')[1].strip())
            if "VM_SELECTION_METHOD" in l:
                simobj.set_Config_vm_selection_method (sim_conf_obj, l.split('=')[1].strip())

            # related to vm billing
            if "VM_BILLING_TIME_UNIT" in l:
                simobj.set_Config_billing_time_unit (sim_conf_obj, l.split('=')[1].strip())
            if "VM_BILLING_TIME_PERIOD" in l:
                simobj.set_Config_billing_time_period (sim_conf_obj, int(l.split('=')[1].strip()))

            # storage system
            if "MAX_CAPACITY_OF_STORAGE" in l:
                simobj.set_Config_max_capacity_of_storage (sim_conf_obj, int(l.split('=')[1].strip()))

            # This is reserved -- 03122015
            # if "MAX_NUM_OF_STORAGE_CONTAINER" in l:
            #     simobj.set_Config_max_num_of_storage_containers (sim_conf_obj, int(l.split('=')[1].strip()))

            if "STORAGE_UNIT_COST" in l:
                simobj.set_Config_storage_unit_cost (sim_conf_obj, float(l.split('=')[1].strip()))
            if "STORAGE_BILLING_TIME_UNIT" in l:
                simobj.set_Config_storage_billing_unit_clock (sim_conf_obj, int(l.split('=')[1].strip()))

            # related to data-transfer
            # data transfer performance
            if "PERF_DATA_TRANSFER_CLOUD" in l:
                perf_data_trans_cloud = float(l.split('=')[1].strip())
            if "PERF_DATA_TRANSFER_IN" in l:
                perf_data_trans_in = float(l.split('=')[1].strip())
            if "PERF_DATA_TRANSFER_OUT" in l:
                perf_data_trans_out = float(l.split('=')[1].strip())

            # data transfer cost
            if "COST_DATA_TRANSFER_CLOUD" in l:
                cost_data_trans_cloud = float(l.split('=')[1].strip())
            if "COST_DATA_TRANSFER_IN" in l:
                cost_data_trans_in = float(l.split('=')[1].strip())
            if "COST_DATA_TRANSFER_OUT" in l:
                cost_data_trans_out = float(l.split('=')[1].strip())

            # job failure model
            if "PROB_JOB_FAILURE" in l:
                simobj.set_Configure_prob_job_failure (sim_conf_obj, float(l.split('=')[1].strip()))
            if "JOB_FAILURE_POLICY" in l:
                simobj.set_Configure_job_failure_policy (sim_conf_obj, l.split('=')[1].strip())

            # system trace...
            if "SIM_TRACE_INTERVAL" in l:
                simobj.set_Config_sim_trace_interval (sim_conf_obj, int(l.split('=')[1].strip()))

    perf_data_transfer = []
    perf_data_transfer.append(perf_data_trans_cloud)
    perf_data_transfer.append(perf_data_trans_in)
    perf_data_transfer.append(perf_data_trans_out)
    simobj.set_Config_perf_data_transfer (sim_conf_obj, perf_data_transfer)

    cost_data_transfer = []
    cost_data_transfer.append(cost_data_trans_cloud)
    cost_data_transfer.append(cost_data_trans_in)
    cost_data_transfer.append(cost_data_trans_out)
    simobj.set_Config_cost_data_transfer (sim_conf_obj, cost_data_transfer)

    print "* Read Config - Done"

    # check configuration
    if sim_conf_obj.min_startup_lag > sim_conf_obj.max_startup_lag:
        print "- [ERROR] Config.txt - MAX_STARTUP_LAG(%d) must be equal to or greater than MIN_STARTUP_LAG(%d)" % (max_startup_lag, min_startup_lag)
        exit()

    # check max number of vms
    if sim_conf_obj.job_assign_max_running_vms is None:
        print "- [ERROR] Conf Error for MAX_NUM_OF_CONCURRENT_VMS! - It should be int [> 0] or 'UNLIMITED'"
        exit()

    # scale down policy check
    if sim_conf_obj.vm_scale_down_policy == None:
        print "- [ERROR] Conf Error for VM_SCALE_DOWN_POLICY_NAME! - \"%s\" is NOT valid configuration." % (conf_txt_vm_sd_policy_name)
        exit()

    # set scale down unit simulation clock
    upd_res = simobj.set_Config_vm_scale_down_policy_timer_unit (sim_conf_obj)
    if upd_res is False:
        print "- [ERROR] Set VM Scale Down Unit Timer Clock Failed!"
        exit()

    # set billing unit simulation clock
    upd_ret = simobj.set_Config_billing_unit_clock (sim_conf_obj)
    if upd_ret is False:
        print "- [ERROR] Set Billing Unit Clock Failed!"
        exit()
    
    # vertical scaling check
    if sim_conf_obj.enable_vertical_scaling == True and sim_conf_obj.job_assign_max_running_vms == sys.maxint:
        print "- [ERROR] Vertical scaling can be activated when using limited # of VMs"
        exit()

    # job failure policy check
    if sim_conf_obj.prob_job_failure > 0 and sim_conf_obj.job_failure_policy == None:
        print "- [ERROR] Job Failure Probability (%s) or Job Failure Policy (%s) Error" % (sim_conf_obj.prob_job_failure, simgval.get_sim_code_str(sim_conf_obj.job_failure_policy))
        exit()




def check_workload_data (sim_conf_obj):

    workload_data_file = sim_conf_obj.workload_file
    if not os.path.isfile(workload_data_file):
        print "- [ERROR] Workload (%s) Data not found!" % workload_data_file
        exit()

    print "* Check Workload Data - Done"

def load_vm_types (sim_conf_obj):

    vm_config_file = sim_conf_obj.vm_config_file_path
    if not os.path.isfile(vm_config_file):
        print "- [ERROR] VM Config File (%s) not found!" % vm_config_file
        exit()
    
    with open(vm_config_file,'r') as f:

        for l in f.readlines():
            if l.startswith('#') or len(l) <= 1:
                continue

            if "NO_OF_VM_TYPES" in l:
                simobj.set_Config_no_of_vm_types (sim_conf_obj, int(l.split('=')[1].strip()))

    no_of_vmt = sim_conf_obj.no_of_vm_types
    vm_types = []

    if no_of_vmt < 1:
        print "- [ERROR] %s - NO_OF_VM_TYPES (%s) should be greater than zero." % (sim_conf_obj.vm_config_file_path, no_of_vmt)
        exit()


    for x in xrange(no_of_vmt):
        vm_type = simobj.VM_Type (x)
        vm_types.append(vm_type);

    with open(sim_conf_obj.vm_config_file_path,'r') as f:
        for l in f.readlines():
            for x in xrange (no_of_vmt):
                tag_type_name = "VM" + str(x+1) + "_TYPE_NAME"
                tag_unit_price = "VM" + str(x+1) + "_UNIT_PRICE"
                tag_cpu_factor = "VM" + str(x+1) + "_CPU_FACTOR"
                tag_net_factor = "VM" + str(x+1) + "_NET_FACTOR"
            
                if tag_type_name in l:
                    vm_types[x].set_vm_type_name (l.split('=')[1].strip())

                if tag_unit_price in l:
                    vm_types[x].set_vm_type_unit_price (float(l.split('=')[1].strip()))

                if tag_cpu_factor in l:
                    vm_types[x].set_vm_type_cpu_factor (float(l.split('=')[1].strip()))

                if tag_net_factor in l:
                    vm_types[x].set_vm_type_net_factor (float(l.split('=')[1].strip()))

    for vm_type in vm_types:
        if vm_type.vm_type_name is None or vm_type.vm_type_unit_price == 0 or vm_type.vm_type_cpu_factor ==0 or vm_type.vm_type_net_factor == 0:
            print "- [ERROR] %s configuration error!" % sim_conf_obj.vm_config_file_path
            clib.sim_exit()

    simobj.set_Config_vm_types (sim_conf_obj, vm_types)

def initialize_logs (sim_conf_obj):

    #now = time.strftime("%c")
    now = str(time.strftime("%Y-%m-%d-%H-%M-%S"))
    dir_prefix = "./Logs/pics_log-"+now

    unit_prefix = ""
    if sim_conf_obj.vm_scale_down_policy_unit is not None:
        unit_prefix = str(sim_conf_obj.vm_scale_down_policy_unit) + "-"

    """
    log_path = dir_prefix + "-SIM-"+ unit_prefix + simgval.get_sim_code_str(sim_conf_obj.vm_scale_down_policy).replace("POLICY_VM_SCALEDOWN_", "").replace(" ","-")  + "-" + sim_conf_obj.workload_file.replace("./workload/", "").replace(".csv", "") + "-" + sim_conf_obj.vm_config_file_path.replace("./config/", "").replace(".txt", "")
    """
    log_path = dir_prefix
    report_path = log_path + "/Report"

    simobj.set_Config_log_path (sim_conf_obj, log_path)
    simobj.set_Config_report_path (sim_conf_obj, report_path)

    if not os.path.exists(sim_conf_obj.log_path):
        os.makedirs(sim_conf_obj.log_path)
        print "* Create Log Directory - Done"

    if not os.path.exists(sim_conf_obj.report_path):
        os.makedirs(sim_conf_obj.report_path)
        print "* Create Report Directory - Done"

    clib.init_message_log (sim_conf_obj.log_path)

    d_log = set_debug_log (sim_conf_obj.log_path)
    simobj.set_Config_g_debug_log (sim_conf_obj, d_log)


def run_simulation ():

    # create config obj.
    sim_conf = simobj.Config(1)
    load_sim_config (sim_conf)

    # check workload data
    check_workload_data (sim_conf)

    # load VM types
    load_vm_types (sim_conf)

    # initialize_logs
    initialize_logs (sim_conf)

    clib.display_conf_obj(0, sim_conf)

    #create queues
    Q_SimClock = Queue.Queue()
    Q_SimEventHandler = Queue.Queue()
    Q_WorkloadGenerator = Queue.Queue()
    Q_Broker = Queue.Queue()
    Q_IaaS = Queue.Queue()

    threads = []

    log_dir = sim_conf.log_path

    thread = thread_sim_clock (simgval.gBLOCK_SIM_CLOCK, sim_conf, Q_SimClock, Q_SimEventHandler)
    thread.start()
    threads.append (thread)
    print "* Create Thread - Sim Clock"

    thread = thread_sim_event (simgval.gBLOCK_SIM_EVENT_HANDLER, sim_conf, Q_SimEventHandler, Q_SimClock, Q_WorkloadGenerator, Q_Broker, Q_IaaS)
    thread.start()
    threads.append (thread)
    print "* Create Thread - Sim Event Handler"

    thread = thread_sim_entity_workload_generator (simgval.gBLOCK_WORKLOAD_GEN, sim_conf, Q_WorkloadGenerator, Q_SimEventHandler)
    thread.start()
    threads.append (thread)
    print "* Create Thread - Sim Workload Generator"

    thread = thread_sim_entity_broker (simgval.gBLOCK_BROKER, sim_conf, Q_Broker, Q_SimEventHandler)
    thread.start()
    threads.append (thread)
    print "* Create Thread - Sim Broker"

    thread = thread_sim_entity_IaaS (simgval.gBLOCK_IAAS, sim_conf, Q_IaaS, Q_SimEventHandler)
    thread.start()
    threads.append (thread)
    print "* Create Thread - Sim IaaS"

    # Simulation Start...
    print ("* Start Simulation in 5 sec")
    for x in xrange(0,5):
        sys.stdout.write('.')
        time.sleep(1)
    sys.stdout.write('\n')

    # ------------------------
    # EVENT MESSAGE
    # ------------------------
    # EVENT_ID   = EVENT UNIQUE ID
    # EVENT_TIME = EVENT_SIM_CLOCK or EVENT_TIME_OUT CLOCK
    # EVENT_SRC  = Event Message Sender
    # EVENT_DST  = Event Message Receiver
    # EVENT_TYPE = Event Type
    # EVENT_SUB_TYPE = Event_Sub_Type
    # EVENT_DATA = Event DaTa
    # ------------------------
    q_evt = clib.make_queue_event (0, simgval.gBLOCK_MAIN, simgval.gBLOCK_SIM_CLOCK, simgval.gEVT_SIM_START, None, None)
    Q_SimClock.put(q_evt)

    for t in threads:
        t.join()
    print "* Exiting Run_Simulation"

def main():
    start = time.time()
    run_simulation()
    print "\n================================================================"
    print "SIMULATOR RUN TIME: %s SEC." % (str(round(time.time() - start, 3)))
    print 'SIMULATION COMPLETE'

def usage():
    print '-----------------------'
    print 'USAGE: run_simulation.py -policy'
    print '-----------------------'

if __name__ == '__main__':
    '''
    args = sys.argv[1:]
    if len(args) != 1: usage()
    elif args[0] == '--help':usage()
    else:main(args)
    '''
    main ()

