import os
import sys
import time
import logging
import random
from threading import Condition
import inspect
import math
import numpy as np
from operator import itemgetter
import sim_global_variables as simgval
import sim_lib_common as clib
import sim_object as simobj
import sim_lib_regression as reg
import sim_lib_timeseries as ts
import sim_entity_iaas as iaas

Global_Curr_Sim_Clock = -99
Global_Curr_Sim_Clock_CV = Condition()

g_flag_sim_entity_term = False  # Sim Entity (Thread) Termination Flag
g_flag_sim_entity_term_CV = Condition()

g_my_block_id = None            # global var for current block id
g_log_handler = None            # global var for log handler
g_job_recv_log_handler = None   # global var for job recv log handler
g_job_comp_log_handler = None   # global var for job comp log handler
g_sim_trace_log_handler = None  # global var for sim trace log handler
g_sim_conf = None

gQ_OUT = None                   # global var for q_out to event handler

g_job_logs = [0,0,0,0]          # job log [0] : Recv (Cumm), Recv (Unit), Comp (Cumm), Comp (Unit)
g_job_logs_cv = Condition()

g_last_job_arrival_clock = 0
g_Job_Arrival_Rate = []
g_Job_Arrival_Rate_CV = Condition()

g_Received_Jobs = []
g_Received_Jobs_CV = Condition()

g_Completed_Jobs = []
g_Completed_Jobs_CV = Condition()

# Running VM List (including creating)
g_Running_VM_Instances = []
g_Running_VM_Instances_CV = Condition()

# Stopped VM List
g_Stopped_VM_Instances = []
g_Stopped_VM_Instances_CV = Condition()

# Temporary VM List
g_Temp_VM_Instances = []
g_Temp_VM_Instances_CV = Condition()

g_Startup_Lags = []
g_Startup_Lags_CV = Condition()

# Related to Logs
def set_log (path):
    logger = logging.getLogger('cloud_broker')
    log_file = path + '/[' + str(g_my_block_id) + ']-cloud_broker.log'

    # for log file write...
    hdlr = logging.FileHandler(log_file)
    formatter = logging.Formatter('%(message)s')
    hdlr.setFormatter(formatter)
    logger.addHandler(hdlr)

    # for stdout...
    ch = logging.StreamHandler(sys.stdout)
    formatter = logging.Formatter('%(message)s')
    ch.setFormatter(formatter)
    logger.addHandler(ch)

    logger.setLevel(logging.INFO)
    return logger

def set_job_recv_log (path):
    logger = logging.getLogger('job_rev_log')
    log_file = path + '/[' + str(g_my_block_id) + ']-job_recv_list.log'
    hdlr = logging.FileHandler(log_file)
    #hdlr = logging.FileHandler(path + '/job_recv_list.log')
    formatter = logging.Formatter('%(message)s')
    hdlr.setFormatter(formatter)
    logger.addHandler(hdlr)
    logger.setLevel(logging.INFO)
    return logger

def set_job_comp_log (path):
    logger = logging.getLogger('job_complete_log')
    log_file = path + '/[' + str(g_my_block_id) + ']-job_complete_list.log'
    hdlr = logging.FileHandler(log_file)
    #hdlr = logging.FileHandler(path + '/job_complete_list.log')
    formatter = logging.Formatter('%(message)s')
    hdlr.setFormatter(formatter)
    logger.addHandler(hdlr)
    logger.setLevel(logging.INFO)
    return logger

def set_broker_job_report_log (path):
    logger = logging.getLogger('broker_job_complete_report_log')
    log_file = path + '/3.report_job_complete_report.csv'
    hdlr = logging.FileHandler(log_file)
    #hdlr = logging.FileHandler(path + '/job_complete_list.log')
    formatter = logging.Formatter('%(message)s')
    hdlr.setFormatter(formatter)
    logger.addHandler(hdlr)
    logger.setLevel(logging.INFO)
    return logger

def set_broker_vm_report_log (path):
    logger = logging.getLogger('broker_vm_complete_report_log')
    log_file = path + '/4.report_vm_usage_report.csv'
    hdlr = logging.FileHandler(log_file)
    #hdlr = logging.FileHandler(path + '/job_complete_list.log')
    formatter = logging.Formatter('%(message)s')
    hdlr.setFormatter(formatter)
    logger.addHandler(hdlr)
    logger.setLevel(logging.INFO)
    return logger

def set_broker_sim_trace_log (path):
    logger = logging.getLogger('vm_job_trace_log')
    log_file = path + '/1.report_simulation_trace_broker.csv'
    hdlr = logging.FileHandler(log_file)
    #hdlr = logging.FileHandler(path + '/job_complete_list.log')
    formatter = logging.Formatter('%(message)s')
    hdlr.setFormatter(formatter)
    logger.addHandler(hdlr)
    logger.setLevel(logging.INFO)
    return logger

def set_g_job_logs (j_recv_cumm, j_recv_unit, j_comp_cumm, j_comp_unit):

    global g_job_logs

    g_job_logs_cv.acquire()
    g_job_logs[0] += j_recv_cumm
    g_job_logs[1] += j_recv_unit
    g_job_logs[2] += j_comp_cumm
    g_job_logs[3] += j_comp_unit
    g_job_logs_cv.notify()
    g_job_logs_cv.release()

def clear_g_job_logs_unit ():

    global g_job_logs

    g_job_logs_cv.acquire()

    g_job_logs[1] = 0
    g_job_logs[3] = 0

    g_job_logs_cv.notify()
    g_job_logs_cv.release()

def set_broker_sim_entity_term_flag (val):

    global g_flag_sim_entity_term
    if val is not True and val is not False:
        return

    if g_flag_sim_entity_term == val:
        return

    g_flag_sim_entity_term_CV.acquire()
    g_flag_sim_entity_term = val
    g_flag_sim_entity_term_CV.notify()
    g_flag_sim_entity_term_CV.release()

def write_job_complete_log (job):

    if job.status != simgval.gJOB_ST_COMPLETED and job.status != simgval.gJOB_ST_FAILED:
        clib.display_job_obj(Global_Curr_Sim_Clock, job)
        clib.sim_exit()
        return

    total_duration = job.time_complete - job.time_create
    run_time = job.time_complete - job.time_start
    diff = job.deadline - total_duration

    #job_comp_log.info ('CL\tID\tJN\tADR\tIN\tCPU\tOUT\tDL\tVM\tTG\tTA\tTS\tTC\tTD\tRT\tDF\tST')
    g_job_comp_log_handler.info ('%d\t%d\t%s\t%d\t%s\t%s\t%s\t%d\t%d\t%d\t%d\t%d\t%d\t%d\t%d\t%d\t%s' %
                                 (Global_Curr_Sim_Clock, job.id, job.name, job.act_duration_on_VM, job.act_nettime_on_VM[0], job.act_cputime_on_VM, job.act_nettime_on_VM[1], job.deadline, job.VM_id,
                                  job.time_create, job.time_assign, job.time_start, job.time_complete,
                                  total_duration, run_time, diff, simgval.get_sim_code_str(job.status)))

# Related to Job Arrival Rate
def insert_job_arrival_rate (clock):

    global g_last_job_arrival_clock
    global g_Job_Arrival_Rate

    arrival_interval = 0
    g_Job_Arrival_Rate_CV.acquire()

    if len(g_Job_Arrival_Rate) > 0:
        arrival_interval = clock - g_last_job_arrival_clock
    else:
        arrival_interval = clock

    g_last_job_arrival_clock = clock
    g_Job_Arrival_Rate.append(arrival_interval)
    g_Job_Arrival_Rate_CV.notify()
    g_Job_Arrival_Rate_CV.release()

def get_mean_job_arrival_rate ():

    return int(math.ceil(np.mean(g_Job_Arrival_Rate)))

def get_max_job_arrival_rate ():

    return max (g_Job_Arrival_Rate)

def get_recent_sample_cnt ():

    return g_sim_conf.vm_scale_down_policy_recent_sample_cnt

def get_alpha_ma ():

    return float(g_sim_conf.vm_scale_down_policy_param1)

def get_beta_ma ():

    return float(g_sim_conf.vm_scale_down_policy_param2)

def get_AR_order ():

    return int(g_sim_conf.vm_scale_down_policy_param1)

def get_MA_order ():

    return int(g_sim_conf.vm_scale_down_policy_param2)

def get_DF_order ():

    return int(g_sim_conf.vm_scale_down_policy_param3)

def get_recent_n_job_arrival_list (cnt):

    index = -cnt
    return g_Job_Arrival_Rate[index:]

def get_mean_recent_n_job_arrival_rate (cnt):

    n_recents = get_recent_n_job_arrival_list (cnt)
    return int(math.ceil(np.mean(n_recents)))

def get_max_recent_n_job_arrival_rate (cnt):

    n_recents = get_recent_n_job_arrival_list (cnt)
    return max (n_recents)

# Related to Jobs
def create_Job_Object (job_evt):

    job_uid = simobj.generate_JOB_UID()
    job = simobj.Job (job_uid, job_evt, Global_Curr_Sim_Clock)

    return job

def insert_job_into_ReceivedJobList (job):
    global g_Received_Jobs
    global g_Received_Jobs_CV

    g_Received_Jobs_CV.acquire()
    g_Received_Jobs.append (job)
    g_Received_Jobs_CV.notify()
    g_Received_Jobs_CV.release()

def get_job_from_ReceivedJobList_by_JobID (job_id):

    global g_Received_Jobs
    global g_Received_Jobs_CV

    for j in g_Received_Jobs:
        if j.id == job_id:
            return j

    return None

def get_job_from_ReceivedJobList (job_id, vm_id):

    global g_Received_Jobs
    global g_Received_Jobs_CV

    job_index = -1
    job = None

    for j in g_Received_Jobs:
        if j.id == job_id and j.VM_id == vm_id:
            job_index = g_Received_Jobs.index(j)
            break

    if job_index > -1:
        g_Received_Jobs_CV.acquire()
        job = g_Received_Jobs.pop(job_index)
        g_Received_Jobs_CV.notify()
        g_Received_Jobs_CV.release()

    return job

def insert_job_into_CompletedJobList (job):

    global g_Completed_Jobs
    global g_Completed_Jobs_CV

    g_Completed_Jobs_CV.acquire()
    g_Completed_Jobs.append (job)
    g_Completed_Jobs_CV.notify()
    g_Completed_Jobs_CV.release()

# Related to VMs
def get_detailed_number_of_VM_status ():

    vm_run = get_number_Running_VM_Instances ()

    vm_act = 0
    vm_stup = 0

    for vm in g_Running_VM_Instances:
        if vm.status == simgval.gVM_ST_CREATING:
            vm_stup += 1
        elif vm.status == simgval.gVM_ST_ACTIVE:
            vm_act += 1
        else:
            g_log_handler.error('[Broker__] %s VM Status (%s) Error in g_Running_VM_Instances' % (Global_Curr_Sim_Clock, vm.id, simgval.get_sim_code_str(vm.status)))
            clib.logwrite_VM_Instance_Lists (g_log_handler, "[Broker__] " + str(Global_Curr_Sim_Clock), "Error g_Running_VM_Instances", g_Running_VM_Instances)
            clib.sim_exit()

    vm_stop = len(g_Stopped_VM_Instances)
    return vm_run, vm_stup, vm_act, vm_stop

# this is only for simulation trace...
def get_vm_current_total_cost ():

    vm_total_cost = 0

    for v in g_Running_VM_Instances:
        vm_running_time = Global_Curr_Sim_Clock - v.time_create
        vm_cost, _ = iaas.get_vm_cost_and_billing_hour (vm_running_time, v.unit_price)

        vm_total_cost += vm_cost

    for v in g_Stopped_VM_Instances:
        vm_running_time = simobj.get_vm_billing_clock (v)
        vm_cost, _ = iaas.get_vm_cost_and_billing_hour (vm_running_time, v.unit_price)

        vm_total_cost += vm_cost

    for v in g_Temp_VM_Instances:
        vm_running_time = Global_Curr_Sim_Clock - vm.time_create
        vm_cost, _ = iaas.get_vm_cost_and_billing_hour (vm_running_time, v.unit_price)

        vm_total_cost += vm_cost

    return vm_total_cost

def get_number_Running_VM_Instances ():

    return len (g_Running_VM_Instances)

def get_running_VM_IDs ():

    IDs = []
    for v in g_Running_VM_Instances:
        IDs.append(v.id)

    return IDs

def get_running_VM_IDs_by_VMType (vm_type_name):

    IDs = []
    for v in g_Running_VM_Instances:
        if v.type_name == vm_type_name:
            IDs.append(v.id)

    return IDs

def get_running_VM_by_VM_ID (vm_id):

    for v in g_Running_VM_Instances:
        if v.id == vm_id:
            return v

    return None

def create_new_VM_Instance (vm_type_obj, vscale_victim_id=-1):

    new_vm_uid = simobj.generate_VM_UID()
    new_vm = simobj.VM_Instance (new_vm_uid, "VM-" + str(new_vm_uid), vm_type_obj.vm_type_name, vm_type_obj.vm_type_unit_price, Global_Curr_Sim_Clock)

    g_log_handler.info('[Broker__] %s VM Created: ID:%d, IID: %s, Type: %s, Price: $%s, Status: %s' % (Global_Curr_Sim_Clock, new_vm.id, new_vm.instance_id, new_vm.type_name, new_vm.unit_price, simgval.get_sim_code_str(new_vm.status)))

    # insert the created VM into running list
    if vscale_victim_id == -1: # which means normal new VM creation

        insert_VM_into_Running_VM_Instances (new_vm)
        g_log_handler.info('[Broker__] %s Insert VM(%d,%s,$%s,%s) into Running VM List' % (Global_Curr_Sim_Clock, new_vm.id, new_vm.type_name, new_vm.unit_price, simgval.get_sim_code_str(new_vm.status)))

    else:   # this means new vm creation for vertical scaling

        # set vscale_victim_id --> this points out previous vm id
        simobj.set_VM_vscale_victim_id(new_vm, vscale_victim_id)
        insert_VM_into_Temp_VM_Instances (new_vm)
        g_log_handler.info('[Broker__] %s Insert VM(%d,%s,$%s,%s) into Temp VM List' % (Global_Curr_Sim_Clock, new_vm.id, new_vm.type_name, new_vm.unit_price, simgval.get_sim_code_str(new_vm.status)))

    return new_vm_uid

def insert_VM_into_Running_VM_Instances (vm):

    global g_Running_VM_Instances
    global g_Running_VM_Instances_CV

    #curframe = inspect.currentframe()
    #calframe = inspect.getouterframes(curframe, 2)

    #g_log_handler.info (str(Global_Curr_Sim_Clock) + "")
    #g_log_handler.info (str(Global_Curr_Sim_Clock) + "  insert_VM_into_Running_VM_Instances")
    #g_log_handler.info (str(Global_Curr_Sim_Clock) + "\tVM INFO  : ID: %d, ST: %s" % (vm.id, simgval.get_sim_code_str(vm.status)))
    #g_log_handler.info (str(Global_Curr_Sim_Clock) + "\tCaller   : %s, %s:%d" % (calframe[1][3], calframe[1][1], calframe[1][2]))
    #g_log_handler.info (str(Global_Curr_Sim_Clock) + "")
    #clib.logwrite_VM_Instance_Lists(g_log_handler, Global_Curr_Sim_Clock, "BEFORE insert_VM_into_Running_VM_Instances", g_Running_VM_Instances)
    #g_log_handler.info (str(Global_Curr_Sim_Clock) + "")

    g_Running_VM_Instances_CV.acquire()
    g_Running_VM_Instances.append(vm)
    g_Running_VM_Instances_CV.notify()
    g_Running_VM_Instances_CV.release()

    #clib.logwrite_VM_Instance_Lists(g_log_handler, Global_Curr_Sim_Clock, "AFTER insert_VM_into_Running_VM_Instances", g_Running_VM_Instances)
    #g_log_handler.info (str(Global_Curr_Sim_Clock) + "")

def find_VM_from_Running_VM_Instances (vm_id):

    index = -1
    for vm in g_Running_VM_Instances:
        if vm.id == vm_id:
            index = g_Running_VM_Instances.index(vm)
            return True, index

    return False, index

def get_VM_from_Running_VM_Instances (vm_id):

    for vm in g_Running_VM_Instances:
        if vm.id == vm_id:
            return vm

    return None

def remove_VM_from_Running_VM_Instances (vm_id):

    global g_Running_VM_Instances
    global g_Running_VM_Instances_CV

    for vm in g_Running_VM_Instances:
        if vm.id == vm_id:

            #curframe = inspect.currentframe()
            #calframe = inspect.getouterframes(curframe, 2)

            #g_log_handler.info (str(Global_Curr_Sim_Clock) + "")
            #g_log_handler.info (str(Global_Curr_Sim_Clock) + "  remove_VM_from_Running_VM_Instances")
            #g_log_handler.info (str(Global_Curr_Sim_Clock) + "\tVM INFO  : ID: %d" % (vm.id))
            #g_log_handler.info (str(Global_Curr_Sim_Clock) + "\tCaller   : %s, %s:%d" % (calframe[1][3], calframe[1][1], calframe[1][2]))
            #g_log_handler.info (str(Global_Curr_Sim_Clock) + "")
            #clib.logwrite_VM_Instance_Lists(g_log_handler, Global_Curr_Sim_Clock, "BEFORE remove_VM_from_Running_VM_Instances", g_Running_VM_Instances)
            #g_log_handler.info (str(Global_Curr_Sim_Clock) + "")

            g_Running_VM_Instances_CV.acquire()
            g_Running_VM_Instances.remove(vm)
            g_Running_VM_Instances_CV.notify()
            g_Running_VM_Instances_CV.release()

            #clib.logwrite_VM_Instance_Lists(g_log_handler, Global_Curr_Sim_Clock, "AFTER remove_VM_from_Running_VM_Instances", g_Running_VM_Instances)
            #g_log_handler.info (str(Global_Curr_Sim_Clock) + "")

            return True

    return False

def insert_VM_into_Stopped_VM_Instances (vm):

    global g_Stopped_VM_Instances
    global g_Stopped_VM_Instances_CV

    #curframe = inspect.currentframe()
    #calframe = inspect.getouterframes(curframe, 2)

    #g_log_handler.info (str(Global_Curr_Sim_Clock) + "")
    #g_log_handler.info (str(Global_Curr_Sim_Clock) + "  insert_VM_into_Stopped_VM_Instances")
    #g_log_handler.info (str(Global_Curr_Sim_Clock) + "\tVM INFO  : ID: %d, ST: %s" % (vm.id, simgval.get_sim_code_str(vm.status)))
    #g_log_handler.info (str(Global_Curr_Sim_Clock) + "\tCaller   : %s, %s:%d" % (calframe[1][3], calframe[1][1], calframe[1][2]))
    #g_log_handler.info (str(Global_Curr_Sim_Clock) + "")
    #clib.logwrite_VM_Instance_Lists(g_log_handler, Global_Curr_Sim_Clock, "BEFORE insert_VM_into_Stopped_VM_Instances", g_Stopped_VM_Instances)
    #g_log_handler.info (str(Global_Curr_Sim_Clock) + "")

    g_Stopped_VM_Instances_CV.acquire()
    g_Stopped_VM_Instances.append(vm)
    g_Stopped_VM_Instances_CV.notify()
    g_Stopped_VM_Instances_CV.release()

    #clib.logwrite_VM_Instance_Lists(g_log_handler, Global_Curr_Sim_Clock, "AFTER insert_VM_into_Stopped_VM_Instances", g_Stopped_VM_Instances)
    #g_log_handler.info (str(Global_Curr_Sim_Clock) + "")

def get_VM_from_Stopped_VM_Instances (vm_id):

    for vm in g_Stopped_VM_Instances:
        if vm.id == vm_id:
            return vm

    return None

# related to Temp_VM_Instances
def insert_VM_into_Temp_VM_Instances (vm):

    global g_Temp_VM_Instances
    global g_Temp_VM_Instances_CV

    #curframe = inspect.currentframe()
    #calframe = inspect.getouterframes(curframe, 2)

    #g_log_handler.info (str(Global_Curr_Sim_Clock) + "")
    #g_log_handler.info (str(Global_Curr_Sim_Clock) + "  insert_VM_into_Temp_VM_Instances")
    #g_log_handler.info (str(Global_Curr_Sim_Clock) + "\tVM INFO  : ID: %d, ST: %s" % (vm.id, simgval.get_sim_code_str(vm.status)))
    #g_log_handler.info (str(Global_Curr_Sim_Clock) + "\tCaller   : %s, %s:%d" % (calframe[1][3], calframe[1][1], calframe[1][2]))
    #g_log_handler.info (str(Global_Curr_Sim_Clock) + "")
    #clib.logwrite_VM_Instance_Lists(g_log_handler, Global_Curr_Sim_Clock, "BEFORE insert_VM_into_Temp_VM_Instances", g_Temp_VM_Instances)
    #g_log_handler.info (str(Global_Curr_Sim_Clock) + "")

    g_Temp_VM_Instances_CV.acquire()
    g_Temp_VM_Instances.append(vm)
    g_Temp_VM_Instances_CV.notify()
    g_Temp_VM_Instances_CV.release()

    #clib.logwrite_VM_Instance_Lists(g_log_handler, Global_Curr_Sim_Clock, "AFTER insert_VM_into_Temp_VM_Instances", g_Temp_VM_Instances)
    #g_log_handler.info (str(Global_Curr_Sim_Clock) + "")

def get_VM_from_Temp_VM_Instances (vm_id):

    for vm in g_Temp_VM_Instances:
        if vm.id == vm_id:
            return vm

    return None

def remove_VM_from_Temp_VM_Instances (vm_id):

    global g_Temp_VM_Instances
    global g_Temp_VM_Instances_CV

    for vm in g_Temp_VM_Instances:
        if vm.id == vm_id:

            #curframe = inspect.currentframe()
            #calframe = inspect.getouterframes(curframe, 2)

            #g_log_handler.info (str(Global_Curr_Sim_Clock) + "")
            #g_log_handler.info (str(Global_Curr_Sim_Clock) + "  remove_VM_from_Temp_VM_Instances")
            #g_log_handler.info (str(Global_Curr_Sim_Clock) + "\tVM INFO  : ID: %d" % (vm.id))
            #g_log_handler.info (str(Global_Curr_Sim_Clock) + "\tCaller   : %s, %s:%d" % (calframe[1][3], calframe[1][1], calframe[1][2]))
            #g_log_handler.info (str(Global_Curr_Sim_Clock) + "")
            #clib.logwrite_VM_Instance_Lists(g_log_handler, Global_Curr_Sim_Clock, "BEFORE remove_VM_from_Temp_VM_Instances", g_Temp_VM_Instances)
            #g_log_handler.info (str(Global_Curr_Sim_Clock) + "")

            g_Temp_VM_Instances_CV.acquire()
            g_Temp_VM_Instances.remove(vm)
            g_Temp_VM_Instances_CV.notify()
            g_Temp_VM_Instances_CV.release()

            #clib.logwrite_VM_Instance_Lists(g_log_handler, Global_Curr_Sim_Clock, "AFTER remove_VM_from_Temp_VM_Instances", g_Temp_VM_Instances)
            #g_log_handler.info (str(Global_Curr_Sim_Clock) + "")

            return True

    return False

def insert_job_into_VM_Job_Queue (vm_id, job, is_migration=False):

    if is_migration is True:
        vm = get_VM_from_Temp_VM_Instances(vm_id)
    else:
        vm = get_VM_from_Running_VM_Instances(vm_id)

    if vm is None:
        job_str = simobj.get_job_info_str(job)
        g_log_handler.error("[Broker__] %s VM (ID:%d) not found for %s Assignment!" % (str(Global_Curr_Sim_Clock), vm_id, job_str))
        clib.logwrite_VM_Instance_Lists (g_log_handler, "[Broker__] " + str(Global_Curr_Sim_Clock), "Current g_Running_VM_Instances", g_Running_VM_Instances)
        clib.sim_exit()

    simobj.insert_job_into_VM_job_queue (vm, job)
    simobj.set_Job_status (job, simgval.gJOB_ST_ASSIGNED)
    simobj.set_Job_time_assign (job, Global_Curr_Sim_Clock)
    simobj.set_Job_VM_info (job, vm.id, vm.type_name)

def get_VM_sd_policy_activated (vm_id):

    vm = get_VM_from_Running_VM_Instances(vm_id)
    if vm is None:
        g_log_handler.error ("[Broker__] %s VM (ID:%d) not found in current running VM List" % (str(Global_Curr_Sim_Clock), vm_id))
        clib.logwrite_VM_Instance_Lists (g_log_handler, "[Broker__] " + str(Global_Curr_Sim_Clock), "Current g_Running_VM_Instances", g_Running_VM_Instances)
        clib.sim_exit()

    return vm.sd_policy_activated

def update_VM_sd_wait_time (vm_id, wait_time):

    vm = get_VM_from_Running_VM_Instances(vm_id)
    if vm is None:
        g_log_handler.error ("[Broker__] %s VM (ID:%d) not found in current running VM List" % (str(Global_Curr_Sim_Clock), vm_id))
        clib.logwrite_VM_Instance_Lists (g_log_handler, "[Broker__] " + str(Global_Curr_Sim_Clock), "Current g_Running_VM_Instances", g_Running_VM_Instances)
        clib.sim_exit()

    simobj.set_VM_sd_wait_time(vm, wait_time)

def get_vm_wait_time_bounds ():

    bounds = [g_sim_conf.vm_scale_down_policy_min_wait_time, g_sim_conf.vm_scale_down_policy_max_wait_time]
    return bounds

def get_vm_scale_down_unit ():

    return g_sim_conf.vm_scale_down_policy_unit

def get_average_startup_lagtime ():
    if len(g_Startup_Lags) < 1:
        return int(math.ceil((g_sim_conf.min_startup_lag + g_sim_conf.max_startup_lag)/2.0))
    else:
        return int(math.ceil(np.mean(g_Startup_Lags)))

def get_max_startup_lagtime ():
    return int(g_sim_conf.max_startup_lag)

# Storage Simulation Debug Point - Done on 10.29.2014
def cal_estimated_job_completion_time (vm_id, job_actual_duration_infos):

    # input parameters
    # vm_id -- id for vm obj
    # job_actual_duration_infos
    #   job_actual_duration[0] == total job duration on VM (including cpu + i/o)
    #   job_actual_duration[1] == cputime on VM
    #   job_actual_duration[2] == nettime on VM

    vm = get_VM_from_Running_VM_Instances (vm_id)
    if vm is None:
        g_log_handler.error("[Broker__] %s CAL_EJCT: VM (ID:%d, TY:%s) not found!" % (str(Global_Curr_Sim_Clock), vm_id, vm.type_name))
        clib.logwrite_VM_Instance_Lists (g_log_handler, "[Broker__] " + str(Global_Curr_Sim_Clock), "Current g_Running_VM_Instances", g_Running_VM_Instances)
        clib.sim_exit()

    # procedure
    # if vm status == simgval.gVM_ST_ACTIVE
    #   if vm's current job is None:
    #       jct = sum (j[2] for j in vm.job_queue) + job_actual_duration[0]
    #   else (is not none)
    #       jct = remaining time of currnet job [duration - (current clock - vm.time_active)] + sum (j[2] for j in vm.job_queue) + job_duration
    # elif vm.status == gVM_ST_CREATING
    #   calculate average_startup_lag = math.ceil(np.average(g_Startup_Lags)) if len(g_Startup_Lags) > 0 else math.ceil((min + max)/2)
    #   jct = remaining startup lag [average_startup_lag - (curr_clock - vm.time_create)] + sum (j[2] for j in vm.job_queue) + job_duration
    # else: return sys.maxint

    est_job_comp_time = sys.maxint

    g_log_handler.info ("[Broker__] %s CAL_EJCT: VM (ID:%d, TY:%s)'s Status: %s" % (str(Global_Curr_Sim_Clock), vm.id, vm.type_name, simgval.get_sim_code_str(vm.status)))
    if vm.status == simgval.gVM_ST_CREATING:

        # JCT = [average_startup_lag - (curr_clock - vm.time_create)] + sum (j[2] for j in vm.job_queue) + job_duration

        avg_startup_lag = get_average_startup_lagtime()
        curr_running_time = Global_Curr_Sim_Clock - vm.time_create

        if avg_startup_lag <= curr_running_time:
            avg_startup_lag = get_max_startup_lagtime()

        exe_time_q_jobs = clib.get_sum_job_actual_duration (vm.job_queue)
        g_log_handler.info("[Broker__] %s\t\tCalculate Estimated Job Completion Time on VM (ID:%d,ST:%s)" % (str(Global_Curr_Sim_Clock), vm.id, simgval.get_sim_code_str(vm.status)))
        g_log_handler.info("[Broker__] %s\t\tCAL_EJCT: AVG Startup Lag: %d" % (str(Global_Curr_Sim_Clock), avg_startup_lag))
        g_log_handler.info("[Broker__] %s\t\tCAL_EJCT: VM(%d)'s Curr RunTime (Clock-TC): %d" % (str(Global_Curr_Sim_Clock), vm.id, curr_running_time))
        g_log_handler.info("[Broker__] %s\t\tCAL_EJCT: VM(%d)'s JRT in Queue[len:%d]: %d" % (str(Global_Curr_Sim_Clock), vm.id, len(vm.job_queue), exe_time_q_jobs))
        g_log_handler.info("[Broker__] %s\t\tCAL_EJCT: Recv Job Actual Duration: %s (CPU:%s, IO:%s)" % (str(Global_Curr_Sim_Clock), job_actual_duration_infos[0], job_actual_duration_infos[1], job_actual_duration_infos[2]))

        # estimated job completion time
        est_job_comp_time = avg_startup_lag - curr_running_time + exe_time_q_jobs + job_actual_duration_infos[0]

        g_log_handler.info("[Broker__] %s\t\tCAL_EJCT: EJCT (%d) = AVG_ST_LAG (%d) - CURR_RT (%d) + JET_JQ (%d) + JR (%s - CPU:%s, IO:%s)" \
              % (str(Global_Curr_Sim_Clock), est_job_comp_time, avg_startup_lag, curr_running_time, exe_time_q_jobs, job_actual_duration_infos[0], job_actual_duration_infos[1], job_actual_duration_infos[2]))

    elif vm.status == simgval.gVM_ST_ACTIVE:

        # vm status is active (complete startup)

        exe_time_q_jobs = clib.get_sum_job_actual_duration (vm.job_queue)

        if vm.current_job == None:

            # current job is None (VM is Idle)
            g_log_handler.info("[Broker__] %s CAL_EJCT: VM (ID:%dm, TY:%s)'s Current Job is None [CST:%d]" \
                  % (str(Global_Curr_Sim_Clock), vm.id, vm.type_name, vm.current_job_start_clock))
            
            if vm.current_job_start_clock != -1:
                g_log_handler.error("[Broker__] %s CAL_EJCT: VM (ID:%d, TY:%s)'s Invalid Current Job (%s) Start Time (%d)" \
                      % (str(Global_Curr_Sim_Clock), vm.id, vm.type_name, vm.current_job, vm.current_job_start_clock))
                clib.logwrite_vm_obj(g_log_handler, "[Broker__] " + str(Global_Curr_Sim_Clock), vm)
                clib.sim_exit() # 2015-03-30

            # JCT = sum (j[2] for j in vm.job_queue) + job_duration

            g_log_handler.info("[Broker__] %s\t\tCalculate Estimated Job Completion Time on VM (ID:%d,ST:%s)" % (str(Global_Curr_Sim_Clock), vm.id, simgval.get_sim_code_str(vm.status)))
            g_log_handler.info("[Broker__] %s\t\tCAL_EJCT: VM (ID:%d, TY:%s)'s JRT in Queue [len:%d]: %d" % (str(Global_Curr_Sim_Clock), vm.id, vm.type_name, len(vm.job_queue), exe_time_q_jobs))
            g_log_handler.info("[Broker__] %s\t\tCAL_EJCT: Recv Job Actual Duration: %s (CPU:%s, IO:%s)" % (str(Global_Curr_Sim_Clock), job_actual_duration_infos[0], job_actual_duration_infos[1], job_actual_duration_infos[2]))

            # estimated job completion time
            est_job_comp_time = exe_time_q_jobs + job_actual_duration_infos[0]
            g_log_handler.info ("[Broker__] %s\t\tCAL_EJCT: EJCT (%d) = JET_JQ (%d) + JR (%s - CPU:%s, IO:%s)" \
                  % (str(Global_Curr_Sim_Clock), est_job_comp_time, exe_time_q_jobs, job_actual_duration_infos[0], job_actual_duration_infos[1], job_actual_duration_infos[2]))

        else:

            # current job is NOT None (VM is processing the current job)
            g_log_handler.info ("[Broker__] %s CAL_EJCT: VM (ID:%d, TY:%s)'s Current Job (ID:%d,NM:%s,ADR:%d (C:%s,IO:%s),DL:%d) is NOT None [CST:%d]" \
                  % (str(Global_Curr_Sim_Clock), vm.id, vm.type_name, vm.current_job.id, vm.current_job.name, vm.current_job.act_duration_on_VM, vm.current_job.act_cputime_on_VM, vm.current_job.act_nettime_on_VM, vm.current_job.deadline, vm.current_job_start_clock))

            if vm.current_job_start_clock == -1:

                g_log_handler.error ("[Broker__] %s CAL_EJCT: VM (ID:%d, TY:%s)'s Invalid Current Job (%s) Start Time (%d)" \
                      % (str(Global_Curr_Sim_Clock), vm.id, vm.type_name, simobj.get_job_info_str(vm.current_job), vm.current_job_start_clock))
                clib.logwrite_vm_obj(g_log_handler, "[Broker__] " + str(Global_Curr_Sim_Clock), vm)
                clib.sim_exit()

            # jct = remaining running time of current job + sum of duration in job queue + job_duration
            # remaining running time of current job =.current_job_start_clock + current_job.duration - clock

            sum_past_job_exe_time = clib.get_sum_job_actual_duration (vm.job_history)
            remaining_time_curr_job = vm.current_job_start_clock + vm.current_job.act_duration_on_VM - Global_Curr_Sim_Clock
            
            if remaining_time_curr_job < 0:
                g_log_handler.error ("[Broker__] %s CAL_EJCT: Remaining Exe Time for Current %s is Negative: %d = CJST (%d) + CJADR (%d - C:%s/IO:%s) - CL(%d)" \
                      % (str(Global_Curr_Sim_Clock), simobj.get_job_info_str(vm.current_job), remaining_time_curr_job, vm.current_job_start_clock, vm.current_job.act_duration_on_VM, vm.current_job.act_cputime_on_VM, vm.current_job.act_nettime_on_VM, Global_Curr_Sim_Clock))
                clib.sim_exit()

            g_log_handler.error("[Broker__] %s\t\tCalculate Estimated Job Completion Time on VM (ID:%d,ST:%s)" % (str(Global_Curr_Sim_Clock), vm.id, simgval.get_sim_code_str(vm.status)))
            g_log_handler.error("[Broker__] %s\t\tCAL_EJCT: Current Simulation Clock: %d" % (str(Global_Curr_Sim_Clock), Global_Curr_Sim_Clock))
            g_log_handler.error("[Broker__] %s\t\tCAL_EJCT: Remaining Exe Time for Current %s: %d = CJST (%d) + CJADR (%d - C:%s/IO:%s) - CL(%d)" \
                      % (str(Global_Curr_Sim_Clock), simobj.get_job_info_str(vm.current_job), remaining_time_curr_job, vm.current_job_start_clock,
                         vm.current_job.act_duration_on_VM, vm.current_job.act_cputime_on_VM, vm.current_job.act_nettime_on_VM, Global_Curr_Sim_Clock))
            g_log_handler.error("[Broker__] %s\t\tCAL_EJCT: VM(ID:%d, TY:%s)'s JRT in Queue [len:%d]: %d" % (str(Global_Curr_Sim_Clock), vm.id, vm.type_name, len(vm.job_queue), exe_time_q_jobs))
            g_log_handler.error("[Broker__] %s\t\tCAL_EJCT: Recv Job Actual Duration: %s (CPU:%s, IO:%s)" % (str(Global_Curr_Sim_Clock), job_actual_duration_infos[0], job_actual_duration_infos[1], job_actual_duration_infos[2]))

            # calculate estimate job execution time on the VM
            est_job_comp_time = remaining_time_curr_job  + exe_time_q_jobs + job_actual_duration_infos[0]

            g_log_handler.info("[Broker__] %s\t\tCAL_EJCT: EJCT (%d) = REMAIN_CR (%d) + JET_JQ (%d) + JR (%s - CPU:%s, IO:%s)" \
                  % (str(Global_Curr_Sim_Clock), est_job_comp_time, remaining_time_curr_job, exe_time_q_jobs, job_actual_duration_infos[0], job_actual_duration_infos[1], job_actual_duration_infos[2]))

    return est_job_comp_time


# Related to Vscale
# Storage Simulation Debug Point - Done on 10.29.2014
def cal_estimation_job_complete_clock_for_VM (vm_obj, job_id):

    # this lib only can be used when [1] vm.status == simgval.gVM_ST_ACTIVE && [2] vm.current_job == None
    if vm_obj.status != simgval.gVM_ST_ACTIVE:
        g_log_handler.error("[Broker__] %s CAL_EJCC: %s Status Error!" % (str(Global_Curr_Sim_Clock), vm_obj.get_str_info()))
        clib.sim_exit()
    
    curr_job_remain = 0
    if vm_obj.current_job != None:

        curr_job_remain = (vm_obj.current_job.time_start + vm_obj.current_job.act_duration_on_VM) - Global_Curr_Sim_Clock
        if curr_job_remain < 0:
            curr_job_remain = 0

    remaining_clock = Global_Curr_Sim_Clock + curr_job_remain
    for qjob in vm_obj.job_queue:

        remaining_clock += qjob.act_duration_on_VM
        if qjob.id == job_id:
            return remaining_clock

    return remaining_clock

# check for storage simulation on 10.29.2014
def check_all_deadline_meet (vm_obj):

    return_val = True
    sum_dl_mismatch = 0
    sum_total_act_duration = 0
    number_of_missed_jobs = 0

    for job in vm_obj.job_queue:

        sum_total_act_duration += job.act_duration_on_VM
        clock_job_deadline = job.time_create + job.deadline
        est_job_comp_time = cal_estimation_job_complete_clock_for_VM (vm_obj, job.id)

        if est_job_comp_time > clock_job_deadline:
            sum_dl_mismatch += (est_job_comp_time - clock_job_deadline)
            number_of_missed_jobs += 1

            return_val = False

    return return_val, sum_dl_mismatch, sum_total_act_duration, number_of_missed_jobs

# VM Evaluation based on vm selection method
def do_VM_evaluation (vm_run_time, vm_unit_cost):
    # 1. cost-based selection
    if g_sim_conf.vm_selection_method == simgval.gVM_SEL_METHOD_COST:
        return vm_unit_cost

    # 2. perf-based selection
    elif g_sim_conf.vm_selection_method == simgval.gVM_SEL_METHOD_PERF:
        return vm_run_time

    # 3. cost-perf balanced selection
    elif g_sim_conf.vm_selection_method == simgval.gVM_SEL_METHOD_COSTPERF:
        return round (vm_run_time * (vm_unit_cost / float (g_sim_conf.billing_unit_clock)), 3)

    # error case
    else:
        g_log_handler.error("[Broker__] %s do_VM_evaluation Error! - Unknown VM Selection Code (%s)" % (str(Global_Curr_Sim_Clock), simgval.get_sim_code_str(g_sim_conf.vm_selection_method)))
        clib.sim_exit()

# if return 0: both vm has same evaluation result
# if return 1: first vm is better
# if returl 2: second vm is better

# Storage Simulation Debug Point - check for storage simulation - 10/29/2014
def do_VM_compare (vm_info1, vm_info2):

    if vm_info1 == vm_info2:
        g_log_handler.error("[Broker__] %s do_VM_compare Error! - Two arguments are the same (arg1:%s, arg2:%s)" \
              % (str(Global_Curr_Sim_Clock), vm_info1, vm_info2))
        clib.sim_exit()

    # vm_info1 = [vm_type_name, vm_type_unit_price, vm_run_time]

    vm_eval1  = do_VM_evaluation (vm_info1[2], vm_info1[1])
    vm_eval2  = do_VM_evaluation (vm_info2[2], vm_info2[1])

    if vm_eval1 < vm_eval2:
        return 1
    elif vm_eval1 == vm_eval2:
        return 0
    else:
        return 2

# Storage Scaling Debug Point - Done 12a.m. 10/29/2014
def VScale_VM_Select (vm_obj):

    # [1] find vm types that can meet all deadline for jobs
    # [2] if there are multiple number of VM types, then select VM based on vm selection method
    # [3] if there is no VMs can meet deadline --> choose least deadline mismatch.

    # data structure, [vm_type_obj info, duration, # of mismatched job, sum of mismatched time]
    vscale_matched_vm_infos = []
    vscale_mismatched_vm_infos = []

    avg_startup_lag = get_average_startup_lagtime()
    jobs = vm_obj.job_queue

    print ""
    print "======================================================================================================================================"
    for vm_type in g_sim_conf.vm_types:


        print "VScale VM Type Info:", vm_type.get_infos()
        print "--------------------------------------------------------------------------------------------------------------------------------------"
        vm_evals = []

        vm_run_time = 1 # 1 is first distance of Vscale from now.
        vm_run_time += avg_startup_lag

        num_match_job = 0
        num_mismatch_job = 0
        sum_mismatch_time = 0

        for job in jobs:
            job_duration_infos = cal_job_actual_duration_on_VM (vm_type, job)
            vm_run_time += job_duration_infos[0]

            job_complete_clock = Global_Curr_Sim_Clock + vm_run_time
            job_deadline_clock = job.time_create + job.deadline

            mismatch_time = 0
            if job_deadline_clock >= job_complete_clock:
                num_match_job += 1
            else:
                num_mismatch_job += 1
                mismatch_time = job_complete_clock - job_deadline_clock
                sum_mismatch_time += mismatch_time

            print "  JOB_COMP_CLOCK = ", job_complete_clock, "\t| JOB_DL_CLOCK = ", job_deadline_clock, "\t| VM RUNTIME = ", vm_run_time, "\t| MISMATCH = ", mismatch_time ,"\t| SUM_MISMATCH = ", sum_mismatch_time

        vm_evals.append(vm_type)
        vm_evals.append(vm_run_time)
        vm_evals.append(num_mismatch_job)
        vm_evals.append(sum_mismatch_time)

        # for vm evaluation -- this can be a function
        vm_evl_result = do_VM_evaluation (vm_run_time, vm_type.vm_type_unit_price)
        vm_evals.append(vm_evl_result)

        if num_mismatch_job == 0:
            vscale_matched_vm_infos.append (vm_evals)
        else:
            vscale_mismatched_vm_infos.append (vm_evals)

        print "======================================================================================================================================"

    print ""
    print "======================================================================================================================================"
    print "Matched VM List"
    print "--------------------------------------------------------------------------------------------------------------------------------------"
    if len(vscale_matched_vm_infos) > 0:
        for v in vscale_matched_vm_infos:
            print " ", v[0].get_infos(), v[1], v[2], v[3], v[4]
    else:
        print "  None -- No VM Type can meet the all deadlines for all jobs"

    print "--------------------------------------------------------------------------------------------------------------------------------------"
    print "Mismatched VM List"
    print "--------------------------------------------------------------------------------------------------------------------------------------"
    if len(vscale_mismatched_vm_infos) > 0:
        for v in vscale_mismatched_vm_infos:
            print " ", v[0].get_infos(), v[1], v[2], v[3], v[4]
    else:
        print "  None -- All VM Type can meet the all deadlines for all jobs"

    print "======================================================================================================================================"
    print ""
    print "======================================================================================================================================"


    if len(vscale_matched_vm_infos) < 1:

        print "Final Sorting (%s) -- No VM Type can meet the all deadlines for all jobs" % simgval.get_sim_code_str(g_sim_conf.vm_selection_method)
        print "--------------------------------------------------------------------------------------------------------------------------------------"
        vscale_mismatched_vm_infos.sort(key=itemgetter(2,3))

        for sort_index, v in enumerate(vscale_mismatched_vm_infos):
            if sort_index == 0:
                print "  VM:", v[0].get_infos(), ",\tVM_RT:", v[1], ",\t#_MISSED_J:", v[2], ",\tSUM_MISSED_TM,", v[3], ",\tEVAL:", v[4], " -- Selected (*)"
            else:
                print "  VM:", v[0].get_infos(), ",\tVM_RT:", v[1], ",\t#_MISSED_J:", v[2], ",\tSUM_MISSED_TM,", v[3], ",\tEVAL:", v[4]

        print "======================================================================================================================================"
        return vscale_mismatched_vm_infos[0][0], vscale_mismatched_vm_infos[0][1], vscale_mismatched_vm_infos[0][3], vscale_mismatched_vm_infos[0][2]

    elif len (vscale_matched_vm_infos) == 1:

        print "Final Sorting (%s) -- Only One VM Type can meet the all deadlines for all jobs" % simgval.get_sim_code_str(g_sim_conf.vm_selection_method)
        print "--------------------------------------------------------------------------------------------------------------------------------------"
        print "  VM:", vscale_matched_vm_infos[0][0].get_infos(), ",\tVM_RT:", vscale_matched_vm_infos[0][1], ",\t#_MISSED_J:", vscale_matched_vm_infos[0][2], ",\tSUM_MISSED_TM,", vscale_matched_vm_infos[0][3], ",\tEVAL:", vscale_matched_vm_infos[0][4], " -- Selected (*)"
        print "======================================================================================================================================"

        return vscale_matched_vm_infos[0][0], vscale_matched_vm_infos[0][1], vscale_matched_vm_infos[0][3], vscale_matched_vm_infos[0][2]

    else:

        print "Final Sorting (%s) -- Multiple VM Types can meet the all deadlines for all jobs" % simgval.get_sim_code_str(g_sim_conf.vm_selection_method)
        print "--------------------------------------------------------------------------------------------------------------------------------------"
        vscale_matched_vm_infos.sort (key=lambda vm_info: vm_info[4])

        for sorted_index, v in enumerate(vscale_matched_vm_infos):
            if sorted_index == 0:
                print "  VM:", v[0].get_infos(), ",\tVM_RT:", v[1], ",\t#_MISSED_J:", v[2], ",\tSUM_MISSED_TM,", v[3], ",\tEVAL:", v[4], " -- Selected (*)"
            else:
                print "  VM:", v[0].get_infos(), ",\tVM_RT:", v[1], ",\t#_MISSED_J:", v[2], ",\tSUM_MISSED_TM,", v[3], ",\tEVAL:", v[4]
        print "======================================================================================================================================"
        return vscale_matched_vm_infos[0][0], vscale_matched_vm_infos[0][1], vscale_matched_vm_infos[0][3], vscale_matched_vm_infos[0][2]

def VScale_Trigger_Condition_Check (vm_obj):

    check_Vscale = False
    # check Vscale trigger condition
    # check num of further jobs

    if len(vm_obj.job_queue) > 0:

        # Vscale enable
        if g_sim_conf.enable_vertical_scaling == True:

            curr_run_vm_cnt = get_number_Running_VM_Instances()
            max_cap = g_sim_conf.job_assign_max_running_vms

            # Vscale can be activated when the num of curr run vms == max cap of vm
            if curr_run_vm_cnt == max_cap:
                check_Vscale = True

            # this is error case
            elif curr_run_vm_cnt > max_cap:
                g_log_handler.error ("[Broker__] %s VM Max Cap (%d) Error! - # of Curr Running VMs (%d)" % (str(Global_Curr_Sim_Clock), max_cap, curr_run_vm_cnt))
                clib.sim_exit()

            # num of curr run VMs < max_cap: Vscale disabled.

    # if there no more job --> no nothing for Vscale
    else:
        print "[Broker__] %s VM (ID:%d, TY:%s, PR:%s, ST:%s) has no futher jobs (Queue:%d)" % (str(Global_Curr_Sim_Clock), vm_obj.id, vm_obj.type_name, vm_obj.unit_price, simgval.get_sim_code_str(vm_obj.status), len(vm_obj.job_queue))


    # if check_Vscale is true --> [1] check deadline satisfaction for all job && [2] if cannot meet any deadline for job in queue, find another type.

    if check_Vscale == True:

        flag_vm_evaluation = False

        # VSCALE Trigger regradless of deadline satisfaction
        all_deadline_meet, curr_sum_dl_mismatch, curr_sum_act_duration, curr_num_missed_jobs = check_all_deadline_meet (vm_obj)
        if g_sim_conf.vscale_operation == simgval.gVSO_VSCALE_BOTH:
            flag_vm_evaluation = True

        elif g_sim_conf.vscale_operation == simgval.gVSO_VSCALE_UP:

            if all_deadline_meet == False:
                flag_vm_evaluation = True

        elif g_sim_conf.vscale_operation == simgval.gVSO_VSCALE_DOWN:

            if all_deadline_meet == True:
                flag_vm_evaluation = True

        else:
            g_log_handler.error("[Broker__] %s Invalid VScale_Operation - %s" % (str(Global_Curr_Sim_Clock), simgval.get_sim_code_str(g_sim_conf.vscale_operation)))
            clib.sim_exit()

        if flag_vm_evaluation == True:

            vscale_vm_type, vscale_vm_runtime, vscale_sum_dl_mismatch, vscale_num_of_missed_jobs = VScale_VM_Select (vm_obj)
            if vm_obj.type_name != vscale_vm_type.vm_type_name:

                # candidate vm type should be different from the type of current vm

                flag_vscale_trigger = False
                return_vscale_case = None

                # this is basically VScale-Down case: should move to lower version
                if all_deadline_meet == True:

                    # # vm_info = [vm_type_name, vm_type_unit_price, vm_run_time]
                    current_vm_info = [vm_obj.type_name, vm_obj.unit_price, curr_sum_act_duration]
                    candidate_vm_info = [vscale_vm_type.vm_type_name, vscale_vm_type.vm_type_unit_price, vscale_vm_runtime]

                    compare_result = do_VM_compare (current_vm_info, candidate_vm_info)

                    # current vm is the same with or better than candidate vm
                    if compare_result <= 1:
                        print "[Broker__] %s Current VM (ID:%s, TY:%s, PR:$%s, RT:%s) has better evaluation results than VScale Candidate VM (TY:%s, PR:$%s, RT:%s) - No VScale Activation!" \
                                              % (str(Global_Curr_Sim_Clock), vm_obj.id, vm_obj.type_name, vm_obj.unit_price, curr_sum_act_duration, vscale_vm_type.vm_type_name, vscale_vm_type.vm_type_unit_price, vscale_vm_runtime)
                    else:
                        return_vscal_case = simgval.gVSO_VSCALE_DOWN
                        flag_vscale_trigger = True

                        print "[Broker__] %s VScale Candidate VM (TY:%s, PR:$%s, RT:%s) has better evaluation results than Current VM (ID:%s, TY:%s, PR:$%s, RT:%s) - VScale Activation! - Case:%s" \
                                              % (str(Global_Curr_Sim_Clock), vscale_vm_type.vm_type_name, vscale_vm_type.vm_type_unit_price, vscale_vm_runtime, vm_obj.id, vm_obj.type_name, vm_obj.unit_price, curr_sum_act_duration, simgval.get_sim_code_str(return_vscal_case))


                else:

                    # job deadline mismatch case

                    if vscale_num_of_missed_jobs < curr_num_missed_jobs:

                        return_vscal_case = simgval.gVSO_VSCALE_UP
                        flag_vscale_trigger = True

                        print "[Broker__] %s VScale Candidate VM (TY:%s, PR:$%s, RT:%s, MJ(*):%s) has better job deadline match rate than Current VM (ID:%s, TY:%s, PR:$%s, RT:%s, MJ(*):%s) - VScale Activation! - Case:%s" \
                                              % (str(Global_Curr_Sim_Clock), vscale_vm_type.vm_type_name, vscale_vm_type.vm_type_unit_price, vscale_vm_runtime, vscale_num_of_missed_jobs, vm_obj.id, vm_obj.type_name, vm_obj.unit_price, curr_sum_act_duration, curr_num_missed_jobs, simgval.get_sim_code_str(return_vscal_case))

                    else:
                        print "[Broker__] %s Current VM (ID:%s, TY:%s, PR:$%s, RT:%s, MJ(*):%s) has better evaluation results than VScale Candidate VM (TY:%s, PR:$%s, RT:%s, MJ(*):%s) - No VScale Activation!" \
                                              % (str(Global_Curr_Sim_Clock), vm_obj.id, vm_obj.type_name, vm_obj.unit_price, curr_sum_act_duration, curr_num_missed_jobs, vscale_vm_type.vm_type_name, vscale_vm_type.vm_type_unit_price, vscale_vm_runtime, vscale_num_of_missed_jobs)

                if flag_vscale_trigger is True:
                    return True, vscale_vm_type, return_vscal_case

            else:
                print "[Broker__] %s VScale Candidate VM (TY:%s, PR:$%s) is the same VM type with current VM (ID:%s, TY:%s, PR:$%s) - No VScale Activation" \
                      % (str(Global_Curr_Sim_Clock), vscale_vm_type.vm_type_name, vscale_vm_type.vm_type_unit_price, vm_obj.id, vm_obj.type_name, vm_obj.unit_price)

        else:
            print "[Broker__] %s flag_vm_evaluation is %s -- no VScale Evaluation" \
                  % (str(Global_Curr_Sim_Clock), flag_vm_evaluation)

    return False, None, None

# Job Migration
# Storage Simulation Debug Point - Done 12a.m. 10/28/2014
def VM_Job_Migrate (curr_vm, new_vm, vm_type_obj):

    # extract job from curr_vm
    jobs = []
    for x in range (len(curr_vm.job_queue)):
        jobs.append(curr_vm.job_queue.pop(0))

    #l = g_sim_conf.g_debug_log

    # update actual running time on vm + insert job into new VM
    for job in jobs:

        new_duration_infos = cal_job_actual_duration_on_VM (vm_type_obj, job)

        #l.info(new_duration_infos)

        simobj.set_Job_act_duration_on_VM (job, new_duration_infos, vm_type_obj.vm_type_cpu_factor)

        insert_job_into_VM_Job_Queue (new_vm.id, job, True)
        #job.logwrite_job_obj(Global_Curr_Sim_Clock, l)

# Related to Job Failure - ceated on 11/05/2014
def get_update_job_duration_for_failure_case (job_obj):

    job_str = simobj.get_job_info_str(job_obj)

    if job_obj.job_failure_recovered != True or job_obj.std_job_failure_time == 0 or job_obj.vm_job_failure_time == 0:
        g_log_handler.error("[Broker__] %s get_update_job_duration_for_failure_case Error! ==> Invalid %s -- RECOVER:%s, STD_JOB_FAILURE_TM:%s, VM_JOB_FAILURE_TM:%s" \
              % (str(Global_Curr_Sim_Clock), job_str, job_obj.job_failure_recovered, job_obj.std_job_failure_time, job_obj.vm_job_failure_time))
        clib.sim_exit()

    update_duration = []
    # act_duration_on_VM = INPUT + VM_FAILURE_TIME
    update_duration.append (job_obj.act_nettime_on_VM[0] + job_obj.vm_job_failure_time)
    # act_cputime_on_VM= VM_FAILURE_TIME
    update_duration.append (job_obj.vm_job_failure_time)
    # act_nettime_on_VM = INPUT, 0
    update_duration.append([job_obj.act_nettime_on_VM[0], 0])

    return update_duration


# Related to VM
def terminate_VM (vm_obj, trigger_clock):

    # general procedures
    # [a] vm = get_VM_from_Running_VM_Instances (vm_id)
    # [b] vm_id validation check
    #       1) if no --> error
    # terminateVM from [c] - [end]
    # [c] check followings: vm.status = active and current job is none (cst == -1 as well) and len(job_queue) == 0 and len(job_history) > 0
    #       1) if yes --> can terminate (scale down)
    #       2) if no --> send timer event to event handler (terminate the vm in next time)
    # [d] remove from g_RunningInstances
    # [e] update vm info
    #       1) vm.status = gVM_ST_TERMINATE
    #       2) vm.time_terminate = current sim clock
    # [f] insert into g_Stopped_VM_Instances
    # [g] Send ByPass Event to IaaS (VM Terminated)

    # [a] vm = get_VM_from_Running_VM_Instances (vm_id)

    # [c] Termination Condition Check
    #   To terminate vm,
    #       a) status has to be active
    #       b) current_job has to be None
    #       c) current_job_start_clock has to be -1
    #       d) length of job_queue has to be zero (no further jobs)
    #       e) length of job_history has to be greater than zero

    # remove condition "or len (vm_obj.job_history) < 1" -- this is not relevant due to job failure model
    if vm_obj.status != simgval.gVM_ST_ACTIVE or vm_obj.current_job is not None or vm_obj.current_job_start_clock != -1 or len (vm_obj.job_queue) > 0:

        g_log_handler.info("[Broker__] %s terminate_VM - VM (ID:%d) cannot be terminated! (1)" % (str(Global_Curr_Sim_Clock), vm_obj.id))
        g_log_handler.info("[Broker__] %s \t\t vm_obj.status                  : %s" % (str(Global_Curr_Sim_Clock), simgval.get_sim_code_str(vm_obj.status)))
        g_log_handler.info("[Broker__] %s \t\t vm_obj.current_job             : %s" % (str(Global_Curr_Sim_Clock), vm_obj.current_job))
        g_log_handler.info("[Broker__] %s \t\t vm_obj.current_job_start_clock : %s" % (str(Global_Curr_Sim_Clock), vm_obj.current_job_start_clock))
        g_log_handler.info("[Broker__] %s \t\t len(vm_obj.job_queue)          : %s" % (str(Global_Curr_Sim_Clock), len(vm_obj.job_queue)))
        g_log_handler.info("[Broker__] %s \t\t len(vm_obj.job_history)        : %s" % (str(Global_Curr_Sim_Clock), len(vm_obj.job_history)))

        # change scale down policy activated flag
        if vm_obj.sd_policy_activated == True:
            simobj.set_VM_sd_policy_activated_flag (vm_obj, False)

        # send timer event for next scale dowtn petriggeriod
        trigger_vm_scale_down (vm_obj.id)
        return

    # [c-1] special routine for Jat-basesd scale down
    # this ("vm_obj.is_vscale_victim == False") is added for VScale Support - 1017 2014
    if use_sd_policy_activated_flag() == True and vm_obj.is_vscale_victim == False:

        if vm_obj.sd_policy_activated == False:

            # not ready for termination --> trigger scale down
            g_log_handler.info("[Broker__] %s terminate_VM - VM (ID:%d) cannot be terminated! (2)" % (str(Global_Curr_Sim_Clock), vm_obj.id))
            simobj.set_VM_sd_policy_activated_flag (vm_obj, True)
            trigger_vm_scale_down (vm_obj.id)
            return

        else:
            
            # ready for termination --> need a termination time validation check.
            if Global_Curr_Sim_Clock != trigger_clock + vm_obj.sd_wait_time:

                g_log_handler.error("[Broker__] %s terminate_VM Error! => VM (ID:%d) Termination Clock Mismatch - Trigger Clock:%d, SD Wait Time:%d" \
                      % (str(Global_Curr_Sim_Clock), vm_obj.id, trigger_clock, vm_obj.sd_wait_time))
                clib.logwrite_vm_obj(g_log_handler, "[Broker__] " + str(Global_Curr_Sim_Clock), vm_obj)
                clib.sim_exit()


    # [d] remove from g_RunningInstances
    if remove_VM_from_Running_VM_Instances (vm_obj.id) == False:
        g_log_handler.error ("[Broker__] %s terminate_VM Error! => Remove VM (ID:%s, ST:%s) from g_Running_VM_Instances Error!" \
              % (str(Global_Curr_Sim_Clock), vm_obj.id, simgval.get_sim_code_str(vm_obj.status)))
        clib.logwrite_VM_Instance_Lists(g_log_handler, "[Broker__] " + str(Global_Curr_Sim_Clock), "g_Running_VM_Instances", g_Running_VM_Instances)
        clib.sim_exit()

    # [e] update vm info
    #       1) vm.status = gVM_ST_TERMINATE
    #       2) vm.time_terminate = current sim clock
    simobj.set_VM_status (vm_obj, simgval.gVM_ST_TERMINATE)
    simobj.set_VM_time_terminate (vm_obj, Global_Curr_Sim_Clock)

    # [f] insert into g_Stopped_VM_Instances
    insert_VM_into_Stopped_VM_Instances (vm_obj)

    # [g] Send ByPass Event to IaaS (VM Terminated)
    print "[Broker__] %s VM (%d,%s,%s) Terminated - Running Time: %d" \
          % (str(Global_Curr_Sim_Clock), vm_obj.id, vm_obj.instance_id, simgval.get_sim_code_str(vm_obj.status), vm_obj.time_terminate - vm_obj.time_create)

    # [h] create bypass event to event handler (actual destination is iaas)
    bpe_data =  [simgval.gEVT_TERMINATE_VM]
    bpe_data.append (vm_obj)

    bpe_evt = clib.make_queue_event (0, g_my_block_id, simgval.gBLOCK_SIM_EVENT_HANDLER, simgval.gEVT_BYPASS, simgval.gBLOCK_IAAS, bpe_data)
    gQ_OUT.put (bpe_evt)



def send_VM_Create_Event_to_IaaS (vm, vscale=False):

    # 1017 2014
    # if vscale == True: vertical scaling case
    # if vscale == False: horizontal scaling case

    # create bypass event to event handler (actual destination is iaas)
    bpe_data =  [simgval.gEVT_CREATE_VM]
    bpe_data.append (vm)

    bpe_evt = clib.make_queue_event (0, g_my_block_id, simgval.gBLOCK_SIM_EVENT_HANDLER, simgval.gEVT_BYPASS, simgval.gBLOCK_IAAS, bpe_data)
    gQ_OUT.put (bpe_evt)

    # scale down activate
    print "[Broker__] %s Scale Down Activated for VM (ID:%d,IID:%s,TC:%d)" % (str(Global_Curr_Sim_Clock), vm.id, vm.instance_id, vm.time_create)
    trigger_vm_scale_down (vm.id)


	# how about job start...

# Related to Lag Time
def insert_LagTime_into_StartupLagList (lag_time):

    global g_Startup_Lags
    global g_Startup_Lags_CV

    g_Startup_Lags_CV.acquire()
    g_Startup_Lags.append(lag_time)
    g_Startup_Lags_CV.notify()
    g_Startup_Lags_CV.release()

def broker_check_sim_entity_termination ():

    # no jobs and no active vms
    no_recv_jobs    = len(g_Received_Jobs)
    no_comp_jobs    = len(g_Completed_Jobs)
    no_running_VMs  = len(g_Running_VM_Instances)
    no_stopped_VMs  = len(g_Stopped_VM_Instances)

    if (no_recv_jobs < 1 and no_comp_jobs > 0) and (no_running_VMs < 1 and no_stopped_VMs > 0):
        g_log_handler.info ("[Broker__] %s Broker can be terminated - Len(rJobs):%d, Len(cJobs):%d, Len(rVMs):%d, Len(sVMs):%d" \
              %(str(Global_Curr_Sim_Clock), no_recv_jobs, no_comp_jobs, no_running_VMs, no_stopped_VMs))
        set_broker_sim_entity_term_flag (True)

        # sim entity termination procedure - send evt to ask sim event handler for termination
        query_evt = clib.make_queue_event (Global_Curr_Sim_Clock, g_my_block_id, simgval.gBLOCK_SIM_EVENT_HANDLER, simgval.gEVT_ASK_SIMENTITY_TERMINATION, None, None)
        gQ_OUT.put (query_evt)

    else:
        g_log_handler.info ("[Broker__] %s Broker can NOT be terminated - Len(rJobs):%d, Len(cJobs):%d, Len(rVMs):%d, Len(sVMs):%d" \
              %(str(Global_Curr_Sim_Clock), no_recv_jobs, no_comp_jobs, no_running_VMs, no_stopped_VMs))

# gEVT_EXP_TIMER processing
def broker_send_evt_ack (evt_id, evt_org_code):

    ack_evt = clib.make_queue_event (0, g_my_block_id, simgval.gBLOCK_SIM_EVENT_HANDLER, simgval.gEVT_ACK, evt_org_code, None)
    ack_evt[0] = evt_id
    gQ_OUT.put (ack_evt)

    return

# Related To Send Event
def send_job_start_evt_to_iaas (vm):

    #curframe = inspect.currentframe()
    #calframe = inspect.getouterframes(curframe, 2)
    #g_log_handler.info ("")
    #g_log_handler.info ("---------------------------------------------------------------------")
    #g_log_handler.info (str(Global_Curr_Sim_Clock) + "\tCaller   : %s, %s:%d" % (calframe[1][3], calframe[1][1], calframe[1][2]))
    #g_log_handler.info ("---------------------------------------------------------------------")
    #g_log_handler.info ("")
    #clib.logwrite_vm_obj(g_log_handler, Global_Curr_Sim_Clock, vm)
    #g_log_handler.info ("")

    evt_code = simgval.gEVT_BYPASS
    if vm.status != simgval.gVM_ST_ACTIVE:
        g_log_handler.info("[Broker__] %s EVT_SEND ERROR! - EC:%s, VM(ID:%d)'s Status (%s) is NOT Active!" \
              %(str(Global_Curr_Sim_Clock), simgval.get_sim_code_str(evt_code), vm.id, simgval.get_sim_code_str(vm.status)))
        clib.sim_exit()


    if len (vm.job_queue) < 1:
        g_log_handler.info("[Broker__] %s EVT_SEND ERROR! - EC:%s, VM(ID:%d)'s Job Queue is Empty (Lenth:%d)" \
              %(str(Global_Curr_Sim_Clock), simgval.get_sim_code_str(evt_code), vm.id, len(vm.job_queue)))
        clib.sim_exit()
        return

    if vm.current_job is not None:
        g_log_handler.info ("[Broker__] %s EVT_SEND ERROR! - EC:%s, VM(ID:%s) has Current Job (ID:%s, NM:%s, ADR:%s (CPU:%s, IO:%s), DL:%d)!" \
              %(str(Global_Curr_Sim_Clock), simgval.get_sim_code_str(evt_code), vm.id, vm.current_job.id, vm.current_job.name, vm.current_job.act_duration_on_VM, vm.current_job.act_cputime_on_VM, vm.current_job.act_nettime_on_VM, vm.current_job.deadline))
        clib.sim_exit()

    # get job id
    try:
        job_id = vm.job_queue[0].id
    except IndexError:
        g_log_handler.info ("[Broker__] %s send_job_start_evt_to_iaas Error!: no job_queue[0].id" % (str(Global_Curr_Sim_Clock)))
        clib.display_vm_obj(Global_Curr_Sim_Clock, vm)
        clib.logwrite_vm_obj(g_log_handler, "[Broker__] " + str(Global_Curr_Sim_Clock), vm)
        clib.sim_exit()

    # create bypass event to event handler (actual destination is iaas)
    evt_r_code = simgval.gEVT_START_JOB
    evt_sub_code = simgval.gBLOCK_IAAS
    bpe_data =  [evt_r_code]
    bpe_data.append (job_id)
    bpe_data.append (vm)

    bpe_evt = clib.make_queue_event (0, g_my_block_id, simgval.gBLOCK_SIM_EVENT_HANDLER, evt_code, evt_sub_code, bpe_data)
    g_log_handler.info("[Broker__] %s EVT_SEND - EC:%s[%s], ESC:%s, VMID:%d, JOBID:%d" \
          %(str(Global_Curr_Sim_Clock), simgval.get_sim_code_str(evt_code), simgval.get_sim_code_str(evt_r_code), simgval.get_sim_code_str(evt_sub_code), vm.id, job_id))
    gQ_OUT.put (bpe_evt)

def send_job_restart_evt_to_iaas (vm, cjob_id):

    evt_code = simgval.gEVT_BYPASS
    if vm.status != simgval.gVM_ST_ACTIVE:
        g_log_handler.error("[Broker__] %s EVT_SEND ERROR! - EC:%s, VM(ID:%d)'s Status (%s) is NOT Active!" \
              %(str(Global_Curr_Sim_Clock), simgval.get_sim_code_str(evt_code), vm.id, simgval.get_sim_code_str(vm.status)))
        clib.sim_exit()


    if vm.current_job is None:
        g_log_handler.info("[Broker__] %s EVT_SEND ERROR! - EC:%s, VM(ID:%d) has no Current Job (%s)!" \
              %(str(Global_Curr_Sim_Clock), simgval.get_sim_code_str(evt_code), vm.id, vm.current_job))
        clib.sim_exit()
    else:
        if vm.current_job.id != cjob_id:
            g_log_handler.info("[Broker__] %s EVT_SEND ERROR! - EC:%s, VM(ID:%d) - Current Job ID mismatch (Stored:%s, Input:%s)" \
                  %(str(Global_Curr_Sim_Clock), simgval.get_sim_code_str(evt_code), vm.id, vm.current_job.id, cjob_id))
            clib.sim_exit()

        if vm.current_job.status != simgval.gJOB_ST_FAILED or vm.current_job.job_failure_recovered != True or vm.current_job.std_job_failure_time <= 0 or vm.current_job.vm_job_failure_time <= 0:
            g_log_handler.info("[Broker__] %s EVT_SEND ERROR! - EC:%s, VM(ID:%d) - Current Job (ID:%s) is Invalid for job re-execution (ST:%s, JF_REC:%s, STD_JFT:%s, VM_JFT:%s)" \
                  %(str(Global_Curr_Sim_Clock), simgval.get_sim_code_str(evt_code), vm.id, vm.current_job.id, simgval.get_sim_code_str(vm.current_job.status), vm.current_job.job_failure_recovered, vm.current_job.std_job_failure_time, vm.current_job.vm_job_failure_time))
            clib.sim_exit()

    # create bypass event to event handler (actual destination is iaas)
    evt_r_code = simgval.gEVT_RESTART_JOB
    evt_sub_code = simgval.gBLOCK_IAAS
    bpe_data =  [evt_r_code]
    bpe_data.append (cjob_id)
    bpe_data.append (vm)

    bpe_evt = clib.make_queue_event (0, g_my_block_id, simgval.gBLOCK_SIM_EVENT_HANDLER, evt_code, evt_sub_code, bpe_data)
    g_log_handler.info ("[Broker__] %s EVT_SEND - EC:%s[%s], ESC:%s, VMID:%d, JOBID:%d" \
          %(str(Global_Curr_Sim_Clock), simgval.get_sim_code_str(evt_code), simgval.get_sim_code_str(evt_r_code), simgval.get_sim_code_str(evt_sub_code), vm.id, cjob_id))

    gQ_OUT.put (bpe_evt)

def send_sd_tmr_clear_event_to_SEH (vm_obj, is_exception=False):

    if is_exception is False:
        if use_sd_policy_activated_flag() is not True or vm_obj.sd_policy_activated is not True:
            clib.sim_long_sleep()
            return
    else:
        # this case is if is_exception (e.g. vscale or job duration change in IaaS) is True

        # added this on 10/20/14
        if vm_obj.sd_policy_activated is not True:
            simobj.set_VM_sd_policy_activated_flag (vm_obj, True)

    tmr_clear_evt_data = [simgval.gEVT_REQ_CLEAR_SD_TMR_EVENT]
    tmr_clear_evt_data.append(vm_obj)
    tmr_clear_evt_data.append(is_exception)
    tmr_clear_evt = clib.make_queue_event (0, g_my_block_id, simgval.gBLOCK_SIM_EVENT_HANDLER, simgval.gEVT_BYPASS, simgval.gBLOCK_SIM_EVENT_HANDLER, tmr_clear_evt_data)

    gQ_OUT.put (tmr_clear_evt)
    return

def broker_evt_sub_vm_scale_down_processing (q_msg):

    evt_id          = q_msg[0] # EVT_ID
    evt_code        = q_msg[4] # EVT_CODE
    evt_sub_code    = q_msg[5] # EVT_SUB_CODE
    evt_data        = q_msg[6] # EVT_DATA

    if evt_sub_code != simgval.gEVT_SUB_VM_SCALE_DOWN:
        g_log_handler.error ("[Broker__] %s EVT_CODE (EC:%s, ESC:%s) Error! => %s" \
              % (str(Global_Curr_Sim_Clock), simgval.get_sim_code_str(evt_code), simgval.get_sim_code_str(evt_sub_code), q_msg))
        clib.sim_exit()

    vm_id = evt_data[0]
    trigger_clock = evt_data[1]

    # procedure
    # [a] vm = get_VM_from_Running_VM_Instances (vm_id)
    # [b] vm_id validation check
    #       1) if no --> error
    # terminateVM from [c] - [end]
    # [c] check followings: vm.status = active and current job is none (cst == -1 as well) and len(job_queue) == 0 and len(job_history) > 0
    #       1) if yes --> can terminate (scale down)
    #       2) if no --> send timer event to event handler (terminate the vm in next time)
    # [d] remove from g_RunningInstances
    # [e] update vm info
    #       1) vm.status = gVM_ST_TERMINATE
    #       2) vm.time_terminate = current sim clock
    # [f] insert into g_Stopped_VM_Instances
    # [g] Send ByPass Event to IaaS (VM Terminated)

    # [a] vm = get_VM_from_Running_VM_Instances (vm_id)

    vm_obj = get_VM_from_Running_VM_Instances(vm_id)
    if vm_obj is None:
         g_log_handler.error ("[Broker__] %s EVT_CODE (EC:%s, ESC:%s) Processing Error! => Cannot found VM with Input Params (ID:%d)" \
               % (str(Global_Curr_Sim_Clock), simgval.get_sim_code_str(evt_code), simgval.get_sim_code_str(evt_sub_code), vm_id))
         clib.sim_exit()

    # [b] vm_id validation check
    #       1) if no --> error
    if vm_obj.id != vm_id:
         g_log_handler.error ("[Broker__] %s EVT_CODE (EC:%s, ESC:%s) Processing Error! => Invalid VM Info (ID:%d) with Input Params (ID:%d)" % (str(Global_Curr_Sim_Clock), simgval.get_sim_code_str(evt_code), simgval.get_sim_code_str(evt_sub_code), vm_obj.id, vm_id))
         clib.logwrite_vm_obj(g_log_handler, "[Broker__] " + str(Global_Curr_Sim_Clock), vm_obj)
         clib.sim_exit()

    terminate_VM (vm_obj, trigger_clock)

    # if every vm is terminated and every job is terminated --> ask sim event handler for this entity's termination
    broker_check_sim_entity_termination ()

def broker_evt_sub_write_sim_trace (q_msg):

    evt_id          = q_msg[0] # EVT_ID
    evt_code        = q_msg[4] # EVT_CODE
    evt_sub_code    = q_msg[5] # EVT_SUB_CODE
    evt_data        = q_msg[6] # EVT_DATA

    if evt_sub_code != simgval.gEVT_SUB_WRITE_SIM_TRACE_BROKER:
        g_log_handler.info("[Broker__] %s EVT_CODE (EC:%s, ESC:%s) Error! => %s" \
              % (str(Global_Curr_Sim_Clock), simgval.get_sim_code_str(evt_code), simgval.get_sim_code_str(evt_sub_code), q_msg))
        clib.sim_exit()

    # procedure
    # [a] write simulation trace
    # [b] register the same timer event
    # CLOCK\tJOB_RECV(CUMM)\tJOB_RECV(UNIT)\tJOB_COMP(CUMM)\tJOB_COMP(UNIT)\tVM_RUN\tVM_ACT\tVM_STUP\tVM_STOP\tVM_COST')

    vm_cnt = get_detailed_number_of_VM_status ()
    vm_total_cost = get_vm_current_total_cost ()
    g_sim_trace_log_handler.info ('%d,%d,%d,%d,%d,%d,%d,%d,%d,%s' % (Global_Curr_Sim_Clock, g_job_logs[0], g_job_logs[1], g_job_logs[2], g_job_logs[3], vm_cnt[0], vm_cnt[1], vm_cnt[2], vm_cnt[3], vm_total_cost))

    # [b] register the same timer event
    register_broker_sim_trace_event ()

def broker_evt_sub_terminate_vscale_victim (q_msg):

    evt_id          = q_msg[0] # EVT_ID
    evt_code        = q_msg[4] # EVT_CODE
    evt_sub_code    = q_msg[5] # EVT_SUB_CODE
    evt_data        = q_msg[6] # EVT_DATA
    
    if evt_sub_code != simgval.gEVT_SUB_TERMINATE_VSCALE_VICTIM:
        g_log_handler.info("[Broker__] %s EVT_CODE (EC:%s, ESC:%s) Error! => %s" \
              % (str(Global_Curr_Sim_Clock), simgval.get_sim_code_str(evt_code), simgval.get_sim_code_str(evt_sub_code), q_msg))
        clib.sim_exit()

    victim_vm_id = evt_data[0]
    term_trigger_clock = evt_data[1]
    vscale_vm_type = evt_data[2]

    """
    [1] get victim vm object
            if victim vm obj is not marked as victim --> terminate simulator

    [2] create new vm for vscale
        set vscale_victim_id = victim_vm_id

    [3] job migrate with cpu factor -- commonlib
    [4] Create New One
    [5] VM Scale Down Event Clear in SEH
    [6] terminate victim vm -- reuse exiting routine
    [7] move vscale vm from temp to running.
    """

    # [1] get victim vm object and validate victim vm
    victim_vm = get_VM_from_Running_VM_Instances(victim_vm_id)

    if victim_vm.is_vscale_victim is not True:
        g_log_handler.error("[Broker__] %s VScale Victime VM Error => Selected Victim VM (ID:%d, TY:%s, PR:$%s, ST:%s, VF:%s, VT:%s)" \
              % (str(Global_Curr_Sim_Clock), victim_vm.id, victim_vm.type_name, victim_vm.unit_price, simgval.get_sim_code_str(victim_vm.status), victim_vm.is_vscale_victim, victim_vm.vscale_victim_id))
        clib.sim_exit()

    # [2] create new vm for vscale and validate victim vm
    new_vscale_vm_id = create_new_VM_Instance (vscale_vm_type, victim_vm_id)
    new_vscale_vm = get_VM_from_Temp_VM_Instances (new_vscale_vm_id)

    if new_vscale_vm.is_vscale_victim is not False or new_vscale_vm.vscale_victim_id != victim_vm_id:
        g_log_handler.error("[Broker__] %s VScale VM Error => Selected VScale VM (ID:%d, TY:%s, PR:$%s, ST:%s, VF:%s, VT:%s)" \
              % (str(Global_Curr_Sim_Clock), new_vscale_vm.id, new_vscale_vm.type_name, new_vscale_vm.unit_price, simgval.get_sim_code_str(new_vscale_vm.status), new_vscale_vm.is_vscale_victim, new_vscale_vm.vscale_victim_id))
        clib.sim_exit()

    # [3] job migrate with cpu factor
    # Storage Scaling Debug Point - Done by 10.29.2014
    VM_Job_Migrate (victim_vm, new_vscale_vm, vscale_vm_type)

    # [4] Terminated Victim VM
    terminate_VM (victim_vm, term_trigger_clock)

    # [5] Move New One from Temp to Running
    if remove_VM_from_Temp_VM_Instances (new_vscale_vm.id) == False:
        g_log_handler.info("[Broker__] %s Remove VScale VM Error! => Remove VM (ID:%s, ST:%s) from g_Temp_VM_Instances Error!" \
              % (str(Global_Curr_Sim_Clock), victim_vm.id, simgval.get_sim_code_str(victim_vm.status)))
        clib.logwrite_VM_Instance_Lists(g_log_handler, "[Broker__] " + str(Global_Curr_Sim_Clock), "g_Temp_VM_Instances", g_Temp_VM_Instances)
        clib.sim_exit()

    insert_VM_into_Running_VM_Instances (new_vscale_vm)

    # [6] Send VScale New one signal to IaaS --> this will trigger automatic job start after creating (including startup lag)
    send_VM_Create_Event_to_IaaS (new_vscale_vm)

    # [7] SD Event Clear
    send_sd_tmr_clear_event_to_SEH (victim_vm, True)

def broker_evt_exp_timer_processing (q_msg):
    evt_id          = q_msg[0]
    evt_code        = q_msg[4]
    evt_sub_code    = q_msg[5] # EVT_SUB_CODE

    if evt_code != simgval.gEVT_EXP_TIMER:
        g_log_handler.error("[Broker__] %s broker_evt_exp_timer_processing EVT_CODE (%s) Error! => %s" \
              %(str(Global_Curr_Sim_Clock), simgval.get_sim_code_str(evt_code), q_msg))
        clib.sim_exit()

    if evt_sub_code == simgval.gEVT_SUB_VM_SCALE_DOWN:

        # evt sub code processing (gEVT_SUB_VM_SCALE_DOWN)
        broker_evt_sub_vm_scale_down_processing (q_msg)

    elif evt_sub_code == simgval.gEVT_SUB_WRITE_SIM_TRACE_BROKER:

        # evt sub code processing (gEVT_SUB_WRITE_SIM_TRACE_BROKER)
        broker_evt_sub_write_sim_trace (q_msg)

    elif evt_sub_code == simgval.gEVT_SUB_TERMINATE_VSCALE_VICTIM:

        # evt sub code processing (gEVT_SUB_TERMINATE_VSCALE_VICTIM)
        broker_evt_sub_terminate_vscale_victim (q_msg)

    else:

        g_log_handler.error("[Broker__] %s EVT_SUB_CODE (%s) Error! => %s" % (str(Global_Curr_Sim_Clock), simgval.get_sim_code_str(evt_sub_code), q_msg))
        clib.sim_exit()

    # step final --> send ack to event handler.
    broker_send_evt_ack(evt_id, evt_code)

    return

# gEVT_WORK_GEN processing
def broker_evt_work_gen_processing (q_msg):

    evt_id          = q_msg[0] # EVT_ID
    evt_code        = q_msg[4] # EVT_CODE
    evt_sub_code    = q_msg[5] # EVT_SUB_CODE
    evt_data        = q_msg[6] # EVT_DATA

    if evt_code != simgval.gEVT_WORK_GEN:

        g_log_handler.error("[Broker__] %s broker_evt_work_gen_processing EVT_CODE (%s) Error! => %s" \
              % (str(Global_Curr_Sim_Clock), simgval.get_sim_code_str(evt_code), q_msg))
        clib.sim_exit()
        return

    print "[Broker__] %s EVT_PROC (%s): JobName:%s, JobDuration:%d, JobDeadline:%d" % (str(Global_Curr_Sim_Clock), simgval.get_sim_code_str(evt_code), evt_data[0], evt_data[1], evt_data[2])
    evt_real_src = evt_sub_code

    if evt_real_src != simgval.gBLOCK_WORKLOAD_GEN:

        g_log_handler.error("[Broker__] %s EVT_REAL_SRC (%d) ERROR! => %s" % (str(Global_Curr_Sim_Clock), evt_real_src, q_msg))
        clib.sim_exit()
        return

    # step 0: insert job arrival time
    insert_job_arrival_rate (Global_Curr_Sim_Clock)

    # step1 : create job object

    job = create_Job_Object (evt_data)
    g_job_recv_log_handler.info ('%s\t%d\t%s\t%d\t%d\t%s\t\t%d\t%s\t\t%d\t\t%s'
                                 % (str(Global_Curr_Sim_Clock), job.id, job.name, job.std_duration, job.deadline, simgval.get_sim_code_str(job.input_file_flow_direction), job.input_file_size, simgval.get_sim_code_str(job.output_file_flow_direction), job.output_file_size, job.std_job_failure_time))

    assign_result, vm_id = assign_job (job)
    vm = get_VM_from_Running_VM_Instances(vm_id)

    job_str = simobj.get_job_info_str(job)
    if vm is None:
        g_log_handler.error("[Broker__] %s VM (ID:%d) not found for %s Assignment!" % (str(Global_Curr_Sim_Clock), vm_id, job_str))
        clib.logwrite_VM_Instance_Lists (g_log_handler, "[Broker__] " + str(Global_Curr_Sim_Clock), "Current g_Running_VM_Instances", g_Running_VM_Instances)
        clib.sim_exit()

    # example of assign_results
    # 1) [simgval.gJOB_ASSIGN_TO_NEW_VM, VM OBJ] ==> "New VM (VM.id VM.type_name) and the job is assigned to the VM (the job is added to the queue of the new VM)"
    #       ==> [a] Append the VM to VM List
    #       ==> [b] Send Bypass Event to IaaS to create the VM
    #       ==> [c] send EVT_ACK to sim event handler
    #
    # 2) [simgval.gJOB_ASSIGN_TO_EXISTING_VM, VM OBJ] ==> "the job is assigned to existing VM (VM OBJ) (the job is added to the queue of the existing VM)"
    #       ==> [a] just send EVT_ACK to sim event handler
    #
    # 3) [simgval.gJOB_ASSIGN_ERROR:, -99] ==> Error Case -> Discard the Job
    #       ==> [a] just send EVT_ACK to sim event handler
    #

    #if assign_result != "JOB_ASSIGN_ERROR":
    if assign_result != simgval.gJOB_ASSIGN_ERROR:
        insert_job_into_ReceivedJobList (job)

    """
    clib.display_JobList (Global_Curr_Sim_Clock, "cloud_broker.Received_Jobs", g_Received_Jobs)
    clib.display_job_obj(Global_Curr_Sim_Clock, g_Received_Jobs[0])
    clib.display_VM_Instances_List (Global_Curr_Sim_Clock, "cloud_broker.RunningVMs",g_Running_VM_Instances)
    """

    # step2 : (OPTIONAL) send bypass event to event handler (iaas clouds) if create case
    if assign_result == simgval.gJOB_ASSIGN_TO_NEW_VM:

        print "[Broker__] %s New VM (%d,%s) Created, %s Assigned" % (str(Global_Curr_Sim_Clock), vm.id, vm.instance_id, simobj.get_job_info_str(job))
        send_VM_Create_Event_to_IaaS (vm)

    elif assign_result == simgval.gJOB_ASSIGN_TO_EXISTING_VM:

        print "[Broker__] %s %s Assigned to Existing VM (%d)" % (str(Global_Curr_Sim_Clock), simobj.get_job_info_str(job), vm_id)
        if vm.current_job is None and vm.status == simgval.gVM_ST_ACTIVE and len(vm.job_queue) == 1:
            send_job_start_evt_to_iaas (vm)

    else:

        g_log_handler.error("[Broker__] %s %s Assign Error!" % (str(Global_Curr_Sim_Clock), job_str))
        clib.sim_exit()

    # step 2.5: update g_job_log
    set_g_job_logs (1,1,0,0)

    # step3 : send ack event -> in order to clear so event list
    broker_send_evt_ack (evt_id, evt_code)

    return

# gEVT_NOTI_VM_ACTIVATED processing
def broker_evt_noti_vm_activated_processing (q_msg):

    evt_id       = q_msg[0]     # EVT_ID
    evt_code     = q_msg[4]     # EVT_CODE
    evt_sub_code = q_msg[5]     # EVT_SUB_CODE (Real Src)
    evt_data     = q_msg[6]     # EVT_DATA

    if evt_code != simgval.gEVT_NOTI_VM_ACTIVATED:
        g_log_handler.error("[Broker__] %s broker_evt_noti_vm_activated_processing EVT_CODE (%s) Error! => %s" \
              %(str(Global_Curr_Sim_Clock), simgval.get_sim_code_str(evt_code), q_msg))
        clib.sim_exit()

    evt_real_src = evt_sub_code
    if evt_real_src != simgval.gBLOCK_IAAS:
        g_log_handler.error("[Broker__] %s EVT_REAL_SRC (%d) ERROR! => %s" % (str(Global_Curr_Sim_Clock), evt_real_src, q_msg))
        clib.sim_exit()

    # receive notification of that vm is activated
    # procedure
    # [a] append startup lag time to g_Startup_Lags
    # [b] send BYPASS EVT (gEVT_START_JOB) to Evt Handler (AD: IaaS)
    # [c] send EVT_ACK to Evt handler

    vm_obj = evt_data[0]

    # step 1 : append startup lag into g_Startup_Lags
    insert_LagTime_into_StartupLagList (vm_obj.startup_lag)

    # step 3 : send BYPASS EVT to IaaS to start the first job in the queue.
    send_job_start_evt_to_iaas (vm_obj)

    # step 4 : send ack to event handler
    broker_send_evt_ack (evt_id, evt_code)

    return

def after_job_completion_processing (cvm_id, force_clear_tmr_evt):

    # this lib got spinned off from broker_evt_job_completed_success_processing - 11/04/2014

    """
    TERM:
    1. Hscale: Horizontal Scaling (e.g. Scale-out/in)
    2. Vscale: Vertical SCaling (e.g. Scale-up/down)
    3. MAX-CAP: the maximum number of VM that the simulation case uses (e.g. if max-cap is 10 -- which means the simulation case can add 10 vms at most)
    4. SEH: Sim Event Handler

    Vscale policies
    1. Vscale can be used for all pricing model -- but, most suitable pricing model is 1-min based pricing model
    2. Vscale will work when the current number of vms == maxcap of vms
    3. Vscale will run when a job is completed && further job(s) exists

    Vscale activation condition
    if ([1] the number number of current vm == max cap of vms) && ([2] a job is completed and further job(s) are in the queue)

    Problem of current Vscale mechanism
    1. Vscale can be executed only if a job is completion -- it does nott support vertical scaling when a job is running.
    2. Hscale always has a higer priority than Vscale

    -- especially for no.2: it can be another paper (e.g. Hscale first vs Vscale first) -- See VScle Docx.

    Procedure of step 7 including (vertical scaling)
        [1] get vm object -- to determine the next step
            [a] no further job --> DO ORIGINAL CODE
                - Hscale-down activate
            [b] further job(s) exists
                1) if vscale is disabled --> DO ORIGINAL CODE
                    - send the next job to VM
                    - Hscale-down activate
                2) if vscale is enabled
                    a) if max-cap < current # of VMs: DO ORIGINAL
                    b) if max-cap > current # of VMs: Error!
                    c) if max-cap == current # of VMS: Do Vscale_check()
                        find a proper vm type based on VM seleciton policy regardless of job deadline satisfaction.
                            - why?
                            even though all jobs can satisfy their job deadline, current VM type might not be the most proper type based on VM selection policy.
                        1] if current VM type == new VM type for Vscale:
                            DO ORIGINAL
                        2] if current VM type != new VM type for Vscale:
                            DO Vscale():

    [1] Send a message (containing Vscale trigger) to SEH
    [2] SEH - All Timer Event Clear (* HScale Down is the most important one)
    [3] SEH - register 1sec timer for VM HScale Down due to VScale
    [4] Broker - Migrate "all jobs in the queue" to "Vscale_job_repository" (e.g. [past vm_id, job1, job2, ..., jobn]
    [5] SEH - HScale Down triggered due to timer event from SEH
    [6] Broker - HScale Down Current VM && Send a message to SEH to announce termination of the VM
    [7] -- do vm create proceduer
    """

    # 7) Get Job Object for the next decision...
    vm_obj = get_VM_from_Running_VM_Instances(cvm_id)
    if vm_obj is None:
        g_log_handler.error("[Broker__] %s VM (ID:%d) not found!" % (str(Global_Curr_Sim_Clock), cvm_id))
        clib.logwrite_VM_Instance_Lists (g_log_handler, "[Broker__] " + str(Global_Curr_Sim_Clock), "Current g_Running_VM_Instances", g_Running_VM_Instances)
        clib.sim_exit()

    trigger_Vscale, vscale_vm_type, vscale_case = VScale_Trigger_Condition_Check(vm_obj)
    if trigger_Vscale is True:

        g_log_handler.info("[Broker__] %s Trigger VScale - Current %s will be updated to new VM Type - %s" % (str(Global_Curr_Sim_Clock), vm_obj.get_str_info(), vscale_vm_type.get_infos()))

        # is_vsale_victim should bet set to True
        simobj.set_VM_is_vscale_victim (vm_obj, True)

        # update vscale_case
        simobj.set_VM_vscale_case(vm_obj, vscale_case)

        # for exact info for scale down
        # ===================================================
        update_VM_sd_wait_time (vm_obj.id, 0)
        simobj.set_VM_sd_policy_activated_flag (vm_obj, True)
        # ===================================================

        q_tmr_evt_data = [vm_obj.id, Global_Curr_Sim_Clock, vscale_vm_type]
        q_tmr_evt = clib.make_queue_event (1,                                           # VScale victim will be terminated after 1sec.
                                           g_my_block_id,                               # src block id (will be returned to src block)
                                           simgval.gBLOCK_SIM_EVENT_HANDLER,            # dst block id (event handler block)
                                           simgval.gEVT_SET_TIMER,                      # evt code: set timer
                                           simgval.gEVT_SUB_TERMINATE_VSCALE_VICTIM,              # evt sub code: vm_scale_down
                                           q_tmr_evt_data)

        gQ_OUT.put (q_tmr_evt)

    else:

        # this is original code for non vscale

        # 7.1) Send the Next Job to VM -- add second condition (vm_obj.current_job is not None -- on 11/12/2014) due to job failure recover policy #2
        if len(vm_obj.job_queue) > 0 and vm_obj.current_job is None:
            send_job_start_evt_to_iaas (vm_obj)

        # 7.5) Adjust Scale Down Time

        # force_clear_tmr_evt is true when job failure case... on 11/04/2014
        if (use_sd_policy_activated_flag() == True and vm_obj.sd_policy_activated == True) or (force_clear_tmr_evt == True):

            # this is a case that a job was arrived and completed after jat-based scaling down triggerd.
            # In this case, broker send a bypass event to clear previous scale down timer event
            # and re-trigger new scale-down event

            send_sd_tmr_clear_event_to_SEH (vm_obj, force_clear_tmr_evt)

            # SEH will send clear success message to broker
            # then broker should do
            #           [1] set vm sd policy activated flag to False
            #           [2] Re-trigger VM scale down
            # this is processed by broker_evt_sucess_clear_sd_tmr_event


def broker_evt_job_completed_success_processing (q_msg):

    evt_id       = q_msg[0]     # EVT_ID
    evt_code     = q_msg[4]     # EVT_CODE
    evt_sub_code = q_msg[5]     # EVT_SUB_CODE (REAL SRC)
    evt_data     = q_msg[6]     # EVT_DATA

    if evt_code != simgval.gEVT_JOB_COMPLETED_SUCCESS:
        g_log_handler.error("[Broker__] %s broker_evt_job_completed_success_processing EVT_CODE (%s) Error! => %s" \
              %(str(Global_Curr_Sim_Clock), simgval.get_sim_code_str(evt_code), q_msg))
        clib.sim_exit()

    evt_real_src = evt_sub_code
    if evt_real_src != simgval.gBLOCK_IAAS:
        g_log_handler.error("[Broker__] %s EVT_REAL_SRC (%d) ERROR! => %s" % (str(Global_Curr_Sim_Clock), evt_real_src, q_msg))
        clib.sim_exit()

    cjob_id = evt_data[0]
    cvm_id = evt_data[1]

    # Procedure
    # 0-a) Find VM from g_Running_VM_Instances based on cvm_id
    # 0-b) check current job status
    # 0-c) insert the current_job into Job History
    # 0-d) update VM' current job to None
    # 0-d) update VM's last job completion time
    # 1) Find Job from g_Received_Jobs based on cjob_id
    # 2) Check Job Status and other values (time)
    # 4) Check VM Status and other values (current job, job history)
    # 5) move the job from g_Received_Jobs to g_Completed JObs
    # 6) write job complete log -- id, name, duration, deadline, vm_id, time_create, time_assign, time_start, time_complete, total_duration, running_time, deadline_match
    # 7) Send Next Job to VM if exists
    # 8) Send Ack

    # 0-a) Find VM from g_Running_VM_Instances based on cvm_id
    vm = get_VM_from_Running_VM_Instances (cvm_id)
    if vm is None:
        g_log_handler.error ("[Broker__] %s broker_evt_job_completed_success_processing Error! - Cannot find VM (ID: %d) from g_Running_VM_Instances(LEN:%d)" % (str(Global_Curr_Sim_Clock), cvm_id, len(g_Running_VM_Instances)))
        clib.logwrite_VM_Instance_Lists (g_log_handler, "[Broker__] " + str(Global_Curr_Sim_Clock), "Current g_Running_VM_Instances", g_Running_VM_Instances)
        clib.sim_exit()

    # 0-b) check current job status
    if vm.current_job.status != simgval.gJOB_ST_COMPLETED:
        g_log_handler.error("[Broker__] %s broker_evt_job_completed_success_processing Error! - VM (ID:%d)'s Current Job [%s] Status has to be %s"  % (str(Global_Curr_Sim_Clock), vm.id, simobj.get_job_info_str(vm.current_job), simgval.get_sim_code_str(simgval.gJOB_ST_COMPLETED)))
        clib.sim_exit()

    # 0-c) insert the current_job into Job History
    simobj.insert_job_into_VM_job_history (vm, vm.current_job)

    # [f] update VM' current job to None
    upd_res = simobj.set_VM_current_job (Global_Curr_Sim_Clock, vm, None)
    if upd_res is False:
        g_log_handler.error("[Broker__] %s broker_evt_job_completed_success_processing Update VM Current Job (%s) Error!" % (str(Global_Curr_Sim_Clock), str(vm.current_job)))
        clib.sim_exit()

    # 1) Find Job from g_Received_Jobs based on cjob_id
    job = get_job_from_ReceivedJobList (cjob_id, cvm_id)

    if job is None:
        g_log_handler.error("[Broker__] %s broker_evt_job_completed_success_processing Error! - Cannot find the job(ID: %d) from g_Received_Jobs(LEN:%d)" % (str(Global_Curr_Sim_Clock), cjob_id, len(g_Received_Jobs)))
        clib.logwrite_JobList (g_log_handler, "[Broker__] " + str(Global_Curr_Sim_Clock), "Current g_Received_Jobs", g_Received_Jobs)
        clib.sim_exit()

    # 2) Check Job Status and other values (time)
    if job.status != simgval.gJOB_ST_COMPLETED \
        or job.time_assign is None \
        or job.time_start is None \
        or job.time_complete is None:

        g_log_handler.error("[Broker__] %s broker_evt_job_completed_success_processing Error! - Invalid Job (ID:%d, ST:%s, TA:%s, TS:%s, TC:%s)" \
              % (str(Global_Curr_Sim_Clock), job.id, simgval.get_sim_code_str(job.status), str(job.time_assign), str(job.time_start), str(job.time_complete)))
        clib.sim_exit()

    # 4) Check VM Status and other values (current job, job history)
    if vm.status != simgval.gVM_ST_ACTIVE \
        or vm.current_job is not None \
        or vm.job_history[-1].id != job.id:

        g_log_handler.error("[Broker__] %s broker_evt_job_completed_success_processing Error! - Invalid VM (ID:%d, IID:%s, ST:%s, CJ:%s, JHL:%d, JH[-1]:%d, JID:%d)" \
              % (str(Global_Curr_Sim_Clock), vm.id, vm.instance_id, simgval.get_sim_code_str(vm.status), str(vm.current_job), len(vm.job_history), vm.job_history[-1].id, job.id))

        clib.display_vm_obj(Global_Curr_Sim_Clock, vm)
        clib.sim_exit()

    # 5) move the job from g_Received_Jobs to g_Completed JObs
    insert_job_into_CompletedJobList (job)

    # 6) write job complete log -- id, name, duration, deadline, vm_id, time_create, time_assign, time_start, time_complete, total_duration, running_time, deadline_match
    write_job_complete_log (job)
    print "[Broker__] %s Job Completed - ID:%d, JN:%s, ADR:%d (CPU:%s, IO:%s), DL:%s, TD:%d, VM_ID:%s,%s, VM_TYPE:%s,%s" \
              % (str(Global_Curr_Sim_Clock), job.id, job.name, job.act_duration_on_VM, job.act_cputime_on_VM, job.act_nettime_on_VM, job.deadline, (job.time_complete-job.time_create), job.VM_id, vm.id, job.VM_type, vm.type_name)

    # 6.5) Set g_job_log for job_completion
    set_g_job_logs(0, 0, 1, 1)

    # vscale trigger, scale-down and next job processing
    after_job_completion_processing (cvm_id, False) # second parameter should be False for normal job completion

    # 8) Send Ack
    broker_send_evt_ack (evt_id, evt_code)

def broker_evt_job_completed_failure_processing (q_msg):

    evt_id       = q_msg[0]     # EVT_ID
    evt_code     = q_msg[4]     # EVT_CODE
    evt_sub_code = q_msg[5]     # EVT_SUB_CODE (REAL SRC)
    evt_data     = q_msg[6]     # EVT_DATA

    if evt_code != simgval.gEVT_JOB_COMPLETED_FAILURE:
        g_log_handler.error("[Broker__] %s broker_evt_job_completed_failure_processing EVT_CODE (%s) Error! => %s" \
              %(str(Global_Curr_Sim_Clock), simgval.get_sim_code_str(evt_code), q_msg))
        clib.sim_exit()

    evt_real_src = evt_sub_code
    if evt_real_src != simgval.gBLOCK_IAAS:
        g_log_handler.error("[Broker__] %s EVT_REAL_SRC (%d) ERROR! => %s" % (str(Global_Curr_Sim_Clock), evt_real_src, q_msg))
        clib.sim_exit()

    cjob_id = evt_data[0]
    cvm_id = evt_data[1]

    # 0-a) Find VM from g_Running_VM_Instances based on cvm_id
    vm = get_VM_from_Running_VM_Instances (cvm_id)

    if vm is None:
        g_log_handler.error("[Broker__] %s broker_evt_job_completed_failure_processing (%s) Error! - Cannot find VM (ID: %d) from g_Running_VM_Instances(LEN:%d)" % (str(Global_Curr_Sim_Clock), simgval.get_sim_code_str(g_sim_conf.job_failure_policy), cvm_id, len(g_Running_VM_Instances)))
        clib.logwrite_VM_Instance_Lists (g_log_handler, "[Broker__] " + str(Global_Curr_Sim_Clock), "Current g_Running_VM_Instances", g_Running_VM_Instances)
        clib.sim_exit()

    # 0-b) check current job status
    if vm.current_job.status != simgval.gJOB_ST_FAILED:
        g_log_handler.error("[Broker__] %s broker_evt_job_completed_failure_processing (%s) Error! - VM (ID:%d)'s Current Job [%s] Status has to be %s" \
              % (str(Global_Curr_Sim_Clock), simgval.get_sim_code_str(g_sim_conf.job_failure_policy), vm.id, simobj.get_job_info_str(vm.current_job), simgval.get_sim_code_str(simgval.gJOB_ST_COMPLETED)))
        clib.sim_exit()

    if g_sim_conf.job_failure_policy == simgval.gJFP1_IGNORE_JOB:

        # 0-c) insert the current_job into Job History
        simobj.insert_job_into_VM_job_history (vm, vm.current_job)

        # [f] update VM' current job to None
        upd_res = simobj.set_VM_current_job (Global_Curr_Sim_Clock, vm, None)
        if upd_res is False:
            g_log_handler.error("[Broker__] %s broker_evt_job_completed_failure_processing (%s) Update VM Current Job (%s) Error!" \
                  % (str(Global_Curr_Sim_Clock), simgval.get_sim_code_str(g_sim_conf.job_failure_policy), str(vm.current_job)))
            clib.sim_exit()

        # 1) Find Job from g_Received_Jobs based on cjob_id
        job = get_job_from_ReceivedJobList (cjob_id, cvm_id)

        if job is None:
            g_log_handler.error("[Broker__] %s broker_evt_job_completed_failure_processing (%s) Error! - Cannot find the job(ID: %d) from g_Received_Jobs(LEN:%d)" \
                  % (str(Global_Curr_Sim_Clock), simgval.get_sim_code_str(g_sim_conf.job_failure_policy), cjob_id, len(g_Received_Jobs)))
            clib.logwrite_JobList (g_log_handler, "[Broker__] " + str(Global_Curr_Sim_Clock), "Current g_Received_Jobs", g_Received_Jobs)
            clib.sim_exit()

        # 2) Check Job Status and other values (time)
        if job.status != simgval.gJOB_ST_FAILED \
            or job.time_assign is None \
            or job.time_start is None \
            or job.time_complete is None:

            g_log_handler.error("[Broker__] %s broker_evt_job_completed_failure_processing (%s) Error! - Invalid Job (ID:%d, ST:%s, TA:%s, TS:%s, TC:%s)" \
                  % (str(Global_Curr_Sim_Clock), simgval.get_sim_code_str(g_sim_conf.job_failure_policy), job.id, simgval.get_sim_code_str(job.status), str(job.time_assign), str(job.time_start), str(job.time_complete)))
            clib.sim_exit()

        # 4) Check VM Status and other values (current job, job history)
        if vm.status != simgval.gVM_ST_ACTIVE \
            or vm.current_job is not None \
            or vm.job_history[-1].id != job.id:

            g_log_handler.error("[Broker__] %s broker_evt_job_completed_failure_processing (%s) Error! - Invalid VM (ID:%d, IID:%s, ST:%s, CJ:%s, JHL:%d, JH[-1]:%d, JID:%d)" \
                  % (str(Global_Curr_Sim_Clock), simgval.get_sim_code_str(g_sim_conf.job_failure_policy),vm.id, vm.instance_id, simgval.get_sim_code_str(vm.status), str(vm.current_job), len(vm.job_history), vm.job_history[-1].id, job.id))
            clib.display_vm_obj(Global_Curr_Sim_Clock, vm)
            clib.sim_exit()

        # update job info related to job failure - on 11/05/2014
        # [a] job_failure_recovered ==> True
        # [b] update job_actual duration
        simobj.set_Job_job_failure_recovered (job, True)
        simobj.set_Job_act_duration_on_VM (job, get_update_job_duration_for_failure_case (job), 0)

        # move job to g_Complete_list
        insert_job_into_CompletedJobList (job)

        # 6) write job complete log -- id, name, duration, deadline, vm_id, time_create, time_assign, time_start, time_complete, total_duration, running_time, deadline_match
        write_job_complete_log (job)
        print "[Broker__] %s Job Completed - ID:%d, JN:%s, ADR:%d (CPU:%s, IO:%s), DL:%s, TD:%d, VM_ID:%s,%s, VM_TYPE:%s,%s" \
                  % (str(Global_Curr_Sim_Clock), job.id, job.name, job.act_duration_on_VM, job.act_cputime_on_VM, job.act_nettime_on_VM, job.deadline, (job.time_complete-job.time_create), job.VM_id, vm.id, job.VM_type, vm.type_name)

        # 6.5) Set g_job_log for job_completion
        set_g_job_logs(0, 0, 1, 1)

    elif g_sim_conf.job_failure_policy == simgval.gJFP2_RE_EXECUTE_JOB:

        job = get_job_from_ReceivedJobList_by_JobID (cjob_id)
        if job is None:
            g_log_handler.error("[Broker__] %s broker_evt_job_completed_failure_processing (%s) Error! - Cannot find the job(ID: %d) from g_Received_Jobs(LEN:%d)" \
                  % (str(Global_Curr_Sim_Clock), simgval.get_sim_code_str(g_sim_conf.job_failure_policy), cjob_id, len(g_Received_Jobs)))
            clib.logwrite_JobList (g_log_handler, "[Broker__] " + str(Global_Curr_Sim_Clock), "Current g_Received_Jobs", g_Received_Jobs)
            clib.sim_exit()

        # 2) Check Job Status and other values (time)
        if job.status != simgval.gJOB_ST_FAILED \
            or job.time_assign is None \
            or job.time_start is None \
            or job.time_complete is None:

            g_log_handler.error("[Broker__] %s broker_evt_job_completed_failure_processing (%s) Error! - Invalid Job (ID:%d, ST:%s, TA:%s, TS:%s, TC:%s)" \
                  % (str(Global_Curr_Sim_Clock), simgval.get_sim_code_str(g_sim_conf.job_failure_policy), job.id, simgval.get_sim_code_str(job.status), str(job.time_assign), str(job.time_start), str(job.time_complete)))
            clib.sim_exit()

        # set failure recover is true
        simobj.set_Job_job_failure_recovered (job, True)
        # set current job start time and last job complete time
        simobj.set_VM_job_clock (vm, Global_Curr_Sim_Clock, -1)

        # re-insert job into the VM && restore original job assignment -- due to insert_job_into_VM_Job_Queue update job assign time to current
        org_job_assign_time = job.time_assign
        simobj.set_Job_time_assign (job, org_job_assign_time)
        send_job_restart_evt_to_iaas (vm, cjob_id)
        print "[Broker__] %s Job Restarted! - ID:%d, JN:%s, ADR:%d (CPU:%s, IO:%s), DL:%s, TD:%d, VM_ID:%s,%s, VM_TYPE:%s,%s" \
                  % (str(Global_Curr_Sim_Clock), job.id, job.name, job.act_duration_on_VM, job.act_cputime_on_VM, job.act_nettime_on_VM, job.deadline, (job.time_complete-job.time_create), job.VM_id, vm.id, job.VM_type, vm.type_name)

    elif g_sim_conf.job_failure_policy == simgval.gJFP3_MOVE_JOB_TO_THE_END_OF_QUEUE:

        # [f] update VM' current job to None
        upd_res = simobj.set_VM_current_job (Global_Curr_Sim_Clock, vm, None)

        if upd_res is False:
            g_log_handler.error("[Broker__] %s broker_evt_job_completed_failure_processing (%s) - Update VM Current Job (%s) Error!" \
                  % (str(Global_Curr_Sim_Clock), simgval.get_sim_code_str(g_sim_conf.job_failure_policy), str(vm.current_job)))
            clib.sim_exit()

        # get job by using get_job_from_ReceivedJobList_by_JobID
        # this is different than other processing routine (e.g. Job Failure Policy #1)
        # this is because the failed to will be re-inserted into VM's job queue -- the job should stay in the g_Received_Jobs
        job = get_job_from_ReceivedJobList_by_JobID (cjob_id)

        if job is None:
            g_log_handler.error("[Broker__] %s broker_evt_job_completed_failure_processing (%s) Error! - Cannot find the job(ID: %d) from g_Received_Jobs(LEN:%d)" \
                  % (str(Global_Curr_Sim_Clock), simgval.get_sim_code_str(g_sim_conf.job_failure_policy), cjob_id, len(g_Received_Jobs)))
            clib.logwrite_JobList (g_log_handler, "[Broker__] " + str(Global_Curr_Sim_Clock), "Current g_Received_Jobs", g_Received_Jobs)
            clib.sim_exit()

        # 2) Check Job Status and other values (time)
        if job.status != simgval.gJOB_ST_FAILED \
            or job.time_assign is None \
            or job.time_start is None \
            or job.time_complete is None:

            g_log_handler.error("[Broker__] %s broker_evt_job_completed_failure_processing (%s) Error! - Invalid Job (ID:%d, ST:%s, TA:%s, TS:%s, TC:%s)" \
                  % (str(Global_Curr_Sim_Clock), simgval.get_sim_code_str(g_sim_conf.job_failure_policy), job.id, simgval.get_sim_code_str(job.status), str(job.time_assign), str(job.time_start), str(job.time_complete)))
            clib.sim_exit()

        # check vm status #1: (st == active and current job == none)
        if vm.status != simgval.gVM_ST_ACTIVE or vm.current_job is not None:
            g_log_handler.error("[Broker__] %s broker_evt_job_completed_failure_processing (%s) Error! - Invalid VM (ID:%d, IID:%s, ST:%s, CJ:%s, JHL:%d, JH[-1]:%d, JID:%d)" \
                  % (str(Global_Curr_Sim_Clock), simgval.get_sim_code_str(g_sim_conf.job_failure_policy), vm.id, vm.instance_id, simgval.get_sim_code_str(vm.status), str(vm.current_job), len(vm.job_history), vm.job_history[-1].id, job.id))
            clib.display_vm_obj(Global_Curr_Sim_Clock, vm)
            clib.sim_exit()

        # check vm status #2: this is different from other cases for job complete or failure processing
        #                   : vm.job_history[-1].id should be different from job.id --> the (failed) job will be reinserted into VM's job queue
        if len(vm.job_history) > 0 and vm.job_history[-1].id == job.id:
            g_log_handler.error("[Broker__] %s broker_evt_job_completed_failure_processing (%s) Error! - Invalid VM (ID:%d, IID:%s, ST:%s, CJ:%s, JHL:%d, JH[-1]:%d, JID:%d)" \
                  % (str(Global_Curr_Sim_Clock), simgval.get_sim_code_str(g_sim_conf.job_failure_policy), vm.id, vm.instance_id, simgval.get_sim_code_str(vm.status), str(vm.current_job), len(vm.job_history), vm.job_history[-1].id, job.id))
            clib.display_vm_obj(Global_Curr_Sim_Clock, vm)
            clib.sim_exit()

        # update job info related to job failure - on 11/05/2014
        # [a] job_failure_recovered ==> True
        # [b] job_status should be ==> gJOB_ST_REASSIGNED
        simobj.set_Job_job_failure_recovered (job, True)

        # re-insert job into the VM && restore original job assignment -- due to insert_job_into_VM_Job_Queue update job assign time to current
        org_job_assign_time = job.time_assign
        insert_job_into_VM_Job_Queue (vm.id, job)
        simobj.set_Job_time_assign (job, org_job_assign_time)

        print "[Broker__] %s Job Reassigned! - ID:%d, JN:%s, ADR:%d (CPU:%s, IO:%s), DL:%s, TD:%d, VM_ID:%s,%s, VM_TYPE:%s,%s" \
                  % (str(Global_Curr_Sim_Clock), job.id, job.name, job.act_duration_on_VM, job.act_cputime_on_VM, job.act_nettime_on_VM, job.deadline, (job.time_complete-job.time_create), job.VM_id, vm.id, job.VM_type, vm.type_name)

    else:

        print "need to implement - %s, %s" % (simgval.get_sim_code_str(g_sim_conf.job_failure_policy), simgval.get_sim_code_str(g_sim_conf.job_failure_policy))
        clib.sim_exit()

    if g_sim_conf.vm_scale_down_policy == simgval.gPOLICY_VM_SCALEDOWN_HOUR or g_sim_conf.vm_scale_down_policy == simgval.gPOLICY_VM_SCALEDOWN_MIN:
        force_clear_tmr_evt = False
    else:
        force_clear_tmr_evt = True

    # vscale trigger, scale-down and next job processing
    after_job_completion_processing (cvm_id, force_clear_tmr_evt) # second parameter should be true for job failure case

    # 8) Send Ack
    broker_send_evt_ack (evt_id, evt_code)

def broker_evt_success_clear_sd_tmr_event (q_msg):

    # this lib is related to a special case for broker_evt_job_completed_success_processing

    evt_id      = q_msg[0]      # EVT_ID
    evt_src     = q_msg[2]      # EVT_SRC
    evt_code    = q_msg[4]      # EVT_CODE
    evt_data    = q_msg[6]      # EVT_DATA

    if evt_code != simgval.gEVT_SUCCESS_CLEAR_SD_TMR_EVENT:
        g_log_handler.error("[Broker__] %s EVT_CODE (%s) Error! => %s" % (str(Global_Curr_Sim_Clock), simgval.get_sim_code_str(evt_code), q_msg))
        clib.sim_exit()

    if evt_src != simgval.gBLOCK_SIM_EVENT_HANDLER:
        g_log_handler.error("[Broker__] %s EVT_SRC (%d) ERROR! => %s" % (str(Global_Curr_Sim_Clock), evt_src, q_msg))
        clib.sim_exit()

    vm_obj = evt_data[0]
    exception_flag = evt_data[1]

    # added for Vscale - 1017 2014
    if vm_obj.is_vscale_victim == False:

        # exception flag is used to handle a tricky case - e.g. job duration is changed in IaaS level due to I/O issue
        # added this on 10/27/2014 - for storage simulation.
        if use_sd_policy_activated_flag(exception_flag) != True:
            g_log_handler.error("[Broker__] %s EVT_CODE (%s) Error! => Invalid Access to this Lib - %s" \
                  %(str(Global_Curr_Sim_Clock), simgval.get_sim_code_str(evt_code), q_msg))
            clib.sim_exit()

        if vm_obj.sd_policy_activated != True:
            g_log_handler.error("[Broker__] %s EVT_CODE (%s) Error! => Invalid VM (ID: %d) Status - %s" \
                  %(str(Global_Curr_Sim_Clock), simgval.get_sim_code_str(evt_code), vm_obj.id, q_msg))
            clib.logwrite_vm_obj(g_log_handler, "[Broker__] " + str(Global_Curr_Sim_Clock), vm_obj)
            clib.sim_exit()

        # [1] set vm sd policy activated flag to False
        simobj.set_VM_sd_policy_activated_flag(vm_obj, False)

        # [2] Re-trigger VM scale down
        trigger_vm_scale_down(vm_obj.id)

    # send ack
    broker_send_evt_ack (evt_id, evt_code)

def broker_evt_debug_display_job_obj (q_msg):

    evt_id      = q_msg[0]      # EVT_ID
    evt_src     = q_msg[2]      # EVT_SRC
    evt_code    = q_msg[4]      # EVT_CODE
    evt_data    = q_msg[6]      # EVT_DATA

    if evt_code != simgval.gEVT_DEBUG_DISPLAY_JOB_OBJ:
        g_log_handler.error("[Broker__] %s EVT_CODE (%s) Error! => %s" % (str(Global_Curr_Sim_Clock), simgval.get_sim_code_str(evt_code), q_msg))
        clib.sim_exit()

    if evt_src != simgval.gBLOCK_SIM_EVENT_HANDLER:
        g_log_handler.error("[Broker__] %s EVT_SRC (%d) ERROR! => %s" % (str(Global_Curr_Sim_Clock), evt_src, q_msg))
        clib.sim_exit()

    job_id = evt_data[0]
    job_obj = get_job_from_ReceivedJobList_by_JobID (job_id)
    clib.display_job_obj(Global_Curr_Sim_Clock, job_obj)

    vm_obj = get_running_VM_by_VM_ID (job_obj.VM_id)
    clib.display_vm_obj(Global_Curr_Sim_Clock, vm_obj)

    # send ack
    broker_send_evt_ack (evt_id, evt_code)


def broker_evt_job_duration_changed (q_msg):

    evt_id      = q_msg[0]      # EVT_ID
    evt_src     = q_msg[2]      # EVT_SRC
    evt_code    = q_msg[4]      # EVT_CODE
    evt_data    = q_msg[6]      # EVT_DATA

    if evt_code != simgval.gEVT_JOB_DURATION_CHANGED:
        g_log_handler.error("[Broker__] %s EVT_CODE (%s) Error! => %s" % (str(Global_Curr_Sim_Clock), simgval.get_sim_code_str(evt_code), q_msg))
        clib.sim_exit()

    if evt_src != simgval.gBLOCK_SIM_EVENT_HANDLER:
        g_log_handler.error("[Broker__] %s EVT_SRC (%d) ERROR! => %s" % (str(Global_Curr_Sim_Clock), evt_src, q_msg))
        clib.sim_exit()

    job_id = evt_data[0]
    vm_obj = evt_data[1]

    if is_SD_affected_by_job_duration_change () == True:
        #simobj.set_VM_sd_policy_activated_flag (vm_obj, True)   # added on 10/27/2014 - especially for SL-based Scale Down
        send_sd_tmr_clear_event_to_SEH (vm_obj, True)

    # send ack
    broker_send_evt_ack (evt_id, evt_code)

def broker_event_processing (q_msg):

    evt_src         = q_msg[2]  # EVT_SRC
    evt_code        = q_msg[4]  # EVT_CODE
    evt_sub_code    = q_msg[5]  # EVT_SUB_CODE
    evt_data        = q_msg[6]  # EVT_DATA

    print "\n\n"
    g_log_handler.info("[Broker__] %s EVT_RECV - EC:%s, ESC:%s, SRC_BLOCK:%s" \
          % (str(Global_Curr_Sim_Clock), simgval.get_sim_code_str(evt_code), simgval.get_sim_code_str(evt_sub_code), simgval.get_sim_code_str(evt_src)))
    if evt_code == simgval.gEVT_EXP_TIMER:

        # timer expired event
        broker_evt_exp_timer_processing (q_msg)

    elif evt_code == simgval.gEVT_WORK_GEN:

        # work gen event processing
        broker_evt_work_gen_processing (q_msg)

    elif evt_code == simgval.gEVT_NOTI_VM_ACTIVATED:

        # noti vm activated event processing
        broker_evt_noti_vm_activated_processing (q_msg)

    elif evt_code == simgval.gEVT_JOB_COMPLETED_SUCCESS:

        # job completed success from iaas
        broker_evt_job_completed_success_processing (q_msg)

    elif evt_code == simgval.gEVT_JOB_COMPLETED_FAILURE:

        # job completed failure from iaas
        broker_evt_job_completed_failure_processing (q_msg)

    elif evt_code == simgval.gEVT_SUCCESS_CLEAR_SD_TMR_EVENT:

        # clear sd event success event
        broker_evt_success_clear_sd_tmr_event (q_msg)

    elif evt_code == simgval.gEVT_DEBUG_DISPLAY_JOB_OBJ:

        # debug event - display job and vm obj
        broker_evt_debug_display_job_obj (q_msg)

    elif evt_code == simgval.gEVT_JOB_DURATION_CHANGED:

        # this event receives when job duration has been changed in IaaS level
        broker_evt_job_duration_changed (q_msg)

    elif evt_code == simgval.gEVT_CONFIRM_SIMENTITY_TERMINATION or evt_code == simgval.gEVT_REJECT_SIMENTITY_TERMINATION:

        g_log_handler.info("[Broker__] %s EVT_PROC: EVT_CODE (%s) => Ignore Event, %s" % (str(Global_Curr_Sim_Clock), simgval.get_sim_code_str(evt_code), q_msg))

    else:

        # event code error!
        g_log_handler.error("[Broker__] %s EVT_PROC: EVT_CODE (%s) Error! => %s" % (str(Global_Curr_Sim_Clock), simgval.get_sim_code_str(evt_code), q_msg))
        clib.sim_exit()

def broker_term_event_processing (q_msg):

    evt_id          = q_msg[0]  # EVT_ID
    evt_src         = q_msg[2]  # EVT_SRC
    evt_code        = q_msg[4]  # EVT_CODE
    evt_sub_code    = q_msg[5]  # EVT_SUB_CODE
    evt_data        = q_msg[6]  # EVT_DATA

    if g_flag_sim_entity_term is not True:
        return False

    if evt_src != simgval.gBLOCK_SIM_EVENT_HANDLER:
        return False

    if evt_code != simgval.gEVT_CONFIRM_SIMENTITY_TERMINATION and evt_code != simgval.gEVT_REJECT_SIMENTITY_TERMINATION:
        return False

    print "\n\n"
    g_log_handler.info("[Broker__] %s EVT_RECV - EC:%s, ESC:%s, SRC_BLOCK:%s" \
          % (str(Global_Curr_Sim_Clock), simgval.get_sim_code_str(evt_code), simgval.get_sim_code_str(evt_sub_code), simgval.get_sim_code_str(evt_src)))
    ret_val = False
    if evt_code == simgval.gEVT_CONFIRM_SIMENTITY_TERMINATION:

        g_log_handler.info("[Broker__] %s Broker Terminated - TERM FLAG:%s, EVT:%s" \
              % (str(Global_Curr_Sim_Clock), str(g_flag_sim_entity_term), simgval.get_sim_code_str(evt_code)))

        # [a] send ack first
        broker_send_evt_ack (evt_id, evt_sub_code)

        # [b] send gEVT_NOTI_SIMENTIY_TERMINATED (common event code) to event handler
        term_evt = clib.make_queue_event (Global_Curr_Sim_Clock, g_my_block_id, simgval.gBLOCK_SIM_EVENT_HANDLER, simgval.gEVT_NOTI_SIMENTIY_TERMINATED, None, None)
        gQ_OUT.put (term_evt)

        # [c] return value is true
        ret_val = True

    elif evt_code == simgval.gEVT_REJECT_SIMENTITY_TERMINATION:

        set_broker_sim_entity_term_flag(False)
        broker_send_evt_ack (evt_id, evt_sub_code)

    return ret_val

def register_broker_sim_trace_event ():

    if g_flag_sim_entity_term == True:
        return

    clear_g_job_logs_unit ()

    tmr_evt = clib.make_queue_event (g_sim_conf.sim_trace_interval,                 # timer (vm_scale_down_policy_timer_unit)
                                       g_my_block_id,                               # src block id (will be returned to src block)
                                       simgval.gBLOCK_SIM_EVENT_HANDLER,            # dst block id (event handler block)
                                       simgval.gEVT_SET_TIMER,                      # evt code: set timer
                                       simgval.gEVT_SUB_WRITE_SIM_TRACE_BROKER,     # evt sub code: gEVT_SUB_WRITE_SIM_TRACE_BROKER
                                       None)                                        # evt data : None

    gQ_OUT.put (tmr_evt)

def update_curr_sim_clock (clock):
    global Global_Curr_Sim_Clock
    global Global_Curr_Sim_Clock_CV

    if Global_Curr_Sim_Clock == clock:
        return

    #update_curr_sim_clock
    Global_Curr_Sim_Clock_CV.acquire()
    Global_Curr_Sim_Clock = clock
    Global_Curr_Sim_Clock_CV.notify()
    Global_Curr_Sim_Clock_CV.release()

    #g_log_handler.info(str(Global_Curr_Sim_Clock) + '  Update Global_Curr_Sim_Clock: %s' % str(Global_Curr_Sim_Clock))
    return

def broker_generate_report ():

    print "[Broker__] %s Generate Report." % (str(Global_Curr_Sim_Clock))

    # Job Log
    sorted_complete_jobs = sorted (g_Completed_Jobs, key=lambda job: job.id, reverse=False)
    rep_job_comp_log = set_broker_job_report_log (g_sim_conf.report_path)
    rep_job_comp_log.info ('ID,JN,ADR,IN,CPU,OUT,DL,VM,TG,TA,TS,TC,TD,RT,DF,CO($),ST')

    for job in sorted_complete_jobs:

        total_duration = job.time_complete - job.time_create
        run_time = job.time_complete - job.time_start
        diff = job.deadline - total_duration

        vm_obj = get_VM_from_Stopped_VM_Instances (job.VM_id)
        jcost = round((run_time * vm_obj.unit_price / float (g_sim_conf.billing_unit_clock)), 5)
        rep_job_comp_log.info ('%d,%s,%d,%s,%s,%s,%d,%d,%d,%d,%d,%d,%d,%d,%d,%s,%s' %
                                     (job.id, job.name, job.act_duration_on_VM, job.act_nettime_on_VM[0], job.act_cputime_on_VM, job.act_nettime_on_VM[1], job.deadline, job.VM_id,
                                      job.time_create, job.time_assign, job.time_start, job.time_complete,
                                      total_duration, run_time, diff, jcost, simgval.get_sim_code_str(job.status)))

    # VM Log
    sorted_stopped_VMs = sorted (g_Stopped_VM_Instances, key=lambda vm: vm.id, reverse=False)
    rep_vm_usage_log = set_broker_vm_report_log (g_sim_conf.report_path)
    rep_vm_usage_log.info ('VMID,RT,CO($),IID,TY,ST,TC,TA,TT,SL,NJ,JR,UT,SR,ID,LJCT,SDWT,IS_VS_VICTIM,VS_CASE,VS_VICTIM_ID')
    for vm in sorted_stopped_VMs:

        vm_rt = simobj.get_vm_billing_clock(vm)
        job_rt = clib.get_sum_job_actual_duration (vm.job_history)

        vm_utilization = job_rt / float (vm_rt)
        stlag_rate = vm.startup_lag / float (vm_rt)
        idle_rate = 1 - vm_utilization - stlag_rate

        rep_vm_usage_log.info("%d,%d,%s,%s,%s,%s,%d,%d,%d,%d,%d,%d,%0.3f,%0.3f,%0.3f,%d,%d,%s,%s, %d"
                              % (vm.id, vm_rt, str(vm.cost), vm.instance_id, vm.type_name,
                                 simgval.get_sim_code_str(vm.status), vm.time_create, vm.time_active, vm.time_terminate,
                                 vm.startup_lag, len(vm.job_history), job_rt, vm_utilization, stlag_rate, idle_rate, vm.last_job_complete_clock, vm.sd_wait_time,
                                 vm.is_vscale_victim, simgval.get_sim_code_str(vm.vscale_case),vm.vscale_victim_id))

    clib.logwrite_vm_obj(g_log_handler, "[Broker__] " + str(Global_Curr_Sim_Clock), g_Stopped_VM_Instances[0])

def th_sim_entity_cloud_broker(my_block_id, conf_obj, q_in, q_out):

    global g_my_block_id
    global g_log_handler
    global g_sim_conf
    global g_job_recv_log_handler
    global g_job_comp_log_handler
    global g_sim_trace_log_handler
    global gQ_OUT

    if my_block_id != simgval.gBLOCK_BROKER:
        print "[Broker__] Broker Block ID (I:%d, G:%d) Error!!!" % (my_block_id, simgval.gBLOCK_BROKER)
        clib.sim_exit()

    # set global var
    g_my_block_id = my_block_id
    g_sim_conf = conf_obj
    gQ_OUT = q_out  # set global q_out

    time.sleep(random.random())

    print "[Broker__] Start Cloud Broker Thread!"
    log = set_log (conf_obj.log_path)
    g_log_handler = log
    #log.info('0' + '  Cloud Broker Log Start...')
    #log.info('0' + '  ======================================================')
    #log.info('0' + '  List of Broker Policies')
    #log.info('0' + '  ------------------------------------------------------')
    #log.info('0' + '  Job Assignment          : %s' % (simgval.get_sim_code_str(conf_obj.job_assign_policy)))
    #log.info('0' + '  VM Scale Down           : %s' % (simgval.get_sim_code_str(conf_obj.vm_scale_down_policy)))
    #log.info('0' + '  VM Scale Down Unit      : %s' % (conf_obj.vm_scale_down_policy_unit))
    ##log.info('0' + '  VM Unit Price           : %s' % (conf_obj.vm_unit_price))
    #log.info('0' + '  VM Billing Time Unit    : %s' % (simgval.get_sim_code_str(conf_obj.billing_time_unit)))
    #log.info('0' + '  VM Billing Time Period  : %s' % (simgval.get_sim_code_str(conf_obj.billing_time_period)))
    #log.info('0' + '  VM Billing Unit Clock   : %s Sim Clocks' % (conf_obj.billing_unit_clock))
    #log.info('0' + '  Enable Vertical Scaling : %s' % (conf_obj.enable_vertical_scaling))
    #log.info('0' + '  VSCALE Operation        : %s' % (simgval.get_sim_code_str(conf_obj.vscale_operation)))
    #log.info('0' + '  ------------------------------------------------------')
    #log.info('0' + '  VM SELECTION METHOD     : %s' % (simgval.get_sim_code_str(conf_obj.vm_selection_method)))
    #log.info('0' + '  NO OF VM TYPES          : %s' % (conf_obj.no_of_vm_types))
    #log.info('0' + '  VM TYPE INFOS           : ')
    #for vm_type in conf_obj.vm_types:
    #    log.info('0' + '                         : %s' % (vm_type.get_infos()))
    #log.info('0' + '  ======================================================')

    job_recv_log = set_job_recv_log (conf_obj.log_path)
    g_job_recv_log_handler = job_recv_log
    job_recv_log.info ('CL\tID\tJN\tADR\tDL\tIFD\t\t\tIFS(KB)\tOFD\t\t\tOFS(KB)\t\tSJFT')
    job_recv_log.info ('-----------------------------------------------------------------------------------------------------------------------')

    job_comp_log = set_job_comp_log (conf_obj.log_path)
    g_job_comp_log_handler = job_comp_log
    job_comp_log.info ('CL\tID\tJN\tADR\tIN\tCPU\tOUT\tDL\tVM\tTG\tTA\tTS\tTC\tTD\tRT\tDF\tST')
    job_comp_log.info ('------------------------------------------------------------------------------------------------------')

    sim_trace_log = set_broker_sim_trace_log (g_sim_conf.report_path)
    g_sim_trace_log_handler = sim_trace_log
    sim_trace_log.info ('CLOCK,JOB_RECV(CUMM),JOB_RECV(UNIT),JOB_COMP(CUMM),JOB_COMP(UNIT),VM_RUN,VM_STUP,VM_ACT,VM_STOP,VM_COST($)')

    register_broker_sim_trace_event()

    while True:
        q_message = q_in.get()
        q_in.task_done()
        wanna_term = False

        time.sleep(0.01)
        evt_clock   = q_message[1]  # EVT_SIMULATION_CLOCK
        evt_src     = q_message[2]  # SRC BLOCK
        evt_dst     = q_message[3]  # DST BLOCK
        evt_code    = q_message[4]  # EVT_CODE

        if evt_dst != my_block_id:
            log.error ("[Broker__] EVT_MSG ERROR! (Wrong EVT_DST) - %s" % (q_message))
            continue

        if evt_src == simgval.gBLOCK_SIM_EVENT_HANDLER:
            # update simulator clock
            update_curr_sim_clock (evt_clock)

        else:
            log.error("[Broker__] EVT_MSG ERROR! (Wrong EVT_SRC:%s) - %s" % (simgval.get_sim_code_str(evt_src), q_message))
            continue

        # Termination Processing
        if g_flag_sim_entity_term is True:
            wanna_term = broker_term_event_processing (q_message)
            if wanna_term is True:
                break

        # Main Event Processing
        broker_event_processing (q_message)

    broker_generate_report()
    print "\n[Broker__] %s Cloud Broker Terminated." % (str(Global_Curr_Sim_Clock))

# ============================================================================
# Space to write job assignment policies (user code)
#   - Job Scheduling
#   - VM Scaling up
# ============================================================================

#Job Assignment Result Definition (Codes have to be defined at sim_global_variables.py
JOB_ASSIGN_TO_NEW_VM        = simgval.gJOB_ASSIGN_TO_NEW_VM
JOB_ASSIGN_TO_EXISTING_VM   = simgval.gJOB_ASSIGN_TO_EXISTING_VM
JOB_ASSIGN_CODE_ERROR       = simgval.gJOB_ASSIGN_ERROR

def assign_job (job_obj):

    if g_sim_conf.job_assign_policy == simgval.gPOLICY_JOBASSIGN_EDF:
        return EDF_job_assignment (job_obj)

    else:
        g_log_handler.error ("[Broker__] %s JOB_ASSIGNMENT_POLICY (%s) Error!!!" % (str(Global_Curr_Sim_Clock), simgval.get_sim_code_str(g_sim_conf.job_assign_policy)))
        clib.sim_exit()

    return JOB_ASSIGN_CODE_ERROR, None

def get_data_transfer_infos (info_type, data_usage_type, data_flow_direction):

    data_transfer_infos = None
    if info_type == "rate":
        data_transfer_infos = g_sim_conf.perf_data_transfer
    elif info_type == "cost":
        data_transfer_infos = g_sim_conf.cost_data_transfer

    if data_transfer_infos is None:
        g_log_handler.error("[Broker__] %s get_data_transfer_infos: Cannot find proper info list! - info_type (%s), data_usage_type (%s), data_flow_direction (%s)" \
              % (str(Global_Curr_Sim_Clock), info_type, simgval.get_sim_code_str(data_usage_type), simgval.get_sim_code_str(data_flow_direction)))
        clib.sim_exit()

    if data_usage_type == simgval.gJOBFILE_INPUT:

        # return value should be either INTER or IN

        # in-cloud data transmission
        if data_flow_direction == simgval.gIFTD_IC:
            return data_transfer_infos[0]

        # from OUT to CLOUD data transmission
        elif data_flow_direction == simgval.gIFTD_OC:
            return data_transfer_infos[1]

        else:
            g_log_handler.error("[Broker__] %s get_data_transfer_infos: Input Data Flow Direction Error! - into_type (%s), data_usage_type (%s), data_flow_direction (%s)" % (str(Global_Curr_Sim_Clock), info_type, simgval.get_sim_code_str(data_usage_type), simgval.get_sim_code_str(data_flow_direction)))
            clib.sim_exit()

    elif data_usage_type == simgval.gJOBFILE_OUTPUT:

        # return value should be either INTER or OUT

        # in-cloud data transmission
        if data_flow_direction == simgval.gOFTD_IC:
            return data_transfer_infos[0]

        elif data_flow_direction == simgval.gOFTD_OC:
            return data_transfer_infos[2]

        else:
            g_log_handler.error("[Broker__] %s get_data_transfer_infos: Input Data Flow Direction Error! - into_type (%s), data_usage_type (%s), data_flow_direction (%s)" \
                  % (str(Global_Curr_Sim_Clock), info_type, simgval.get_sim_code_str(data_usage_type), simgval.get_sim_code_str(data_flow_direction)))
            clib.sim_exit()

    else:
        g_log_handler.error("[Broker__] %s get_data_transfer_infos: Data Usage Type Error! - into_type (%s), data_usage_type (%s), data_flow_direction (%s)" \
              % (str(Global_Curr_Sim_Clock), info_type, simgval.get_sim_code_str(data_usage_type), simgval.get_sim_code_str(data_flow_direction)))
        clib.sim_exit()


# calculate network duration
def cal_actual_network_duration_on_VMs (vm_net_factor, file_usage_type, file_flow_direction, file_size):

    # UNIT of data_transfer_rate is Megabyte/sec
    data_transfer_rate = get_data_transfer_infos ("rate", file_usage_type, file_flow_direction)
    mb_file_size = file_size/1024.0

    data_transfer_time = int(math.ceil((mb_file_size / data_transfer_rate) * vm_net_factor))
    return data_transfer_time

# calculate actual job duration - checked for storage simulation on 10/28/2014
def cal_job_actual_duration_on_VM (vm_type_obj, job_obj):

    job_act_cputime = int(math.ceil(vm_type_obj.vm_type_cpu_factor * job_obj.std_duration))

    job_act_nettime = [0,0]
    if job_obj.use_input_file == True:
        job_act_nettime[0] = cal_actual_network_duration_on_VMs (vm_type_obj.vm_type_net_factor, simgval.gJOBFILE_INPUT, job_obj.input_file_flow_direction, job_obj.input_file_size)

    if job_obj.use_output_file == True:
        job_act_nettime[1] = cal_actual_network_duration_on_VMs (vm_type_obj.vm_type_net_factor, simgval.gJOBFILE_OUTPUT, job_obj.output_file_flow_direction, job_obj.output_file_size)

    job_act_duration = job_act_cputime + sum(job_act_nettime)

    # return list = [act_total_duration, cpu_runtime, net_time [IN,OUT]]
    return [job_act_duration, job_act_cputime, job_act_nettime]

# this should calculate network transfer time
# Storage Scaling Debug Point - Done
def cal_actual_job_duration_on_VMs (job_obj):

    # act_runtime_infos = set of act_runtime_info
    # act_runtime_info =    [0]: vm_type_obj
    #                       [1]: actual runtime
    #                       [2]: deadline match
    #                       [3]: final evaluation
    #                       [4]: actual runtime with startup lag
    #                       [5]: deadline match with startup lag
    #                       [6]: final evaluation with startup lag

    vm_types = g_sim_conf.vm_types
    act_runtime_infos = []
    est_statup_lag = get_average_startup_lagtime()

    for vm_type in vm_types:
        act_runtime_info = []
        act_runtime_info.append(vm_type)            # [0] - vm_type_obj
        job_duration_infos = cal_job_actual_duration_on_VM (vm_type, job_obj)

        # Use Existing VM for Job Execution
        act_runtime_info.append(job_duration_infos[0])      # [1] - actual runtime : cpu duration + network (I/O)
        act_runtime_info.append(job_duration_infos[1])      # [2] - cpu duration
        act_runtime_info.append(job_duration_infos[2])      # [3] - net duration [IN/OUT]

        if job_duration_infos[0] <= job_obj.deadline:
            act_runtime_info.append(1)                      # [4] - deadline match - 1: match
        else:
            act_runtime_info.append(0)                      # [4] - deadline match - 0: mismatch

        act_runtime_info.append(0)                          # [5] - RESERVED for evaluation item

        # Create a New VM for Job Execution
        job_duration_infos[0] += est_statup_lag
        act_runtime_info.append(job_duration_infos[0])      # [6] - actual run time with startup lag (actual runtime (cpu + 2 I/O) + startuplag)
        act_runtime_info.append(job_duration_infos[1])      # [7] - cpu duration
        act_runtime_info.append(job_duration_infos[2])      # [8] - net duration [IN/OUT]

        if job_duration_infos[0] <= job_obj.deadline:
            act_runtime_info.append(1)                      # [9] - deadline match w/ startup lag - 1: match
        else:
            act_runtime_info.append(0)                      # [9] - deadline match w/ startup lag- 0: mismatch

        act_runtime_info.append(0)                          # [10] - booking for evaluation item with startup lag

        act_runtime_infos.append (act_runtime_info)

        # VM EVALUATION
        act_runtime_info[5] = do_VM_evaluation(act_runtime_info[1], act_runtime_info[0].vm_type_unit_price)
        act_runtime_info[10] = do_VM_evaluation(act_runtime_info[6], act_runtime_info[0].vm_type_unit_price)

    return act_runtime_infos

# Storage Scaling - Debug Point -- Done
def VM_type_selection (job_obj, act_runtime_infos, vm_use_type):

    deadline_index = 0
    sort_index_criteria = 0
    sort_index_perf = 0

    if vm_use_type == JOB_ASSIGN_TO_NEW_VM:
        deadline_index = 9
        sort_index_criteria = 10
        sort_index_perf = 6
    elif vm_use_type == JOB_ASSIGN_TO_EXISTING_VM:
        deadline_index = 4
        sort_index_criteria = 5
        sort_index_perf = 1

    dmatch_vm_ts = [act_runtime for act_runtime in act_runtime_infos if act_runtime[deadline_index] > 0]
    ret_vm_type_obj = None

    # [total_duration, cpu_time, [input net, output net]]
    ret_act_duration = []

    if len (dmatch_vm_ts) > 0:

        dmatch_vm_ts.sort(key=lambda x: x[sort_index_criteria])
        ret_vm_type_obj = dmatch_vm_ts[0][0]

        ret_act_duration.append(dmatch_vm_ts[0][1])
        ret_act_duration.append(dmatch_vm_ts[0][2])
        ret_act_duration.append(dmatch_vm_ts[0][3])

    else:

        # no deadline match vm types -- select best performance one.
        act_runtime_infos.sort (key=lambda x: x[sort_index_perf])
        ret_vm_type_obj = act_runtime_infos[0][0]

        # comment added on 10/25/2014
        # event though vm_usage_case is "JOB_ASSIGN_TO_NEW_VM", this library should return job execution time on existing VM
        # we will take care of startup lag time in another routine. -- don't worry about returning job duration info for exsiting value for new creation

        ret_act_duration.append(act_runtime_infos[0][1])
        ret_act_duration.append(act_runtime_infos[0][2])    # this was changed from "dmatch_vm_ts" to "act_runtime_infos" -- on 12/26/2014
        ret_act_duration.append(act_runtime_infos[0][3])    # this was changed from "dmatch_vm_ts" to "act_runtime_infos" -- on 12/26/2014

    if ret_vm_type_obj != None:
        return ret_vm_type_obj, ret_act_duration
    else:
        clib.sim_exit()


def get_actual_run_time_by_type (act_runtime_infos, vm_type_name):

    for act_runtime_info in act_runtime_infos:
        curr_vm_type = act_runtime_info[0]
        if curr_vm_type.vm_type_name == vm_type_name:

            ret_rt_infos = []
            ret_rt_infos.append(act_runtime_info[1])   # total
            ret_rt_infos.append(act_runtime_info[2])   # cpu time
            ret_rt_infos.append(act_runtime_info[3])   # io(net) time

            return ret_rt_infos

    return None

def display_actual_runtime_infos_on_VM (act_runtime_infos):

    curframe = inspect.currentframe()
    calframe = inspect.getouterframes(curframe, 2)

    print ""
    print "======================================================================================================================================================="
    print "\tCalculation Actual Runtime on VM Types"
    print "\t\t\tCaller Lib:", calframe[1][3]
    print "\t\t\tCaller Loc:", calframe[1][1].split("/")[-1], ":", calframe[1][2]
    print "-------------------------------------------------------------------------------------------------------------------------------------------------------"
    print "\tVM Type Infos\t\t\t\t|ADR(E)\t|CPU(E)\t|NET [IN/OUT]\t|DLM(E)\t|EVAL1\t|ADR(N)\t|CPU(N)\t|NET [IN/OUT]\t|DLM(N)\t|EVAL2"
    print "-------------------------------------------------------------------------------------------------------------------------------------------------------"
    for a in act_runtime_infos:
        print a[0].get_infos(),"\t|", a[1],"\t|", a[2],"\t|", a[3],"\t|", "Yes\t|" if a[4] == 1 else "No\t|", a[5],"\t|", a[6],"\t|", a[7],"\t|", a[8],"\t|", "Yes\t|" if a[9] == 1 else "No\t|", a[10]

    print "======================================================================================================================================================="
    print ""

def EDF_job_assignment (job_obj):

    # EDF_job_assignment_BTU_Min
    # [1] select best vm option for job
    # [2] find it from the current running VMs
    # [3] if the same type of VM with option from [1] exists, then assign input job to the vm.
    # [4] otherwise, create new one.

    policy_str = simgval.get_sim_code_str(g_sim_conf.job_assign_policy)
    job_str = simobj.get_job_info_str(job_obj)

    g_log_handler.info("[Broker__] %s %s - Recv %s" % (str(Global_Curr_Sim_Clock), policy_str, job_str))

    curr_run_vm_cnt = get_number_Running_VM_Instances()
    max_vm_cap = g_sim_conf.job_assign_max_running_vms

    act_runtime_infos = cal_actual_job_duration_on_VMs (job_obj)
    display_actual_runtime_infos_on_VM (act_runtime_infos)

    # EDF-Assignment Algorithm
    #
    # 1. if len(vm_ids) < 1:
    #       if curr_run_vm_cnt < max_cap:
    #           create new one and assign the job to the vm.
    #       elif curr_run_vm_cnt == max_cap:
    #           [a] get all vm_id (call running instance is candidate!)
    #           [b] find min execution time
    #           [c] assign job to the min execution time
    #       else curr_run_vm_cnt > max_cap
    #           sim_terminate --> this is error case
    # 2. if len (vm_ids) >= 1:
    #       find min job execution time
    #       if min job execution time < deadline
    #           assign job to the vm.
    #       else : deadline mismatch
    #
    #           if current_run_vm_cnt < max_cap
    #               create new one and assign job to the vm
    #           elif curr_run_vm == max_cap
    #              [a] get all vm_id (call running instance is candidate!)
    #              [b] find min execution time
    #              [c] assign job to the min execution time
    #           else curr_run_vm > max_cap
    #               raise error!


    sel_vm_type, actual_job_duration_infos = VM_type_selection (job_obj, act_runtime_infos, JOB_ASSIGN_TO_EXISTING_VM)
    vm_ids = get_running_VM_IDs_by_VMType (sel_vm_type.vm_type_name)

    print "\tSelected VM Type for %s" % (job_str)
    print "-------------------------------------------------------------------------------------------------------"
    print "\tSelection Policy     :", simgval.get_sim_code_str(g_sim_conf.vm_selection_method)
    print "\tSel VM Type Infos    :", sel_vm_type.get_infos()
    print "\tSel VM Job Exe Infos :", actual_job_duration_infos
    print "\tCurr Running VM IDs  :", vm_ids
    print "======================================================================================================="
    print ""

    # case 1 - cannot find the selected vm type from current running vms.
    if len (vm_ids) < 1:

        g_log_handler.info("[Broker__] %s %s - No Current Running VM (TY:%s, PR:$%s)" \
              % (str(Global_Curr_Sim_Clock), policy_str, sel_vm_type.vm_type_name, sel_vm_type.vm_type_unit_price))

        # we have buffer for adding new one.
        # case 1 - [a] -
        if curr_run_vm_cnt < max_vm_cap:

            # create new one with a specific vm type
            g_log_handler.info("[Broker__] %s %s - Create new VM for Job %s" % (str(Global_Curr_Sim_Clock), policy_str, job_str))
            return assign_job_to_new_VM (job_obj, act_runtime_infos)

        # we dont have buffer for adding new one --> we leverage current running pool as much as we can.
        # case 1 - [b]
        elif curr_run_vm_cnt == max_vm_cap:

            g_log_handler.info("[Broker__] %s %s - Current # of VM (%d) == MAX VM CAP (%d) - Assign Job (%s) to Existing VM " \
                  % (str(Global_Curr_Sim_Clock), policy_str, curr_run_vm_cnt, max_vm_cap, job_str))

            # assign existing vm, which offers earliest job completion time. (any type)
            return assign_job_to_any_type_of_existing_VM (job_obj, act_runtime_infos)

        # max cap error
        # case 1 - [C]
        elif curr_run_vm_cnt > max_vm_cap:

            # job assign error -- current running vms exceed the max cap of vms.
            job_assign_maxcap_error()

    # case 2 - can find the selected vm type from current running vms.
    else:

        g_log_handler.info("[Broker__] %s %s - Current %d Running VM (TY:%s, PR:$%s) - VM:%s" \
              % (str(Global_Curr_Sim_Clock), policy_str, len(vm_ids), sel_vm_type.vm_type_name, sel_vm_type.vm_type_unit_price, vm_ids))

        #=======================================================================================+
        # find min job execution time...on VM candidates...
        est_job_completion_times = []

        for vm_id in vm_ids:
            vm_info = [vm_id]

            vm_info.append(cal_estimated_job_completion_time(vm_id, actual_job_duration_infos))
            est_job_completion_times.append(vm_info)

        g_log_handler.info("[Broker__] %s %s - Estimate JOB Completion Time of VM [ID,CompTime]: %s" % (str(Global_Curr_Sim_Clock), policy_str, str(est_job_completion_times)))

        min_info = min (est_job_completion_times, key=lambda x: x[1])
        min_vm_id = min_info[0]
        min_job_complete_time = min_info[1]
        #=======================================================================================+

        # case 2 - [a] : current running VM can meet the job's deadline
        if min_job_complete_time <= job_obj.deadline:

            # assign job to a specific current running VM with ID.
            return assign_job_to_specific_existing_VM (job_obj, actual_job_duration_infos, min_vm_id, min_job_complete_time)

        # case 2 - [b] : current running VM CANNOT meet the job deadline
        else:

            # current vm cannot meet the deadline.
            g_log_handler.info("[Broker__] %s %s - NO Current Running VM can meet the DEADLINE (Min JCT:%d, DL:%d)" \
                  % (str(Global_Curr_Sim_Clock), policy_str, min_job_complete_time, job_obj.deadline))

            # case 2 - [b] - 1 : current vms cannot meet deadline => create new one (less than max cap)
            if curr_run_vm_cnt < max_vm_cap:

                # create new one with a specific vm type
                g_log_handler.info("[Broker__] %s %s - Create new VM for Job %s" % (str(Global_Curr_Sim_Clock), policy_str, job_str))
                return assign_job_to_new_VM (job_obj, act_runtime_infos)

            # we dont have buffer for adding new one --> we leverage current running pool as much as we can.
            # case 2 - [b] - 2 : current vms cannot meet deadline => cannot create new one (due to the max cap of vm)
            elif curr_run_vm_cnt == max_vm_cap:

                # assign existing vm, which offers earliest job completion time. (any type)
                g_log_handler.info("[Broker__] %s %s - Current # of VM (%d) == MAX VM CAP (%d) - Assign Job (%s) to Existing VM " \
                      % (str(Global_Curr_Sim_Clock), policy_str, curr_run_vm_cnt, max_vm_cap, job_str))
                return assign_job_to_any_type_of_existing_VM (job_obj, act_runtime_infos)

            # max cap error
            # case 2 - [b] - 3
            elif curr_run_vm_cnt > max_vm_cap:

                # vm max cap error.

                # job assign error -- current running vms exceed the max cap of vms.
                job_assign_maxcap_error()


# Job Assignment and VM Scaling Libraries
def job_assign_maxcap_error():

    curr_run_vm_cnt = get_number_Running_VM_Instances()
    if curr_run_vm_cnt <= g_sim_conf.job_assign_max_running_vms:
        return

    g_log_handler.error("[Broker__] %s %s Max Cap Error! - Current VM Cnt:%d exceeds the Max Cap (%d)" \
          % (str(Global_Curr_Sim_Clock), simgval.get_sim_code_str(g_sim_conf.job_assign_policy), curr_run_vm_cnt, g_sim_conf.job_assign_max_running_vms))
    clib.sim_exit()

# Storage Simulation Debug Point - done by 10252014
def assign_job_to_new_VM (job_obj, act_runtime_infos):

    policy_str = simgval.get_sim_code_str(g_sim_conf.job_assign_policy)
    job_str = simobj.get_job_info_str(job_obj)

    g_log_handler.info("[Broker__] %s %s - Exec assign_job_to_new_VM" % (str(Global_Curr_Sim_Clock), policy_str))
    g_log_handler.info("[Broker__] %s %s - Reevalute VM type with Startup Lag" % (str(Global_Curr_Sim_Clock), policy_str))

    # re-evaluate vm selection with startup lag.
    sel_vm_type, actual_job_duration_infos = VM_type_selection (job_obj, act_runtime_infos, JOB_ASSIGN_TO_NEW_VM)

    # update actual running time on VM right before assign or create the input job to any VM
    simobj.set_Job_act_duration_on_VM(job_obj, actual_job_duration_infos, sel_vm_type.vm_type_cpu_factor)

    # ========================================================================
    # Place for old assign_job_to_new_VM code...

    # create a VM
    new_vm_id = create_new_VM_Instance (sel_vm_type)

    # insert input job to the new VM
    insert_job_into_VM_Job_Queue (new_vm_id, job_obj)

    # return: job_assignment_result_code, vm_id

    g_log_handler.info("[Broker__] %s %s - %s Assigned to New VM (ID:%d, TY:%s, PR:$%s)" \
          % (str(Global_Curr_Sim_Clock), policy_str, job_str, new_vm_id, sel_vm_type.vm_type_name, sel_vm_type.vm_type_unit_price))
    # ======================================================================

    return JOB_ASSIGN_TO_NEW_VM, new_vm_id

# storage simulation debug point - Done on 10/25/2014
def assign_job_to_any_type_of_existing_VM (job_obj, act_runtime_infos):

    policy_str = simgval.get_sim_code_str(g_sim_conf.job_assign_policy)
    job_str = simobj.get_job_info_str(job_obj)

    g_log_handler.info("[Broker__] %s %s - Exec assign_job_to_any_type_of_existing_VM" % (str(Global_Curr_Sim_Clock), policy_str))

    # get all current running vm ids.
    curr_vm_ids = get_running_VM_IDs ()

    est_job_completion_times = []
    for curr_vm_id in curr_vm_ids:
        curr_vm_info = [curr_vm_id]
        curr_vm = get_VM_from_Running_VM_Instances (curr_vm_id)
        curr_act_runtimes = get_actual_run_time_by_type (act_runtime_infos, curr_vm.type_name)

        if curr_act_runtimes is None:
            continue

        curr_vm_info.append(cal_estimated_job_completion_time(curr_vm_id, curr_act_runtimes))
        curr_vm_info.extend(curr_act_runtimes)
        curr_vm_info.append(curr_vm)

        est_job_completion_times.append(curr_vm_info)

    if len (est_job_completion_times) < 1:
        g_log_handler.info("[Broker__] %s %s - Est Job Completion Time Calculation Error!" % (str(Global_Curr_Sim_Clock), policy_str))
        clib.sim_exit()

    min_info = min (est_job_completion_times, key=lambda x: x[1])
    min_vm_id = min_info[0]
    min_job_complete_time = min_info[1]
    min_vm_type_obj = iaas.get_VM_type_obj_by_type_name (min_info[5].type_name)

    # update job actual running time on VM right before assigning the job or create a new VM.
    simobj.set_Job_act_duration_on_VM(job_obj, min_info[2:5], min_vm_type_obj.vm_type_cpu_factor)
    # ========================================================================
    # Place for old assign_job_to_existing_VM

    # insert input job to the existing VM
    insert_job_into_VM_Job_Queue (min_vm_id, job_obj)
    g_log_handler.info("[Broker__] %s %s - %s Assigned to Existing VM (ID:%d, TY:%s, PR:$%s, EJCT:%d)" \
            % (str(Global_Curr_Sim_Clock), policy_str, job_str, min_vm_id, min_info[-1].type_name, min_info[-1].unit_price, min_job_complete_time))
    # ========================================================================

    return JOB_ASSIGN_TO_EXISTING_VM, min_vm_id

# Storage Simulation Debug Point - check done by 10.29.2014
def assign_job_to_specific_existing_VM (job_obj, actual_job_duration_infos, min_vm_id, min_job_complete_time):

    policy_str = simgval.get_sim_code_str(g_sim_conf.job_assign_policy)
    g_log_handler.info("[Broker__] %s %s - Exec assign_job_to_specific_existing_VM" \
          % (str(Global_Curr_Sim_Clock), policy_str))

    target_vm = get_VM_from_Running_VM_Instances(min_vm_id)
    if target_vm is None:
        g_log_handler.error("[Broker__] %s %s - Cannot find a proper VM (ID:%d)" % (str(Global_Curr_Sim_Clock), policy_str, min_vm_id))
        clib.sim_exit()

    target_vm_type_obj = iaas.get_VM_type_obj_by_type_name(target_vm.type_name)

    # update job actual running time on VM right before assign the input job.
    simobj.set_Job_act_duration_on_VM(job_obj, actual_job_duration_infos, target_vm_type_obj.vm_type_cpu_factor)

    # ========================================================================
    # Place for old assign_job_to_existing_VM

    # insert input job to the existing VM
    insert_job_into_VM_Job_Queue (min_vm_id, job_obj)

    job_str = simobj.get_job_info_str(job_obj)
    g_log_handler.info("[Broker__] %s %s - %s Assigned to Existing VM (ID:%d, TY:%s, PR:$%s, EJCT:%d)" \
            % (str(Global_Curr_Sim_Clock), policy_str, job_str, min_vm_id, target_vm.type_name, target_vm.unit_price, min_job_complete_time))
    # ========================================================================

    return JOB_ASSIGN_TO_EXISTING_VM, min_vm_id

# ============================================================================
# Space to write vm scale down policies (user code)
#   - VM Scale Down
#   - This lib is called whenever a vm is created.
# ============================================================================

# Scale Down Trigger
def trigger_vm_scale_down (vm_id):

    ###########################################################################
    # Procedure
    # [a] parameter error check
    #   1] vm id > 0
    #   2] vm should be in g_Running_VM_Instances List
    # [b] Send Timer Event to Event Handler based on VM Scale Down Policy
    #   1] Event Code Locates...
    ###########################################################################

    # [a] - 1 : Parameter Error Check #1
    if vm_id < 1:
        g_log_handler.error("[Broker__] %s VM SCALE DOWN ERROR! - Invalid VM ID(%d)" % (str(Global_Curr_Sim_Clock), vm_id))
        clib.sim_exit()

    # [a] - 2 : Parameter Error Check #2
    find_result, find_index = find_VM_from_Running_VM_Instances (vm_id)
    if find_result == False:
        g_log_handler.error("[Broker__] %s VM SCALE DOWN ERROR! - Cannot Find VM(ID:%d) from g_Running_VM_Instances List" % (str(Global_Curr_Sim_Clock), vm_id))
        clib.logwrite_VM_Instance_Lists (g_log_handler, "[Broker__] " + str(Global_Curr_Sim_Clock), "Current g_Running_VM_Instances", g_Running_VM_Instances)
        clib.sim_exit()

    if g_sim_conf.vm_scale_down_policy == simgval.gPOLICY_VM_SCALEDOWN_IMMEDIATE:

        # p1: Immediate Scale Down
        run_SD_IM_policy (vm_id)

    elif g_sim_conf.vm_scale_down_policy == simgval.gPOLICY_VM_SCALEDOWN_HOUR or g_sim_conf.vm_scale_down_policy == simgval.gPOLICY_VM_SCALEDOWN_MIN:

        # p2: basic scaling down - Hour/Min
        # p3: basic scaling down - Hour/Min
        run_SD_PM_policy (vm_id)

    elif g_sim_conf.vm_scale_down_policy == simgval.gPOLICY_VM_SCALEDOWN_STARTUPLAG:

        # p4: Immediate Scale Down
        run_SD_SL_policy (vm_id)

    elif g_sim_conf.vm_scale_down_policy == simgval.gPOLICY_VM_SCALEDOWN_JAT_MEAN:

        # p5: Mean Job Arrival Time
        run_SD_JAT_MEAN_policy (vm_id)

    elif g_sim_conf.vm_scale_down_policy == simgval.gPOLICY_VM_SCALEDOWN_JAT_MAX:

        # p6: Max Job Arrival Time
        run_SD_JAT_MAX_policy (vm_id)

    elif g_sim_conf.vm_scale_down_policy == simgval.gPOLICY_VM_SCALEDOWN_JAT_MEAN_RECENT:

        # p7: Mean Recent Job Arrival Time
        run_SD_JAT_MEAN_RECENT_policy (vm_id)

    elif g_sim_conf.vm_scale_down_policy == simgval.gPOLICY_VM_SCALEDOWN_JAT_MAX_RECENT:

        # p8: Max Recent Job Arrival Rate
        run_SD_JAT_MAX_RECENT_policy (vm_id)

    elif g_sim_conf.vm_scale_down_policy == simgval.gPOLICY_VM_SCALEDOWN_JAT_SLR:

        # p9: Jat Simple Linear Regression
        run_SD_JAT_SLR_policy (vm_id)

    elif g_sim_conf.vm_scale_down_policy == simgval.gPOLICY_VM_SCALEDOWN_JAT_2PR:

        # p10: Jat Quadratic (2 degree) Regression
        run_SD_JAT_2PR_policy (vm_id)

    elif g_sim_conf.vm_scale_down_policy == simgval.gPOLICY_VM_SCALEDOWN_JAT_3PR:

        # p11: Jat Cubic (3 degree) Regression
        run_SD_JAT_3PR_policy (vm_id)

    elif g_sim_conf.vm_scale_down_policy == simgval.gPOLICY_VM_SCALEDOWN_JAT_LLR:

        # p12: Jat Local Linear Regression
        run_SD_JAT_LLR_policy (vm_id)

    elif g_sim_conf.vm_scale_down_policy == simgval.gPOLICY_VM_SCALEDOWN_JAT_L2PR:

        # p13: Jat Local Quadratic Regression
        run_SD_JAT_L2PR_policy (vm_id)

    elif g_sim_conf.vm_scale_down_policy == simgval.gPOLICY_VM_SCALEDOWN_JAT_L3PR:

        # p14: Jat Local Cubic Regression
        run_SD_JAT_L3PR_policy (vm_id)

    elif g_sim_conf.vm_scale_down_policy == simgval.gPOLICY_VM_SCALEDOWN_JAT_WMA:
        # p15: Weighted Moving Average
        run_SD_JAT_WMA_policy (vm_id)

    elif g_sim_conf.vm_scale_down_policy == simgval.gPOLICY_VM_SCALEDOWN_JAT_ES:
        # p16: Exponential Smoothing
        run_SD_JAT_ES_policy (vm_id)

    elif g_sim_conf.vm_scale_down_policy == simgval.gPOLICY_VM_SCALEDOWN_JAT_HWDES:
        # p17: Holt Winters Double Exponential Smoothing
        run_SD_JAT_HWDES_policy (vm_id)

    elif g_sim_conf.vm_scale_down_policy == simgval.gPOLICY_VM_SCALEDOWN_JAT_BRDES:
        # p18: Brown Double Exponential Smoothing
        run_SD_JAT_BRDES_policy (vm_id)

    elif g_sim_conf.vm_scale_down_policy == simgval.gPOLICY_VM_SCALEDOWN_JAT_AR:
        # p19: Autoregressive
        run_SD_JAT_AR_policy (vm_id)

    elif g_sim_conf.vm_scale_down_policy == simgval.gPOLICY_VM_SCALEDOWN_JAT_ARMA:
        # p20: Autoregressive and Moving Average
        run_SD_JAT_ARMA_policy (vm_id)

    elif g_sim_conf.vm_scale_down_policy == simgval.gPOLICY_VM_SCALEDOWN_JAT_ARIMA:
        # p21: Autoregressive Integrated Moving Average
        run_SD_JAT_ARIMA_policy (vm_id)

    else:
        g_log_handler.error("[Broker__] %s VM SCALE DOWN ERROR! - Invalid Policy(%s,%s)" \
              % (str(Global_Curr_Sim_Clock), simgval.get_sim_code_str(g_sim_conf.vm_scale_down_policy), str(g_sim_conf.vm_scale_down_policy_timer_unit)))
        clib.sim_exit()

# Scale Down Policy #1 --> SD-IM : Immediate Scaling Down
def run_SD_IM_policy (vm_id):

    timer = estimate_next_idle_clock (vm_id)

    q_tmr_evt_data = [vm_id, Global_Curr_Sim_Clock]
    q_tmr_evt = clib.make_queue_event (timer,                               # estimated next idle time (not precise)
                                       g_my_block_id,                       # src block id (will be returned to src block)
                                       simgval.gBLOCK_SIM_EVENT_HANDLER,    # dst block id (event handler block)
                                       simgval.gEVT_SET_TIMER,              # evt code: set timer
                                       simgval.gEVT_SUB_VM_SCALE_DOWN,      # evt sub code: vm_scale_down
                                       q_tmr_evt_data)                      # event data = [vm_id]

    # send timer event to event handler
    send_scale_down_timer_event (vm_id, q_tmr_evt)

# Scale Down Policy #2 and #3 --> SD-HR, SD-MN (Pricing Model based Scaling Down)
def run_SD_PM_policy (vm_id):

    q_tmr_evt_data = [vm_id, Global_Curr_Sim_Clock]
    q_tmr_evt = clib.make_queue_event (g_sim_conf.vm_scale_down_policy_timer_unit,  # timer (vm_scale_down_policy_timer_unit)
                                       g_my_block_id,                               # src block id (will be returned to src block)
                                       simgval.gBLOCK_SIM_EVENT_HANDLER,            # dst block id (event handler block)
                                       simgval.gEVT_SET_TIMER,                      # evt code: set timer
                                       simgval.gEVT_SUB_VM_SCALE_DOWN,              # evt sub code: vm_scale_down
                                       q_tmr_evt_data)                              # event data = [vm_id]

    # send timer event to event handler
    send_scale_down_timer_event (vm_id, q_tmr_evt)

# Scale Down Policy #4 - SD-SL : Startup Lag
def run_SD_SL_policy (vm_id):

    state = get_VM_sd_policy_activated (vm_id)
    if state == True:
        # timer: average startup lag time. --> actual scale down is triggered
        timer = get_average_startup_lagtime()
    else:
        # timer: time to be idle. --> just wait for vm being idle.
        timer = estimate_next_idle_clock (vm_id)

    q_tmr_evt_data = [vm_id, Global_Curr_Sim_Clock]
    q_tmr_evt = clib.make_queue_event (timer,                               # timer (vm_scale_down_policy_timer_unit)
                                       g_my_block_id,                       # src block id (will be returned to src block)
                                       simgval.gBLOCK_SIM_EVENT_HANDLER,    # dst block id (event handler block)
                                       simgval.gEVT_SET_TIMER,              # evt code: set timer
                                       simgval.gEVT_SUB_VM_SCALE_DOWN,      # evt sub code: vm_scale_down
                                       q_tmr_evt_data)                      # event data = [vm_id]

    # send timer event to event handler
    send_scale_down_timer_event (vm_id, q_tmr_evt)

# Scale Down Policy #5 - SD-JAT-MEAN : Mean Job Arrival Time
def run_SD_JAT_MEAN_policy (vm_id):

    msg_identifier = None

    state = get_VM_sd_policy_activated (vm_id)
    if state == True:
        # timer: mean job arrival time. --> actual scale down is triggered
        timer = get_mean_job_arrival_rate()
    else:
        # timer: time to be idle. --> just wait for vm being idle.
        timer = estimate_next_idle_clock (vm_id)

    q_tmr_evt_data = [vm_id, Global_Curr_Sim_Clock]
    q_tmr_evt = clib.make_queue_event (timer,                               # timer (vm_scale_down_policy_timer_unit)
                                       g_my_block_id,                       # src block id (will be returned to src block)
                                       simgval.gBLOCK_SIM_EVENT_HANDLER,    # dst block id (event handler block)
                                       simgval.gEVT_SET_TIMER,              # evt code: set timer
                                       simgval.gEVT_SUB_VM_SCALE_DOWN,      # evt sub code: vm_scale_down
                                       q_tmr_evt_data)                      # event data = [vm_id]

    # send timer event to event handler
    send_scale_down_timer_event (vm_id, q_tmr_evt)

# Scale Down Policy #6 - SD-JAT-MAX : Max Job Arrival Time
def run_SD_JAT_MAX_policy (vm_id):

    state = get_VM_sd_policy_activated (vm_id)
    if state == True:
        # timer: mean job arrival time. --> actual scale down is triggered
        timer =  get_max_job_arrival_rate()

    else:
        # timer: time to be idle. --> just wait for vm being idle.
        timer = estimate_next_idle_clock (vm_id)

    q_tmr_evt_data = [vm_id, Global_Curr_Sim_Clock]
    q_tmr_evt = clib.make_queue_event (timer,                               # timer (vm_scale_down_policy_timer_unit)
                                       g_my_block_id,                       # src block id (will be returned to src block)
                                       simgval.gBLOCK_SIM_EVENT_HANDLER,    # dst block id (event handler block)
                                       simgval.gEVT_SET_TIMER,              # evt code: set timer
                                       simgval.gEVT_SUB_VM_SCALE_DOWN,      # evt sub code: vm_scale_down
                                       q_tmr_evt_data)                      # event data = [vm_id]

    # send timer event to event handler
    send_scale_down_timer_event (vm_id, q_tmr_evt)

# Scale Down Policy #7 - SD-JAT-MEAN-RECENT: Mean Recent Job Arrival Time
def run_SD_JAT_MEAN_RECENT_policy (vm_id):

    state = get_VM_sd_policy_activated (vm_id)
    if state == True:
        # timer: mean recent n job arrival time. --> actual scale down is triggered
        cnt = get_recent_sample_cnt()
        timer =  get_mean_recent_n_job_arrival_rate(cnt)

    else:
        # timer: time to be idle. --> just wait for vm being idle.
        timer = estimate_next_idle_clock (vm_id)

    q_tmr_evt_data = [vm_id, Global_Curr_Sim_Clock]
    q_tmr_evt = clib.make_queue_event (timer,                               # timer (vm_scale_down_policy_timer_unit)
                                       g_my_block_id,                       # src block id (will be returned to src block)
                                       simgval.gBLOCK_SIM_EVENT_HANDLER,    # dst block id (event handler block)
                                       simgval.gEVT_SET_TIMER,              # evt code: set timer
                                       simgval.gEVT_SUB_VM_SCALE_DOWN,      # evt sub code: vm_scale_down
                                       q_tmr_evt_data)                      # event data = [vm_id]

    # send timer event to event handler
    send_scale_down_timer_event (vm_id, q_tmr_evt)

# Scale Down Policy #8 - SD-JAT-MAX-RECENT: Max Recent Job Arrival Time
def run_SD_JAT_MAX_RECENT_policy (vm_id):

    state = get_VM_sd_policy_activated (vm_id)
    if state == True:
        # timer: mean recent n job arrival time. --> actual scale down is triggered
        cnt = get_recent_sample_cnt()
        timer =  get_max_recent_n_job_arrival_rate(cnt)

    else:
        # timer: time to be idle. --> just wait for vm being idle.
        timer = estimate_next_idle_clock (vm_id)

    q_tmr_evt_data = [vm_id, Global_Curr_Sim_Clock]
    q_tmr_evt = clib.make_queue_event (timer,                               # timer (vm_scale_down_policy_timer_unit)
                                       g_my_block_id,                       # src block id (will be returned to src block)
                                       simgval.gBLOCK_SIM_EVENT_HANDLER,    # dst block id (event handler block)
                                       simgval.gEVT_SET_TIMER,              # evt code: set timer
                                       simgval.gEVT_SUB_VM_SCALE_DOWN,      # evt sub code: vm_scale_down
                                       q_tmr_evt_data)                      # event data = [vm_id]

    # send timer event to event handler
    send_scale_down_timer_event (vm_id, q_tmr_evt)

def run_SD_JAT_SLR_policy (vm_id):

    state = get_VM_sd_policy_activated (vm_id)
    if state == True:

        x = []
        y = g_Job_Arrival_Rate
        v = len(y) + 1

        for n in xrange (v-1):
            x.append(n+1)

        # timer: simple linear regression for job arrival rate
        result, coefs = reg.regression (1, x, y, v)
        bounds = get_vm_wait_time_bounds()
        timer = validate_prediction_result (result, bounds)

        write_debug_log_for_regression_method (vm_id, timer, coefs, x, y, v)

    else:

        # timer: time to be idle. --> just wait for vm being idle.
        timer = estimate_next_idle_clock (vm_id)

    q_tmr_evt_data = [vm_id, Global_Curr_Sim_Clock]
    q_tmr_evt = clib.make_queue_event (timer,                               # timer (vm_scale_down_policy_timer_unit)
                                       g_my_block_id,                       # src block id (will be returned to src block)
                                       simgval.gBLOCK_SIM_EVENT_HANDLER,    # dst block id (event handler block)
                                       simgval.gEVT_SET_TIMER,              # evt code: set timer
                                       simgval.gEVT_SUB_VM_SCALE_DOWN,      # evt sub code: vm_scale_down
                                       q_tmr_evt_data)                      # event data = [vm_id]

    # send timer event to event handler
    send_scale_down_timer_event (vm_id, q_tmr_evt)

def run_SD_JAT_2PR_policy (vm_id):

    state = get_VM_sd_policy_activated (vm_id)
    if state == True:

        x = []
        y = g_Job_Arrival_Rate
        v = len(y) + 1

        for n in xrange (v-1):
            x.append(n+1)

        # timer: Quadratic regression for job arrival rate
        result, coefs = reg.regression (2, x, y, v)
        bounds = get_vm_wait_time_bounds()
        timer = validate_prediction_result (result, bounds)

        write_debug_log_for_regression_method (vm_id, timer, coefs, x, y, v)

    else:

        # timer: time to be idle. --> just wait for vm being idle.
        timer = estimate_next_idle_clock (vm_id)

    q_tmr_evt_data = [vm_id, Global_Curr_Sim_Clock]
    q_tmr_evt = clib.make_queue_event (timer,                               # timer (vm_scale_down_policy_timer_unit)
                                       g_my_block_id,                       # src block id (will be returned to src block)
                                       simgval.gBLOCK_SIM_EVENT_HANDLER,    # dst block id (event handler block)
                                       simgval.gEVT_SET_TIMER,              # evt code: set timer
                                       simgval.gEVT_SUB_VM_SCALE_DOWN,      # evt sub code: vm_scale_down
                                       q_tmr_evt_data)                      # event data = [vm_id]

    # send timer event to event handler
    send_scale_down_timer_event (vm_id, q_tmr_evt)

def run_SD_JAT_3PR_policy (vm_id):

    state = get_VM_sd_policy_activated (vm_id)
    if state == True:

        x = []
        y = g_Job_Arrival_Rate
        v = len(y) + 1

        for n in xrange (v-1):
            x.append(n+1)

        # timer: Cubic regression for job arrival rate
        result, coefs = reg.regression (3, x, y, v)
        bounds = get_vm_wait_time_bounds()
        timer = validate_prediction_result (result, bounds)

        write_debug_log_for_regression_method (vm_id, timer, coefs, x, y, v)

    else:

        # timer: time to be idle. --> just wait for vm being idle.
        timer = estimate_next_idle_clock (vm_id)

    q_tmr_evt_data = [vm_id, Global_Curr_Sim_Clock]
    q_tmr_evt = clib.make_queue_event (timer,                               # timer (vm_scale_down_policy_timer_unit)
                                       g_my_block_id,                       # src block id (will be returned to src block)
                                       simgval.gBLOCK_SIM_EVENT_HANDLER,    # dst block id (event handler block)
                                       simgval.gEVT_SET_TIMER,              # evt code: set timer
                                       simgval.gEVT_SUB_VM_SCALE_DOWN,      # evt sub code: vm_scale_down
                                       q_tmr_evt_data)                      # event data = [vm_id]

    # send timer event to event handler
    send_scale_down_timer_event (vm_id, q_tmr_evt)

def run_SD_JAT_LLR_policy (vm_id):

    state = get_VM_sd_policy_activated (vm_id)
    if state == True:

        kernel_sample_cnt = get_recent_sample_cnt()
        x = []
        y = get_recent_n_job_arrival_list (kernel_sample_cnt)
        v = len(y) + 1

        for n in xrange (v-1):
            x.append(n+1)

        # timer: Local linear regression for job arrival rate
        result, coefs = reg.regression (1, x, y, v)
        bounds = get_vm_wait_time_bounds()
        timer = validate_prediction_result (result, bounds)

        write_debug_log_for_regression_method (vm_id, timer, coefs, x, y, v)

    else:

        # timer: time to be idle. --> just wait for vm being idle.
        timer = estimate_next_idle_clock (vm_id)

    q_tmr_evt_data = [vm_id, Global_Curr_Sim_Clock]
    q_tmr_evt = clib.make_queue_event (timer,                               # timer (vm_scale_down_policy_timer_unit)
                                       g_my_block_id,                       # src block id (will be returned to src block)
                                       simgval.gBLOCK_SIM_EVENT_HANDLER,    # dst block id (event handler block)
                                       simgval.gEVT_SET_TIMER,              # evt code: set timer
                                       simgval.gEVT_SUB_VM_SCALE_DOWN,      # evt sub code: vm_scale_down
                                       q_tmr_evt_data)                      # event data = [vm_id]

    # send timer event to event handler
    send_scale_down_timer_event (vm_id, q_tmr_evt)

def run_SD_JAT_L2PR_policy (vm_id):

    state = get_VM_sd_policy_activated (vm_id)
    if state == True:

        kernel_sample_cnt = get_recent_sample_cnt()
        x = []
        y = get_recent_n_job_arrival_list (kernel_sample_cnt)
        v = len(y) + 1

        for n in xrange (v-1):
            x.append(n+1)

        # timer: Local Quadratic Regression for job arrival rate
        result, coefs = reg.regression (2, x, y, v)
        bounds = get_vm_wait_time_bounds()
        timer = validate_prediction_result (result, bounds)

        write_debug_log_for_regression_method (vm_id, timer, coefs, x, y, v)

    else:

        # timer: time to be idle. --> just wait for vm being idle.
        timer = estimate_next_idle_clock (vm_id)

    q_tmr_evt_data = [vm_id, Global_Curr_Sim_Clock]
    q_tmr_evt = clib.make_queue_event (timer,                               # timer (vm_scale_down_policy_timer_unit)
                                       g_my_block_id,                       # src block id (will be returned to src block)
                                       simgval.gBLOCK_SIM_EVENT_HANDLER,    # dst block id (event handler block)
                                       simgval.gEVT_SET_TIMER,              # evt code: set timer
                                       simgval.gEVT_SUB_VM_SCALE_DOWN,      # evt sub code: vm_scale_down
                                       q_tmr_evt_data)                      # event data = [vm_id]

    # send timer event to event handler
    send_scale_down_timer_event (vm_id, q_tmr_evt)

def run_SD_JAT_L3PR_policy (vm_id):

    state = get_VM_sd_policy_activated (vm_id)
    if state == True:

        kernel_sample_cnt = get_recent_sample_cnt()
        x = []
        y = get_recent_n_job_arrival_list (kernel_sample_cnt)
        v = len(y) + 1

        for n in xrange (v-1):
            x.append(n+1)

        # timer: Local Cubic Regression for job arrival rate
        result, coefs = reg.regression (3, x, y, v)
        bounds = get_vm_wait_time_bounds()
        timer = validate_prediction_result (result, bounds)

        write_debug_log_for_regression_method (vm_id, timer, coefs, x, y, v)

    else:

        # timer: time to be idle. --> just wait for vm being idle.
        timer = estimate_next_idle_clock (vm_id)

    q_tmr_evt_data = [vm_id, Global_Curr_Sim_Clock]
    q_tmr_evt = clib.make_queue_event (timer,                               # timer (vm_scale_down_policy_timer_unit)
                                       g_my_block_id,                       # src block id (will be returned to src block)
                                       simgval.gBLOCK_SIM_EVENT_HANDLER,    # dst block id (event handler block)
                                       simgval.gEVT_SET_TIMER,              # evt code: set timer
                                       simgval.gEVT_SUB_VM_SCALE_DOWN,      # evt sub code: vm_scale_down
                                       q_tmr_evt_data)                      # event data = [vm_id]

    # send timer event to event handler
    send_scale_down_timer_event (vm_id, q_tmr_evt)

def run_SD_JAT_WMA_policy (vm_id):

    state = get_VM_sd_policy_activated (vm_id)
    if state == True:

        kernel_sample_cnt = get_recent_sample_cnt()
        y = np.array(get_recent_n_job_arrival_list (kernel_sample_cnt))

        # timer: Weighted Moving Average
        result = int(math.ceil(ts.WMA(y)))
        bounds = get_vm_wait_time_bounds()
        timer = validate_prediction_result (result, bounds)

        write_debug_log_for_timeseries_method (vm_id, timer, result, y, "WMA")

    else:

        # timer: time to be idle. --> just wait for vm being idle.
        timer = estimate_next_idle_clock (vm_id)

    q_tmr_evt_data = [vm_id, Global_Curr_Sim_Clock]
    q_tmr_evt = clib.make_queue_event (timer,                               # timer (vm_scale_down_policy_timer_unit)
                                       g_my_block_id,                       # src block id (will be returned to src block)
                                       simgval.gBLOCK_SIM_EVENT_HANDLER,    # dst block id (event handler block)
                                       simgval.gEVT_SET_TIMER,              # evt code: set timer
                                       simgval.gEVT_SUB_VM_SCALE_DOWN,      # evt sub code: vm_scale_down
                                       q_tmr_evt_data)                      # event data = [vm_id]

    # send timer event to event handler
    send_scale_down_timer_event (vm_id, q_tmr_evt)

def run_SD_JAT_ES_policy (vm_id):

    state = get_VM_sd_policy_activated (vm_id)
    if state == True:

        kernel_sample_cnt = get_recent_sample_cnt()
        y = np.array(get_recent_n_job_arrival_list (kernel_sample_cnt))

        # timer: Exponential Smoothing
        alpha = get_alpha_ma()
        result = int(math.ceil(ts.EWMA(y, alpha))) # Exponential Smoothing == Exponential Weighted Moving Average
        bounds = get_vm_wait_time_bounds()
        timer = validate_prediction_result (result, bounds)

        write_debug_log_for_timeseries_method (vm_id, timer, result, y, "EWMA - alpha:" + str(alpha))

    else:

        # timer: time to be idle. --> just wait for vm being idle.
        timer = estimate_next_idle_clock (vm_id)

    q_tmr_evt_data = [vm_id, Global_Curr_Sim_Clock]
    q_tmr_evt = clib.make_queue_event (timer,                               # timer (vm_scale_down_policy_timer_unit)
                                       g_my_block_id,                       # src block id (will be returned to src block)
                                       simgval.gBLOCK_SIM_EVENT_HANDLER,    # dst block id (event handler block)
                                       simgval.gEVT_SET_TIMER,              # evt code: set timer
                                       simgval.gEVT_SUB_VM_SCALE_DOWN,      # evt sub code: vm_scale_down
                                       q_tmr_evt_data)                      # event data = [vm_id]

    # send timer event to event handler
    send_scale_down_timer_event (vm_id, q_tmr_evt)

def run_SD_JAT_HWDES_policy (vm_id):

    state = get_VM_sd_policy_activated (vm_id)
    if state == True:

        kernel_sample_cnt = get_recent_sample_cnt()
        y = np.array(get_recent_n_job_arrival_list (kernel_sample_cnt))

        # timer: Holt Winters Double Exponential Smoothing
        alpha = get_alpha_ma()
        beta = get_beta_ma()
        result = int(math.ceil(ts.Holt_Winters_Double_EWMA(y, alpha, beta))) # Holt Winters Double Exponential Smoothing (alpha and beta are required)
        bounds = get_vm_wait_time_bounds()
        timer = validate_prediction_result (result, bounds)

        write_debug_log_for_timeseries_method (vm_id, timer, result, y, "Hot Winters Double EWMA - alpha:" + str(alpha) + ", beta:" + str(beta))

    else:

        # timer: time to be idle. --> just wait for vm being idle.
        timer = estimate_next_idle_clock (vm_id)

    q_tmr_evt_data = [vm_id, Global_Curr_Sim_Clock]
    q_tmr_evt = clib.make_queue_event (timer,                               # timer (vm_scale_down_policy_timer_unit)
                                       g_my_block_id,                       # src block id (will be returned to src block)
                                       simgval.gBLOCK_SIM_EVENT_HANDLER,    # dst block id (event handler block)
                                       simgval.gEVT_SET_TIMER,              # evt code: set timer
                                       simgval.gEVT_SUB_VM_SCALE_DOWN,      # evt sub code: vm_scale_down
                                       q_tmr_evt_data)                      # event data = [vm_id]

    # send timer event to event handler
    send_scale_down_timer_event (vm_id, q_tmr_evt)

def run_SD_JAT_BRDES_policy (vm_id):

    state = get_VM_sd_policy_activated (vm_id)
    if state == True:

        kernel_sample_cnt = get_recent_sample_cnt()
        y = np.array(get_recent_n_job_arrival_list (kernel_sample_cnt))

        # timer: Brown Double Exponential Smoothing
        alpha = get_alpha_ma()
        result = int(math.ceil(ts.Brown_Double_EWMA(y, alpha))) # Brown Double EWMA - only requires alpha
        bounds = get_vm_wait_time_bounds()
        timer = validate_prediction_result (result, bounds)

        write_debug_log_for_timeseries_method (vm_id, timer, result, y, "Brown Double EWMA - alpha:" + str(alpha))

    else:

        # timer: time to be idle. --> just wait for vm being idle.
        timer = estimate_next_idle_clock (vm_id)

    q_tmr_evt_data = [vm_id, Global_Curr_Sim_Clock]
    q_tmr_evt = clib.make_queue_event (timer,                               # timer (vm_scale_down_policy_timer_unit)
                                       g_my_block_id,                       # src block id (will be returned to src block)
                                       simgval.gBLOCK_SIM_EVENT_HANDLER,    # dst block id (event handler block)
                                       simgval.gEVT_SET_TIMER,              # evt code: set timer
                                       simgval.gEVT_SUB_VM_SCALE_DOWN,      # evt sub code: vm_scale_down
                                       q_tmr_evt_data)                      # event data = [vm_id]

    # send timer event to event handler
    send_scale_down_timer_event (vm_id, q_tmr_evt)

def run_SD_JAT_AR_policy (vm_id):

    state = get_VM_sd_policy_activated (vm_id)
    if state == True:

        kernel_sample_cnt = get_recent_sample_cnt()
        y = np.array(get_recent_n_job_arrival_list (kernel_sample_cnt))

        # timer: Brown Double Exponential Smoothing
        p = get_AR_order()

        # AR restriction: order p should be less than nobs
        if p >= y.size:
            p = y.size-1

        ar_result = None
        while True:
            print "[Broker__] %s VM (ID:%d) AR(%d), Samples =>" % (str(Global_Curr_Sim_Clock), vm_id, p), y
            result = ts.AR(y, p) # Autoregressive requires p order for AR(p)
            if math.isnan(result) == False and result > 0:
                ar_result = int(math.ceil(result))
                break
            else:
                p -= 1
                if p < 1:
                    g_log_handler.error("[Broker__] %s VM (ID:%d) AR(%d) Error!, Samples => %s" % (str(Global_Curr_Sim_Clock), vm_id, p, y))
                    ar_result = 1

        bounds = get_vm_wait_time_bounds()
        timer = validate_prediction_result (ar_result, bounds)

        write_debug_log_for_timeseries_method (vm_id, timer, ar_result, y, "AR (" + str(p) + ")")

    else:

        # timer: time to be idle. --> just wait for vm being idle.
        timer = estimate_next_idle_clock (vm_id)

    q_tmr_evt_data = [vm_id, Global_Curr_Sim_Clock]
    q_tmr_evt = clib.make_queue_event (timer,                               # timer (vm_scale_down_policy_timer_unit)
                                       g_my_block_id,                       # src block id (will be returned to src block)
                                       simgval.gBLOCK_SIM_EVENT_HANDLER,    # dst block id (event handler block)
                                       simgval.gEVT_SET_TIMER,              # evt code: set timer
                                       simgval.gEVT_SUB_VM_SCALE_DOWN,      # evt sub code: vm_scale_down
                                       q_tmr_evt_data)                      # event data = [vm_id]

    # send timer event to event handler
    send_scale_down_timer_event (vm_id, q_tmr_evt)

def run_SD_JAT_ARMA_policy (vm_id):

    state = get_VM_sd_policy_activated (vm_id)
    if state == True:

        kernel_sample_cnt = get_recent_sample_cnt()
        y = np.array(get_recent_n_job_arrival_list (kernel_sample_cnt))

        # timer: Brown Double Exponential Smoothing
        p = get_AR_order()
        q = get_MA_order()

        arma_result = None
        while True:
            print "[Broker__] %s VM (ID:%d) ARMA(%d, %d), Samples =>" % (str(Global_Curr_Sim_Clock), vm_id, p, q), y
            result = ts.ARMA(y, p, q) # ARMA requires p order for AR(p) and q order for MA(q)
            if math.isnan(result) == False and result > 0:
                arma_result = int(math.ceil(result))
                break
            else:
                if p > q:
                    p -= 1
                else:
                    q -=1

                if p < 1 and q < 1:
                    
                    g_log_handler.error("[Broker__] %s VM (ID:%d) ARMA(%d, %d) Error!, Samples => %s" % (str(Global_Curr_Sim_Clock), vm_id, p, q, y))
                    arma_result = 1

        bounds = get_vm_wait_time_bounds()
        timer = validate_prediction_result (arma_result, bounds)

        write_debug_log_for_timeseries_method (vm_id, timer, arma_result, y, "ARMA (" + str(p) + ", " + str(q) + ")")

    else:

        # timer: time to be idle. --> just wait for vm being idle.
        timer = estimate_next_idle_clock (vm_id)

    q_tmr_evt_data = [vm_id, Global_Curr_Sim_Clock]
    q_tmr_evt = clib.make_queue_event (timer,                               # timer (vm_scale_down_policy_timer_unit)
                                       g_my_block_id,                       # src block id (will be returned to src block)
                                       simgval.gBLOCK_SIM_EVENT_HANDLER,    # dst block id (event handler block)
                                       simgval.gEVT_SET_TIMER,              # evt code: set timer
                                       simgval.gEVT_SUB_VM_SCALE_DOWN,      # evt sub code: vm_scale_down
                                       q_tmr_evt_data)                      # event data = [vm_id]

    # send timer event to event handler
    send_scale_down_timer_event (vm_id, q_tmr_evt)

def run_SD_JAT_ARIMA_policy (vm_id):

    state = get_VM_sd_policy_activated (vm_id)
    if state == True:

        kernel_sample_cnt = get_recent_sample_cnt()
        y = np.array(get_recent_n_job_arrival_list (kernel_sample_cnt))

        # timer: Brown Double Exponential Smoothing
        p = get_AR_order()
        q = get_MA_order()
        d = get_DF_order()

        arima_result = None
        while True:
            print "[Broker__] %s VM (ID:%d) ARIMA(%d, %d, %d), Samples =>" % (str(Global_Curr_Sim_Clock), vm_id, p, d, q), y
            result = ts.ARIMA(y, p, d, q) # ARIMA requires p order for AR(p), d order for Integrated, and q order for MA(q)
            if math.isnan(result) == False and result > 0:
                arima_result = int(math.ceil(result))
                break
            else:

                # order decreasing sequence: d => q => p
                if q >= d and q >= p:
                    q -= 1
                elif d >= p and d >= q:
                    d -= 1
                else:
                    p -= 1 
                if p < 1 and d < 1 and q < 1:

                    g_log_handler.error("[Broker__] %s VM (ID:%d) ARIMA(%d, %d, %d) Error!, Samples => %s" % (str(Global_Curr_Sim_Clock), vm_id, p, d, q, y))
                    arima_result = 1

        bounds = get_vm_wait_time_bounds()
        timer = validate_prediction_result (arima_result, bounds)

        write_debug_log_for_timeseries_method (vm_id, timer, arima_result, y, "ARIMA (" + str(p) + ", " + str(d) + ", " + str(q) + ")")

    else:

        # timer: time to be idle. --> just wait for vm being idle.
        timer = estimate_next_idle_clock (vm_id)

    q_tmr_evt_data = [vm_id, Global_Curr_Sim_Clock]
    q_tmr_evt = clib.make_queue_event (timer,                               # timer (vm_scale_down_policy_timer_unit)
                                       g_my_block_id,                       # src block id (will be returned to src block)
                                       simgval.gBLOCK_SIM_EVENT_HANDLER,    # dst block id (event handler block)
                                       simgval.gEVT_SET_TIMER,              # evt code: set timer
                                       simgval.gEVT_SUB_VM_SCALE_DOWN,      # evt sub code: vm_scale_down
                                       q_tmr_evt_data)                      # event data = [vm_id]

    # send timer event to event handler
    send_scale_down_timer_event (vm_id, q_tmr_evt)

# Scale Down Libraries
def estimate_next_idle_clock (vm_id):

    vm = get_VM_from_Running_VM_Instances(vm_id)
    if vm is None:
        g_log_handler.error("[Broker__] %s VM (ID:%d) not found in g_Running_VM_Instances" % (str(Global_Curr_Sim_Clock), vm_id))
        clib.logwrite_VM_Instance_Lists(g_log_handler, "[Broker__] " + str(Global_Curr_Sim_Clock), "g_Running_VM_Instances", g_Running_VM_Instances)
        clib.logwrite_VM_Instance_Lists(g_log_handler, "[Broker__] " + str(Global_Curr_Sim_Clock), "g_Stopped_VM_Instances", g_Stopped_VM_Instances)
        clib.sim_exit()

    est_idle_time = clib.get_sum_job_actual_duration(vm.job_queue) + 1

    if vm.current_job is None:
        if est_idle_time < 1:

            g_log_handler.error("[Broker__] %s VM (ID:%d) has no jobs to process - to debug!!!" % (str(Global_Curr_Sim_Clock), vm_id))
            clib.logwrite_vm_obj(g_log_handler, "[Broker__] " + str(Global_Curr_Sim_Clock), vm)
            clib.logwrite_VM_Instance_Lists(g_log_handler, "[Broker__] " + str(Global_Curr_Sim_Clock), "g_Running_VM_Instances", g_Running_VM_Instances)
            clib.logwrite_VM_Instance_Lists(g_log_handler, "[Broker__] " + str(Global_Curr_Sim_Clock), "g_Stopped_VM_Instances", g_Stopped_VM_Instances)
            clib.logwrite_JobList(g_log_handler, "[Broer__] " + str(Global_Curr_Sim_Clock), "g_Received_Jobs", g_Received_Jobs)
            clib.logwrite_JobList(g_log_handler, "[Broer__] " + str(Global_Curr_Sim_Clock), "g_Completed_Jobs", g_Completed_Jobs)
            clib.sim_exit()
    else:

        est_idle_time += (vm.current_job.act_duration_on_VM - (Global_Curr_Sim_Clock - vm.current_job_start_clock))

    return est_idle_time

def use_sd_policy_activated_flag (exception_flag = False):

    # this is special case for storage simulation added on 10/27/2014 -- i dont like this.
    if exception_flag == True and g_sim_conf.vm_scale_down_policy == simgval.gPOLICY_VM_SCALEDOWN_IMMEDIATE:
        return True

    if  g_sim_conf.vm_scale_down_policy == simgval.gPOLICY_VM_SCALEDOWN_STARTUPLAG or \
        g_sim_conf.vm_scale_down_policy == simgval.gPOLICY_VM_SCALEDOWN_JAT_MEAN or \
        g_sim_conf.vm_scale_down_policy == simgval.gPOLICY_VM_SCALEDOWN_JAT_MAX or \
        g_sim_conf.vm_scale_down_policy == simgval.gPOLICY_VM_SCALEDOWN_JAT_MEAN_RECENT or \
        g_sim_conf.vm_scale_down_policy == simgval.gPOLICY_VM_SCALEDOWN_JAT_MAX_RECENT or \
        g_sim_conf.vm_scale_down_policy == simgval.gPOLICY_VM_SCALEDOWN_JAT_SLR or \
        g_sim_conf.vm_scale_down_policy == simgval.gPOLICY_VM_SCALEDOWN_JAT_2PR or \
        g_sim_conf.vm_scale_down_policy == simgval.gPOLICY_VM_SCALEDOWN_JAT_3PR or \
        g_sim_conf.vm_scale_down_policy == simgval.gPOLICY_VM_SCALEDOWN_JAT_LLR or \
        g_sim_conf.vm_scale_down_policy == simgval.gPOLICY_VM_SCALEDOWN_JAT_L2PR or \
        g_sim_conf.vm_scale_down_policy == simgval.gPOLICY_VM_SCALEDOWN_JAT_L3PR or \
        g_sim_conf.vm_scale_down_policy == simgval.gPOLICY_VM_SCALEDOWN_JAT_WMA or \
        g_sim_conf.vm_scale_down_policy == simgval.gPOLICY_VM_SCALEDOWN_JAT_ES or \
        g_sim_conf.vm_scale_down_policy == simgval.gPOLICY_VM_SCALEDOWN_JAT_HWDES or \
        g_sim_conf.vm_scale_down_policy == simgval.gPOLICY_VM_SCALEDOWN_JAT_BRDES or \
        g_sim_conf.vm_scale_down_policy == simgval.gPOLICY_VM_SCALEDOWN_JAT_AR or \
        g_sim_conf.vm_scale_down_policy == simgval.gPOLICY_VM_SCALEDOWN_JAT_ARMA or \
        g_sim_conf.vm_scale_down_policy == simgval.gPOLICY_VM_SCALEDOWN_JAT_ARIMA:

        return True

    return False

def is_SD_affected_by_job_duration_change():

    if g_sim_conf.vm_scale_down_policy != simgval.gPOLICY_VM_SCALEDOWN_HOUR and g_sim_conf.vm_scale_down_policy != simgval.gPOLICY_VM_SCALEDOWN_MIN:
        return True

    return False

def send_scale_down_timer_event (vm_id, sd_tmr_evt):

    if use_sd_policy_activated_flag() == True and get_VM_sd_policy_activated(vm_id) == True:
        wait_time = sd_tmr_evt[1]

        #curframe = inspect.currentframe()
        #calframe = inspect.getouterframes(curframe, 2)
        #g_log_handler.info (str(Global_Curr_Sim_Clock) + "  %s" % (str(g_Job_Arrival_Rate)))
        #g_log_handler.info (str(Global_Curr_Sim_Clock) + "  =========================================================")
        #g_log_handler.info (str(Global_Curr_Sim_Clock) + "  Lib      : send_scale_down_timer_event")
        #g_log_handler.info (str(Global_Curr_Sim_Clock) + "  Param    : vm_id: %d, sd_tmr_evt: %s" % (vm_id, str(sd_tmr_evt)))
        #g_log_handler.info (str(Global_Curr_Sim_Clock) + "  Wait_Time: %d" % (wait_time))
        #g_log_handler.info (str(Global_Curr_Sim_Clock) + "  Caller   : %s, %s:%d" % (calframe[1][3], calframe[1][1], calframe[1][2]))
        #g_log_handler.info (str(Global_Curr_Sim_Clock) + "  =========================================================")

        update_VM_sd_wait_time (vm_id, wait_time)

    # send timer event to event handler
    #g_log_handler.info(str(Global_Curr_Sim_Clock) + '  EVT_SEND - EC:%s, ESC:%s, DST:%s, DATA: %s, EVT_MSG: %s'
    #         % (simgval.get_sim_code_str(sd_tmr_evt[4]), simgval.get_sim_code_str(sd_tmr_evt[5]),
    #            simgval.get_sim_code_str(sd_tmr_evt[3]), str(sd_tmr_evt[6]), str(sd_tmr_evt)))

    #g_log_handler.info(sd_tmr_evt)
    #g_log_handler.info(sd_tmr_evt[1])
    gQ_OUT.put (sd_tmr_evt)

def write_debug_log_for_regression_method (vm_id, timer, coefs, x, y, v):

        bounds = get_vm_wait_time_bounds()
        equation = reg.build_regression_equation(coefs)

        debug_logger = g_sim_conf.g_debug_log
        debug_logger.info (str(Global_Curr_Sim_Clock))
        debug_logger.info (str(Global_Curr_Sim_Clock) + '  -------------------------------------')
        debug_logger.info (str(Global_Curr_Sim_Clock) + '  Linear Regression (VM:%d)' % (vm_id))
        debug_logger.info (str(Global_Curr_Sim_Clock) + '  -------------------------------------')
        debug_logger.info (str(Global_Curr_Sim_Clock) + '  VM ID            : %d' % (vm_id))
        debug_logger.info (str(Global_Curr_Sim_Clock) + '  RECENT CNT       : %s' % ('ALL'))
        debug_logger.info (str(Global_Curr_Sim_Clock) + '  ALL ARRIVAL RATE : %s'   % (str(g_Job_Arrival_Rate)))
        debug_logger.info (str(Global_Curr_Sim_Clock) + '  X                : %s' % (str(x)))
        debug_logger.info (str(Global_Curr_Sim_Clock) + '  Y                : %s' % (str(y)))
        debug_logger.info (str(Global_Curr_Sim_Clock) + '  V                : %s' % (str(v)))
        debug_logger.info (str(Global_Curr_Sim_Clock) + '  RESULT (TIMER)   : %s' % (str(timer)))
        debug_logger.info (str(Global_Curr_Sim_Clock) + '  %s' % (equation))
        #debug_logger.info (str(Global_Curr_Sim_Clock) + '  Eq(X) = %0.2f*X^3 + %0.2f*X^2 + %0.2f*X + %0.2f' % (coefs[0], coefs[1], coefs[2], coefs[3]))

        debug_logger.info (str(Global_Curr_Sim_Clock) + '  MIN/MAX BOUND    : %s' % (bounds))
        debug_logger.info (str(Global_Curr_Sim_Clock) + '  -------------------------------------')
        debug_logger.info (str(Global_Curr_Sim_Clock))

def write_debug_log_for_timeseries_method (vm_id, timer, result, samples, title):

        bounds = get_vm_wait_time_bounds()

        debug_logger = g_sim_conf.g_debug_log
        debug_logger.info (str(Global_Curr_Sim_Clock))
        debug_logger.info (str(Global_Curr_Sim_Clock) + '  -------------------------------------')
        debug_logger.info (str(Global_Curr_Sim_Clock) + '  TimeSeries:%s (VM:%d)' % (title, vm_id))
        debug_logger.info (str(Global_Curr_Sim_Clock) + '  -------------------------------------')
        debug_logger.info (str(Global_Curr_Sim_Clock) + '  VM ID            : %d' % (vm_id))
        debug_logger.info (str(Global_Curr_Sim_Clock) + '  RECENT CNT       : %s' % ('ALL'))
        debug_logger.info (str(Global_Curr_Sim_Clock) + '  ALL ARRIVAL RATE : %s'   % (str(g_Job_Arrival_Rate)))
        debug_logger.info (str(Global_Curr_Sim_Clock) + '  Samples          : %s' % (str(samples.tolist())))
        debug_logger.info (str(Global_Curr_Sim_Clock) + '  RESULT           : %s' % (str(result)))
        debug_logger.info (str(Global_Curr_Sim_Clock) + '  Timer            : %s' % (str(timer)))
        debug_logger.info (str(Global_Curr_Sim_Clock) + '  MIN/MAX BOUND    : %s' % (bounds))
        debug_logger.info (str(Global_Curr_Sim_Clock) + '  -------------------------------------')
        debug_logger.info (str(Global_Curr_Sim_Clock))


def validate_prediction_result (prediction_result, bounds):

    min_bound = bounds[0]
    max_bound = bounds[1]

    # prediciton result is less than min sd wait time
    if prediction_result < min_bound:
        return min_bound
    elif prediction_result > max_bound:
        return max_bound

    return prediction_result
