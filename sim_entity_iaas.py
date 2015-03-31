import sys
import time
import inspect
import logging
import random
import math
from threading import Condition
import sim_global_variables as simgval
import sim_lib_common as clib
import sim_object as simobj
import sim_entity_cloudbroker as broker

Global_Curr_Sim_Clock = -99
Global_Curr_Sim_Clock_CV = Condition()

g_flag_sim_entity_term = False
g_flag_sim_entity_term_CV = Condition()

g_sim_trace_log_handler = None  # global var for sim trace log handler

g_my_block_id = None
g_log_handler = None
g_vm_log_handler = None
gQ_OUT = None
g_sim_conf = None

g_Creating_VMs = []
g_Creating_VMs_CV = Condition()

g_Running_VMs = []
g_Running_VMs_CV = Condition()

g_Stopped_VMs = []
g_Stopped_VMs_CV = Condition()

g_Activated_Storage_Containers = []
g_Activated_Storage_Containers_CV = Condition()

def set_log (path):
    logger = logging.getLogger('iaas_cloud')
    log_file = path + '/[' + str(g_my_block_id) + ']-iaas_cloud.log'

    # for file logger...
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

def set_vm_log (path):
    logger = logging.getLogger('vm_usage_log')
    log_file = path + '/[' + str(g_my_block_id) + ']-vm_usage_list.log'
    hdlr = logging.FileHandler(log_file)
    #hdlr = logging.FileHandler(path + '/vm_usage_list.log')
    formatter = logging.Formatter('%(message)s')
    hdlr.setFormatter(formatter)
    logger.addHandler(hdlr)
    logger.setLevel(logging.INFO)
    return logger

def set_iaas_storage_usage_report_log (path):
    logger = logging.getLogger('iaas_storage_usage_report_log')
    log_file = path + '/5.report_storage_usage.csv'
    hdlr = logging.FileHandler(log_file)
    #hdlr = logging.FileHandler(path + '/job_complete_list.log')
    formatter = logging.Formatter('%(message)s')
    hdlr.setFormatter(formatter)
    logger.addHandler(hdlr)
    logger.setLevel(logging.INFO)
    return logger

def set_iaas_network_usage_report_log (path):
    logger = logging.getLogger('iaas_network_usage_report_log')
    log_file = path + '/6.report_network_usage.csv'
    hdlr = logging.FileHandler(log_file)
    #hdlr = logging.FileHandler(path + '/job_complete_list.log')
    formatter = logging.Formatter('%(message)s')
    hdlr.setFormatter(formatter)
    logger.addHandler(hdlr)
    logger.setLevel(logging.INFO)
    return logger

def set_iaas_sim_trace_log (path):
    logger = logging.getLogger('storage_network_trace_log')
    log_file = path + '/2.report_simulation_trace_iaas.csv'
    hdlr = logging.FileHandler(log_file)
    #hdlr = logging.FileHandler(path + '/job_complete_list.log')
    formatter = logging.Formatter('%(message)s')
    hdlr.setFormatter(formatter)
    logger.addHandler(hdlr)
    logger.setLevel(logging.INFO)
    return logger


def set_iaas_sim_entity_term_flag (val):

    global g_flag_sim_entity_term
    if val is not True and val is not False:
        return

    if g_flag_sim_entity_term == val:
        return

    g_flag_sim_entity_term_CV.acquire()
    g_flag_sim_entity_term = val
    g_flag_sim_entity_term_CV.notify()
    g_flag_sim_entity_term_CV.release()


def iaas_check_sim_entity_termination ():
    no_creating_VMs = len (g_Creating_VMs)
    no_running_VMs  = len (g_Running_VMs)
    no_stopped_VMs  = len (g_Stopped_VMs)

    if no_creating_VMs < 1 and no_running_VMs < 1 and no_stopped_VMs > 0:

        g_log_handler.info("[IaaS____] %s IaaS can be terminated - Len(creating_VMs):%d, Len(running_VMs):%d, Len(stopped_VMs):%d" \
              %(str(Global_Curr_Sim_Clock), no_creating_VMs, no_running_VMs, no_stopped_VMs))
        set_iaas_sim_entity_term_flag (True)

        # sim entity termination procedure - send evt to ask sim event handler for termination
        query_evt = clib.make_queue_event (Global_Curr_Sim_Clock, g_my_block_id, simgval.gBLOCK_SIM_EVENT_HANDLER, simgval.gEVT_ASK_SIMENTITY_TERMINATION, None, None)
        gQ_OUT.put (query_evt)

    else:

        g_log_handler.info("[IaaS____] %s IaaS can NOT be terminated - Len(creating_VMs):%d, Len(running_VMs):%d, Len(stopped_VMs):%d" \
              %(str(Global_Curr_Sim_Clock), no_creating_VMs, no_running_VMs, no_stopped_VMs))

def write_vm_usage_log (vm):

    if vm.status != simgval.gVM_ST_TERMINATE:
        clib.display_vm_obj(Global_Curr_Sim_Clock, vm)
        clib.sim_exit()
        return

    vm_rt = simobj.get_vm_billing_clock(vm)
    job_rt = clib.get_sum_job_actual_duration (vm.job_history)

    vm_utilization = job_rt / float (vm_rt)
    stlag_rate = vm.startup_lag / float (vm_rt)
    idle_rate = 1 - vm_utilization - stlag_rate


    # CL    VMID  RT  CO  IID TY  ST  TC  TA  TT  SL  NJ  JR')
    g_vm_log_handler.info("%d\t%d\t%d\t$%s\t%s\t%s\t%s\t%d\t%d\t%d\t%d\t%d\t%d\t%0.3f\t%0.3f\t%0.3f"
                          % (Global_Curr_Sim_Clock, vm.id, vm_rt, str(vm.cost), vm.instance_id, vm.type_name,
                             simgval.get_sim_code_str(vm.status), vm.time_create, vm.time_active, vm.time_terminate,
                             vm.startup_lag, len(vm.job_history), job_rt, vm_utilization, stlag_rate, idle_rate))

def iaas_send_evt_ack (evt_id, evt_org_code):

    ack_evt = clib.make_queue_event (0, g_my_block_id, simgval.gBLOCK_SIM_EVENT_HANDLER, simgval.gEVT_ACK, evt_org_code, None)
    ack_evt[0] = evt_id
    gQ_OUT.put (ack_evt)

    return

def calculate_Startup_Lag ():
    return random.randint (g_sim_conf.min_startup_lag, g_sim_conf.max_startup_lag)

def get_vm_cost_and_billing_hour (vm_running_time, vm_unit_price):

    billing_hour = math.ceil (vm_running_time/float(g_sim_conf.billing_unit_clock))
    vm_cost = billing_hour * vm_unit_price
    return vm_cost,billing_hour

def calculate_vm_cost (vm_obj):

    vm_index = get_vm_index_from_List (3, vm_obj)

    if vm_index < 0:
        g_log_handler.error("[IaaS____] %s calculate_vm_cost Error! => Cannot Find VM (ID:%d, IID:%s, ST:%s) from Stopped VM List" \
                          % (str(Global_Curr_Sim_Clock), vm_obj.id, vm_obj.instance_id, simgval.get_sim_code_str(vm_obj.status)))
        clib.logwrite_VM_Instance_Lists(g_log_handler, "[IaaS____] " + str(Global_Curr_Sim_Clock), "Current Stopped VM Lists", g_Stopped_VMs)
        clib.sim_exit()


    # calculate vm running time
    vm_running_time = simobj.get_vm_billing_clock (vm_obj)
    if vm_running_time is None:
        g_log_handler.error("[IaaS____] %s calculate_vm_cost Error! => Invalid VM (ID:%d, IID:%s, ST:%s) for Cost Calculation" \
                          % (str(Global_Curr_Sim_Clock), vm_obj.id, vm_obj.instance_id, simgval.get_sim_code_str(vm_obj.status)))
        clib.logwrite_vm_obj(g_log_handler, "[IaaS____] " + str(Global_Curr_Sim_Clock), vm_obj)
        clib.sim_exit()

    vm_cost, billing_hour = get_vm_cost_and_billing_hour (vm_running_time, vm_obj.unit_price)
    simobj.set_VM_cost (vm_obj, vm_cost)

    g_log_handler.info("[IaaS____] %s BILLING COST for VM (ID:%d, TY:%s, PR:$%s, ST:%s, TC:%d, TT:%d) is $%f" \
          % (str(Global_Curr_Sim_Clock), vm_obj.id, vm_obj.type_name, vm_obj.unit_price, simgval.get_sim_code_str(vm_obj.status), vm_obj.time_create, vm_obj.time_terminate,vm_obj.cost))
    g_log_handler.info("[IaaS____] %s BILLING COST ($%f) = BILL_HR(%d) *  VM_PRICE(%f), BILL_HR = RNDUP(VM_RT(%d) / BTU(%d)" % (str(Global_Curr_Sim_Clock), vm_obj.cost, billing_hour, vm_obj.unit_price, vm_running_time, g_sim_conf.billing_unit_clock))

def get_vm_index_from_List (list_type, ref_vm):

    return_val = -1 # cannot find index

    if list_type == 1:
        global g_Creating_VMs
        global g_Creating_VMs_CV

        g_Creating_VMs_CV.acquire()
        for vm in g_Creating_VMs:
            if vm.id == ref_vm.id and vm.time_create == ref_vm.time_create and vm.time_active is None:
                return_val = g_Creating_VMs.index(vm)
                break
        g_Creating_VMs_CV.notify()
        g_Creating_VMs_CV.release()

    elif list_type == 2:
        global g_Running_VMs
        global g_Running_VMs_CV

        g_Running_VMs_CV.acquire()
        for vm in g_Running_VMs:
            if vm.id == ref_vm.id and vm.time_create == ref_vm.time_create and vm.time_active == ref_vm.time_active and vm.startup_lag == ref_vm.startup_lag:
                return_val = g_Running_VMs.index(vm)
                break
        g_Running_VMs_CV.notify()
        g_Running_VMs_CV.release()

    elif list_type == 3:
        global g_Stopped_VMs
        global g_Stopped_VMs_CV

        g_Stopped_VMs_CV.acquire()
        for vm in g_Stopped_VMs:
            if vm.id == ref_vm.id and vm.time_create == ref_vm.time_create and vm.time_active == ref_vm.time_active and vm.startup_lag == ref_vm.startup_lag and vm.time_terminate == ref_vm.time_terminate:
                return_val = g_Stopped_VMs.index(vm)
                break
        g_Stopped_VMs_CV.notify()
        g_Stopped_VMs_CV.release()

    else:
        g_log_handler.error("[IaaS____] %s get_vm_index_from_List Error! => Invalid List Type (%s) for VM Instance (ID:%d, IID:%s, TY:%s, PR:$%s, TC:%d)" \
                          % (str(Global_Curr_Sim_Clock), str(list_type), ref_vm.id, ref_vm.instance_id, ref_vm.type_name, ref_vm.unit_price, ref_vm.time_create))

    return return_val

def insert_VM_into_Creating_VMs (vm):
    global g_Creating_VMs
    global g_Creating_VMs_CV

    vm_index = get_vm_index_from_List (1, vm)

    if vm_index == -1:   # same vm not found
        g_Creating_VMs_CV.acquire()
        g_Creating_VMs.append (vm)
        vm_index = g_Creating_VMs.index (vm)
        g_Creating_VMs_CV.notify()
        g_Creating_VMs_CV.release()
    else:
        g_log_handler.error("[IaaS____] %s insert_VM_into_Creating_VMs Error! => Same VM Instance (ID:%d, IID:%s, TY:%s, PR:$%s, TC:%d) exists in g_Creating_VMs[%d]" \
                          % (str(Global_Curr_Sim_Clock), vm.id, vm.instance_id, vm.type_name, vm.unit_price, vm.time_create, vm_index))
        clib.sim_exit()

    #clib.logwrite_VM_Instance_Lists(g_log_handler, "[IaaS____] " + str(Global_Curr_Sim_Clock), "[INS] iaas_cloud.Creating_VMs", g_Creating_VMs)
    return vm_index

def extract_VM_from_Creating_VM_List (vm):
    global g_Creating_VMs
    global g_Creating_VMs_CV

    ret_val = None
    vm_index = get_vm_index_from_List (1, vm)

    if vm_index > -1:
        g_Creating_VMs_CV.acquire()
        vm_obj = g_Creating_VMs.pop(vm_index)
        ret_val = vm_obj
        g_Creating_VMs_CV.notify()
        g_Creating_VMs_CV.release()

    if vm_index == -1:
        g_log_handler.error("[IaaS____] %s extract_VM_from_Creating_VM_List Error! => VM Instance (ID:%d, IID:%s, TY:%s, PR:$%s, TC:%d, TA:%d) is NOT existed in g_Creating_VMs" \
              % (str(Global_Curr_Sim_Clock), vm.id, vm.instance_id, vm.type_name, vm.unit_price, vm.time_create, vm.time_active))

        clib.sim_exit()

    #clib.logwrite_VM_Instance_Lists(g_log_handler, "[IaaS____] " + str(Global_Curr_Sim_Clock), "[EXT] iaas_cloud.Creating_VMs", g_Creating_VMs)
    return ret_val

def insert_VM_into_Running_VMs (vm):
    global g_Running_VMs
    global g_Running_VMs_CV

    vm_index = get_vm_index_from_List (2, vm)

    if vm_index == -1:   # same vm not found -> ok insert
        g_Running_VMs_CV.acquire()
        g_Running_VMs.append (vm)
        vm_index = g_Running_VMs.index (vm)
        g_Running_VMs_CV.notify()
        g_Running_VMs_CV.release()

    else:
        g_log_handler.error("[IaaS____] %s insert_VM_into_Running_VMs Error! => Same VM Instance (ID:%d, IID:%s, TY:%s, PR:$%s, TC:%s, TA:%s, SD:%s) exists in g_Creating_VMs[%d]" \
              % (str(Global_Curr_Sim_Clock), vm.id, vm.instance_id, vm.type_name, vm.unit_price, str(vm.time_create), str(vm.time_active), str(vm.startup_lag), vm_index))
        clib.sim_exit()

    #clib.logwrite_VM_Instance_Lists(g_log_handler, "[IaaS____] " + str(Global_Curr_Sim_Clock), "[INS] iaas_cloud.g_Running_VMs", g_Running_VMs)
    return vm_index

def extract_VM_from_Running_VM_List (vm):

    global g_Running_VMs
    global g_Running_VMs_CV

    ret_val = None
    vm_index = get_vm_index_from_List (2, vm)

    if vm_index > -1:
        g_Running_VMs_CV.acquire()
        vm_obj = g_Running_VMs.pop(vm_index)
        ret_val = vm_obj
        g_Running_VMs_CV.notify()
        g_Running_VMs_CV.release()

    if vm_index == -1:
        g_log_handler.error("[IaaS____] %s extract_VM_from_Running_VM_List Error! => VM Instance (ID:%d, IID:%s, TY:%s, PR:$%s, TC:%d, TE:%d) is NOT existed in g_Running_VMs" \
              % (str(Global_Curr_Sim_Clock), vm.id, vm.instance_id, vm.type_name, vm.unit_price, vm.time_create, vm.time_terminate))
        clib.sim_exit()

    #clib.logwrite_VM_Instance_Lists(g_log_handler, "[IaaS____] " + str(Global_Curr_Sim_Clock), "[EXT] iaas_cloud.Running_VMs", g_Running_VMs)
    return ret_val

def insert_VM_into_Stopped_VMs (vm):
    global g_Stopped_VMs
    global g_Stopped_VMs_CV

    vm_index = get_vm_index_from_List (3, vm)

    if vm_index == -1:   # same vm not found -> ok insert
        g_Stopped_VMs_CV.acquire()
        g_Stopped_VMs.append (vm)
        vm_index = g_Stopped_VMs.index (vm)
        g_Stopped_VMs_CV.notify()
        g_Stopped_VMs_CV.release()

    else:
        g_log_handler.error("[IaaS____] %s insert_VM_into_Stopped_VMs Error! => Same VM Instance (ID:%s, IID:%s, TY:%s, PR:$%s, TC:%s, TA:%s, TT:%s, SD:%s) exists in g_Creating_VMs[%s]" \
                          % (str(Global_Curr_Sim_Clock), vm.id, vm.instance_id, vm.type_name, vm.unit_price, str(vm.time_create), str(vm.time_active), str(vm.time_terminate), str(vm.startup_lag), vm_index))
        clib.sim_exit()

    #clib.logwrite_VM_Instance_Lists(g_log_handler, "[IaaS____] " + str(Global_Curr_Sim_Clock), "[INS] iaas_cloud.g_Stopped_VMs", g_Stopped_VMs)
    return vm_index

# this is added for storage simulation especially for processing finer grained job model
def get_job_sub_runtime (job_obj, job_sub_exec_code):

    job_sub_runtime = 0

    if job_sub_exec_code == simgval.gEVT_SUB_JOB_IN_CPU_OP_COMPLETE:

        if job_obj.use_input_file == True:
            job_sub_runtime += job_obj.act_nettime_on_VM[0]

        job_sub_runtime += job_obj.act_cputime_on_VM

    elif job_sub_exec_code == simgval.gEVT_SUB_JOB_OUT_OP_COMPLETE:

        if job_obj.use_output_file == True:
            job_sub_runtime += job_obj.act_nettime_on_VM[1]
        else:
            clib.sim_exit()

    elif job_sub_exec_code == simgval.gEVT_SUB_JOB_FAILED:

        if job_obj.job_failure_recovered == False:
            if job_obj.use_input_file == True:
                job_sub_runtime += job_obj.act_nettime_on_VM[0]

            job_sub_runtime += job_obj.vm_job_failure_time

        else:

            clib.sim_exit()

    else:
        job_str = simobj.get_job_info_str(job_obj)
        g_log_handler.error("[IaaS____] %s get_job_sub_runtime Error! => Invalid Job Sub Exec Code (%s) for %s" \
                          % (str(Global_Curr_Sim_Clock), simgval.get_sim_code_str(job_sub_exec_code), job_str))
        clib.sim_exit()

    return job_sub_runtime

def get_VM_type_obj_by_type_name (type_name):

    for vm_type in g_sim_conf.vm_types:
        if vm_type.vm_type_name == type_name:
            return vm_type

    curframe = inspect.currentframe()
    callframe = inspect.getouterframes(curframe, 2)
    callfile = callframe[1][1].split("/")[-1]

    g_log_handler.error("[IaaS____] %s get_VM_type_obj_by_type_name Error! - Cannot find VM type obj for %s type VM - Caller:%s, %s:%d" %(str(Global_Curr_Sim_Clock), type_name, callframe[1][3], callfile, callframe[1][2]))
    clib.sim_exit()

#########################################################################################
#
# related to cloud storage
#
#########################################################################################

def get_kb_storage_max_capacity ():

    return g_sim_conf.max_capacity_of_storage * 1024

def get_max_num_of_storage_containers():

    return g_sim_conf.max_num_of_storage_containers

def get_currently_completed_cloud_storage_capacity ():

    total_storage_capacity = 0 # unit kb
    for s in g_Activated_Storage_Containers:

        storage_container_capacity = 0
        for sc_file_object in s.sc_file_objects:

            if sc_file_object.sfo_status == simgval.gSFO_ST_FILE_UPLOAD_COMPLETED:
                storage_container_capacity += sc_file_object.sfo_size

        total_storage_capacity += storage_container_capacity

    return total_storage_capacity

def get_current_cloud_storage_capacity ():

    total_storage_capacity = 0 # unit kb
    for s in g_Activated_Storage_Containers:

        storage_container_capacity = 0
        for sc_file_object in s.sc_file_objects:

            if sc_file_object.sfo_status == simgval.gSFO_ST_FILE_UPLOAD_START or sc_file_object.sfo_status == simgval.gSFO_ST_FILE_UPLOAD_COMPLETED:
                storage_container_capacity += sc_file_object.sfo_size

        if storage_container_capacity == s.sc_usage_volume:
            total_storage_capacity += storage_container_capacity
        else:
            g_log_handler.error("[IaaS____] %s get_current_cloud_storage_capacity Error! - Storage Container (ID:%s, CR:%s, ST:%s, CT:%s, PR:$%s) Volume Mismatch - SC (%s kb) vs CAL (%s kb)" \
                  % (str(Global_Curr_Sim_Clock), s.sc_id, s.sc_creator, simgval.get_sim_code_str(s.sc_status), s.sc_create_time, s.sc_usage_price, s.sc_usage_volume, storage_container_capacity))
            clib.sim_exit()

    return total_storage_capacity

def get_total_number_of_current_SFOs ():

    num_of_sfos = 0
    for s in g_Activated_Storage_Containers:
        num_of_sfos += len(s.sc_file_objects)

    return num_of_sfos

def get_total_num_of_active_storage_containers ():

    return len(g_Activated_Storage_Containers)

def create_storage_container (creator, region):

    new_sc_uid = simobj.generate_SC_UID ()

    new_sc_obj = simobj.Storage_Container (new_sc_uid, creator, region, Global_Curr_Sim_Clock)
    new_sc_obj.set_sc_permission (simgval.gSC_PERMISSION_PUBLIC)

    # insert SC into g_activated cloud storage list
    insert_SC_into_Activated_Storage_Containers (new_sc_obj)

    return new_sc_obj

def get_most_recent_storage_container ():

    return g_Activated_Storage_Containers[-1]

def insert_SC_into_Activated_Storage_Containers (sc_obj):

    global g_Activated_Storage_Containers
    global g_Activated_Storage_Containers_CV

    existing_sc_index = get_Index_from_activated_storage_container (sc_obj.sc_id)
    if existing_sc_index < 0:

        g_Activated_Storage_Containers_CV.acquire()
        g_Activated_Storage_Containers.append (sc_obj)
        g_Activated_Storage_Containers_CV.notify()
        g_Activated_Storage_Containers_CV.release()

    else:

        existing_sco = g_Activated_Storage_Containers[existing_sc_index]
        g_log_handler.error("[IaaS____] %s insert_SC_into_Activated_Storage_Containers Error! => Same SC Obj %s exists in g_Activated_Storage_Containers [%d]" % (str(Global_Curr_Sim_Clock), existing_sco.get_infos(), existing_sc_index))
        clib.sim_exit()

def get_Index_from_activated_storage_container (sc_id):

    global g_Activated_Storage_Containers

    for sc in g_Activated_Storage_Containers:
        if sc.sc_id == sc_id:
            return g_Activated_Storage_Containers.index (sc)

    return -1

def get_SC_from_activated_storage_container (sc_id):

    global g_Activated_Storage_Containers

    sc_obj = None
    for sc in g_Activated_Storage_Containers:
        if sc.sc_id == sc_id and sc.sc_status == simgval.gSC_ST_CREATE:
            return sc

    return sc_obj

def get_SFO_from_storage_container (sc_obj, sfo_id):

    for sfo in sc_obj.sc_file_objects:
        if sfo.sfo_id == sfo_id:
            return sfo

    return None

def error_check_for_data_update_to_cloud_storage (job_obj):

    job_str = simobj.get_job_info_str(job_obj)

    # job status must be "Started"
    if job_obj.status != simgval.gJOB_ST_STARTED:
        g_log_handler.error("[IaaS____] %s upload_data_to_cloud_storage Error! - Job Status Error - %s" % (str(Global_Curr_Sim_Clock), job_str))
        clib.sim_exit()

    # job's use_out_file must TRUE
    if job_obj.use_output_file != True:
        g_log_handler.error("[IaaS____] %s upload_data_to_cloud_storage Error! - %s not having output file to store in Cloud Storage - use_output_file (%s)" % (str(Global_Curr_Sim_Clock), job_str, job_obj.use_output_file))
        clib.sim_exit()

    # job's output_file_flow_direction must gOFTD_IC (OUTPUT FILE DIRECTION IN-CLOUD)
    if job_obj.output_file_flow_direction != simgval.gOFTD_IC:
        g_log_handler.error("[IaaS____] %s upload_data_to_cloud_storage Error! - %s output file shouldn't be stored in Cloud Storage - output_file_flow_direction (%s)" % (str(Global_Curr_Sim_Clock), job_str, simgval.get_sim_code_str(job_obj.output_file_flow_direction)))
        clib.sim_exit()

    # job's output_file_size should be > 0 kb
    if job_obj.output_file_size <= 0:
        g_log_handler.error("[IaaS____] %s upload_data_to_cloud_storage Error! - %s output file size error! - output_file_size (%s KB)" % (str(Global_Curr_Sim_Clock), job_str, job_obj.output_file_size))
        clib.sim_exit()

    curr_vm_type = get_VM_type_obj_by_type_name (job_obj.VM_type)
    data_transfer_duration = broker.cal_actual_network_duration_on_VMs (curr_vm_type.vm_type_net_factor, simgval.gJOBFILE_OUTPUT, job_obj.output_file_flow_direction, job_obj.output_file_size)

    if job_obj.act_nettime_on_VM[1] != data_transfer_duration:
        g_log_handler.error("[IaaS____] %s upload_data_to_cloud_storage Error! - %s Data Transfer Time Mismatch! - job(%s sec) vs. cal(%s sec)" \
              %(str(Global_Curr_Sim_Clock), job_str, job_obj.act_nettime_on_VM[1], data_transfer_duration))
        clib.sim_exit()

# storage simulation libs - created on 10/26/2014
def upload_data_to_cloud_storage (job_obj):

    job_str = simobj.get_job_info_str(job_obj)

    # this is for error detection.
    error_check_for_data_update_to_cloud_storage (job_obj)

    curr_cloud_storage_capacity = get_current_cloud_storage_capacity ()
    max_cloud_storage_capacity = get_kb_storage_max_capacity ()
    max_cap_of_sc = get_max_num_of_storage_containers()

    # storage_container
    storage_container = None
    data_transfer_size = 0
    sfo_data_status = None

    if max_cloud_storage_capacity >= curr_cloud_storage_capacity + job_obj.output_file_size:
        data_transfer_size  = job_obj.output_file_size
    else:
        data_transfer_size  = max_cloud_storage_capacity - curr_cloud_storage_capacity

    if data_transfer_size > 0:

        num_act_containers = get_total_num_of_active_storage_containers ()
        if num_act_containers > max_cap_of_sc:

            g_log_handler.error("[IaaS____] %s upload_data_to_cloud_storage Error! - Current Num of Act Storage Containers (%d) exceeds Max Cap of Storage Containers (%d) - Job Infos:%s" % (str(Global_Curr_Sim_Clock), num_act_containers, max_cap_sc, job_str))
            clib.sim_exit()

        if num_act_containers == 0:
            # create new one
            storage_container = create_storage_container (job_obj.id, None)
        else:
            # use most recent one
            storage_container = get_most_recent_storage_container ()

        if storage_container is None:
            g_log_handler.error("[IaaS____] %s upload_data_to_cloud_storage Error! - Cannot find/create a proper Storage Container for %s" \
                  %(str(Global_Curr_Sim_Clock), job_str))
            clib.sim_exit()

        sfo_id = storage_container.upload_file_start (job_obj.id, Global_Curr_Sim_Clock, data_transfer_size, job_obj.output_file_size)

        return data_transfer_size, storage_container.sc_id, sfo_id

    elif data_transfer_size == 0:

        # no capacity remained.
        data_transfer_size = 0
        return data_transfer_size, None, None

    else:

        g_log_handler.error("[IaaS____] %s upload_data_to_cloud_storage Error! - Current Storage Volume exceeds (%s KB) Max Capacity of Storage (%s KB)" % (str(Global_Curr_Sim_Clock), curr_cloud_storage_capacity, max_cloud_storage_capacity))
        clib.sim_exit()


def proc_file_transfer_to_cloud_storage (job_obj):

    job_str = simobj.get_job_info_str(job_obj)
    # temp validation - this might be changed at some point. 10/27/2014
    # =================================================================================================================================
    if job_obj.use_output_file is not True or job_obj.output_file_flow_direction != simgval.gOFTD_IC or job_obj.output_file_size <= 0:
        job_str = simobj.get_job_info_str(job_obj)
        g_log_handler.error("[IaaS____] %s upload_data_to_cloud_storage Error! - Invalid Job Object (%s) for File Transfer: UOF (%s), OFFD (%s), OFS (%s KB)" % (str(Global_Curr_Sim_Clock), job_str, job_obj.use_output_file, job_obj.output_file_flow_direction, job_obj.output_file_size))
        clib.sim_exit()
    # =================================================================================================================================

    data_transfer_size, sc_id, sfo_id = upload_data_to_cloud_storage (job_obj)
    full_data_transfer = True

    if data_transfer_size == job_obj.output_file_size:

        q_ft_tmr_time = job_obj.act_nettime_on_VM[1]
        simobj.set_Job_output_file_transfer_infos (job_obj, simgval.gSFO_DST_FULL_UPLOAD, job_obj.output_file_size)
        full_data_transfer = True

    elif data_transfer_size < job_obj.output_file_size:

        curr_vm_type = get_VM_type_obj_by_type_name (job_obj.VM_type)
        q_ft_tmr_time = broker.cal_actual_network_duration_on_VMs (curr_vm_type.vm_type_net_factor, simgval.gJOBFILE_OUTPUT, job_obj.output_file_flow_direction, data_transfer_size)

        updated_act_nettime_on_VM = [job_obj.act_nettime_on_VM[0], q_ft_tmr_time]

        simobj.set_Job_act_nettime_on_VM (job_obj, updated_act_nettime_on_VM)
        simobj.set_Job_output_file_transfer_infos (job_obj, simgval.gSFO_DST_PARTIAL_UPLOAD, data_transfer_size)
        simobj.adjust_Job_act_duration_on_VM (job_obj)

        # full data transfer is False because data_transfer_size < job_obj.output_file_size:
        full_data_transfer = False

    else:

        g_log_handler.error ("[IaaS____] %s proc_file_transfer_to_cloud_storage Error! - Data Transfer Size (%s KB) to Cloud Storage exceeds File Size (%s KB) - %s" \
              %(str(Global_Curr_Sim_Clock), data_transfer_size, job_obj.output_file_size, job_str))
        clib.sim_exit()

    # file transfer completion timer.
    if q_ft_tmr_time > 0:

        q_ft_tmr_evt_data = [sc_id, sfo_id, job_obj.id, job_obj.output_file_transfer_size]
        q_ft_tmr_evt = clib.make_queue_event (  q_ft_tmr_time,                           # timer (job actual duration)
                                                g_my_block_id,                           # src block id (will be returned to src block)
                                                simgval.gBLOCK_SIM_EVENT_HANDLER,        # dst block id (event handler block)
                                                simgval.gEVT_SET_TIMER,                  # evt code: set timer
                                                simgval.gEVT_SUB_FILE_TRANSFER_COMPLETE_CLOUD_STORAGE,
                                                q_ft_tmr_evt_data)

        gQ_OUT.put (q_ft_tmr_evt)

    return True if data_transfer_size > 0 else False, full_data_transfer

# related to Event Processing - this execute job until cpu_duration
def iaas_evt_start_job_processing (q_msg):

    # need to think of basic procedure of this ...
    # [a] read the first job from vm's job queue
    # [b] assign the job to vm's current job
    # [c] job status change to gJOB_ST_STARTED
    # [d] assign current clock to job's time_start
    # [e] read duration and register timer event
    # [f] send ack to event handler

    evt_id          = q_msg[0] # EVT_ID
    evt_code        = q_msg[4] # EVT_CODE
    evt_data        = q_msg[6] # EVT_DATA

    if evt_code != simgval.gEVT_START_JOB:
        g_log_handler.error("[IaaS____] %s EVT(%s) ERROR! - INVALID EVT_CODE (%s) => %s " \
              %(str(Global_Curr_Sim_Clock), simgval.get_sim_code_str(simgval.gEVT_START_JOB), simgval.get_sim_code_str(evt_code), q_msg))
        clib.sim_exit()

    job_id = evt_data[0]
    vm = evt_data[1]

    # vm can process a new job if its status is active
    if vm.status != simgval.gVM_ST_ACTIVE:
        g_log_handler.error("[IaaS____] %s EVT (%s) ERROR! - VM(%d)'s Status (%s) is NOT Active! => %s" \
              %(str(Global_Curr_Sim_Clock), simgval.get_sim_code_str(evt_code), vm.id, simgval.get_sim_code_str(vm.status), q_msg))
        clib.sim_exit()

    # vm can process a new job if its current job is None
    if vm.current_job is not None:

        if vm.current_job.id == job_id and vm.current_job.status == simgval.gJOB_ST_STARTED:
            # this is error case that iaas gets duplicated events from broker but ok
            g_log_handler.error("[IaaS____] %s EVT (%s) ERROR! - VM(%d) gets Duplicated %s Start Event! => %s" \
                  %(str(Global_Curr_Sim_Clock), simgval.get_sim_code_str(evt_code), vm.id, simobj.get_job_info_str(vm.current_job), q_msg))
            clib.logwrite_vm_obj(g_log_handler, Global_Curr_Sim_Clock, vm)

            # clib.sim_long_sleep()
            # ttt need to be improved
            clib.sim_exit()

            # [f] send ack event to event handler
            iaas_send_evt_ack(evt_id, evt_code)
            return
        else:

            g_log_handler.error("[IaaS____] %s EVT (%s) ERROR! - VM(%d) has current Running %s! => %s" \
                  %(str(Global_Curr_Sim_Clock), simgval.get_sim_code_str(evt_code), vm.id, simobj.get_job_info_str(vm.current_job), q_msg))
            clib.sim_exit()

    # [a] read the first job from vm's job queue
    job = simobj.get_job_from_VM_job_queue (vm, job_id)
    if job is None:
        g_log_handler.error("[IaaS____] %s EVT (%s) ERROR! - VM (ID:%d) Job Queue has been corrupted! => %s" \
              %(str(Global_Curr_Sim_Clock), simgval.get_sim_code_str(evt_code), vm.id, vm.job_queue))
        clib.logwrite_vm_obj(g_log_handler, "[IaaS____] " + str(Global_Curr_Sim_Clock), vm)
        clib.sim_exit()

    # [b] assign the job to vm's current job
    upd_res = simobj.set_VM_current_job (Global_Curr_Sim_Clock, vm, job)
    if upd_res is False:

        clib.display_vm_obj(Global_Curr_Sim_Clock, vm)
        clib.display_job_obj(Global_Curr_Sim_Clock, job)

        g_log_handler.error("[IaaS____] %s EVT (%s) ERROR! - VM (ID:%d) has current running job (%d, %s, %d, %d)!" \
             %(str(Global_Curr_Sim_Clock), simgval.get_sim_code_str(evt_code), vm.id, vm.current_job.id, vm.current_job.name, vm.current_job.act_duration_on_VM, vm.current_job.deadline))
        clib.sim_exit()

    # [c] job status change to gJOB_ST_STARTED
    simobj.set_Job_status (job, simgval.gJOB_ST_STARTED)

    # [d] assign current clock to job's time_start
    simobj.set_Job_time_start (job, Global_Curr_Sim_Clock)

    job_str = simobj.get_job_info_str(job)
    print "[IaaS____] %s Actual Duration for %s on VM (%d, %s, $%s, %s) : %d (CPU:%s, I/O:%s) sec." \
          % (str(Global_Curr_Sim_Clock), job_str, vm.id, vm.type_name, vm.unit_price, simgval.get_sim_code_str(vm.status), job.act_duration_on_VM, job.act_cputime_on_VM, job.act_nettime_on_VM)

    # [e] read duration and register timer event
    q_tmr_evt_data = [job.id]
    q_tmr_evt_data.append(vm)

    if job.job_failure_recovered == True:
        q_tmr_evt_sub_code = simgval.gEVT_SUB_JOB_IN_CPU_OP_COMPLETE
        q_tmr_time = get_job_sub_runtime (job, q_tmr_evt_sub_code)
        q_tmr_evt = clib.make_queue_event (q_tmr_time,                              # timer (job actual duration)
                                           g_my_block_id,                           # src block id (will be returned to src block)
                                           simgval.gBLOCK_SIM_EVENT_HANDLER,        # dst block id (event handler block)
                                           simgval.gEVT_SET_TIMER,                  # evt code: set timer
                                           q_tmr_evt_sub_code,                      # evt sub code: job exec complete
                                           q_tmr_evt_data)                          # event data = [job_id, vm_obj]
    else:

        # job failure happens
        q_tmr_evt_sub_code = simgval.gEVT_SUB_JOB_FAILED
        q_tmr_time = get_job_sub_runtime (job, q_tmr_evt_sub_code)
        q_tmr_evt = clib.make_queue_event (q_tmr_time,                              # timer (job actual duration)
                                           g_my_block_id,                           # src block id (will be returned to src block)
                                           simgval.gBLOCK_SIM_EVENT_HANDLER,        # dst block id (event handler block)
                                           simgval.gEVT_SET_TIMER,                  # evt code: set timer
                                           q_tmr_evt_sub_code,                      # evt sub code: job exec complete
                                           q_tmr_evt_data)                          # event data = [job_id, vm_obj]


    # send timer event to event handler
    gQ_OUT.put (q_tmr_evt)

    # [f] send ack event to event handler
    iaas_send_evt_ack(evt_id, evt_code)

# this lib is related to restart failured job.
def iaas_evt_restart_job_processing (q_msg):

    # need to think of basic procedure of this ...
    # [a] read the first job from vm's job queue
    # [b] assign the job to vm's current job
    # [c] job status change to gJOB_ST_STARTED
    # [d] assign current clock to job's time_start
    # [e] read duration and register timer event
    # [f] send ack to event handler

    evt_id          = q_msg[0] # EVT_ID
    evt_code        = q_msg[4] # EVT_CODE
    evt_data        = q_msg[6] # EVT_DATA

    if evt_code != simgval.gEVT_RESTART_JOB:
        g_log_handler.error("[IaaS____] %s EVT(%s) ERROR! - INVALID EVT_CODE (%s) => %s" \
              %(str(Global_Curr_Sim_Clock), simgval.get_sim_code_str(simgval.gEVT_START_JOB), simgval.get_sim_code_str(evt_code), q_msg))
        clib.sim_exit()

    job_id = evt_data[0]
    vm = evt_data[1]

    # vm can process a new job if its status is active
    if vm.status != simgval.gVM_ST_ACTIVE:
        g_log_handler.error ("[IaaS____] %s EVT (%s) ERROR! - VM(%d)'s Status (%s) is NOT Active! => %s" \
              %(str(Global_Curr_Sim_Clock), simgval.get_sim_code_str(evt_code), vm.id, simgval.get_sim_code_str(vm.status), q_msg))
        clib.sim_exit()

    # vm can process a new job if its current job is None
    if vm.current_job is None:

        # this is error case that iaas gets duplicated events from broker but ok
        g_log_handler.error("[IaaS____] %s EVT (%s) ERROR! - VM(%d) should HAVE current_job! => %s" \
              %(str(Global_Curr_Sim_Clock), simgval.get_sim_code_str(evt_code), vm.id, q_msg))
        clib.logwrite_vm_obj(g_log_handler, "[IaaS____] " + str(Global_Curr_Sim_Clock), vm)
        clib.sim_exit()

    else:   # vm.current_job is not None

        if vm.current_job.id != job_id or vm.current_job.status != simgval.gJOB_ST_FAILED or vm.current_job.job_failure_recovered is not True:
            g_log_handler.error("[IaaS____] %s EVT (%s) ERROR! - VM(%d)'s Current JobID (CURR:%s, INPUT:%s) Mismatch or Status (%s) Error or Recovery Status (%s) Error! => %s" \
                  %(str(Global_Curr_Sim_Clock), simgval.get_sim_code_str(evt_code), vm.id, vm.current_job.id, job_id, simgval.get_sim_code_str(vm.current_job.status), simgval.get_sim_code_str(vm.current_job.job_failure_recovered), q_msg))
            clib.logwrite_vm_obj(g_log_handler, "[IaaS____] " + str(Global_Curr_Sim_Clock), vm)
            clib.sim_exit()

    curr_job = vm.current_job


    # [c] job status change to gJOB_ST_STARTED
    simobj.set_Job_status (curr_job, simgval.gJOB_ST_STARTED)

    # start time shouldn't be updated
    # [d] assign current clock to job's time_start
    simobj.set_Job_time_start (curr_job, Global_Curr_Sim_Clock)

    job_str = simobj.get_job_info_str(curr_job)
    print "[IaaS____] %s Actual Duration for %s on VM (%d, %s, $%s, %s) : %d (CPU:%s, I/O:%s) sec." \
          % (str(Global_Curr_Sim_Clock), job_str, vm.id, vm.type_name, vm.unit_price, simgval.get_sim_code_str(vm.status), curr_job.act_duration_on_VM, curr_job.act_cputime_on_VM, curr_job.act_nettime_on_VM)


    """
    clib.display_job_obj(Global_Curr_Sim_Clock, curr_job)
    clib.sim_exit()
    """

    # [e] read duration and register timer event
    q_tmr_evt_data = [curr_job.id]
    q_tmr_evt_data.append(vm)


    q_tmr_evt_sub_code = simgval.gEVT_SUB_JOB_IN_CPU_OP_COMPLETE
    q_tmr_time = get_job_sub_runtime (curr_job, q_tmr_evt_sub_code)
    q_tmr_evt = clib.make_queue_event (q_tmr_time,                              # timer (job actual duration)
                                       g_my_block_id,                           # src block id (will be returned to src block)
                                       simgval.gBLOCK_SIM_EVENT_HANDLER,        # dst block id (event handler block)
                                       simgval.gEVT_SET_TIMER,                  # evt code: set timer
                                       q_tmr_evt_sub_code,                      # evt sub code: job exec complete
                                       q_tmr_evt_data)                          # event data = [job_id, vm_obj]

    # send timer event to event handler
    gQ_OUT.put (q_tmr_evt)

    # [f] send ack event to event handler
    iaas_send_evt_ack(evt_id, evt_code)

def iaas_evt_create_vm_processing (q_msg):

    evt_id          = q_msg[0] # EVT_ID
    evt_src         = q_msg[2] # EVT_SRC
    evt_code        = q_msg[4] # EVT_CODE
    evt_sub_code    = q_msg[5] # EVT_SUB_CODE
    evt_data        = q_msg[6] # EVT_DATA

    if evt_code != simgval.gEVT_CREATE_VM:
        g_log_handler.error("[IaaS____] %s EVT (%s) ERROR! - INVALID EVT_CODE (%s) => %s" \
              %(str(Global_Curr_Sim_Clock), simgval.get_sim_code_str(simgval.gEVT_CREATE_VM), simgval.get_sim_code_str(evt_code), q_msg))
        clib.sim_exit()

    evt_real_src = evt_sub_code
    vm = evt_data[0]

    if evt_real_src != simgval.gBLOCK_BROKER:
        g_log_handler.error("[IaaS____] %s EVT (%s) ERROR! - EVT_REAL_SRC (%s) Should be BLOCK_BROKER => %s" \
              % (str(Global_Curr_Sim_Clock), simgval.get_sim_code_str(evt_code), simgval.get_sim_code_str(evt_real_src), q_msg))
        clib.sim_exit()

    g_log_handler.info("[IaaS____] %s %s: ID:%d, IID:%s, TY:%s, PR:$%s, ST:%s, TC:%s, TA:%s" \
          % (str(Global_Curr_Sim_Clock), simgval.get_sim_code_str(evt_code), vm.id, vm.instance_id, vm.type_name, vm.unit_price, simgval.get_sim_code_str(vm.status), str(vm.time_create), str(vm.time_active)))

    if vm.status != simgval.gVM_ST_CREATING or vm.time_active is not None:
        g_log_handler.error("[IaaS____] %s EVT (%s) ERROR! - VM Data Corrupted - Discard Event" % (str(Global_Curr_Sim_Clock), simgval.get_sim_code_str(evt_code)))
        clib.sim_exit()

    startup_lag = calculate_Startup_Lag()

    print "[IaaS____] %s Startup Lag for VM (%d, %s, $%s, %s) : %d sec." \
          % (str(Global_Curr_Sim_Clock), vm.id, vm.type_name, vm.unit_price, simgval.get_sim_code_str(vm.status), startup_lag)

    # insert vm obj into creating vm list.
    insert_result = insert_VM_into_Creating_VMs (vm)

    if insert_result > -1:
        # insert success

        # create timer event since insert into creating vm list --> send timer event for startup lag
        q_tmr_evt_data = [startup_lag]
        q_tmr_evt_data.append(vm)

        q_tmr_evt = clib.make_queue_event (startup_lag,                         # timer (startup time)
                                           g_my_block_id,                         # src block id (will be returned to src block)
                                           simgval.gBLOCK_SIM_EVENT_HANDLER,    # dst block id (event handler block)
                                           simgval.gEVT_SET_TIMER,              # evt code: set timer
                                           simgval.gEVT_SUB_VM_STARTUP_COMPLETE,# evt sub code: startup completed
                                           q_tmr_evt_data)                      # event data = [startup lag, vm_obj]

        # send timer event to event handler
        gQ_OUT.put (q_tmr_evt)

    else:
        g_log_handler.error("[IaaS____] %s insert_VM_into_Creating_VMs Error! - res_code: %d" % (str(Global_Curr_Sim_Clock), insert_result))
        clib.sim_exit()

    # send ack event to event handler
    iaas_send_evt_ack(evt_id, evt_code)

    return

def iaas_evt_terminate_vm_processing (q_msg):

    evt_id          = q_msg[0] # EVT_ID
    evt_src         = q_msg[2] # EVT_SRC
    evt_code        = q_msg[4] # EVT_CODE
    evt_sub_code    = q_msg[5] # EVT_SUB_CODE
    evt_data        = q_msg[6] # EVT_DATA

    if evt_code != simgval.gEVT_TERMINATE_VM:
        g_log_handler.error("[IaaS____] %s EVT (%s) ERROR! - INVALID EVT_CODE (%s) => %s" \
              %(str(Global_Curr_Sim_Clock), simgval.get_sim_code_str(simgval.gEVT_TERMINATE_VM), simgval.get_sim_code_str(evt_code), q_msg))
        clib.sim_exit()

    evt_real_src = evt_sub_code
    ref_vm = evt_data[0]

    if evt_real_src != simgval.gBLOCK_BROKER:
        g_log_handler.error("[IaaS____] %s EVT (%s) ERROR! - EVT_REAL_SRC (%s) Should be BLOCK_BROKER => %s" \
              % (str(Global_Curr_Sim_Clock), simgval.get_sim_code_str(evt_code), simgval.get_sim_code_str(evt_real_src), q_msg))
        clib.sim_exit()

    # procedure for gEVT_TERMINATE_VM processing
    # [a] find index from g_Running_VMs
    # [b] extract from g_Running_VMs
    #   [b-1] check valid termination of vm
    # [c] insert vm into g_Stopped_VMs
    # [d] cost calculate - price and billing_time_unit, billing_time_period
    # [e] write log -- vm info
    # [f] sim entity term check
    # [g] send ack

    # [a] find index from g_Running_VMs
    #clib.display_vm_obj(Global_Curr_Sim_Clock, ref_vm)
    #clib.display_VM_Instances_List(Global_Curr_Sim_Clock, "BEFORE-Running", g_Running_VMs)
    #clib.display_VM_Instances_List(Global_Curr_Sim_Clock, "BEFORE-Stopped", g_Stopped_VMs)

    # [b] extract from g_Running_VMs
    vm = extract_VM_from_Running_VM_List (ref_vm)

    # [b-1] check valid termination of vm
    # condition "or len (vm.job_history) < 1" is removed from the termination condition - on 11/07/2014 -- this is not relevant to now due to job failure model
    if vm.status != simgval.gVM_ST_TERMINATE or vm.time_terminate is None or vm.current_job is not None or vm.current_job_start_clock != -1 or len (vm.job_queue) > 0 or vm.cost != 0:

        g_log_handler.error("[IaaS____] %s %s Processing Error => Invalid VM Termination: ID:%d, IID:%s, ST:%s, TC:%s, TA:%s, TT:%s, CT:%f, CJ:%s, CJST:%d, LEN(JQ):%d, LEN(JH):%d" \
              % (str(Global_Curr_Sim_Clock), simgval.get_sim_code_str(evt_sub_code), vm.id, vm.instance_id, simgval.get_sim_code_str(vm.status), vm.time_create, vm.time_active, vm.time_terminate, vm.cost, str(vm.current_job), vm.current_job_start_clock, len(vm.job_queue), len(vm.job_history)))
        clib.sim_exit()

    # [c] insert vm into g_Stopped_VMs
    insert_VM_into_Stopped_VMs (vm)

    g_log_handler.info("[IaaS____] %s %s: ID:%d, IID:%s, TY:%s, PR:$%s, ST:%s, TC:%s, TA:%s, TE:%s" \
          % (str(Global_Curr_Sim_Clock), simgval.get_sim_code_str(evt_code), vm.id, vm.instance_id, vm.type_name, vm.unit_price, simgval.get_sim_code_str(vm.status), str(vm.time_create), str(vm.time_active), str(vm.time_terminate)))

    # [d] cost calculate - price and billing_time_unit, billing_time_period
    calculate_vm_cost (vm)

    # [e] write log -- vm info
    write_vm_usage_log (vm)

    # [f] iaas sim entity term check
    iaas_check_sim_entity_termination()

    # [g] send ack
    iaas_send_evt_ack(evt_id, evt_code)

def iaas_evt_sub_vm_startup_complete_processing (q_msg):

    evt_id          = q_msg[0] # EVT_ID
    evt_code        = q_msg[4] # EVT_CODE
    evt_sub_code    = q_msg[5] # EVT_SUB_CODE
    evt_data        = q_msg[6] # EVT_DATA

    if evt_sub_code != simgval.gEVT_SUB_VM_STARTUP_COMPLETE:
        g_log_handler.error("[IaaS____] %s iaas_evt_sub_vm_startup_complete_processing EVT_CODE (EC:%s, ESC:%s) Error! => %s" \
              % (str(Global_Curr_Sim_Clock), simgval.get_sim_code_str(evt_code), simgval.get_sim_code_str(evt_sub_code), q_msg))
        clib.sim_exit()

    # VM Startup Complete
    exp_startup_lag = evt_data[0]
    ref_vm = evt_data[1]

    # step 1 --> extract target vm obj from Creating VM list
    vm_obj = extract_VM_from_Creating_VM_List (ref_vm)
    if vm_obj is None:


        g_log_handler.error("[IaaS____] %s %s Processing Error! => Cannot Find VM(ID:%d, IID:%s, TY:%s, PR:$%s, TC:%d) from Creating VM List" \
              % (str(Global_Curr_Sim_Clock), simgval.get_sim_code_str(evt_sub_code), ref_vm.id, ref_vm.instance_id, ref_vm.type_name, vm.unit_price, ref_vm.time_create))
        clib.sim_exit()

    # step 2 --> update vm obj infos
    simobj.set_VM_status (vm_obj, simgval.gVM_ST_ACTIVE)
    simobj.set_VM_time_active (vm_obj, Global_Curr_Sim_Clock)
    simobj.set_VM_startup_lag (vm_obj, exp_startup_lag)

    g_log_handler.info('[IaaS____] %s VM (%d, %s, %s, $%s) Info Updated - status:%s, time_active:%s, startup_lag:%s'
         % (Global_Curr_Sim_Clock, vm_obj.id, vm_obj.instance_id, vm_obj.type_name, vm_obj.unit_price, simgval.get_sim_code_str(vm_obj.status), str(vm_obj.time_active), str(vm_obj.startup_lag)))

    # step 3 insert into running list.
    insert_result = insert_VM_into_Running_VMs (vm_obj)
    if insert_result > -1:
        # send bypass event to event handler (act dest: broker block) : vm activated
        bpe_data =  [simgval.gEVT_NOTI_VM_ACTIVATED]
        bpe_data.append (vm_obj)

        bpe_evt = clib.make_queue_event (0, g_my_block_id, simgval.gBLOCK_SIM_EVENT_HANDLER, simgval.gEVT_BYPASS, simgval.gBLOCK_BROKER, bpe_data)
        gQ_OUT.put (bpe_evt)
    else:
        print "[IaaS____] %s insert_VM_into_Running_VMs Error! - res_code: %d" % (str(Global_Curr_Sim_Clock), insert_result)
        clib.sim_exit()

    # step final --> send ack to event handler.
    iaas_send_evt_ack(evt_id, evt_code)

def iaas_evt_sub_job_in_cpu_op_complete_processing (q_msg):

    evt_id          = q_msg[0] # EVT_ID
    evt_code        = q_msg[4] # EVT_CODE
    evt_sub_code    = q_msg[5] # EVT_SUB_CODE
    evt_data        = q_msg[6] # EVT_DATA

    if evt_sub_code != simgval.gEVT_SUB_JOB_IN_CPU_OP_COMPLETE:
        g_log_handler.error("[IaaS____] %s iaas_evt_sub_job_in_cpu_op_complete_processing EVT_CODE (EC:%s, ESC:%s) Error! => %s" \
              % (str(Global_Curr_Sim_Clock), simgval.get_sim_code_str(evt_code), simgval.get_sim_code_str(evt_sub_code), q_msg))
        clib.sim_exit()

    # [a] get job_id (evt_data[0]), vm (evt_data[1]), curr_job (vm.current_job)
    # [b] job_id (evt_data[0]) should be the same with job_obj.id (evt_data[0].id)
    curr_job_id = evt_data[0]
    c_vm = evt_data[1]
    c_job = c_vm.current_job

    if curr_job_id != c_job.id:
        g_log_handler.error("[IaaS____] %s iaas_evt_sub_job_in_cpu_op_complete_processing Job ID Mismatch! => evt_data[0]:%d, evt_data[0].id:%d" \
              % (str(Global_Curr_Sim_Clock), comp_job_id, c_job.id))
        clib.sim_exit()

    if c_job.use_output_file == True:

        # processing file transfer to cloud storage
        # data transfer first and timer event next --> data transfer routine can change nettime[1] --> it will affect total job duration
        actual_output_file_transfer = True
        full_data_transmission = True
        if c_job.output_file_flow_direction == simgval.gOFTD_IC:
            actual_output_file_transfer, full_data_transmission = proc_file_transfer_to_cloud_storage (c_job)
        elif c_job.output_file_flow_direction == simgval.gOFTD_OC:
            simobj.set_Job_output_file_transfer_infos (c_job, simgval.gSFO_DST_FULL_UPLOAD, c_job.output_file_size)

        # if full_data_transmission means job duration has been changed.
        #       --> this affects vm scale down time.
        if full_data_transmission == False:
            bpe_data =  [simgval.gEVT_JOB_DURATION_CHANGED]
            bpe_data.append (c_job.id)
            bpe_data.append (c_vm)

            bpe_evt = clib.make_queue_event (0, g_my_block_id, simgval.gBLOCK_SIM_EVENT_HANDLER, simgval.gEVT_BYPASS, simgval.gBLOCK_BROKER, bpe_data)
            gQ_OUT.put (bpe_evt)

        if actual_output_file_transfer == True:

            # [e] read duration and register timer event
            q_tmr_evt_data = [c_job.id]
            q_tmr_evt_data.append(c_vm)

            q_tmr_evt_sub_code = simgval.gEVT_SUB_JOB_OUT_OP_COMPLETE

            # remaining job duration for output file processing
            q_tmr_time = get_job_sub_runtime (c_job, q_tmr_evt_sub_code)
            q_tmr_evt = clib.make_queue_event (q_tmr_time,                              # timer (job actual duration)
                                               g_my_block_id,                           # src block id (will be returned to src block)
                                               simgval.gBLOCK_SIM_EVENT_HANDLER,        # dst block id (event handler block)
                                               simgval.gEVT_SET_TIMER,                  # evt code: set timer
                                               q_tmr_evt_sub_code,                      # evt sub code: gEVT_SUB_JOB_OUT_OP_COMPLETE
                                               q_tmr_evt_data)                          # event data = [job_id, vm_obj]

            # send timer event to event handler
            gQ_OUT.put (q_tmr_evt)

        else:

            # job completion processing because there no ACTUAL JOB for output file (e.g. output file exists but no actual data transmission due to lack of space)
            iaas_proc_job_completion (c_vm, c_job, simgval.gJOB_ST_COMPLETED)

    else:

        # job completion processing because there no job for output file processing
        iaas_proc_job_completion (c_vm, c_job, simgval.gJOB_ST_COMPLETED)

    # [h] send evt_ack to event handler
    iaas_send_evt_ack(evt_id, evt_code)

def iaas_evt_sub_file_transfer_complete_cloud_storage (q_msg):

    evt_id          = q_msg[0] # EVT_ID
    evt_code        = q_msg[4] # EVT_CODE
    evt_sub_code    = q_msg[5] # EVT_SUB_CODE
    evt_data        = q_msg[6] # EVT_DATA

    if evt_sub_code != simgval.gEVT_SUB_FILE_TRANSFER_COMPLETE_CLOUD_STORAGE:
        g_log_handler.error("[IaaS____] %s iaas_evt_sub_file_transfer_complete_cloud_storage EVT_CODE (EC:%s, ESC:%s) Error! => %s" \
              % (str(Global_Curr_Sim_Clock), simgval.get_sim_code_str(evt_code), simgval.get_sim_code_str(evt_sub_code), q_msg))
        clib.sim_exit()

    sc_id       = evt_data[0]
    sfo_id      = evt_data[1]
    job_id      = evt_data[2]
    file_size   = evt_data[3]

    sc_obj = get_SC_from_activated_storage_container (sc_id)
    sfo_obj = get_SFO_from_storage_container (sc_obj, sfo_id)

    # sfo validation
    # [a] owner should be job id
    # [b] status should be simgval.gSFO_ST_FILE_UPLOAD_START
    # [c] sfo_size should be file_size
    if sfo_obj.sfo_owner != job_id:
        g_log_handler.error("[IaaS____] %s iaas_evt_sub_file_transfer_complete_cloud_storage Error! => SFO Owner Should be Job-%s - SFO Infos:%s" \
              % (str(Global_Curr_Sim_Clock), job_id, sfo_obj.get_infos()))
        clib.sim_exit()

    if sfo_obj.sfo_status != simgval.gSFO_ST_FILE_UPLOAD_START:
        g_log_handler.error("[IaaS____] %s iaas_evt_sub_file_transfer_complete_cloud_storage Error! => SFO Status Should be %s - SFO Infos:%s" \
              % (str(Global_Curr_Sim_Clock), simgval.get_sim_code_str(simgval.gSFO_ST_FILE_UPLOAD_START), sfo_obj.get_infos()))
        clib.sim_exit()


    if sfo_obj.sfo_size != file_size:
        g_log_handler.error( "[IaaS____] %s iaas_evt_sub_file_transfer_complete_cloud_storage Error! => SFO sfo_size Should be %s KB - SFO Infos:%s" % (str(Global_Curr_Sim_Clock), file_size, sfo_obj.get_infos()))
        clib.sim_exit()


    update_result = sc_obj.upload_file_complete (Global_Curr_Sim_Clock, sfo_obj.sfo_id, sfo_obj.sfo_status, sfo_obj.sfo_size)
    if update_result == False:
        g_log_handler.error("[IaaS____] %s iaas_evt_sub_file_transfer_complete_cloud_storage Error! => upload_file_complete Error! - %s" \
              % (str(Global_Curr_Sim_Clock), sfo_obj.get_infos()))
        clib.sim_exit()

    iaas_send_evt_ack(evt_id, evt_code)

def iaas_evt_sub_job_failure (q_msg):

    evt_id          = q_msg[0] # EVT_ID
    evt_code        = q_msg[4] # EVT_CODE
    evt_sub_code    = q_msg[5] # EVT_SUB_CODE
    evt_data        = q_msg[6] # EVT_DATA

    if evt_sub_code != simgval.gEVT_SUB_JOB_FAILED:
        g_log_handler.error("[IaaS____] %s iaas_evt_sub_job_failure EVT_CODE (EC:%s, ESC:%s) Error! => %s" \
              % (str(Global_Curr_Sim_Clock), simgval.get_sim_code_str(evt_code), simgval.get_sim_code_str(evt_sub_code), q_msg))
        clib.sim_exit()

    # [a] get job_id (evt_data[0]), vm (evt_data[1]), curr_job (vm.current_job)
    # [b] job_id (evt_data[0]) should be the same with job_obj.id (evt_data[0].id)
    curr_job_id = evt_data[0]
    c_vm = evt_data[1]
    c_job = c_vm.current_job

    if curr_job_id != c_job.id:
        g_log_handler.error("[IaaS____] %s iaas_evt_sub_job_failure Job ID Mismatch! => evt_data[0]:%d, evt_data[0].id:%d" \
              % (str(Global_Curr_Sim_Clock), comp_job_id, c_job.id))
        clib.sim_exit()

    iaas_proc_job_completion (c_vm, c_job, simgval.gJOB_ST_FAILED)
    clib.display_job_obj(Global_Curr_Sim_Clock, c_job)

    iaas_send_evt_ack(evt_id, evt_code)

def iaas_evt_sub_job_out_op_complete_processing (q_msg):

    evt_id          = q_msg[0] # EVT_ID
    evt_code        = q_msg[4] # EVT_CODE
    evt_sub_code    = q_msg[5] # EVT_SUB_CODE
    evt_data        = q_msg[6] # EVT_DATA

    if evt_sub_code != simgval.gEVT_SUB_JOB_OUT_OP_COMPLETE:
        g_log_handler.error("[IaaS____] %s iaas_evt_sub_job_out_op_complete_processing EVT_CODE (EC:%s, ESC:%s) Error! => %s" \
              % (str(Global_Curr_Sim_Clock), simgval.get_sim_code_str(evt_code), simgval.get_sim_code_str(evt_sub_code), q_msg))
        clib.sim_exit()

    # [a] get job_id (evt_data[0]), vm (evt_data[1]), curr_job (vm.current_job)
    # [b] job_id (evt_data[0]) should be the same with job_obj.id (evt_data[0].id)
    curr_job_id = evt_data[0]
    c_vm = evt_data[1]
    c_job = c_vm.current_job

    if curr_job_id != c_job.id:
        g_log_handler.error("[IaaS____] %s iaas_evt_sub_job_out_op_complete_processing Job ID Mismatch! => evt_data[0]:%d, evt_data[0].id:%d" \
              % (str(Global_Curr_Sim_Clock), comp_job_id, c_job.id))
        clib.sim_exit()

    # job completion processing because there no job for output file processing
    iaas_proc_job_completion (c_vm, c_job, simgval.gJOB_ST_COMPLETED)

    # [h] send evt_ack to event handler
    iaas_send_evt_ack(evt_id, evt_code)

def iaas_proc_job_completion (c_vm, c_job, job_complete_status):

    if c_vm.current_job.id != c_job.id:
        g_log_handler.error("[IaaS____] %s iaas_proc_job_completion - Job ID Mismatch! => vm.current_job.id:%d, job.id:%d" \
              % (str(Global_Curr_Sim_Clock), c_vm.current_job.id, c_job.id))
        clib.sim_exit()

    job_str = simobj.get_job_info_str(c_job)
    if job_complete_status != simgval.gJOB_ST_COMPLETED and job_complete_status != simgval.gJOB_ST_FAILED:
        g_log_handler.error("[IaaS____] %s iaas_proc_job_completion - Invalid job_complete_status (%s) for %s" \
              % (str(Global_Curr_Sim_Clock), simgval.get_sim_code_str(job_complete_status), job_str))
        clib.sim_exit()

    if c_job.status != simgval.gJOB_ST_STARTED:
        g_log_handler.error("[IaaS____] %s iaas_proc_job_completion - Job Status (%s) Error for %s"\
              % (str(Global_Curr_Sim_Clock), simgval.get_sim_code_str(c_job.status), job_str))
        clib.sim_exit()

    tmp_job_duration = Global_Curr_Sim_Clock - c_job.time_create
    if (tmp_job_duration < c_job.act_duration_on_VM) and (job_complete_status == simgval.gJOB_ST_COMPLETED):
        g_log_handler.error("[IaaS____] %s iaas_proc_job_completion Error! - Actual Job Duration (%s) should be the same with or greater than job.act_duration_on_VM (%s) for %s"\
              % (str(Global_Curr_Sim_Clock), tmp_job_duration, c_job.act_duration_on_VM, job_str))
        clib.sim_exit()

    # [c] job status --> gJOB_ST_COMPLETED
    simobj.set_Job_status (c_job, job_complete_status)

    # [d] updte job's time_complete
    simobj.set_Job_time_complete (c_job, Global_Curr_Sim_Clock)

    # [g] send bypass event to broker (job completed)
    bpe_data = []
    if job_complete_status == simgval.gJOB_ST_COMPLETED:
        bpe_data.append(simgval.gEVT_JOB_COMPLETED_SUCCESS)
    elif job_complete_status == simgval.gJOB_ST_FAILED:
        bpe_data.append(simgval.gEVT_JOB_COMPLETED_FAILURE)

    bpe_data.append (c_job.id)
    bpe_data.append (c_vm.id)

    bpe_evt = clib.make_queue_event (0, g_my_block_id, simgval.gBLOCK_SIM_EVENT_HANDLER, simgval.gEVT_BYPASS, simgval.gBLOCK_BROKER, bpe_data)
    gQ_OUT.put (bpe_evt)

    print "[IaaS____] %s iaas_proc_job_completion - %s, VM_ID:%s" % (str(Global_Curr_Sim_Clock), job_str, c_vm.id)

def iaas_evt_sub_write_sim_trace (q_msg):

    evt_id          = q_msg[0] # EVT_ID
    evt_code        = q_msg[4] # EVT_CODE
    evt_sub_code    = q_msg[5] # EVT_SUB_CODE
    evt_data        = q_msg[6] # EVT_DATA

    if evt_sub_code != simgval.gEVT_SUB_WRITE_SIM_TRACE_IAAS:
        g_log_handler.error("[IAAS____] %s EVT_CODE (EC:%s, ESC:%s) Error! => %s" \
              % (str(Global_Curr_Sim_Clock), simgval.get_sim_code_str(evt_code), simgval.get_sim_code_str(evt_sub_code), q_msg))
        clib.sim_exit()

    # procedure
    # [a] write simulation trace
    # [b] register the same timer event
    # ('CLOCK,# SC,# SFO,ST_SIZE (KB),ST_COST ($),NET-IN (KB),NET-OUT (KB),NET-CLOUD (KB),NET_COST ($)')

    num_of_SCs = get_total_num_of_active_storage_containers()
    num_of_SFOs = get_total_number_of_current_SFOs ()
    st_size = get_currently_completed_cloud_storage_capacity ()
    st_cost = calculate_current_total_storage_cost ()

    # [0]: IN-CLOUD, CLOUD-OUT, INTER-CLOUD
    net_usage = calculate_current_network_usage()

    # [0]: IN-CLOID, CLOUD-OUT, INTER-CLOUD
    net_cost = [0,0,0]
    net_cost[0] = net_usage[0] * g_sim_conf.cost_data_transfer[1] * (1/(1024.0*1024.0))
    net_cost[1] = net_usage[1] * g_sim_conf.cost_data_transfer[2] * (1/(1024.0*1024.0))
    net_cost[2] = net_usage[2] * g_sim_conf.cost_data_transfer[0] * (1/(1024.0*1024.0))

    g_sim_trace_log_handler.info ('%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s'
                                  % (Global_Curr_Sim_Clock, num_of_SCs, num_of_SFOs, st_size, st_cost, net_usage[0], net_usage[1], net_usage[2], net_cost[0], net_cost[1], net_cost[2], sum(net_cost)))

    """
    vm_cnt = get_detailed_number_of_VM_status ()
    vm_total_cost = get_vm_current_total_cost ()
    g_sim_trace_log_handler.info ('%d,%d,%d,%d,%d,%d,%d,%d,%d,%s' % (Global_Curr_Sim_Clock, g_job_logs[0], g_job_logs[1], g_job_logs[2], g_job_logs[3], vm_cnt[0], vm_cnt[1], vm_cnt[2], vm_cnt[3], vm_total_cost))
    """

    # [b] register the same timer event
    register_iaas_sim_trace_event ()

    iaas_send_evt_ack(evt_id, evt_code)

def iaas_evt_exp_timer_processing (q_msg):

    evt_code        = q_msg[4] # EVT_CODE
    evt_sub_code    = q_msg[5] # EVT_SUB_CODE

    if evt_code != simgval.gEVT_EXP_TIMER:
        g_log_handler.error("[IaaS____] %s iaas_evt_exp_timer_processing EVT_CODE (%s) Error! => %s" %(str(Global_Curr_Sim_Clock), simgval.get_sim_code_str(evt_code), q_msg))
        clib.sim_exit()

    if evt_sub_code == simgval.gEVT_SUB_VM_STARTUP_COMPLETE:

        # evt sub code processing (gEVT_SUB_VM_STARTUP_COMPLETE)
        iaas_evt_sub_vm_startup_complete_processing (q_msg)


    # this is unused any more... but this should be used for reference 10/26/2014
    #elif evt_sub_code == simgval.gEVT_SUB_JOB_EXEC_COMPLETE:

        # evt sub code processing (gEVT_SUB_JOB_EXEC_COMPLETE)
        #iaas_evt_sub_job_exec_complete_processing (q_msg)

    elif evt_sub_code == simgval.gEVT_SUB_JOB_IN_CPU_OP_COMPLETE:

        # evt sub code processing (gEVT_SUB_JOB_IN_CPU_OP_COMPLETE)
        iaas_evt_sub_job_in_cpu_op_complete_processing (q_msg)

    elif evt_sub_code == simgval.gEVT_SUB_JOB_OUT_OP_COMPLETE:

        # evt sub code processing (gEVT_SUB_JOB_IN_CPU_OP_COMPLETE)
        iaas_evt_sub_job_out_op_complete_processing (q_msg)

    elif evt_sub_code == simgval.gEVT_SUB_FILE_TRANSFER_COMPLETE_CLOUD_STORAGE:

        # evt sub code processing (gEVT_SUB_FILE_TRANSFER_COMPLETE_CLOUD_STORAGE -- data transfer to cloud storage
        iaas_evt_sub_file_transfer_complete_cloud_storage (q_msg)

    elif evt_sub_code == simgval.gEVT_SUB_JOB_FAILED:

        # evt sub code processing (gEVT_SUB_JOB_FAILED -- job failure)
        iaas_evt_sub_job_failure (q_msg)

    elif evt_sub_code == simgval.gEVT_SUB_WRITE_SIM_TRACE_IAAS:

        # evt sub code processing (gEVT_SUB_WRITE_SIM_TRACE_BROKER)
        iaas_evt_sub_write_sim_trace (q_msg)

    else:

        g_log_handler.error("[IaaS____] %s EVT_SUB_CODE (%s) Error! => %s" % (str(Global_Curr_Sim_Clock), simgval.get_sim_code_str(evt_sub_code), q_msg))
        clib.sim_exit()

    return

def iaas_event_processing (q_msg):

    evt_src         = q_msg[2]  # EVT_SRC
    evt_code        = q_msg[4]  # EVT_CODE
    evt_sub_code    = q_msg[5]  # EVT_SUB_CODE
    evt_data        = q_msg[6]  # EVT_DATA

    g_log_handler.info("\n\n[IaaS____] %s EVT_RECV : EC:%s, ESC:%s, SRC_BLOCK:%s, data:%s" \
          % (str(Global_Curr_Sim_Clock), simgval.get_sim_code_str(evt_code), simgval.get_sim_code_str(evt_sub_code), simgval.get_sim_code_str(evt_src), evt_data))

    if evt_code == simgval.gEVT_EXP_TIMER:

        # timer expired event
        iaas_evt_exp_timer_processing (q_msg)

    elif evt_code == simgval.gEVT_CREATE_VM:

        # evt code for creating a new VM
        iaas_evt_create_vm_processing (q_msg)

    elif evt_code == simgval.gEVT_START_JOB:

        #evt code for evt start up
        iaas_evt_start_job_processing (q_msg)

    elif evt_code == simgval.gEVT_RESTART_JOB:

        #evt code for evt start up
        iaas_evt_restart_job_processing (q_msg)


    elif evt_code == simgval.gEVT_TERMINATE_VM:

        # evt code for vm termination
        iaas_evt_terminate_vm_processing (q_msg)

    elif evt_code == simgval.gEVT_CONFIRM_SIMENTITY_TERMINATION or evt_code == simgval.gEVT_REJECT_SIMENTITY_TERMINATION:

        g_log_handler.info("[IaaS____] %s EVT_CODE (%s) => Ignore Event, EVT:%s" % (str(Global_Curr_Sim_Clock), simgval.get_sim_code_str(evt_code), q_msg))

    else:

        g_log_handler.error("[IaaS____] %s EVT_CODE (%s) Error! => %s" % (str(Global_Curr_Sim_Clock), simgval.get_sim_code_str(evt_code), q_msg))
        clib.sim_exit()


def iaas_term_event_processing (q_msg):

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

    print "\n\n[IaaS____] %s EVT_RECV - EC:%s, ESC:%s, SRC_BLOCK:%s" \
          % (str(Global_Curr_Sim_Clock), simgval.get_sim_code_str(evt_code), simgval.get_sim_code_str(evt_sub_code), simgval.get_sim_code_str(evt_src))

    ret_val = False

    if evt_code == simgval.gEVT_CONFIRM_SIMENTITY_TERMINATION:
        g_log_handler.info("[IaaS____] %s IaaS Terminated - TERM FLAG:%s, EVT:%s" \
              % (str(Global_Curr_Sim_Clock), str(g_flag_sim_entity_term), simgval.get_sim_code_str(evt_code)))

        # [a] send ack first
        iaas_send_evt_ack (evt_id, evt_sub_code)

        # [b] send gEVT_NOTI_SIMENTIY_TERMINATED (common event code) to event handler
        term_evt = clib.make_queue_event (Global_Curr_Sim_Clock, g_my_block_id, simgval.gBLOCK_SIM_EVENT_HANDLER, simgval.gEVT_NOTI_SIMENTIY_TERMINATED, None, None)
        gQ_OUT.put (term_evt)

        # [c] return value is true
        ret_val = True

    elif evt_code == simgval.gEVT_REJECT_SIMENTITY_TERMINATION:

        set_iaas_sim_entity_term_flag (False)
        iaas_send_evt_ack (evt_id, evt_sub_code)

    return ret_val


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

def update_storage_delete_time ():

    global g_Activated_Storage_Containers

    for sc in g_Activated_Storage_Containers:
        for sfo in sc.sc_file_objects:
            sfo.set_sfo_status (simgval.gSFO_ST_FILE_DELETED, Global_Curr_Sim_Clock)

        sc.delete_storage_container (Global_Curr_Sim_Clock, "Term_Simulator")

def cal_storage_file_object_cost (sfo_duration, sfo_size):

    cost = sfo_duration * sfo_size * g_sim_conf.storage_unit_cost * (1/float(g_sim_conf.storage_billing_unit_clock)) * (1/(1024.0*1024.0))
    return cost

def calculate_current_total_storage_cost ():

    global g_Activated_Storage_Containers

    total_current_storage_cost = 0
    for sc in g_Activated_Storage_Containers:

        sc_cost = 0
        for sfo in sc.sc_file_objects:

            if sfo.sfo_active_time is None:
                continue

            if sfo.sfo_status == simgval.gSFO_ST_FILE_DELETED:
                sfo_duration = sfo.get_sfo_duration()
            elif sfo.sfo_status == simgval.gSFO_ST_FILE_UPLOAD_COMPLETED:
                sfo_duration = Global_Curr_Sim_Clock - sfo.sfo_active_time
            else:
                continue

            sfo_cost = cal_storage_file_object_cost (sfo_duration, sfo.sfo_size)
            if sfo_cost < 0:
                g_log_handler.error("[IaaS____] %s calculate_current_total_storage_cost - SFO_COST ($%d)/Duration (%s) Error! - %s" \
                      % (str(Global_Curr_Sim_Clock), sfo_cost, sfo_duration, sfo.get_infos()))
                clib.sim_exit()

            sc_cost += sfo_cost

        total_current_storage_cost += sc_cost

    return total_current_storage_cost


def update_storage_cost ():

    global g_Activated_Storage_Containers

    for sc in g_Activated_Storage_Containers:
        sc_cost = 0
        for sfo in sc.sc_file_objects:
            sfo_duration = sfo.get_sfo_duration()
            sfo_cost = cal_storage_file_object_cost (sfo_duration, sfo.sfo_size)

            if sfo_cost <= 0:
                g_log_handler.error("[IaaS____] %s update_storage_cost - SFO_COST ($%d) Error! - %s" \
                      % (str(Global_Curr_Sim_Clock), sfo_cost, sfo.get_infos()))
                clib.sim_exit()

            sfo.set_sfo_usage_price (sfo_cost)
            sc_cost += sfo_cost

        sc.set_sc_usage_price (sc_cost)

def iaas_generate_storage_usage_report():

    update_storage_delete_time ()
    update_storage_cost ()

    rep_storage_log = set_iaas_storage_usage_report_log (g_sim_conf.report_path)
    rep_storage_log.info ('TY,ID,CR,TR,RG,PM,ST,CT,DT,DR,NF,VL(KB),CO($)')

    for sc in g_Activated_Storage_Containers:
        rep_storage_log.info ('SC,%s,Job-%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s' % (sc.sc_id, sc.sc_creator, sc.sc_terminator, sc.sc_region, simgval.get_sim_code_str(sc.sc_permission), simgval.get_sim_code_str(sc.sc_status), sc.sc_create_time, sc.sc_delete_time, sc.get_sc_duration(), len(sc.sc_file_objects), sc.sc_usage_volume, sc.sc_usage_price))

    rep_storage_log.info ("")
    rep_storage_log.info ('TY,ID,SC,SZ(KB),ON,ST,DST,PSZ(KB),CT,AT,DT,DR,CO($)')

    for sc in g_Activated_Storage_Containers:
        for sfo in sc.sc_file_objects:
            rep_storage_log.info ('SFO,%s,%s,%s,Job-%s,%s,%s,%s,%s,%s,%s,%s,%s' % (sfo.sfo_id, sfo.sfo_sc_id, sfo.sfo_size, sfo.sfo_owner, simgval.get_sim_code_str(sfo.sfo_status), simgval.get_sim_code_str(sfo.sfo_data_status), sfo.sfo_planned_size, sfo.sfo_create_time, sfo.sfo_active_time, sfo.sfo_delete_time, sfo.get_sfo_duration(), sfo.sfo_usage_price))

def calculate_current_network_usage ():

    net_usage = [0,0,0] # [0]: IN-CLOUD, CLOUD-OUT, INTER-CLOUD

    jobs = []
    for v in g_Stopped_VMs:
        for j in v.job_history:
            jobs.append(j)

    for v in g_Running_VMs:
        for j in v.job_history:
            jobs.append(j)

    for j in jobs:

        if j.use_input_file == True:

            if j.input_file_flow_direction == simgval.gIFTD_OC:
                net_usage[0] += j.input_file_size

            elif j.input_file_flow_direction == simgval.gIFTD_IC:
                net_usage[2] += j.input_file_size

        if j.use_output_file == True:

            if j.output_file_flow_direction == simgval.gOFTD_OC:
                net_usage[1] += j.output_file_size
            elif j.output_file_flow_direction == simgval.gOFTD_IC:
                net_usage[2] += j.output_file_transfer_size

    return net_usage

def calculate_network_cost (input_file_size, input_file_direction, output_file_size, output_file_direction):

    # g_sim_conf.cost_data_transfer [inter-cloud, in, out]

    # total, input, output
    costs = [0,0,0]

    # input file cost
    if input_file_direction == simgval.gIFTD_IC:
        costs[1] = input_file_size * (1/(1024.0*1024.0) * g_sim_conf.cost_data_transfer[0])
    elif input_file_direction == simgval.gIFTD_OC:
        costs[1] = input_file_size * (1/(1024.0*1024.0) * g_sim_conf.cost_data_transfer[1])

    # output file cost
    if output_file_direction == simgval.gOFTD_IC:
        costs[2] = output_file_size * (1/(1024.0*1024.0) * g_sim_conf.cost_data_transfer[0])
    elif output_file_direction == simgval.gOFTD_OC:
        costs[2] = output_file_size * (1/(1024.0*1024.0) * g_sim_conf.cost_data_transfer[2])

    costs[0] = costs[1] + costs[2]

    return costs

def iaas_generate_network_usage_report ():

    rep_network_log = set_iaas_network_usage_report_log (g_sim_conf.report_path)

    jobs = []
    for v in g_Stopped_VMs:
        for j in v.job_history:
            jobs.append(j)

    sorted_jobs = sorted (jobs, key=lambda job: job.id, reverse=False)

    rep_network_log.info ('JOB_ID,IN_TS(KB),IN_DR,IN_COST($),OUT_TS_PLANNED(KB),OUTPUT_TS_ACTUAL(KB),OUT_DR,OUT_COST($),TOTAL_COST($)')
    for j in sorted_jobs:

        if j.use_input_file is True or j.use_output_file is True:
            # j.output_file_transfer_size --> real data transfer size (not output_file_size)
            net_costs = calculate_network_cost (j.input_file_size, j.input_file_flow_direction, j.output_file_transfer_size, j.output_file_flow_direction)

            rep_network_log.info ('%s,%s,%s,%s,%s,%s,%s,%s,%s' % (j.id, j.input_file_size, simgval.get_sim_code_str(j.input_file_flow_direction), net_costs[1], j.output_file_size, j.output_file_transfer_size, simgval.get_sim_code_str(j.output_file_flow_direction), net_costs[2], net_costs[0]))

def iaas_generate_report ():

    global g_Activated_Storage_Containers
    print "[IaaS____] %s Generate Report." % (str(Global_Curr_Sim_Clock))

    iaas_generate_storage_usage_report ()
    iaas_generate_network_usage_report ()

def register_iaas_sim_trace_event ():

    if g_flag_sim_entity_term == True:
        return

    tmr_evt = clib.make_queue_event (g_sim_conf.sim_trace_interval,                 # timer (vm_scale_down_policy_timer_unit)
                                       g_my_block_id,                               # src block id (will be returned to src block)
                                       simgval.gBLOCK_SIM_EVENT_HANDLER,            # dst block id (event handler block)
                                       simgval.gEVT_SET_TIMER,                      # evt code: set timer
                                       simgval.gEVT_SUB_WRITE_SIM_TRACE_IAAS,       # evt sub code: gEVT_SUB_WRITE_SIM_TRACE_IAAS
                                       None)                                        # evt data : None

    gQ_OUT.put (tmr_evt)

def th_sim_entity_iaas_cloud (my_block_id, config_obj, q_in, q_out):

    global g_my_block_id
    global g_log_handler
    global g_vm_log_handler
    global g_sim_conf
    global gQ_OUT
    global g_sim_trace_log_handler

    time.sleep(random.random())

    if my_block_id != simgval.gBLOCK_IAAS:
        print "[IaaS____] 0 IaaS Cloud Block ID (I:%d, G:%d) Error!!!" % (my_block_id, simgval.gBLOCK_IAAS)
        clib.sim_exit()

    g_my_block_id = my_block_id
    g_sim_conf = config_obj
    gQ_OUT = q_out

    log = set_log (config_obj.log_path)
    g_log_handler = log

    log.info('[IaaS____] 0 IaaS Cloud Log Start...')

    vm_log = set_vm_log (config_obj.log_path)
    g_vm_log_handler = vm_log
    vm_log.info ('CL\tVMID\tRT\tCO\tIID\tTY\t\tST\t\t\tTC\tTA\tTT\tSL\tNJ\tJR\tUT\tSR\tID')
    vm_log.info ('-----------------------------------------------------------------------------------------------------------------------------------------------------')

    sim_trace_log = set_iaas_sim_trace_log (g_sim_conf.report_path)
    g_sim_trace_log_handler = sim_trace_log
    sim_trace_log.info ('CLOCK,# SC,# SFO,ST_SIZE (KB),ST_COST ($),NET-IN (KB),NET-OUT (KB),NET-CLOUD (KB),NET-IN_COST ($),NET-OUT_COST ($),NET-CLOUD_COST ($),NET_COST ($)')

    register_iaas_sim_trace_event ()

    while True:
        q_message = q_in.get()
        q_in.task_done()

        #time.sleep (0.01) duplicate message issue (08062014)
        #print "\n[IaaS____] Queue Message  :", q_message

        evt_clock = q_message[1]    # EVT_SIMULATION_CLOCK
        evt_src = q_message[2]      # SRC BLOCK
        evt_dst = q_message[3]      # DST BLOCK

        if evt_dst != my_block_id:
            log.error ("[IaaS____] %s EVT_MSG ERROR! (Wrong EVT_DST) - %s" %(Global_Curr_Sim_Clock,  q_message))
            continue

        if evt_src == simgval.gBLOCK_SIM_EVENT_HANDLER:
            # update simulator clock
            update_curr_sim_clock (evt_clock)

        else:
            log.error ("[IaaS____] %s EVT_MSG ERROR! (Wrong EVT_SRC) - %s" % (Global_Curr_Sim_Clock, q_message))
            continue

        # Termination Processing
        if g_flag_sim_entity_term is True:
            wanna_term = iaas_term_event_processing (q_message)
            if wanna_term is True:
                break

        # Main Event Processing for IaaS
        iaas_event_processing (q_message)

    iaas_generate_report()
    print "\n\n[IaaS____] %s IaaS Cloud Terminated." % (str(Global_Curr_Sim_Clock))
