import inspect
import platform
import os
import sys
import time
import logging
import sim_global_variables as simgval
from threading import Condition

Global_EventID_CV = Condition()
Global_EventID = 0

g_msg_logger = None
g_cv = Condition()

def set_msg_log (path):
    logger = logging.getLogger('message_log')
    log_file = path + '/[' + str(simgval.gBLOCK_MAIN) + ']-sim_message_trace.log'
    hdlr = logging.FileHandler(log_file)
    #hdlr = logging.FileHandler(path + '/sim_message_trace.log')
    formatter = logging.Formatter('%(message)s')
    hdlr.setFormatter(formatter)
    logger.addHandler(hdlr)
    logger.setLevel(logging.INFO)
    return logger

def init_message_log (path):
    global g_msg_logger
    g_msg_logger = set_msg_log (path)

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
def make_queue_event (evt_clock, evt_src, evt_dst, evt_code, evt_sub_code, evt_data):
    q_event = []

    evt_id = get_event_id ()

    q_event.append(evt_id)
    q_event.append(evt_clock)
    q_event.append(evt_src)
    q_event.append(evt_dst)
    q_event.append(evt_code)
    q_event.append(evt_sub_code)
    q_event.append(evt_data) # can be None

    curframe = inspect.currentframe()
    calframe = inspect.getouterframes(curframe, 2)
    g_cv.acquire()
    g_msg_logger.info (str(evt_clock) + "\tcreate make_queue_event")
    g_msg_logger.info (str(evt_clock) + "\tCaller   : %s, %s:%d" % (calframe[1][3], calframe[1][1], calframe[1][2]))
    g_msg_logger.info (str(evt_clock) + "\tEVT ID   : %d" % (evt_id))
    g_msg_logger.info (str(evt_clock) + "\tEVT CLK  : %d" % (evt_clock))
    g_msg_logger.info (str(evt_clock) + "\tEVT SRC  : %s" % (simgval.get_sim_code_str(evt_src)))
    g_msg_logger.info (str(evt_clock) + "\tEVT DST  : %s" % (simgval.get_sim_code_str(evt_dst)))
    g_msg_logger.info (str(evt_clock) + "\tEVT CODE : %s" % (simgval.get_sim_code_str(evt_code)))
    g_msg_logger.info (str(evt_clock) + "\tEVT SUBC : %s" % (simgval.get_sim_code_str(evt_sub_code)))
    g_msg_logger.info (str(evt_clock) + "\tEVT DATA : %s" % (evt_data))
    g_msg_logger.info ("")
    g_cv.notify()
    g_cv.release()

    #print 'caller name:', calframe[1][3], "Event ID:", evt_id

    return q_event

def get_event_id ():
    global Global_EventID
    Global_EventID_CV.acquire()

    Global_EventID += 1

    Global_EventID_CV.notify()
    Global_EventID_CV.release()

    return Global_EventID

'''
def get_VM_status_str (st):
    if st == simgval.gVM_ST_CREATING:
        return "CREATING"
    elif st == simgval.gVM_ST_ACTIVE:
        return "ACTIVE"
    elif st == simgval.gVM_ST_TERMINATE:
        return "TERMINATED"
    else:
        return "VM_STATUS_ERROR (" + str(st) + ")"
'''

def display_vm_obj (clock, vm):

    print "[Comm.Lib] %d -----------------------------------" % clock
    print "[Comm.Lib] %d Display VM Obj" % clock
    print "[Comm.Lib] %d -----------------------------------" % clock
    print "[Comm.Lib] %d VM ID                    = %d" % (clock, vm.id)
    print "[Comm.Lib] %d VM Instance ID           = %s" % (clock, vm.instance_id)
    print "[Comm.Lib] %d VM Type Name             = %s" % (clock, vm.type_name)
    print "[Comm.Lib] %d VM Unit Price            = %s" % (clock, vm.unit_price)
    print "[Comm.Lib] %d VM Status                = %s" % (clock, simgval.get_sim_code_str(vm.status))
    print "[Comm.Lib] %d VM Time Create           = %s" % (clock, str(vm.time_create))
    print "[Comm.Lib] %d VM Time Active           = %s" % (clock, str(vm.time_active))
    print "[Comm.Lib] %d VM Time Terminate        = %s" % (clock, str(vm.time_terminate))
    print "[Comm.Lib] %d VM Cost                  = %f" % (clock, vm.cost)
    print "[Comm.Lib] %d VM startup_lag           = %d" % (clock, vm.startup_lag)
    print "[Comm.Lib] %d -----------------------------------" % clock

    if vm.current_job is not None:
        print "[Comm.Lib] %d Current Job              = (ID:%d, NM:%s, ADR:%d (CPU:%s, NET:%s), DL:%d, SDR:%d, TC:%d, ST:%s)" \
              % (clock, vm.current_job.id, vm.current_job.name, vm.current_job.act_duration_on_VM, vm.current_job.act_cputime_on_VM, vm.current_job.act_nettime_on_VM, vm.current_job.deadline, vm.current_job.std_duration, vm.current_job.time_create, simgval.get_sim_code_str(vm.current_job.status))
    else:
        print "[Comm.Lib] %d Current Job              = %s" % (clock, vm.current_job)

    print "[Comm.Lib] %d VM Curr Job Start Clock  = %s" % (clock, vm.current_job_start_clock)
    print "[Comm.Lib] %d VM Last Job Compl Clock  = %s" % (clock, vm.last_job_complete_clock)

    if len(vm.job_queue) < 1:
        print "[Comm.Lib] %d Job Queue                = None" % (clock)
    else:
        sys.stdout.write ("[Comm.Lib] %d Job Queue                = [" % clock)
        for job in vm.job_queue:
            sys.stdout.write ("(ID:%d, NM:%s, ADR:%s (CPU:%s, I/O:%s), DL%d, SDR:%d, TC:%d)," % (job.id, job.name, job.act_duration_on_VM, job.act_cputime_on_VM, job.act_nettime_on_VM, job.deadline, job.std_duration, job.time_create))
        sys.stdout.write ("]\n")

    if len(vm.job_history) < 1:
        print "[Comm.Lib] %d Job History              = None" % (clock)
    else:
        sys.stdout.write ("[Comm.Lib] %d Job History              = [" % clock)
        for job in vm.job_history:
            sys.stdout.write ("(ID:%d, NM:%s, ADR:%s (CPU:%s, I/O:%s), DL:%d, SDR:%d, TC:%d, ST:%s),"
                              % (job.id, job.name, job.act_duration_on_VM, job.act_cputime_on_VM, job.act_nettime_on_VM, job.deadline, job.std_duration, job.time_create, simgval.get_sim_code_str(job.status)))
        sys.stdout.write ("]\n")
    print "[Comm.Lib] %d VM SD Policy Activated   = %s" % (clock, vm.sd_policy_activated)
    print "[Comm.Lib] %d VM SD Wait Time          = %d" % (clock, vm.sd_wait_time)
    print "[Comm.Lib] %d VM IS VScale Victim???   = %s" % (clock, vm.is_vscale_victim)
    print "[Comm.Lib] %d VM VScale Case           = %s" % (clock, simgval.get_sim_code_str(vm.vscale_case))
    print "[Comm.Lib] %d VM VScale Victim ID      = %d" % (clock, vm.vscale_victim_id)
    print "[Comm.Lib] %d -----------------------------------" % clock

def logwrite_vm_obj (log_handler, header, vm):
    log_handler.info (str(header) + " Display VM Object" )
    log_handler.info (str(header) + " ================================================================")
    log_handler.info (str(header) + " VM ID                    = %d" % (vm.id))
    log_handler.info (str(header) + " VM Instance ID           = %s" % (vm.instance_id))
    log_handler.info (str(header) + " VM Type Name             = %s" % (vm.type_name))
    log_handler.info (str(header) + " VM Unit Price            = %s" % (vm.unit_price))
    log_handler.info (str(header) + " VM Status                = %s" % (simgval.get_sim_code_str(vm.status)))
    log_handler.info (str(header) + " VM Time Create           = %s" % (str(vm.time_create)))
    log_handler.info (str(header) + " VM Time Active           = %s" % (str(vm.time_active)))
    log_handler.info (str(header) + " VM Time Terminate        = %s" % (str(vm.time_terminate)))
    log_handler.info (str(header) + " VM Cost                  = %f" % (vm.cost))
    log_handler.info (str(header) + " VM startup_lag           = %d" % (vm.startup_lag))

    log_handler.info (str(header) + " ================================================================")

    if vm.current_job is not None:
        log_handler.info (str(header) + " Current Job              = (ID:%d, NM:%s, ADR:%s (CPU:%s, I/O:%s), DL:%d, SDR:%d)"
                          % (vm.current_job.id, vm.current_job.name, vm.current_job.act_duration_on_VM, vm.current_job.act_cputime_on_VM, vm.current_job.act_nettime_on_VM, vm.current_job.deadline, vm.current_job.std_duration))
    else:
        log_handler.info (str(header) + " Current Job              = %s" % (vm.current_job))

    log_handler.info (str(header) + " VM Curr Job Start Clock  = %d" % (vm.current_job_start_clock))
    log_handler.info (str(header) + " VM Last Job Compe Clock  = %d" % (vm.last_job_complete_clock))

    if len(vm.job_queue) < 1:
        log_handler.info (str(header) + " Job Queue                = None")
    else:
        job_queue_list = []
        for job in vm.job_queue:
            job_info = []
            job_info.append(job.id)
            job_info.append(job.name)
            job_info.append(job.act_duration_on_VM)
            job_info.append(job.act_cputime_on_VM)
            job_info.append(job.act_nettime_on_VM)
            job_info.append(job.deadline)
            job_info.append(job.std_duration)
            job_info.append(job.time_create)
            job_queue_list.append (job_info)

        log_handler.info (str(header) + " Job Queue                = %s" % (str(job_queue_list)))

    if len(vm.job_history) < 1:
        log_handler.info (str(header) + " Job History              = None")
    else:
        jh_list = []
        for jh in vm.job_history:
            jh_info = []
            jh_info.append(jh.id)
            jh_info.append(jh.name)
            jh_info.append(jh.act_duration_on_VM)
            jh_info.append(jh.act_cputime_on_VM)
            jh_info.append(jh.act_nettime_on_VM)
            jh_info.append(jh.deadline)
            jh_info.append(jh.std_duration)
            jh_info.append(jh.time_create)
            jh_list.append(jh_info)
        log_handler.info (str(header) + " Job History              = %s" % (str(jh_list)))

    log_handler.info (str(header) + " VM SD Policy Activated   = %s" % (vm.sd_policy_activated))
    log_handler.info (str(header) + " VM SD Wait Time          = %d" % (vm.sd_wait_time))
    log_handler.info (str(header) + " VM IS VScale Victim???   = %s" % (vm.is_vscale_victim))
    log_handler.info (str(header) + " VM VM VScale Case        = %s" % (simgval.get_sim_code_str(vm.vscale_case)))
    log_handler.info (str(header) + " VM VScale Victim ID      = %d" % (vm.vscale_victim_id))
    log_handler.info (str(header) + " ================================================================")

def display_job_obj (clock, job):

    print "[Comm.Lib] %d ----------------------------------------" % clock
    print "[Comm.Lib] %d Display Job Obj" % clock
    print "[Comm.Lib] %d ----------------------------------------" % clock
    print "[Comm.Lib] %d Job ID                        = %d" % (clock, job.id)
    print "[Comm.Lib] %d Job Name                      = %s" % (clock, job.name)
    print "[Comm.Lib] %d Job Standard Duration         = %d" % (clock, job.std_duration)
    print "[Comm.Lib] %d Job Deadline                  = %d" % (clock, job.deadline)
    print "[Comm.Lib] %d Job Status                    = %s" % (clock, simgval.get_sim_code_str(job.status))
    print "[Comm.Lib] %d Job Time Create               = %s" % (clock, str(job.time_create))
    print "[Comm.Lib] %d Job Time Assign               = %s" % (clock, str(job.time_assign))
    print "[Comm.Lib] %d Job Time Start                = %s" % (clock, str(job.time_start))
    print "[Comm.Lib] %d Job Time Complete             = %s" % (clock, str(job.time_complete))
    print "[Comm.Lib] %d Job VM ID                     = %s" % (clock, str(job.VM_id))
    print "[Comm.Lib] %d Job VM Type                   = %s" % (clock, str(job.VM_type))
    print "[Comm.Lib] %d Job Actual Duration on VM     = %s" % (clock, str(job.act_duration_on_VM))
    print "[Comm.Lib] %d Job Actual CPU Time on VM     = %s" % (clock, str(job.act_cputime_on_VM))
    print "[Comm.Lib] %d Job Actual Network Time on VM = %s" % (clock, str(job.act_nettime_on_VM))
    print "[Comm.Lib] %d Job Use Input File            = %s" % (clock, str(job.use_input_file))
    print "[Comm.Lib] %d Job Input File Flow Direction = %s" % (clock, simgval.get_sim_code_str(job.input_file_flow_direction))
    print "[Comm.Lib] %d Job Input File Size           = %s KB (%s MB, %s GB, %s TB)" \
          % (clock, str(job.input_file_size), str(job.input_file_size/1024.0), str(job.input_file_size/(1024.0*1024)), str(round(job.input_file_size/(1024.0*1024*1024), 5)))
    print "[Comm.Lib] %d Job Use Output File           = %s" % (clock, str(job.use_output_file))
    print "[Comm.Lib] %d Job Output File Flow Direction= %s" % (clock, simgval.get_sim_code_str(job.output_file_flow_direction))
    print "[Comm.Lib] %d Job Output File Size          = %s KB (%s MB, %s GB, %s TB)" \
          % (clock, str(job.output_file_size), str(job.output_file_size/1024.0), str(job.output_file_size/(1024.0*1024)), str(round(job.output_file_size/(1024.0*1024*1024), 5)))
    print "[Comm.Lib] %d Job Output File Trans Status  = %s" % (clock, simgval.get_sim_code_str(job.output_file_transfer_status))
    print "[Comm.Lib] %d Job Output File Trans Size    = %s KB (%s MB, %s GB, %s TB)" \
          % (clock, str(job.output_file_transfer_size), str(job.output_file_transfer_size/1024.0), str(job.output_file_transfer_size/(1024.0*1024)), str(round(job.output_file_transfer_size/(1024.0*1024*1024), 5)))
    print "[Comm.Lib] %d Standard Job Failure Time     = %s" % (clock, str(job.std_job_failure_time))
    print "[Comm.Lib] %d VM Job Failure Time           = %s" % (clock, str(job.vm_job_failure_time))
    print "[Comm.Lib] %d Job Failure Recovered         = %s" % (clock, str(job.job_failure_recovered))
    print "[Comm.Lib] %d ----------------------------------------" % clock

def display_JobList (clock, title, recv_jobs):

    print ""
    print "[Comm.Lib] %d ===================================" % clock
    print "[Comm.Lib] %d Display %s" % (clock, title)
    print "[Comm.Lib] %d ===================================" % clock
    seq = 0
    for recv_job in recv_jobs:
        print "[Comm.Lib] %d [%d] ID:%d, NM:%s, ADR:%s (CPU:%s, IO:%s), SDR:%d, DL:%d, ST:%s, TC:%s, TA:%s, TS:%s, TE:%s, VM:%d" \
              % (clock, seq,
                 recv_job.id, recv_job.name, recv_job.act_duration_on_VM, recv_job.act_cputime_on_VM, recv_job.act_nettime_on_VM, recv_job.std_duration, recv_job.deadline, simgval.get_sim_code_str(recv_job.status),
                 recv_job.time_create, recv_job.time_assign, recv_job.time_start, recv_job.time_complete,recv_job.VM_id)

        seq += 1
    print "[Comm.Lib] %d ===================================" % clock

def logwrite_JobList (log, header, title, recv_jobs):
    log.info(str(header) + ' ==============================================================')
    log.info(str(header) + ' Display %s' % title)
    log.info(str(header) + ' ==============================================================')
    seq = 0
    for recv_job in recv_jobs:
        log.info(str(header) + ' [%d] ID:%d, NM:%s, SDR:%d, DL:%d, ST:%s, TC:%s TA:%s, TS:%s, TE:%s, VM:%d, VMTY:%s, ADR:%d (CPU:%s, IO:%s)'
                    %(seq, recv_job.id, recv_job.name, recv_job.std_duration, recv_job.deadline, simgval.get_sim_code_str(recv_job.status),
                 recv_job.time_create, recv_job.time_assign, recv_job.time_start, recv_job.time_complete,recv_job.VM_id, recv_job.VM_type, recv_job.act_duration_on_VM, recv_job.act_cputime_on_VM, recv_job.act_nettime_on_VM))
        seq += 1
    log.info(str(header) + ' ==============================================================')

def display_VM_Instances_List (clock, title, vms):
    print ""
    print "[Comm.Lib] %d ==============================================================" % clock
    print "[Comm.Lib] %d Display %s" % (clock, title)
    print "[Comm.Lib] %d ==============================================================" % clock
    seq = 0
    for vm in vms:
        print "[Comm.Lib] %d [%d] ID:%d, IID:%s, TY:%s, PR:$%sST:%s, TC:%s TA:%s TT:%s C$:%f, SL:%d, CJ:%s, NJ:%d, JH:%d" \
              % (clock, seq,
                 vm.id, vm.instance_id, vm.type_name, vm.unit_price, simgval.get_sim_code_str(vm.status),
                 str(vm.time_create), str(vm.time_active), str(vm.time_terminate),
                 vm.cost, vm.startup_lag, str(vm.current_job) if vm.current_job is None else 'JID: ' + str(vm.current_job.id), len(vm.job_queue), len(vm.job_history))

        seq += 1
    print "[Comm.Lib] %d ==============================================================" % clock

def logwrite_VM_Instance_Lists (log, header, title, vms):
    log.info(str(header) + ' ==============================================================')
    log.info(str(header) + ' Display %s' % title)
    log.info(str(header) + ' ==============================================================')
    seq = 0
    for vm in vms:
        log.info(str(header) + ' [%d] ID:%d, IID:%s, TY:%s, PR:$%s, ST:%s, TC:%s TA:%s TT:%s C$:%f, SL:%d, CJ:%s, NJ:%d, JH:%d'
                    %(seq, vm.id, vm.instance_id, vm.type_name, vm.unit_price, simgval.get_sim_code_str(vm.status),
                     str(vm.time_create), str(vm.time_active), str(vm.time_terminate),
                     vm.cost, vm.startup_lag, str(vm.current_job), len(vm.job_queue), len(vm.job_history)))
        seq += 1
    log.info(str(header) + ' ==============================================================')

def print_loc():
    callerframerecord = inspect.stack()[1]    # 0 represents this line
                                                # 1 represents line at caller
    frame = callerframerecord[0]
    info = inspect.getframeinfo(frame)
    print "Error Loc: File:%s, Func:%s, Line:%s" % (info.filename, info.function, info.lineno)

def sim_exit():
    callerframerecord = inspect.stack()[1]    # 0 represents this line
                                                # 1 represents line at caller
    frame = callerframerecord[0]
    info = inspect.getframeinfo(frame)
    print "\nError Loc: File:%s, Func:%s, Line:%s\n" % (info.filename, info.function, info.lineno)

    os._exit(1)

def sim_long_sleep():
    time.sleep(3)

def sim_clearscreen():
    os_name = str(platform.system())
    if os_name == "Windows":
        os.system("cls")
    elif os_name == "Linux":
        os.system("clear")

def display_conf_obj (clock, conf_obj):
    print ""
    print "[Comm.Lib] %d ======================================================================" % clock
    print "[Comm.Lib] %d Display Simulation Config Obj." % clock
    print "[Comm.Lib] %d ======================================================================" % clock
    print "[Comm.Lib] %d Config ID                                   : %d" % (clock, conf_obj.id)
    print "[Comm.Lib] %d Simulation Trace Int                        : %d" % (clock, conf_obj.sim_trace_interval)
    print "[Comm.Lib] %d Log Path                                    : %s" % (clock, conf_obj.log_path)
    print "[Comm.Lib] %d Report Path                                 : %s" % (clock, conf_obj.report_path)
    print "[Comm.Lib] %d Workload File                               : %s" % (clock, conf_obj.workload_file)
    print "[Comm.Lib] %d Workload Type                               : %s" % (clock, simgval.get_sim_code_str(conf_obj.workload_type))
    print "[Comm.Lib] %d Min Startup Lag                             : %s" % (clock, conf_obj.min_startup_lag)
    print "[Comm.Lib] %d Max Startup Lag                             : %s" % (clock, conf_obj.max_startup_lag)
    print "[Comm.Lib] %d Job Assign Policy                           : %s" % (clock, simgval.get_sim_code_str(conf_obj.job_assign_policy))
    print "[Comm.Lib] %d Job Assign Max Running_VMs                  : %s" % (clock, conf_obj.job_assign_max_running_vms)
    print "[Comm.Lib] %d Enable Vertical Scaling                     : %s" % (clock, conf_obj.enable_vertical_scaling)
    print "[Comm.Lib] %d Vertical Scaling Operation                  : %s" % (clock, simgval.get_sim_code_str(conf_obj.vscale_operation))
    print "[Comm.Lib] %d VM SD Policy                                : %s" % (clock, simgval.get_sim_code_str(conf_obj.vm_scale_down_policy))
    print "[Comm.Lib] %d VM SD Policy Unit                           : %s" % (clock, conf_obj.vm_scale_down_policy_unit)
    print "[Comm.Lib] %d VM SD Policy Rec Smp Cnt                    : %d" % (clock, conf_obj.vm_scale_down_policy_recent_sample_cnt)
    print "[Comm.Lib] %d VM SD Policy Param1                         : %s" % (clock, str(conf_obj.vm_scale_down_policy_param1))
    print "[Comm.Lib] %d VM SD Policy Param2                         : %s" % (clock, str(conf_obj.vm_scale_down_policy_param2))
    print "[Comm.Lib] %d VM SD Policy Param3                         : %s" % (clock, str(conf_obj.vm_scale_down_policy_param3))
    print "[Comm.Lib] %d VM SD Policy Timer Clock                    : %s" % (clock, conf_obj.vm_scale_down_policy_timer_unit)
    print "[Comm.Lib] %d VM SD Policy Min Wait Time                  : %d" % (clock, conf_obj.vm_scale_down_policy_min_wait_time)
    print "[Comm.Lib] %d VM SD Policy Max Wait Time                  : %d" % (clock, conf_obj.vm_scale_down_policy_max_wait_time)
    #print "[Comm.Lib] %d VM Unit Price                               : %s" % (clock, conf_obj.vm_unit_price)
    print "[Comm.Lib] %d VM Billing Time Unit                        : %s" % (clock, simgval.get_sim_code_str(conf_obj.billing_time_unit))
    print "[Comm.Lib] %d VM Billing Time Period                      : %s" % (clock, conf_obj.billing_time_period)
    print "[Comm.Lib] %d VM Billing Unit Clock                       : %s" % (clock, conf_obj.billing_unit_clock)
    print "[Comm.Lib] %d VM Selection Method                         : %s" % (clock, simgval.get_sim_code_str(conf_obj.vm_selection_method))
    print "[Comm.Lib] %d VM Config File                              : %s" % (clock, conf_obj.vm_config_file_path)
    print "[Comm.Lib] %d NO OF VM TYPES                              : %s" % (clock, conf_obj.no_of_vm_types)
    print "[Comm.Lib] %d Registered VM Types Infos                   : %s" % (clock, conf_obj.get_vm_types_str())
    print "[Comm.Lib] %d Max Capacity of Storage                     : %s KB (%s MB, %s GB, %s TB)" \
          % (clock, conf_obj.max_capacity_of_storage*1024, conf_obj.max_capacity_of_storage, conf_obj.max_capacity_of_storage/1024.0, conf_obj.max_capacity_of_storage/(1024.0*1024))
    print "[Comm.Lib] %d Max # of Storage Containers                 : %s" % (clock, conf_obj.max_num_of_storage_containers)
    print "[Comm.Lib] %d Storage Unit Cost                           : $%s" % (clock, conf_obj.storage_unit_cost)
    print "[Comm.Lib] %d Storage Billing Unit Clock                  : %s" % (clock, conf_obj.storage_billing_unit_clock)
    print "[Comm.Lib] %d Performance of Data Transfer [INTER,IN,OUT] : %s (Unit MB/s)" % (clock, conf_obj.perf_data_transfer)
    print "[Comm.Lib] %d Cost of Data Transfer        [INTER,IN,OUT] : %s (Unit Dollars)" % (clock, conf_obj.cost_data_transfer)
    print "[Comm.Lib] %d Probability of Job Failure                  : %s" % (clock, conf_obj.prob_job_failure)
    print "[Comm.Lib] %d Job Failure Policy                          : %s" % (clock, simgval.get_sim_code_str(conf_obj.job_failure_policy))
    print "[Comm.Lib] %d ======================================================================" % clock

def get_sum_job_actual_duration (jobs):

    # input param: job list
    if len(jobs) < 1:
        return 0
    else:
        return int(sum(job.act_duration_on_VM for job in jobs))

