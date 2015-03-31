import sys
import time
import math
import logging
import random
from threading import Condition
import sim_global_variables as simgval
import sim_lib_common as clib

g_flag_sim_entity_term = False

Global_Curr_Sim_Clock = -99
Global_Curr_Sim_Clock_CV = Condition()

g_log_handler = None
g_joblog_handler = None
g_my_block_id = None
gQ_OUT = None

g_Workloads = []
g_Workloads_CV = Condition()

def set_log (path):
    logger = logging.getLogger('workload_generator')
    log_file = path + '/[' + str(g_my_block_id) + ']-workload_generator.log'

    # for file log
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

def set_job_log (path):
    logger = logging.getLogger('job_gen_log')
    log_file = path + '/[' + str(g_my_block_id) + ']-job_list.log'
    #hdlr = logging.FileHandler(path + '/job_list.log')
    hdlr = logging.FileHandler(log_file)
    formatter = logging.Formatter('%(message)s')
    hdlr.setFormatter(formatter)
    logger.addHandler(hdlr)
    logger.setLevel(logging.INFO)
    return logger

def insert_job_into_Workloads (job):
    global g_Workloads
    global g_Workloads_CV

    g_Workloads_CV.acquire()
    g_Workloads.append(job)
    g_Workloads_CV.notify()
    g_Workloads_CV.release()

def pop_job_from_Workloads ():
    global g_Workloads
    global g_Workloads_CV

    job = None

    if len(g_Workloads) > 0:
        g_Workloads_CV.acquire()
        job = g_Workloads.pop(0)
        g_Workloads_CV.notify()
        g_Workloads_CV.release()

    return job

def generate_job_errors (error_prob):

    global g_Workloads

    num = len(g_Workloads)
    num_of_error_jobs = int(math.floor(num * error_prob))
    window_size = int (math.floor(1/error_prob))

    if num_of_error_jobs < 1:
        return

    error_infos = []
    for x in xrange (num_of_error_jobs):
        error_infos.append ([x*window_size + random.randint(0, window_size-1), 0])

    for error in error_infos:
        job_index = error[0]
        job = g_Workloads[job_index]

        std_duration = job[2]
        job[8] = random.randint(1, std_duration)

def workgen_send_evt_ack (evt_id, evt_org_code):

    ack_evt = clib.make_queue_event (0, g_my_block_id, simgval.gBLOCK_SIM_EVENT_HANDLER, simgval.gEVT_ACK, evt_org_code, None)
    ack_evt[0] = evt_id
    gQ_OUT.put (ack_evt)

    return

def read_next_job_and_register_timer_event ():

    global g_flag_sim_entity_term
    job = pop_job_from_Workloads()
    if job is not None:
        timer = job.pop(1)
        evt_data = job

        # ------------------------
        # EVENT MESSAGE
        # ------------------------
        # EVENT_ID   = EVENT UNIQUE ID
        # EVENT_TIME = EVENT_SIM_CLOCK or EVENT_TIME_OUT CLOCK
        # EVENT_SRC  = Event Message Sender
        # EVENT_DST  = Event Message Receiver
        # EVENT_CODE = Event Type
        # EVENT_SUB_CODE = Event Sub Type
        # EVENT_DATA = Event DaTa
        # ------------------------

        # register timer event
        q_evt = clib.make_queue_event (timer, g_my_block_id, simgval.gBLOCK_SIM_EVENT_HANDLER, simgval.gEVT_SET_TIMER, simgval.gEVT_SUB_SEND_JOB, evt_data)

        gQ_OUT.put (q_evt)
    else:

        g_flag_sim_entity_term = True

        # Timer Event for Workload Gen Termination
        q_evt = clib.make_queue_event (1, g_my_block_id, simgval.gBLOCK_SIM_EVENT_HANDLER, simgval.gEVT_SET_TIMER, simgval.gEVT_SUB_WORKGEN_TERMINATE, None)
        gQ_OUT.put (q_evt)

def workgen_evt_sub_vm_wakeup_processing (q_msg):

    # WAKE UP EVENT FROM BLOCK_SIM_EVENT_HANDLER --> Workload Gen Start

    evt_id          = q_msg[0]  # EVT_ID
    evt_code        = q_msg[4]  # EVT_CODE
    evt_sub_code    = q_msg[5]  # EVT_SUB_CODE

    if evt_sub_code != simgval.gEVT_SUB_WAKEUP:
        g_log_handler.error("[Work_Gen] %s workgen_evt_sub_vm_wakeup_processing EVT_CODE (EC:%s, ESC:%s) Error! => %s" % (str(Global_Curr_Sim_Clock), simgval.get_sim_code_str(evt_code), simgval.get_sim_code_str(evt_sub_code), q_msg))
        clib.sim_exit()

    # read the first job from Workloads
    # register timer event
    read_next_job_and_register_timer_event ()

    # send ack --> in order to clear sent out event list
    workgen_send_evt_ack (evt_id, evt_code)

def workgen_evt_sub_send_job_processing (q_msg):

    # Send Job to Broker
    evt_id          = q_msg[0]  # EVT_ID
    evt_code        = q_msg[4]  # EVT_CODE
    evt_sub_code    = q_msg[5]  # EVT_SUB_CODE
    evt_data        = q_msg[6]  # EVT_DATA

    if evt_sub_code != simgval.gEVT_SUB_SEND_JOB:
        g_log_handler.error("[Work_Gen] %s workgen_evt_sub_send_job_processing EVT_CODE (EC:%s, ESC:%s) Error! => %s" %(str(Global_Curr_Sim_Clock), simgval.get_sim_code_str(evt_code), simgval.get_sim_code_str(evt_sub_code), q_msg))
        clib.sim_exit()

    qe_data =  [simgval.gEVT_WORK_GEN]
    qe_data.extend (evt_data)

    q_evt = clib.make_queue_event (0, g_my_block_id, simgval.gBLOCK_SIM_EVENT_HANDLER, simgval.gEVT_BYPASS, simgval.gBLOCK_BROKER, qe_data)
    gQ_OUT.put (q_evt)

    g_joblog_handler.info ('%s\t%s\t%d\t%d\t%s\t%d\t%s\t%d\t\t%s'
                           % (str(Global_Curr_Sim_Clock), evt_data[0], evt_data[1], evt_data[2], simgval.get_sim_code_str(evt_data[3]), evt_data[4], simgval.get_sim_code_str(evt_data[5]), evt_data[6], evt_data[7]))

    # read the next job
    read_next_job_and_register_timer_event ()

    # send event ack
    workgen_send_evt_ack (evt_id, evt_code)

def workgen_evt_exp_timer_processing (q_msg):
    evt_code        = q_msg[4]  # EVT_CODE
    evt_sub_code    = q_msg[5]  # EVT_SUB_CODE

    if evt_code != simgval.gEVT_EXP_TIMER:
        g_log_handler.error("[Work_Gen] %s workgen_evt_exp_timer_processing EVT_CODE (%s) Error! => %s" % (str(Global_Curr_Sim_Clock), simgval.get_sim_code_str(evt_code), q_msg))
        clib.sim_exit()

    if evt_sub_code == simgval.gEVT_SUB_WAKEUP:

        # EVT SUB Wakeup Processing --> Workload Gen Start..
        workgen_evt_sub_vm_wakeup_processing (q_msg)

    elif evt_sub_code == simgval.gEVT_SUB_SEND_JOB:

        # EVT SYB SEND JOB TO BROKER
        workgen_evt_sub_send_job_processing (q_msg)

    else:
        g_log_handler.error("[Work_Gen] %s EVT_SUB_CODE (%s) Error! => %s" % (str(Global_Curr_Sim_Clock), simgval.get_sim_code_str(evt_sub_code), q_msg))
        clib.sim_exit()

def workgen_event_processing (q_msg):
    evt_src         = q_msg[2]  # SRC BLOCK
    evt_code        = q_msg[4]  # EVT_CODE
    evt_sub_code    = q_msg[5]  # EVT_SUB_CODE
    evt_data        = q_msg[6]  # EVT_DATA

    if evt_code == simgval.gEVT_EXP_TIMER:

        # workload timer event processing
        workgen_evt_exp_timer_processing (q_msg)

    else:
        g_log_handler.error("[Work_Gen] %s EVT_CODE (%s) Error! => %s" % (str(Global_Curr_Sim_Clock), simgval.get_sim_code_str(evt_code), q_msg))
        clib.sim_exit()

def terminate_sim_entity_workloadgen (q_msg):

    evt_id          = q_msg[0]  # EVT_ID
    evt_code        = q_msg[4]  # EVT_CODE
    evt_sub_code    = q_msg[5]  # EVT_SUB_CODE

    if evt_code != simgval.gEVT_EXP_TIMER or evt_sub_code != simgval.gEVT_SUB_WORKGEN_TERMINATE:
        g_log_handler.error("[Work_Gen] %s EVT_CODE (%s)/EVT_SUB_CODE (%s) Error! => %s" \
              %(str(Global_Curr_Sim_Clock), simgval.get_sim_code_str(evt_code), simgval.get_sim_code_str(evt_sub_code), q_msg))
        clib.sim_exit()

    # [a] ack send first
    workgen_send_evt_ack (evt_id, evt_code)

    # [b] send gEVT_NOTI_SIMENTIY_TERMINATED (common event code) to event handler
    term_evt = clib.make_queue_event (Global_Curr_Sim_Clock, g_my_block_id, simgval.gBLOCK_SIM_EVENT_HANDLER, simgval.gEVT_NOTI_SIMENTIY_TERMINATED, None, None)
    gQ_OUT.put (term_evt)

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

def th_sim_entity_workgen(my_block_id, conf_obj, q_in, q_out):

    global Global_Curr_Sim_Clock
    global g_log_handler
    global g_joblog_handler
    global g_my_block_id
    global gQ_OUT

    if my_block_id != simgval.gBLOCK_WORKLOAD_GEN:
        print "[Work_Gen] Workload Generator Block ID (I:%d, G:%d) Error!!!" % (my_block_id, simgval.gBLOCK_WORKLOAD_GEN)
        clib.sim_exit()
    else:
        g_my_block_id = my_block_id
        gQ_OUT = q_out

    time.sleep(random.random())

    log = set_log (conf_obj.log_path)
    g_log_handler = log
    log.info ("[Work_Gen] 0 Start Workload Generator Thread!")

    job_log = set_job_log (conf_obj.log_path)
    g_joblog_handler = job_log
    job_log.info ('CL\tJN\tSDR\tDL\tIFD\t\tIFS(KB)\tOFD\t\tOFS(KB)\t\tEOT')
    job_log.info ('---------------------------------------------------------------------------------------------')

    f = open(conf_obj.workload_file, 'r')

    no_of_jobs = 0
    for line in f:

        if not line.startswith('#') and len(line) > 1:

            data = line.replace('\n', '').replace('_', ',').split(',')
            #data = map(str, data)
            data[0] = int(data[0])
            data[1] = int(data[1])
            data[2] = int(data[2])
            data[3] = simgval.get_file_transfer_direction_config_val (1, data[3])
            data[4] = int(data[4])
            data[5] = simgval.get_file_transfer_direction_config_val (2, data[5])
            data[6] = int(data[6])
            data.append (0)         # reserved for job error 11/03/2014

            no_of_jobs += 1

            job = []
            job.append("job-" + str(no_of_jobs))
            job.extend(data)
            insert_job_into_Workloads (job)


    log.info('0' + '  Read Workload - %d Jobs' % no_of_jobs)

    if conf_obj.prob_job_failure > 0.0:
        generate_job_errors (conf_obj.prob_job_failure)

    if len(g_Workloads) > 0:
        print " ---------------------------------------------------------------------------------------------------"
        print " JSEQ\tJN\tCL\tSDR\tDL\tIFD\t\tIFS\tOFD\t\tOFS\t\tEOT"
        print " ---------------------------------------------------------------------------------------------------"

        for i, job in enumerate(g_Workloads):
            print " " + str(i+1) \
                  + "\t" + job[0] \
                  + "\t" + str(job[1]) \
                  + "\t" + str(job[2]) \
                  + "\t" + str(job[3]) \
                  + "\t" + simgval.get_sim_code_str(job[4]) \
                  + "\t" + str(job[5]) \
                  + "\t" + simgval.get_sim_code_str(job[6]) \
                  + "\t" + str(job[7]) \
                  + "\t\t" + str(job[8])

        print " ---------------------------------------------------------------------------------------------------"
        print "[Work_Gen] 0 Read Workload - %d Jobs" % no_of_jobs

    else:
        print "[Work_Gen] 0 No Jobs for Simulation"
        clib.sim_exit()

    while True:
        q_message = q_in.get()
        q_in.task_done()

        time.sleep(0.01)
        #print "\n[Work_Gen] %s Queue Message  :" % str(Global_Curr_Sim_Clock), q_message

        evt_clock = q_message[1]    # EVT_SIMULATION_CLOCK
        evt_src = q_message[2]      # SRC BLOCK
        evt_dst = q_message[3]      # DST BLOCK

        if evt_dst != my_block_id:
            log.error ("[Work_Gen] 0 EVT_MSG ERROR! (Wrong EVT_DST) - %s" % (q_message))
            clib.sim_exit()

        if evt_src == simgval.gBLOCK_SIM_EVENT_HANDLER:

            # update simulator clock
            update_curr_sim_clock (evt_clock)

        else:
            # event received only from sim event handler
            log.error("[Work_Gen] 0 EVT_MSG ERROR! (Wrong EVT_SRC) - %s" % (q_message))
            clib.sim_exit()
            continue

        if g_flag_sim_entity_term is True:
            terminate_sim_entity_workloadgen (q_message)
            break

        # Workload Gen Main Event Processing
        workgen_event_processing (q_message)

    print "[Work_Gen] %d Workload Generator Terminated." % (Global_Curr_Sim_Clock)

