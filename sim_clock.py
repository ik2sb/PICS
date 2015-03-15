import time
import sys
import os
import random
import logging
import sim_lib_common as clib
from threading import Condition
import sim_global_variables as simgval

Global_Wall_Clock_CV = Condition()
Global_Wall_Clock = 0
gQ_OUT = None

g_my_block_id = None
g_log_handler = None

def set_log (path):
    logger = logging.getLogger('sim_clock')
    log_file = path + '/[' + str(g_my_block_id) + ']-sim_clock.log'
    hdlr = logging.FileHandler(log_file)
    #hdlr = logging.FileHandler(path + '/sim_clock.log')
    formatter = logging.Formatter('%(message)s')
    hdlr.setFormatter(formatter)
    logger.addHandler(hdlr)
    logger.setLevel(logging.INFO)
    return logger

def increase_Wall_Clock (tick):
    global Global_Wall_Clock
    global Global_Wall_Clock_CV

    Global_Wall_Clock_CV.acquire()
    Global_Wall_Clock += tick
    Global_Wall_Clock_CV.notify()
    Global_Wall_Clock_CV.release()

def sim_clock_evt_sim_start_processing (q_msg):

    evt_code = q_msg[4]
    if evt_code != simgval.gEVT_SIM_START:
        print "[SimClock] %s sim_clock_evt_sim_start_processing EVT_CODE (%s) Error! => " %(str(Global_Wall_Clock), simgval.get_sim_code_str(evt_code)), q_msg
        g_log_handler.error(str(Global_Wall_Clock) + '  sim_clock_evt_sim_start_processing EVT_CODE (%s) Error - %s' % (simgval.get_sim_code_str(evt_code), str(q_msg)))
        clib.sim_exit()

    print "[SimClock] Simulation Start..."

    # send msg to sim_evt_handler ==> Wake up event for simulation event handler
    q_evt = clib.make_queue_event (Global_Wall_Clock, g_my_block_id, simgval.gBLOCK_SIM_EVENT_HANDLER, simgval.gEVT_SET_CLOCK, None, None)
    gQ_OUT.put(q_evt)

    # update global wall clock
    # increase_Wall_Clock (1)

def sim_clock_evt_sim_end_processing (q_msg):

    evt_code = q_msg[4]

    if evt_code != simgval.gEVT_SIM_END:
        print "[SimClock] %s sim_clock_evt_sim_end_processing EVT_CODE (%s) Error! => " %(str(Global_Wall_Clock), simgval.get_sim_code_str(evt_code)), q_msg
        g_log_handler.error(str(Global_Wall_Clock) + '  sim_clock_evt_sim_end_processing EVT_CODE (%s) Error - %s' % (simgval.get_sim_code_str(evt_code), str(q_msg)))
        clib.sim_exit()

    print "[SimClock] %s EVT_RECV: %s" % (str(Global_Wall_Clock), simgval.get_sim_code_str(evt_code))
    g_log_handler.info(str(Global_Wall_Clock) + '  EVT_RECV: %s' % (simgval.get_sim_code_str(evt_code)))

    # simulation complete event
    sim_end_evt = clib.make_queue_event (Global_Wall_Clock, g_my_block_id, simgval.gBLOCK_SIM_EVENT_HANDLER, simgval.gEVT_SIM_END, None, None)
    gQ_OUT.put(sim_end_evt)


def sim_clock_evt_set_clock_ack_processing (q_msg):

    evt_clock = q_msg[1]
    evt_code = q_msg[4]
    evt_sub_code = q_msg[5]
    evt_data = q_msg[6]

    if evt_code != simgval.gEVT_SET_CLOCK_ACK:
        print "[SimClock] %s sim_clock_evt_sim_ack_processing EVT_CODE (%s) Error! => " %(str(Global_Wall_Clock), simgval.get_sim_code_str(evt_code)), q_msg
        g_log_handler.error(str(Global_Wall_Clock) + '  sim_clock_evt_sim_ack_processing EVT_CODE (%s) Error - %s' % (simgval.get_sim_code_str(evt_code), str(q_msg)))
        clib.sim_exit()

    if Global_Wall_Clock == (evt_clock):

        #time.sleep(1)
        print "[SimClock] Simulation Clock OK: SIM_TIMER:%ds, SIM_EVENT_HANDLER:%ds" % (Global_Wall_Clock, evt_clock)

        # increase wall clock
        tick = evt_sub_code
        increase_Wall_Clock (tick)

        q_evt = clib.make_queue_event (Global_Wall_Clock, g_my_block_id, simgval.gBLOCK_SIM_EVENT_HANDLER, simgval.gEVT_SET_CLOCK, tick, None)
        gQ_OUT.put(q_evt)



        #time.sleep(4)

    else:
        print "[SimClock] Simulation Clock Mismatch: SIM_TIMER:%ds, SIM_EVENT_HANDLER:%ds" \
              % (Global_Wall_Clock, evt_clock)
        g_log_handler.error(str(Global_Wall_Clock) + '  Simulation Clock Mismatch: SIM_TIMER:%ds, SIM_EVENT_HANDLER:%ss'
                            % (Global_Wall_Clock, str(evt_clock)))
        clib.sim_exit()

def sim_clock_event_processing (q_msg):

    ret_val = False
    evt_code = q_msg[4]

    if evt_code == simgval.gEVT_SIM_START:

        # processing simulation start event
        sim_clock_evt_sim_start_processing (q_msg)

    elif evt_code == simgval.gEVT_SIM_END:

        # processing simulation end event
        sim_clock_evt_sim_end_processing (q_msg)
        ret_val = True

    elif evt_code == simgval.gEVT_SET_CLOCK_ACK:

        # clock ack (from event handler) processing
        sim_clock_evt_set_clock_ack_processing (q_msg)

    else:
        print "[SimClock] %s EVT_CODE (%s) Error! => " % (str(Global_Curr_Sim_Clock), simgval.get_sim_code_str(evt_code)), q_msg
        g_log_handler.error(str(Global_Wall_Clock) + '  EVT_CODE ERROR! - %s' % str(q_msg))
        clib.sim_exit()

    return ret_val

def th_sim_clock (my_block_id, conf_obj, q_in, q_evt_handler):

    global g_my_block_id
    global g_log_handler
    global gQ_OUT

    if my_block_id != simgval.gBLOCK_SIM_CLOCK:
        print "[SimClock] SimClock Block ID (I:%d, G:%d) Error!!!" % (my_block_id, simgval.gBLOCK_SIM_CLOCK)
        clib.sim_exit()
    else:
        g_my_block_id = my_block_id
        gQ_OUT = q_evt_handler

    time.sleep(random.random())

    print "[SimClock] Start Sim Clock Thread!"
    log = set_log (conf_obj.log_path)
    log.info(str(Global_Wall_Clock) + '  Sim Clock Log Start...')
    g_log_handler = log

    while True:
        q_message = q_in.get()
        q_in.task_done()

        # ------------------------
        # EVENT MESSAGE
        # ------------------------
        # EVENT_SRC = Event Message Sender
        # EVENT_DST = Event Message Receiver
        # EVENT_CODE = Event Code
        # EVENT_SUB_CODE = Event Sub Code
        # EVENT_DATA = Event DaTa
        # ------------------------
        evt_src = q_message[2]
        evt_dst = q_message[3]

        if evt_dst != my_block_id:
            print "[SimClock] %s EVT_MSG ERROR! (Wrong EVT_DST) - " % (str(Global_Wall_Clock)), q_message
            log.error(str(Global_Wall_Clock) + '  EVT_MSG ERROR! (Wrong EVT_DST) - %s' % str(q_message))
            clib.sim_exit()
            continue

        if evt_src != simgval.gBLOCK_MAIN and evt_src != simgval.gBLOCK_SIM_EVENT_HANDLER:
            print "[SimClock] %s EVT_MSG ERROR! (Wrong EVT_SRC) - " % (str(Global_Wall_Clock)), q_message
            log.error(str(Global_Wall_Clock) + '  EVT_MSG ERROR! (Wrong EVT_SRC) - %s' % str(q_message))
            clib.sim_exit()
            continue

        # EVENT_CODE PROCESSING
        term_flag = sim_clock_event_processing (q_message)
        if term_flag is True:
            print "[SimClock] %s Last Sim Clock: %s" % (str(Global_Wall_Clock), str(Global_Wall_Clock))
            break


    print "[SimClock] %s Sim Clock Thread Terminated." % (str(Global_Wall_Clock))